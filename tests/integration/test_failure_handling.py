"""Quarantine, retry, and redrive against Compose PostgreSQL and Kafka (P03-T04).

Each test consumes its own single-partition topic with its own group. Failure
events go to the shared processing.failed.v1 topic and are found by their
deterministic event ID.
"""

import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError, ContractErrorCategory
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    ProcessingFailedPayload,
    decode_event,
)
from signalops.persistence import database as db
from signalops.persistence.models import ConsumerReceipt
from signalops.streaming.consumer import (
    ConsumerRunner,
    EventHandler,
    FailureStage,
    MessagePublisher,
    ProcessOutcome,
    RecordProcessingError,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.failures import REDRIVE_HEADER, RecordPosition, failure_event_id
from signalops.streaming.producer import (
    DeliveryError,
    DeliveryErrorCategory,
    DeliveryResult,
    EventProducer,
    OutgoingMessage,
)
from signalops.streaming.redrive import RedriveError, redrive_record
from signalops.streaming.retry import RetryPolicy
from signalops.streaming.topics import TopicSpec, ensure_topics, message_key

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
POLL_DEADLINE_SECONDS = 30.0
FAILED_TOPIC = "processing.failed.v1"


@dataclass
class RecordEvents:
    """Applies nothing but the receipt; rejects or fails chosen events."""

    reject: set[uuid.UUID] = field(default_factory=set)
    # Remaining transient failures per event.
    fail: dict[uuid.UUID, int] = field(default_factory=dict)
    applied: list[uuid.UUID] = field(default_factory=list)

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        if envelope.event_id in self.reject:
            raise ContractError(ContractErrorCategory.INVALID_PAYLOAD, "record.level: rejected")
        if self.fail.get(envelope.event_id, 0) > 0:
            self.fail[envelope.event_id] -= 1
            raise ConnectionError("database unavailable")
        self.applied.append(envelope.event_id)


@dataclass
class UnacknowledgedPublisher:
    """Fails the first `failures` publishes as unacknowledged, then delegates."""

    inner: EventProducer
    failures: int

    def publish(self, messages: list[OutgoingMessage]) -> list[DeliveryResult]:
        if self.failures > 0:
            self.failures -= 1
            error = DeliveryError(DeliveryErrorCategory.TIMEOUT, "no acknowledgement")
            return [DeliveryResult(message, error=error) for message in messages]
        return self.inner.publish(messages)


@pytest.fixture
def topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    # The shared failure topic must exist; init-topics normally creates it.
    ensure_topics(admin)
    name = f"test.failures.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=1, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


@pytest.fixture
async def database(migrated: tuple[Settings, Engine]) -> AsyncIterator[db.Database]:
    engine = db.create_engine(migrated[0])
    try:
        yield db.Database.from_engine(engine)
    finally:
        await engine.dispose()


@pytest.fixture
def group() -> str:
    return f"test-failures-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def producer(infra_settings: Settings) -> EventProducer:
    return EventProducer(infra_settings, client_id="test-failures-producer")


def _envelope() -> EventEnvelope[EventPayload]:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    return envelope.model_copy(update={"event_id": uuid.uuid4()})


def _publish(producer: EventProducer, topic: str, values: list[bytes]) -> None:
    messages = [OutgoingMessage(topic, "key", value, None) for value in values]
    assert all(result.delivered for result in producer.publish(messages))


def _runner(
    settings: Settings,
    database: db.Database,
    group: str,
    topic: str,
    handler: EventHandler,
    publisher: MessagePublisher,
    *,
    max_attempts: int = 5,
) -> tuple[Consumer, ConsumerRunner]:
    consumer = create_consumer(
        settings, group_id=group, client_id=f"{group}-member", topics=[topic]
    )
    runner = ConsumerRunner(
        consumer,
        TransactionalProcessor(database, group, handler),
        consumer_group=group,
        failure_publisher=publisher,
        retry_policy=RetryPolicy(max_attempts, base_delay_seconds=0.05, max_delay_seconds=0.2),
    )
    return consumer, runner


async def _outcomes(runner: ConsumerRunner, count: int) -> list[ProcessOutcome]:
    outcomes: list[ProcessOutcome] = []
    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while len(outcomes) < count and time.monotonic() < deadline:
        outcome = await runner.run_once()
        if outcome is not None:
            outcomes.append(outcome)
    return outcomes


def _committed_offset(settings: Settings, group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": group})
    try:
        [position] = probe.committed([TopicPartition(topic, 0)], timeout=10)
        return position.offset
    finally:
        probe.close()


def _failure_events(settings: Settings, event_id: uuid.UUID) -> list[ProcessingFailedPayload]:
    """Every copy of one failure event on processing.failed.v1."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-failure-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([FAILED_TOPIC])
    found: list[ProcessingFailedPayload] = []
    try:
        deadline = time.monotonic() + POLL_DEADLINE_SECONDS
        while time.monotonic() < deadline:
            record = consumer.poll(1.0)
            if record is None:
                continue
            if record.error() is not None:
                if found:  # end of partition after at least one match
                    break
                continue
            if json.loads(record.value() or b"{}").get("event_id") == str(event_id):
                payload = decode_event(record.value() or b"").payload
                assert isinstance(payload, ProcessingFailedPayload)
                found.append(payload)
    finally:
        consumer.close()
    return found


async def _receipts(database: db.Database, group: str, event_id: uuid.UUID) -> int:
    async with database.transaction() as session:
        query = select(func.count()).where(
            ConsumerReceipt.consumer_group == group, ConsumerReceipt.event_id == event_id
        )
        return (await session.execute(query)).scalar_one()


async def test_poison_record_is_quarantined_and_later_records_proceed(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    first, third = _envelope(), _envelope()
    _publish(producer, topic, [first.encode(), b"\x00not an envelope", third.encode()])
    handler = RecordEvents()

    consumer, runner = _runner(infra_settings, database, group, topic, handler, producer)
    try:
        outcomes = await _outcomes(runner, 3)
    finally:
        consumer.close()

    assert outcomes == [ProcessOutcome.APPLIED, ProcessOutcome.QUARANTINED, ProcessOutcome.APPLIED]
    assert handler.applied == [first.event_id, third.event_id]
    assert _committed_offset(infra_settings, group, topic) == 3
    failure_id = failure_event_id(group, RecordPosition(topic, 0, 1))
    [failure] = _failure_events(infra_settings, failure_id)
    assert (failure.consumer_group, failure.original_topic, failure.original_offset) == (
        group,
        topic,
        1,
    )
    assert failure.error_category is ContractErrorCategory.MALFORMED_JSON
    assert failure.original_event_id is None


async def test_unpublished_failure_event_never_acknowledges_the_original(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    rejected = _envelope()
    _publish(producer, topic, [_envelope().encode(), rejected.encode()])
    handler = RecordEvents(reject={rejected.event_id})
    failing = UnacknowledgedPublisher(producer, failures=100)

    consumer, runner = _runner(
        infra_settings, database, group, topic, handler, failing, max_attempts=3
    )
    try:
        assert await _outcomes(runner, 1) == [ProcessOutcome.APPLIED]
        # Real pause, seek, and resume: two scheduled retries, then the runner stops.
        assert await _outcomes(runner, 2) == [ProcessOutcome.RETRY_SCHEDULED] * 2
        with pytest.raises(RecordProcessingError) as raised:
            await _outcomes(runner, 1)
    finally:
        consumer.close()
    assert raised.value.stage is FailureStage.QUARANTINE
    assert raised.value.attempts == 3
    assert _committed_offset(infra_settings, group, topic) == 1

    # After a restart with a working producer the record is quarantined and acknowledged.
    consumer, runner = _runner(infra_settings, database, group, topic, handler, producer)
    try:
        assert await _outcomes(runner, 1) == [ProcessOutcome.QUARANTINED]
    finally:
        consumer.close()
    assert _committed_offset(infra_settings, group, topic) == 2
    failure_id = failure_event_id(group, RecordPosition(topic, 0, 1))
    [failure] = _failure_events(infra_settings, failure_id)
    assert failure.original_event_id == rejected.event_id
    assert failure.error_detail == "record.level: rejected"
    # Rejection rolled back the receipt, so a redrive can still apply the event.
    assert await _receipts(database, group, rejected.event_id) == 0


async def test_failure_event_retry_recovers_without_restart(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    _publish(producer, topic, [b"[]", _envelope().encode()])
    flaky = UnacknowledgedPublisher(producer, failures=1)

    consumer, runner = _runner(infra_settings, database, group, topic, RecordEvents(), flaky)
    try:
        outcomes = await _outcomes(runner, 3)
    finally:
        consumer.close()

    assert outcomes == [
        ProcessOutcome.RETRY_SCHEDULED,
        ProcessOutcome.QUARANTINED,
        ProcessOutcome.APPLIED,
    ]
    assert _committed_offset(infra_settings, group, topic) == 2


async def test_transient_handler_failure_is_retried_in_place(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    first, flaky, third = _envelope(), _envelope(), _envelope()
    _publish(producer, topic, [first.encode(), flaky.encode(), third.encode()])
    handler = RecordEvents(fail={flaky.event_id: 2})

    consumer, runner = _runner(infra_settings, database, group, topic, handler, producer)
    try:
        outcomes = await _outcomes(runner, 5)
    finally:
        consumer.close()

    assert outcomes == [
        ProcessOutcome.APPLIED,
        ProcessOutcome.RETRY_SCHEDULED,
        ProcessOutcome.RETRY_SCHEDULED,
        ProcessOutcome.APPLIED,
        ProcessOutcome.APPLIED,
    ]
    assert handler.applied == [first.event_id, flaky.event_id, third.event_id]
    assert _committed_offset(infra_settings, group, topic) == 3
    assert await _receipts(database, group, flaky.event_id) == 1


async def test_redrive_republishes_the_original_and_the_fixed_consumer_applies_it(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    rejected = _envelope()
    messages = [OutgoingMessage(topic, message_key(rejected), rejected.encode(), None)]
    assert all(result.delivered for result in producer.publish(messages))
    handler = RecordEvents(reject={rejected.event_id})

    consumer, runner = _runner(infra_settings, database, group, topic, handler, producer)
    try:
        assert await _outcomes(runner, 1) == [ProcessOutcome.QUARANTINED]

        # The operator redrives the record named by the failure event.
        result = redrive_record(
            infra_settings, RecordPosition(topic, 0, 0), allowed_topics=frozenset({topic})
        )
        assert (result.event_id, result.offset) == (rejected.event_id, 1)

        handler.reject.clear()  # the consumer bug is fixed
        assert await _outcomes(runner, 1) == [ProcessOutcome.APPLIED]
    finally:
        consumer.close()

    assert handler.applied == [rejected.event_id]
    assert await _receipts(database, group, rejected.event_id) == 1
    assert _committed_offset(infra_settings, group, topic) == 2

    # The redriven copy keeps the key and value and carries the attempt ID.
    reader = Consumer(
        {
            "bootstrap.servers": infra_settings.kafka_bootstrap_servers,
            "group.id": f"test-redrive-reader-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    reader.assign([TopicPartition(topic, 0, 1)])
    try:
        record = None
        deadline = time.monotonic() + POLL_DEADLINE_SECONDS
        while record is None and time.monotonic() < deadline:
            candidate = reader.poll(1.0)
            if candidate is not None and candidate.error() is None:
                record = candidate
    finally:
        reader.close()
    assert record is not None
    assert record.key() == message_key(rejected).encode()
    assert record.value() == rejected.encode()
    assert (REDRIVE_HEADER, str(result.attempt_id).encode()) in (record.headers() or [])


def test_redrive_of_an_unavailable_offset_publishes_nothing(
    infra_settings: Settings, producer: EventProducer, topic: str
) -> None:
    _publish(producer, topic, [_envelope().encode()])
    with pytest.raises(RedriveError, match="beyond the end of the partition"):
        redrive_record(
            infra_settings, RecordPosition(topic, 0, 5), allowed_topics=frozenset({topic})
        )
