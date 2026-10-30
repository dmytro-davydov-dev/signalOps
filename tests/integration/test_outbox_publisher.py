"""Outbox publication against Compose PostgreSQL and Kafka (P03-T03).

Each test starts from an empty outbox in the module's scratch database and
publishes to its own single-partition topic.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from sqlalchemy import Engine, delete, select

from signalops.config import Settings
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.persistence import database as db
from signalops.persistence.models import OutboxEvent
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.streaming.consumer import (
    ConsumerRunner,
    ProcessOutcome,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.outbox import OutboxPublisher, PublishSummary
from signalops.streaming.producer import DeliveryResult, EventProducer, OutgoingMessage
from signalops.streaming.retry import RetryPolicy
from signalops.streaming.topics import TopicSpec, ensure_topics

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
POLL_DEADLINE_SECONDS = 30.0
FAST_RETRY = RetryPolicy(base_delay_seconds=0.05, max_delay_seconds=0.05)


class SimulatedCrashError(Exception):
    """Stands in for the process dying at a chosen point."""


@dataclass
class CrashAfterAcknowledgement:
    """Publishes through a real producer, then dies before the publisher marks rows."""

    inner: EventProducer

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        results = self.inner.publish(messages)
        assert all(result.delivered for result in results)
        raise SimulatedCrashError


@pytest.fixture
def topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.outbox.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=1, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


@pytest.fixture
async def database(migrated: tuple[Settings, Engine]) -> AsyncIterator[db.Database]:
    engine = db.create_engine(migrated[0])
    database = db.Database.from_engine(engine)
    async with database.transaction() as session:
        await session.execute(delete(OutboxEvent))
    try:
        yield database
    finally:
        await engine.dispose()


@pytest.fixture
def producer(infra_settings: Settings) -> EventProducer:
    # A short delivery timeout bounds the wait for a missing topic.
    settings = infra_settings.model_copy(update={"kafka_delivery_timeout_ms": 5_000})
    return EventProducer(settings, client_id="test-outbox-publisher")


def _envelope() -> EventEnvelope[EventPayload]:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    return envelope.model_copy(update={"event_id": uuid.uuid4()})


async def _stage(
    database: db.Database,
    topic: str,
    *,
    aggregate_id: uuid.UUID | None = None,
    aggregate_version: int = 1,
) -> uuid.UUID:
    """Stage one row through the repository, then point it at the test topic."""
    envelope = _envelope()
    async with database.transaction() as session:
        row = await OutboxRepository(session).add(
            envelope,
            aggregate_id=aggregate_id or uuid.uuid4(),
            aggregate_version=aggregate_version,
        )
        row.topic = topic
    return envelope.event_id


async def _row(database: db.Database, event_id: uuid.UUID) -> OutboxEvent:
    async with database.transaction() as session:
        return (await session.scalars(select(OutboxEvent).where(OutboxEvent.id == event_id))).one()


def _read_event_ids(settings: Settings, topic: str, expected: int) -> list[uuid.UUID]:
    """Event ids on the topic in offset order, read by a fresh group from the start."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    ids: list[uuid.UUID] = []
    try:
        deadline = time.monotonic() + POLL_DEADLINE_SECONDS
        while len(ids) < expected and time.monotonic() < deadline:
            record = consumer.poll(1.0)
            if record is None or record.error() is not None:
                continue
            ids.append(uuid.UUID(json.loads(record.value() or b"{}")["event_id"]))
        # One more poll shows nothing beyond the expected messages was published.
        extra = consumer.poll(2.0)
        assert extra is None or extra.error() is not None, "unexpected extra message"
    finally:
        consumer.close()
    return ids


async def test_crash_after_ack_republishes_and_consumer_deduplicates(
    infra_settings: Settings, database: db.Database, producer: EventProducer, topic: str
) -> None:
    event_id = await _stage(database, topic)

    with pytest.raises(SimulatedCrashError):
        await OutboxPublisher(database, CrashAfterAcknowledgement(producer)).publish_once()
    stranded = await _row(database, event_id)
    assert stranded.published_at is None

    summary = await OutboxPublisher(database, producer).publish_once()
    assert summary == PublishSummary(published=1)
    marked = await _row(database, event_id)
    assert marked.published_at is not None
    assert marked.attempts == 1
    assert _read_event_ids(infra_settings, topic, expected=2) == [event_id, event_id]

    # A later poll finds nothing left to send.
    assert await OutboxPublisher(database, producer).publish_once() == PublishSummary()

    # The downstream consumer applies the duplicate once.
    group = f"test-outbox-consumer-{uuid.uuid4().hex[:8]}"
    applied: list[uuid.UUID] = []

    async def record(session: object, envelope: EventEnvelope[EventPayload]) -> None:
        applied.append(envelope.event_id)

    consumer = create_consumer(infra_settings, group_id=group, client_id=group, topics=[topic])
    runner = ConsumerRunner(
        consumer,
        TransactionalProcessor(database, group, record),
        consumer_group=group,
        failure_publisher=producer,
    )
    outcomes: list[ProcessOutcome] = []
    try:
        deadline = time.monotonic() + POLL_DEADLINE_SECONDS
        while len(outcomes) < 2 and time.monotonic() < deadline:
            outcome = await runner.run_once()
            if outcome is not None:
                outcomes.append(outcome)
    finally:
        consumer.close()
    assert outcomes == [ProcessOutcome.APPLIED, ProcessOutcome.DUPLICATE]
    assert applied == [event_id]


async def test_failed_delivery_leaves_row_pending(
    database: db.Database, producer: EventProducer
) -> None:
    missing = f"test.missing.{uuid.uuid4().hex[:8]}"
    event_id = await _stage(database, missing)

    summary = await OutboxPublisher(database, producer).publish_once()

    assert summary == PublishSummary(failed=1)
    row = await _row(database, event_id)
    assert row.published_at is None
    assert row.attempts == 1
    # A timeout is transient: the row is deferred, not quarantined.
    assert row.next_attempt_at is not None
    assert row.quarantined_at is None
    assert (row.last_error or "").startswith("timeout: ")


async def test_failed_version_holds_back_later_versions_of_its_aggregate(
    infra_settings: Settings, database: db.Database, producer: EventProducer, topic: str
) -> None:
    missing = f"test.missing.{uuid.uuid4().hex[:8]}"
    held = uuid.uuid4()
    first = await _stage(database, missing, aggregate_id=held, aggregate_version=1)
    second = await _stage(database, topic, aggregate_id=held, aggregate_version=2)
    other = await _stage(database, topic)

    publisher = OutboxPublisher(database, producer, retry_policy=FAST_RETRY)
    summary = await publisher.publish_once()

    assert summary == PublishSummary(published=1, failed=1, deferred=1)
    assert (await _row(database, second)).published_at is None
    assert (await _row(database, other)).published_at is not None

    # Once the first version can be delivered, both go out in version order.
    async with database.transaction() as session:
        row = await session.get_one(OutboxEvent, first)
        row.topic = topic
    await asyncio.sleep(FAST_RETRY.max_delay_seconds)
    assert await publisher.publish_once() == PublishSummary(published=2)
    assert _read_event_ids(infra_settings, topic, expected=3) == [other, first, second]


async def test_deferred_row_holds_back_its_aggregate_until_due(database: db.Database) -> None:
    held = uuid.uuid4()
    first = await _stage(database, "t", aggregate_id=held, aggregate_version=1)
    await _stage(database, "t", aggregate_id=held, aggregate_version=2)
    other = await _stage(database, "t")

    async with database.transaction() as session:
        await OutboxRepository(session).schedule_retry(first, 60.0, "timeout: test")
        due = await OutboxRepository(session).pending(10)
    assert [row.id for row in due] == [other]

    async with database.transaction() as session:
        await OutboxRepository(session).schedule_retry(first, 0.0, "timeout: test")
        due = await OutboxRepository(session).pending(10)
    # Once due, the deferred row and the version behind it are read again.
    assert len(due) == 3
    assert first in {row.id for row in due}


@pytest.fixture
def tiny_topic(infra_settings: Settings) -> Iterator[str]:
    """A topic whose broker-side size limit rejects any envelope."""
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.tiny.{uuid.uuid4().hex[:8]}"
    config = {"max.message.bytes": "128"}
    spec = NewTopic(name, num_partitions=1, replication_factor=1, config=config)
    for future in admin.create_topics([spec]).values():
        future.result()
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


async def test_rejected_row_is_quarantined_blocks_its_aggregate_and_can_be_redriven(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    tiny_topic: str,
) -> None:
    held = uuid.uuid4()
    first = await _stage(database, tiny_topic, aggregate_id=held, aggregate_version=1)
    second = await _stage(database, topic, aggregate_id=held, aggregate_version=2)
    publisher = OutboxPublisher(database, producer, retry_policy=FAST_RETRY)

    assert await publisher.publish_once() == PublishSummary(failed=1, quarantined=1, deferred=1)
    row = await _row(database, first)
    assert row.quarantined_at is not None
    assert row.next_attempt_at is None
    assert (row.last_error or "").startswith("rejected: ")

    # A quarantined row is not retried and keeps its later versions back.
    await asyncio.sleep(FAST_RETRY.max_delay_seconds)
    assert await publisher.publish_once() == PublishSummary()
    assert (await _row(database, second)).published_at is None

    # The operator fixes the cause and redrives the row; both versions go out in order.
    async with database.transaction() as session:
        assert await OutboxRepository(session).redrive(first)
        (await session.get_one(OutboxEvent, first)).topic = topic
    async with database.transaction() as session:
        assert not await OutboxRepository(session).redrive(first)  # no longer quarantined
    assert await publisher.publish_once() == PublishSummary(published=2)
    assert _read_event_ids(infra_settings, topic, expected=2) == [first, second]
    assert (await _row(database, first)).attempts == 2
