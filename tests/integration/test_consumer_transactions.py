"""Receipts, outbox rows, and offset commits against Compose PostgreSQL and Kafka (P03-T02).

Each test uses its own single-partition topic and consumer group, and a scratch
database from the module's migrated fixture.
"""

import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pytest
from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.persistence import database as db
from signalops.persistence.models import ConsumerReceipt, OutboxEvent
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.streaming.consumer import (
    ConsumerRunner,
    FailureStage,
    ProcessOutcome,
    RecordProcessingError,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.producer import EventProducer, OutgoingMessage
from signalops.streaming.topics import TopicSpec, ensure_topics, message_key

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
POLL_DEADLINE_SECONDS = 30.0


class SimulatedCrashError(Exception):
    """Stands in for the process dying at a chosen point."""


@dataclass
class EmitDerivedEvent:
    """A domain effect with no business uniqueness: only the receipt prevents duplicates.

    Each call stages an outbox row under a fresh aggregate and event id, caused
    by the consumed event, so a repeated effect shows up as an extra row.
    """

    fail_on: set[uuid.UUID] = field(default_factory=set)

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        derived = envelope.model_copy(
            update={"event_id": uuid.uuid4(), "causation_id": envelope.event_id}
        )
        await OutboxRepository(session).add(derived, aggregate_id=uuid.uuid4(), aggregate_version=1)
        if envelope.event_id in self.fail_on:
            raise RuntimeError("handler failed after writing")


@dataclass
class CrashingCommit:
    """Delegates to a real consumer, but dies instead of committing an offset."""

    inner: Consumer

    def poll(self, timeout: float) -> Message | None:
        return self.inner.poll(timeout)

    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[False]) -> None:
        raise SimulatedCrashError

    def close(self) -> None:
        self.inner.close()


@pytest.fixture
def topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.consumer.{uuid.uuid4().hex[:8]}"
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
    return f"test-consumer-{uuid.uuid4().hex[:8]}"


def _envelope() -> EventEnvelope[EventPayload]:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    return envelope.model_copy(update={"event_id": uuid.uuid4()})


def _publish(settings: Settings, topic: str, envelopes: list[EventEnvelope[EventPayload]]) -> None:
    producer = EventProducer(settings, client_id="test-consumer-producer")
    messages = [OutgoingMessage(topic, message_key(e), e.encode(), e.event_id) for e in envelopes]
    assert all(result.delivered for result in producer.publish(messages))


def _consumer(settings: Settings, group: str, topic: str) -> Consumer:
    return create_consumer(settings, group_id=group, client_id=f"{group}-member", topics=[topic])


async def _next_outcome(runner: ConsumerRunner) -> ProcessOutcome:
    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        outcome = await runner.run_once()
        if outcome is not None:
            return outcome
    raise AssertionError("no record within the poll deadline")


def _committed_offset(settings: Settings, group: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": group})
    try:
        [position] = probe.committed([TopicPartition(topic, 0)], timeout=10)
        return position.offset
    finally:
        probe.close()


async def _effects(database: db.Database, caused_by: uuid.UUID) -> int:
    async with database.transaction() as session:
        query = select(func.count()).where(
            OutboxEvent.envelope["causation_id"].astext == str(caused_by)
        )
        return (await session.execute(query)).scalar_one()


async def _receipts(database: db.Database, group: str) -> int:
    async with database.transaction() as session:
        query = select(func.count()).where(ConsumerReceipt.consumer_group == group)
        return (await session.execute(query)).scalar_one()


async def test_crash_after_db_commit_redelivers_without_duplicate_effect(
    infra_settings: Settings, database: db.Database, topic: str, group: str
) -> None:
    envelope = _envelope()
    _publish(infra_settings, topic, [envelope])
    processor = TransactionalProcessor(database, group, EmitDerivedEvent())

    crashing = CrashingCommit(_consumer(infra_settings, group, topic))
    try:
        with pytest.raises(SimulatedCrashError):
            await _next_outcome(ConsumerRunner(crashing, processor))
    finally:
        crashing.close()
    # The effect committed; the offset did not.
    assert await _effects(database, envelope.event_id) == 1
    assert _committed_offset(infra_settings, group, topic) < 0

    restarted = _consumer(infra_settings, group, topic)
    try:
        assert await _next_outcome(ConsumerRunner(restarted, processor)) is ProcessOutcome.DUPLICATE
    finally:
        restarted.close()
    assert await _effects(database, envelope.event_id) == 1
    assert await _receipts(database, group) == 1
    assert _committed_offset(infra_settings, group, topic) == 1


async def test_duplicate_message_has_one_logical_effect(
    infra_settings: Settings, database: db.Database, topic: str, group: str
) -> None:
    envelope = _envelope()
    _publish(infra_settings, topic, [envelope, envelope])
    processor = TransactionalProcessor(database, group, EmitDerivedEvent())
    consumer = _consumer(infra_settings, group, topic)
    runner = ConsumerRunner(consumer, processor)
    try:
        outcomes = [await _next_outcome(runner), await _next_outcome(runner)]
    finally:
        consumer.close()
    assert outcomes == [ProcessOutcome.APPLIED, ProcessOutcome.DUPLICATE]
    assert await _effects(database, envelope.event_id) == 1
    assert _committed_offset(infra_settings, group, topic) == 2

    # The stored topic and key are what the publisher will send.
    [row] = await _outbox_rows(database, envelope.event_id)
    assert row.topic == "signals.raw.v1"
    assert row.message_key == message_key(envelope)


async def _outbox_rows(database: db.Database, caused_by: uuid.UUID) -> list[OutboxEvent]:
    async with database.transaction() as session:
        query = select(OutboxEvent).where(
            OutboxEvent.envelope["causation_id"].astext == str(caused_by)
        )
        return list((await session.scalars(query)).all())


async def test_failed_record_is_redelivered_and_never_skipped(
    infra_settings: Settings, database: db.Database, topic: str, group: str
) -> None:
    first, failing, third = _envelope(), _envelope(), _envelope()
    _publish(infra_settings, topic, [first, failing, third])

    broken = EmitDerivedEvent(fail_on={failing.event_id})
    consumer = _consumer(infra_settings, group, topic)
    runner = ConsumerRunner(consumer, TransactionalProcessor(database, group, broken))
    try:
        assert await _next_outcome(runner) is ProcessOutcome.APPLIED
        with pytest.raises(RecordProcessingError) as raised:
            await _next_outcome(runner)
    finally:
        consumer.close()
    assert raised.value.stage is FailureStage.PROCESS
    assert raised.value.position.offset == 1
    # The failed transaction rolled back its receipt and outbox row; the
    # committed offset stops before it even though offset 2 may be fetched.
    assert await _effects(database, failing.event_id) == 0
    assert await _receipts(database, group) == 1
    assert _committed_offset(infra_settings, group, topic) == 1

    consumer = _consumer(infra_settings, group, topic)
    healthy = ConsumerRunner(consumer, TransactionalProcessor(database, group, EmitDerivedEvent()))
    try:
        outcomes = [await _next_outcome(healthy), await _next_outcome(healthy)]
    finally:
        consumer.close()
    assert outcomes == [ProcessOutcome.APPLIED, ProcessOutcome.APPLIED]
    for envelope in (first, failing, third):
        assert await _effects(database, envelope.event_id) == 1
    assert _committed_offset(infra_settings, group, topic) == 3
