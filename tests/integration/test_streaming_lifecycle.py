"""Consumer shutdown, rebalance, and lag reporting against Compose PostgreSQL and Kafka (P03-T05).

Each test consumes its own single-partition topic with its own group.
"""

import asyncio
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
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.persistence import database as db
from signalops.persistence.models import ConsumerReceipt
from signalops.streaming.consumer import (
    ConsumerRunner,
    EventHandler,
    ProcessOutcome,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.producer import EventProducer, OutgoingMessage
from signalops.streaming.retry import RetryPolicy
from signalops.streaming.topics import TopicSpec, ensure_topics

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
POLL_DEADLINE_SECONDS = 30.0


@dataclass
class StopAfter:
    """Records applied events and sets `stop` while applying the `count`-th one."""

    stop: asyncio.Event
    count: int
    # Remaining transient failures per event.
    fail: dict[uuid.UUID, int] = field(default_factory=dict)
    applied: list[uuid.UUID] = field(default_factory=list)

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        if self.fail.get(envelope.event_id, 0) > 0:
            self.fail[envelope.event_id] -= 1
            raise ConnectionError("database unavailable")
        self.applied.append(envelope.event_id)
        if len(self.applied) == self.count:
            self.stop.set()


@pytest.fixture
def topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.lifecycle.{uuid.uuid4().hex[:8]}"
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
    return f"test-lifecycle-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def producer(infra_settings: Settings) -> EventProducer:
    return EventProducer(infra_settings, client_id="test-lifecycle-producer")


def _publish(producer: EventProducer, topic: str, count: int) -> list[uuid.UUID]:
    template = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    envelopes = [template.model_copy(update={"event_id": uuid.uuid4()}) for _ in range(count)]
    messages = [OutgoingMessage(topic, "key", e.encode(), e.event_id) for e in envelopes]
    assert all(result.delivered for result in producer.publish(messages))
    return [e.event_id for e in envelopes]


def _runner(
    settings: Settings,
    database: db.Database,
    group: str,
    topic: str,
    handler: EventHandler,
    producer: EventProducer,
    *,
    base_delay_seconds: float = 0.05,
) -> tuple[Consumer, ConsumerRunner]:
    consumer = create_consumer(settings, group_id=group, client_id=f"{group}-member")
    runner = ConsumerRunner(
        consumer,
        TransactionalProcessor(database, group, handler),
        consumer_group=group,
        failure_publisher=producer,
        retry_policy=RetryPolicy(
            base_delay_seconds=base_delay_seconds, max_delay_seconds=base_delay_seconds
        ),
    )
    runner.subscribe([topic])
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


async def _receipts(database: db.Database, group: str) -> int:
    async with database.transaction() as session:
        query = select(func.count()).where(ConsumerReceipt.consumer_group == group)
        return (await session.execute(query)).scalar_one()


async def _run_until_stopped(runner: ConsumerRunner, stop: asyncio.Event) -> None:
    await asyncio.wait_for(runner.run(stop), POLL_DEADLINE_SECONDS)


async def test_stop_mid_stream_resumes_from_the_last_committed_record(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    event_ids = _publish(producer, topic, 5)

    # The stop arrives while the second record is being applied.
    first_stop = asyncio.Event()
    first = StopAfter(first_stop, count=2)
    _, runner = _runner(infra_settings, database, group, topic, first, producer)
    await _run_until_stopped(runner, first_stop)
    assert first.applied == event_ids[:2]
    assert _committed_offset(infra_settings, group, topic) == 2

    second_stop = asyncio.Event()
    second = StopAfter(second_stop, count=3)
    _, restarted = _runner(infra_settings, database, group, topic, second, producer)
    await _run_until_stopped(restarted, second_stop)
    # Nothing skipped, nothing repeated.
    assert second.applied == event_ids[2:]
    assert await _receipts(database, group) == 5
    assert _committed_offset(infra_settings, group, topic) == 5


async def test_growing_backlog_is_visible_as_lag(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    _publish(producer, topic, 3)
    handler = StopAfter(asyncio.Event(), count=0)
    consumer, runner = _runner(infra_settings, database, group, topic, handler, producer)
    try:
        assert await _outcomes(runner, 1) == [ProcessOutcome.APPLIED]
        first = await runner.report_stats()
        assert first["applied"] == 1
        assert first["lag"] == {f"{topic}[0]": 2}

        _publish(producer, topic, 4)
        grown = await runner.report_stats()
        assert grown["lag_total"] == 6
        assert grown["applied"] == 0

        assert await _outcomes(runner, 6) == [ProcessOutcome.APPLIED] * 6
        drained = await runner.report_stats()
        assert drained["lag_total"] == 0
        assert drained["consumed"] == 6
        assert drained["processing_ms_max"] > 0
    finally:
        consumer.close()


async def test_revoke_drops_a_pending_retry_and_reassignment_resumes(
    infra_settings: Settings,
    database: db.Database,
    producer: EventProducer,
    topic: str,
    group: str,
) -> None:
    failing, later = _publish(producer, topic, 2)
    handler = StopAfter(asyncio.Event(), count=0, fail={failing: 1})
    # A backoff far beyond the test deadline: only the revoke can end the wait.
    consumer, runner = _runner(
        infra_settings, database, group, topic, handler, producer, base_delay_seconds=300
    )
    try:
        assert await _outcomes(runner, 1) == [ProcessOutcome.RETRY_SCHEDULED]
        assert (await runner.report_stats())["paused_partitions"] == 1

        # Leaving and rejoining the group revokes and then reassigns the partition.
        consumer.unsubscribe()
        await runner.run_once()
        assert (await runner.report_stats())["paused_partitions"] == 0
        runner.subscribe([topic])

        assert await _outcomes(runner, 2) == [ProcessOutcome.APPLIED, ProcessOutcome.APPLIED]
    finally:
        consumer.close()
    assert handler.applied == [failing, later]
    assert _committed_offset(infra_settings, group, topic) == 2
