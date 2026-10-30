"""The normalizer against Compose PostgreSQL and Kafka (P04-T03, P04-T04).

Raw envelopes are produced by the real ingestion code path (`ingest_batch`
with a capturing publisher), so the normalizer sees exactly what the API
publishes. One module-scoped scratch database is seeded with the demo catalog;
tests use distinct source event IDs so their rows never overlap.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient
from pydantic import JsonValue
from sqlalchemy import Engine, func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource, ingest_batch
from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP, Normalizer
from signalops.adapters.quality import TimingPolicy
from signalops.config import Settings
from signalops.contracts.common import ContractError
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
    RawSignalPayload,
    decode_event,
)
from signalops.contracts.evidence import QualityFlag, QualityStatus, SignalKind, SourceKind
from signalops.demo.generator import Scenario, generate
from signalops.persistence import database as db
from signalops.persistence.models import ConsumerReceipt, Deployment, OutboxEvent, Signal
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID, seed_demo
from signalops.streaming.consumer import (
    ConsumerRunner,
    ProcessOutcome,
    TransactionalProcessor,
    create_consumer,
)
from signalops.streaming.producer import DeliveryResult, EventProducer, OutgoingMessage
from signalops.streaming.retry import RetryPolicy
from signalops.streaming.topics import TopicSpec, ensure_topics

pytestmark = pytest.mark.integration

SOURCE_BY_KIND = {source.kind: source for source in DEMO_SOURCES}
SERVICE_IDS = {service.name: service.id for service in DEMO_SERVICES}
POLL_DEADLINE_SECONDS = 30.0
TIMING = TimingPolicy(max_clock_skew=timedelta(minutes=5), late_arrival=timedelta(minutes=15))


class CapturingPublisher:
    """Acknowledges every message and keeps it, standing in for Kafka at ingestion."""

    def __init__(self) -> None:
        self.messages: list[OutgoingMessage] = []

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        self.messages.extend(messages)
        return [DeliveryResult(message, partition=0, offset=0) for message in messages]

    def close(self, timeout_seconds: float = 10.0) -> int:
        return 0


@pytest.fixture(scope="module")
def seeded(migrated: tuple[Settings, Engine]) -> tuple[Settings, Engine]:
    async def seed() -> None:
        engine = db.create_engine(migrated[0])
        try:
            await seed_demo(db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    asyncio.run(seed())
    return migrated


@pytest.fixture
async def database(seeded: tuple[Settings, Engine]) -> AsyncIterator[db.Database]:
    engine = db.create_engine(seeded[0])
    try:
        yield db.Database.from_engine(engine)
    finally:
        await engine.dispose()


async def _raw_envelopes(
    kind: SourceKind, items: list[dict[str, Any]], now: datetime
) -> list[EventEnvelope[EventPayload]]:
    seed_source = SOURCE_BY_KIND[kind]
    publisher = CapturingPublisher()
    response = await ingest_batch(
        items,
        source=IngestSource(seed_source.id, DEMO_WORKSPACE_ID, kind, enabled=True),
        publisher=publisher,
        policy=IngestPolicy(
            max_future=timedelta(days=1), max_age=timedelta(days=30), redact_fields=("password",)
        ),
        now=now,
        batch_id=uuid.uuid4(),
    )
    assert response.accepted == len(items), response.results
    return [decode_event(message.value) for message in publisher.messages]


def _item(
    payload: dict[str, JsonValue], occurred_at: datetime, *, with_id: bool = True
) -> dict[str, Any]:
    item: dict[str, Any] = {"occurred_at": occurred_at.isoformat(), "payload": payload}
    if with_id:
        item["source_event_id"] = f"it-{uuid.uuid4().hex}"
    return item


async def _process(
    database: db.Database,
    envelopes: list[EventEnvelope[EventPayload]],
    group: str = NORMALIZER_CONSUMER_GROUP,
) -> list[ProcessOutcome]:
    processor = TransactionalProcessor(database, group, Normalizer(TIMING))
    return [await processor.process(envelope) for envelope in envelopes]


def payload_event_id(envelope: EventEnvelope[EventPayload]) -> str:
    assert isinstance(envelope.payload, RawSignalPayload)
    return envelope.payload.source_event_id


async def _signal(database: db.Database, envelope: EventEnvelope[EventPayload]) -> Signal:
    async with database.transaction() as session:
        return (
            await session.scalars(
                select(Signal).where(
                    Signal.source_id == envelope.source_id,
                    Signal.source_event_id == payload_event_id(envelope),
                )
            )
        ).one()


async def _outbox_for(database: db.Database, signal_ids: list[uuid.UUID]) -> list[OutboxEvent]:
    async with database.transaction() as session:
        query = select(OutboxEvent).where(OutboxEvent.aggregate_id.in_(signal_ids))
        return list((await session.scalars(query)).all())


async def test_demo_dataset_becomes_queryable_evidence(database: db.Database) -> None:
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
    dataset = generate(
        Scenario.DOWNSTREAM_TIMEOUT, seed=7001, start=start, duration=timedelta(seconds=600)
    )
    now = start + timedelta(seconds=600)
    envelopes: list[EventEnvelope[EventPayload]] = []
    for kind, seed_source in SOURCE_BY_KIND.items():
        items = [
            record.to_json() for record in dataset.records if record.source_id == seed_source.id
        ]
        for item in items:
            del item["source_id"]
        envelopes += await _raw_envelopes(kind, items, now)
    assert len(envelopes) == len(dataset.records)

    outcomes = await _process(database, envelopes)
    assert set(outcomes) == {ProcessOutcome.APPLIED}

    raw_ids = [str(envelope.event_id) for envelope in envelopes]
    async with database.transaction() as session:
        signals = list(
            (
                await session.scalars(
                    select(Signal).where(
                        Signal.attributes["provenance"]["raw_event_id"].astext.in_(raw_ids)
                    )
                )
            ).all()
        )
        assert len(signals) == len(dataset.records)
        assert {signal.kind for signal in signals} == set(SignalKind)
        # Every demo record names a seeded service, so identity resolves for all.
        assert all(signal.quality_status == QualityStatus.ACCEPTED for signal in signals)
        assert {signal.service_id for signal in signals} <= set(SERVICE_IDS.values())

        # Service + time window, the detector's access pattern.
        payment = SERVICE_IDS["payment"]
        window = (start + timedelta(minutes=4), start + timedelta(minutes=8))
        in_window = (
            await session.scalars(
                select(Signal)
                .where(
                    Signal.service_id == payment,
                    Signal.occurred_at >= window[0],
                    Signal.occurred_at < window[1],
                    Signal.attributes["provenance"]["raw_event_id"].astext.in_(raw_ids),
                )
                .order_by(Signal.occurred_at.desc())
            )
        ).all()
        assert in_window
        assert {signal.kind for signal in in_window} >= {SignalKind.LOG, SignalKind.TRACE}

        # Trace lookup joins logs and trace summaries of one request.
        trace_id = next(s.trace_id for s in signals if s.kind == SignalKind.LOG and s.trace_id)
        same_trace = (
            await session.scalars(select(Signal).where(Signal.trace_id == trace_id))
        ).all()
        assert {signal.kind for signal in same_trace} == {SignalKind.LOG, SignalKind.TRACE}

        deployment_signals = [s for s in signals if s.kind == SignalKind.DEPLOYMENT]
        deployments = (
            await session.scalars(
                select(Deployment).where(
                    Deployment.signal_id.in_([s.id for s in deployment_signals])
                )
            )
        ).all()
        assert len(deployments) == len(deployment_signals) >= 1
        for deployment in deployments:
            assert deployment.service_id == SERVICE_IDS["inventory"]
            assert deployment.revision.startswith("inventory-")
            assert "previous_revision" in deployment.attributes

    rows = await _outbox_for(database, [signal.id for signal in signals])
    assert len(rows) == len(signals)
    assert {row.topic for row in rows} == {"signals.normalized.v1"}
    by_signal = {signal.id: signal for signal in signals}
    for row in rows:
        event = decode_event(json.dumps(row.envelope).encode())
        assert event.event_type is EventType.SIGNAL_NORMALIZED
        assert isinstance(event.payload, NormalizedSignalPayload)
        signal = by_signal[event.payload.signal_id]
        assert event.payload.service_id == signal.service_id
        assert event.causation_id == uuid.UUID(signal.attributes["provenance"]["raw_event_id"])
        assert row.message_key == f"{DEMO_WORKSPACE_ID}:{signal.service_id}"


async def test_signal_preserves_source_attributes_and_provenance(database: db.Database) -> None:
    now = datetime.now(UTC)
    payload: dict[str, JsonValue] = {
        "service": "payment",
        "environment": "demo",
        "level": "warn",
        "message": "retrying charge",
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "00f067aa0ba902b7",
        "host": "10.0.4.17",
        "password": "hunter2",
        "attributes": {"http.route": "/v1/charges"},
    }
    [envelope] = await _raw_envelopes(SourceKind.STRUCTURED_LOG, [_item(payload, now)], now)

    await _process(database, [envelope])

    signal = await _signal(database, envelope)
    assert signal.severity == "warning"
    assert signal.service_id == SERVICE_IDS["payment"]
    assert signal.occurred_at == envelope.occurred_at
    assert signal.ingested_at == envelope.ingested_at
    assert signal.attributes["source"] == {
        "http.route": "/v1/charges",
        "host": "10.0.4.17",
        "password": "[REDACTED]",
    }
    assert signal.attributes["record"] == {"span_id": "00f067aa0ba902b7"}
    assert signal.attributes["provenance"] == {
        "raw_event_id": str(envelope.event_id),
        "source_kind": "structured_log",
        "adapter_version": 1,
        "reported_service": "payment",
        "reported_environment": "demo",
        "correlation_id": envelope.correlation_id,
    }


@pytest.mark.parametrize(
    ("identity", "status", "flag"),
    [
        ({"environment": "demo"}, QualityStatus.DEGRADED, QualityFlag.MISSING_SERVICE),
        ({"service": "payment"}, QualityStatus.DEGRADED, QualityFlag.MISSING_SERVICE),
        (
            {"service": "ghost", "environment": "demo"},
            QualityStatus.DEGRADED,
            QualityFlag.UNKNOWN_SERVICE,
        ),
        # Known name, unknown environment: never matched to the demo service.
        (
            {"service": "payment", "environment": "prod"},
            QualityStatus.DEGRADED,
            QualityFlag.UNKNOWN_SERVICE,
        ),
        (
            {"service": "payment", "environment": "demo", "service.name": "checkout"},
            QualityStatus.QUARANTINED,
            QualityFlag.CONTRADICTORY_IDENTITY,
        ),
    ],
)
async def test_unresolved_identity_is_flagged_not_guessed(
    database: db.Database,
    identity: dict[str, JsonValue],
    status: QualityStatus,
    flag: QualityFlag,
) -> None:
    now = datetime.now(UTC)
    payload: dict[str, JsonValue] = {**identity, "level": "error", "message": "charge failed"}
    [envelope] = await _raw_envelopes(SourceKind.STRUCTURED_LOG, [_item(payload, now)], now)

    assert await _process(database, [envelope]) == [ProcessOutcome.APPLIED]

    signal = await _signal(database, envelope)
    assert signal.service_id is None
    assert signal.quality_status == status
    assert signal.quality_flags == [flag]
    provenance = signal.attributes["provenance"]
    assert provenance["reported_service"] == identity.get("service")
    [row] = await _outbox_for(database, [signal.id])
    assert row.envelope["payload"]["quality_status"] == status
    assert row.envelope["payload"]["service_id"] is None
    assert row.message_key == str(DEMO_WORKSPACE_ID)


async def test_contradictory_trace_and_deployment_are_flagged(database: db.Database) -> None:
    now = datetime.now(UTC)
    trace: dict[str, JsonValue] = {
        "trace_id": "9b1c",
        "service": "checkout",
        "environment": "demo",
        "operation": "POST /checkout",
        "duration_ms": 40,
        "status": "ok",
        "span_count": 3,
        "attributes": {"deployment.environment": "prod"},
    }
    deployment: dict[str, JsonValue] = {
        "service": "payment",
        "environment": "demo",
        "revision": "payment-9",
        "attributes": {"service.name": "bank-gateway"},
    }
    [trace_envelope] = await _raw_envelopes(SourceKind.TRACE_SUMMARY, [_item(trace, now)], now)
    [deploy_envelope] = await _raw_envelopes(SourceKind.DEPLOYMENT, [_item(deployment, now)], now)

    await _process(database, [trace_envelope, deploy_envelope])

    for envelope, conflict in (
        (trace_envelope, "deployment.environment"),
        (deploy_envelope, "service.name"),
    ):
        signal = await _signal(database, envelope)
        assert signal.service_id is None
        assert signal.quality_flags == [QualityFlag.CONTRADICTORY_IDENTITY]
        assert signal.attributes["provenance"]["identity_conflicts"] == [conflict]
    # A deployment without a resolved service is evidence only.
    async with database.transaction() as session:
        deploy_signal = await _signal(database, deploy_envelope)
        count = await session.scalar(
            select(func.count()).where(Deployment.signal_id == deploy_signal.id)
        )
    assert count == 0


async def test_redelivery_and_other_groups_store_one_signal(database: db.Database) -> None:
    now = datetime.now(UTC)
    payload: dict[str, JsonValue] = {
        "service": "inventory",
        "environment": "demo",
        "revision": "inventory-77",
    }
    [envelope] = await _raw_envelopes(SourceKind.DEPLOYMENT, [_item(payload, now)], now)

    assert await _process(database, [envelope, envelope]) == [
        ProcessOutcome.APPLIED,
        ProcessOutcome.DUPLICATE,
    ]
    # A second group has no receipt, so only the business key stops a second row.
    assert await _process(database, [envelope], group="normalizer-replay") == [
        ProcessOutcome.APPLIED
    ]

    signal = await _signal(database, envelope)
    async with database.transaction() as session:
        deployments = await session.scalar(
            select(func.count()).where(Deployment.signal_id == signal.id)
        )
    assert deployments == 1
    assert len(await _outbox_for(database, [signal.id])) == 1


async def _counts(
    database: db.Database, envelope: EventEnvelope[EventPayload]
) -> tuple[int | None, ...]:
    async with database.transaction() as session:
        receipts = await session.scalar(
            select(func.count()).where(ConsumerReceipt.event_id == envelope.event_id)
        )
        signals = await session.scalar(
            select(func.count()).where(
                Signal.source_id == envelope.source_id,
                Signal.source_event_id == payload_event_id(envelope),
            )
        )
        deployments = await session.scalar(
            select(func.count())
            .select_from(Deployment)
            .join(Signal, Deployment.signal_id == Signal.id)
            .where(
                Signal.source_id == envelope.source_id,
                Signal.source_event_id == payload_event_id(envelope),
            )
        )
    return receipts, signals, deployments


async def test_malformed_record_is_quarantined_evidence(database: db.Database) -> None:
    now = datetime.now(UTC)
    # Deployments require a revision; the rest of the record is readable.
    payload: dict[str, JsonValue] = {
        "service": "payment",
        "environment": "demo",
        "password": "hunter2",
        "attributes": {"ticket": "CHG-1"},
    }
    [envelope] = await _raw_envelopes(SourceKind.DEPLOYMENT, [_item(payload, now)], now)

    assert await _process(database, [envelope, envelope]) == [
        ProcessOutcome.APPLIED,
        ProcessOutcome.DUPLICATE,
    ]

    assert await _counts(database, envelope) == (1, 1, 0)
    signal = await _signal(database, envelope)
    assert signal.kind == SignalKind.DEPLOYMENT
    assert signal.quality_status == QualityStatus.QUARANTINED
    assert signal.quality_flags == [QualityFlag.MALFORMED_PAYLOAD]
    assert signal.service_id is None
    assert signal.trace_id is None
    assert signal.severity == "info"
    assert "revision" in signal.message
    assert "revision" in signal.attributes["record"]["contract_errors"]
    # The raw record is kept as ingestion redacted it.
    assert signal.attributes["source"] == {**payload, "password": "[REDACTED]"}
    provenance = signal.attributes["provenance"]
    assert (provenance["reported_service"], provenance["reported_environment"]) == (
        "payment",
        "demo",
    )
    [row] = await _outbox_for(database, [signal.id])
    assert row.envelope["payload"]["quality_status"] == QualityStatus.QUARANTINED


async def test_oversized_malformed_record_keeps_only_its_size(database: db.Database) -> None:
    now = datetime.now(UTC)
    # Over the 16 KiB attributes limit, under the 64 KiB raw record limit; no message.
    payload: dict[str, JsonValue] = {"service": "payment", "level": "info", "blob": "x" * 20_000}
    [envelope] = await _raw_envelopes(SourceKind.STRUCTURED_LOG, [_item(payload, now)], now)

    assert await _process(database, [envelope]) == [ProcessOutcome.APPLIED]

    signal = await _signal(database, envelope)
    assert signal.quality_flags == [QualityFlag.MALFORMED_PAYLOAD]
    assert signal.attributes["source"] == {}
    assert signal.attributes["provenance"]["record_omitted_bytes"] > 20_000
    assert "message" in signal.attributes["record"]["contract_errors"]


async def test_envelope_faults_still_fail_without_evidence(database: db.Database) -> None:
    now = datetime.now(UTC)
    payload: dict[str, JsonValue] = {"service": "payment", "level": "info", "message": "ok"}
    [envelope] = await _raw_envelopes(SourceKind.STRUCTURED_LOG, [_item(payload, now)], now)
    assert isinstance(envelope.payload, RawSignalPayload)
    # The record claims a different kind than its source has: a pipeline fault.
    mismatched = envelope.model_copy(
        update={
            "payload": envelope.payload.model_copy(update={"source_kind": SourceKind.TRACE_SUMMARY})
        }
    )

    with pytest.raises(ContractError) as caught:
        await _process(database, [mismatched])

    assert caught.value.event_id == envelope.event_id
    assert await _counts(database, envelope) == (0, 0, 0)


@pytest.mark.parametrize(
    ("offset", "status", "flags"),
    [
        (timedelta(minutes=10), QualityStatus.DEGRADED, [QualityFlag.CLOCK_SKEW]),
        (-timedelta(hours=2), QualityStatus.ACCEPTED, [QualityFlag.LATE_ARRIVAL]),
        (-timedelta(minutes=1), QualityStatus.ACCEPTED, []),
    ],
    ids=["skewed", "late", "on-time"],
)
async def test_timing_is_flagged_and_identity_still_resolves(
    database: db.Database, offset: timedelta, status: QualityStatus, flags: list[QualityFlag]
) -> None:
    now = datetime.now(UTC)
    payload: dict[str, JsonValue] = {
        "service": "payment",
        "environment": "demo",
        "level": "warn",
        "message": "retrying charge",
    }
    [envelope] = await _raw_envelopes(
        SourceKind.STRUCTURED_LOG, [_item(payload, now + offset)], now
    )

    await _process(database, [envelope])

    signal = await _signal(database, envelope)
    assert signal.service_id == SERVICE_IDS["payment"]
    assert signal.quality_status == status
    assert signal.quality_flags == flags
    # The source's timestamp is kept, never corrected.
    assert signal.occurred_at == envelope.occurred_at


async def test_records_without_ids_get_stable_derived_identity(database: db.Database) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    line: dict[str, JsonValue] = {
        "service": "checkout",
        "environment": "demo",
        "level": "info",
        "message": "order placed",
    }
    trace: dict[str, JsonValue] = {
        "trace_id": f"t{uuid.uuid4().hex}",
        "service": "checkout",
        "environment": "demo",
        "operation": "POST /checkout",
        "duration_ms": 120,
        "status": "ok",
        "span_count": 4,
    }
    # Identical message text a millisecond apart: two events.
    log_items = [
        _item(line, now - timedelta(seconds=2), with_id=False),
        _item(line, now - timedelta(seconds=2, milliseconds=-1), with_id=False),
    ]
    trace_items = [_item(trace, now - timedelta(seconds=1), with_id=False)]

    first = [
        *await _raw_envelopes(SourceKind.STRUCTURED_LOG, log_items, now),
        *await _raw_envelopes(SourceKind.TRACE_SUMMARY, trace_items, now),
    ]
    # The client retries everything, ingested a second later.
    retry = [
        *await _raw_envelopes(SourceKind.STRUCTURED_LOG, log_items, now + timedelta(seconds=1)),
        *await _raw_envelopes(SourceKind.TRACE_SUMMARY, trace_items, now + timedelta(seconds=1)),
    ]
    assert [e.event_id for e in retry] == [e.event_id for e in first]

    assert (
        await _process(database, first + retry)
        == [ProcessOutcome.APPLIED] * 3 + [ProcessOutcome.DUPLICATE] * 3
    )

    signals = [await _signal(database, envelope) for envelope in first]
    assert len({signal.id for signal in signals}) == 3
    assert [signal.message for signal in signals[:2]] == ["order placed", "order placed"]
    for signal in signals:
        assert signal.source_event_id.startswith("derived:")
        assert signal.quality_status == QualityStatus.ACCEPTED
        assert signal.quality_flags == [QualityFlag.DERIVED_EVENT_ID]
        assert signal.service_id == SERVICE_IDS["checkout"]
    for envelope in first:
        assert await _counts(database, envelope) == (1, 1, 0)
    assert len(await _outbox_for(database, [signal.id for signal in signals])) == 3


@pytest.fixture
def raw_topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.normalizer.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=1, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


async def test_worker_consumes_raw_topic_and_commits_offsets(
    infra_settings: Settings, database: db.Database, raw_topic: str
) -> None:
    now = datetime.now(UTC)
    items = [
        _item({"service": "checkout", "environment": "demo", "level": "info", "message": m}, now)
        for m in ("order placed", "order placed", "order shipped")
    ]
    envelopes = await _raw_envelopes(SourceKind.STRUCTURED_LOG, items, now)
    producer = EventProducer(infra_settings, client_id="test-normalizer-producer")
    messages = [
        OutgoingMessage(raw_topic, message.key, message.value, message.event_id)
        for message in map(OutgoingMessage.from_envelope, envelopes)
    ]
    # The second message is a redelivery of the first.
    assert all(result.delivered for result in producer.publish([*messages, messages[0]]))
    producer.close()

    group = f"test-normalizer-{uuid.uuid4().hex[:8]}"
    consumer = create_consumer(infra_settings, group_id=group, client_id=f"{group}-member")
    failures = EventProducer(infra_settings, client_id=f"{group}-failures")
    runner = ConsumerRunner(
        consumer,
        TransactionalProcessor(database, group, Normalizer(TIMING)),
        consumer_group=group,
        failure_publisher=failures,
        retry_policy=RetryPolicy(3, base_delay_seconds=0.05, max_delay_seconds=0.2),
    )
    runner.subscribe([raw_topic])
    outcomes: list[ProcessOutcome] = []
    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    try:
        while len(outcomes) < 4 and time.monotonic() < deadline:
            outcome = await runner.run_once()
            if outcome is not None:
                outcomes.append(outcome)
        committed = consumer.committed(consumer.assignment(), timeout=10)
    finally:
        consumer.close()
        failures.close()

    assert outcomes == [ProcessOutcome.APPLIED] * 3 + [ProcessOutcome.DUPLICATE]
    assert [tp.offset for tp in committed] == [4]
    # Identical message text with distinct source event IDs stays separate evidence.
    signals = [await _signal(database, envelope) for envelope in envelopes]
    assert len({signal.id for signal in signals}) == 3
    assert [signal.message for signal in signals] == [
        "order placed",
        "order placed",
        "order shipped",
    ]
