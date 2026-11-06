"""The detector against Compose PostgreSQL (P05-T01).

Most tests store signals directly and feed the detector `signal.normalized`
envelopes for them, each test in its own replay scope so windows never
overlap. The last test runs a generated demo dataset through ingestion, the
normalizer, and the detector, using the normalizer's own outbox events.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource, ingest_batch
from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP, Normalizer, signal_id_for
from signalops.adapters.quality import TimingPolicy
from signalops.config import Settings
from signalops.contracts.common import ContractError, replay_scope_for
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    NormalizedSignalPayload,
    RawSignalPayload,
    decode_event,
)
from signalops.contracts.evidence import (
    NormalizedSignal,
    QualityFlag,
    QualityStatus,
    Severity,
    SignalKind,
    SourceKind,
)
from signalops.demo.generator import Scenario, generate
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP, DetectionPolicy, Detector
from signalops.detection.rules import DEFAULT_RULES
from signalops.persistence import database as db
from signalops.persistence.models import (
    DetectionSample,
    DetectionTrigger,
    OutboxEvent,
    SignalAggregate,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID, seed_demo
from signalops.streaming.consumer import ProcessOutcome, TransactionalProcessor
from signalops.streaming.producer import DeliveryResult, OutgoingMessage

pytestmark = pytest.mark.integration

SOURCE_BY_KIND = {source.kind: source for source in DEMO_SOURCES}
SERVICE_IDS = {service.name: service.id for service in DEMO_SERVICES}
PAYMENT = SERVICE_IDS["payment"]
POLICY = DetectionPolicy(window_seconds=60, rules=DEFAULT_RULES)
# A window boundary well in the past, so live timing never matters.
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


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


@pytest.fixture
def run_id() -> uuid.UUID:
    """A replay run per test: its scope isolates the test's windows."""
    return uuid.uuid4()


def _detector() -> Detector:
    return Detector(POLICY)


def _processor(database: db.Database) -> TransactionalProcessor:
    return TransactionalProcessor(database, DETECTOR_CONSUMER_GROUP, _detector())


async def _store(
    database: db.Database,
    run_id: uuid.UUID,
    occurred_at: datetime,
    *,
    kind: SignalKind = SignalKind.TRACE,
    service_id: uuid.UUID | None = PAYMENT,
    error: bool = False,
    duration_ms: int = 80,
    status: QualityStatus = QualityStatus.ACCEPTED,
    flags: tuple[QualityFlag, ...] = (),
) -> EventEnvelope[EventPayload]:
    """Store one signal and return the `signal.normalized` envelope the normalizer would emit."""
    source_kind = {
        SignalKind.TRACE: SourceKind.TRACE_SUMMARY,
        SignalKind.LOG: SourceKind.STRUCTURED_LOG,
        SignalKind.DEPLOYMENT: SourceKind.DEPLOYMENT,
    }[kind]
    record: dict[str, object] = {}
    if kind is SignalKind.TRACE:
        record = {"duration_ms": duration_ms, "status": "error" if error else "ok"}
    signal = NormalizedSignal(
        id=uuid.uuid4(),
        workspace_id=DEMO_WORKSPACE_ID,
        source_id=SOURCE_BY_KIND[source_kind].id,
        service_id=service_id,
        source_event_id=f"det-{uuid.uuid4().hex}",
        replay_scope=replay_scope_for(run_id),
        kind=kind,
        occurred_at=occurred_at,
        ingested_at=occurred_at + timedelta(seconds=1),
        severity=Severity.ERROR if error else Severity.INFO,
        message="test signal",
        attributes={"record": record},  # type: ignore[dict-item]
        quality_status=status,
        quality_flags=flags,
    )
    async with database.transaction() as session:
        assert await EvidenceRepository(session).insert_signal(signal)
    return _envelope(signal, run_id)


def _envelope(signal: NormalizedSignal, run_id: uuid.UUID | None) -> EventEnvelope[EventPayload]:
    return EventEnvelope[EventPayload](
        event_id=uuid.uuid4(),
        event_type=EventType.SIGNAL_NORMALIZED,
        schema_version=1,
        workspace_id=signal.workspace_id,
        source_id=signal.source_id,
        occurred_at=signal.occurred_at,
        ingested_at=signal.ingested_at,
        replay_run_id=run_id,
        payload=NormalizedSignalPayload(
            signal_id=signal.id,
            service_id=signal.service_id,
            kind=signal.kind,
            quality_status=signal.quality_status,
        ),
    )


def _redelivered(envelope: EventEnvelope[EventPayload]) -> EventEnvelope[EventPayload]:
    """The same signal under a new event ID, so the consumer receipt does not catch it."""
    return envelope.model_copy(update={"event_id": uuid.uuid4()})


async def _process(
    database: db.Database, envelopes: Sequence[EventEnvelope[EventPayload]]
) -> list[ProcessOutcome]:
    processor = _processor(database)
    return [await processor.process(envelope) for envelope in envelopes]


async def _aggregates(
    database: db.Database, run_id: uuid.UUID, service_id: uuid.UUID = PAYMENT
) -> list[SignalAggregate]:
    async with database.transaction() as session:
        result = await session.scalars(
            select(SignalAggregate)
            .where(
                SignalAggregate.service_id == service_id,
                SignalAggregate.replay_scope == replay_scope_for(run_id),
            )
            .order_by(SignalAggregate.metric_name, SignalAggregate.window_start)
        )
        return list(result)


async def _triggers(
    database: db.Database, replay_scope: str, service_id: uuid.UUID | None = PAYMENT
) -> list[DetectionTrigger]:
    async with database.transaction() as session:
        statement = select(DetectionTrigger).where(DetectionTrigger.replay_scope == replay_scope)
        if service_id is not None:
            statement = statement.where(DetectionTrigger.service_id == service_id)
        result = await session.scalars(
            statement.order_by(DetectionTrigger.window_start, DetectionTrigger.rule_id)
        )
        return list(result)


async def _sample_totals(database: db.Database, run_id: uuid.UUID) -> tuple[int, int]:
    async with database.transaction() as session:
        row = (
            await session.execute(
                select(
                    func.count(),
                    func.count().filter(DetectionSample.is_error),
                ).where(DetectionSample.replay_scope == replay_scope_for(run_id))
            )
        ).one()
        return int(row[0]), int(row[1])


async def test_duplicate_delivery_does_not_inflate_error_rate(
    database: db.Database, run_id: uuid.UUID
) -> None:
    envelopes = [
        await _store(database, run_id, T0 + timedelta(seconds=i), error=i < 4) for i in range(20)
    ]
    assert set(await _process(database, envelopes)) == {ProcessOutcome.APPLIED}

    # Kafka redelivery: the receipt stops it.
    assert set(await _process(database, envelopes)) == {ProcessOutcome.DUPLICATE}
    # The same signals under new event IDs: the sample key stops them.
    assert set(await _process(database, [_redelivered(e) for e in envelopes])) == {
        ProcessOutcome.APPLIED
    }

    [aggregate] = await _aggregates(database, run_id)
    assert aggregate.metric_name == "trace.60s"
    assert (aggregate.sample_count, aggregate.error_count) == (20, 4)
    assert aggregate.window_start == T0
    assert aggregate.window_end == T0 + timedelta(seconds=60)
    assert aggregate.value_sum == pytest.approx(1600)
    assert aggregate.histogram == {"100": 20}
    assert await _sample_totals(database, run_id) == (20, 4)
    # trace_error_rate fired once, at its 15th sample; the duplicates added none.
    [trigger] = await _triggers(database, replay_scope_for(run_id))
    assert trigger.rule_id == "trace_error_rate"
    assert (trigger.sample_count, trigger.error_count) == (15, 4)


async def test_low_sample_window_does_not_trigger(database: db.Database, run_id: uuid.UUID) -> None:
    # Every request failed and was slow, but each window holds fewer than min_samples
    # (15 and 20 for the trace rules, 10 for logs).
    envelopes = [
        await _store(database, run_id, T0 + timedelta(seconds=i), error=True, duration_ms=4000)
        for i in range(14)
    ]
    envelopes += [
        await _store(database, run_id, T0 + timedelta(seconds=i), kind=SignalKind.LOG, error=True)
        for i in range(9)
    ]
    assert set(await _process(database, envelopes)) == {ProcessOutcome.APPLIED}

    aggregates = await _aggregates(database, run_id)
    assert [(a.metric_name, a.sample_count, a.error_count) for a in aggregates] == [
        ("log.60s", 9, 9),
        ("trace.60s", 14, 14),
    ]
    assert await _triggers(database, replay_scope_for(run_id)) == []


async def test_rule_fires_once_per_window_and_respects_cooldown(
    database: db.Database, run_id: uuid.UUID
) -> None:
    async def failing_window(start: datetime, count: int) -> None:
        envelopes = [
            await _store(database, run_id, start + timedelta(seconds=i), error=i % 2 == 0)
            for i in range(count)
        ]
        await _process(database, envelopes)

    await failing_window(T0, 25)  # fires at the 15th sample; later samples do not refire
    await failing_window(T0 + timedelta(seconds=60), 20)  # within the 300 s cooldown
    await failing_window(T0 + timedelta(seconds=300), 20)  # still within (inclusive)
    await failing_window(T0 + timedelta(seconds=360), 20)  # past the cooldown

    triggers = await _triggers(database, replay_scope_for(run_id))
    assert [(t.rule_id, t.window_start) for t in triggers] == [
        ("trace_error_rate", T0),
        ("trace_error_rate", T0 + timedelta(seconds=360)),
    ]
    first = triggers[0]
    assert (first.sample_count, first.error_count) == (15, 8)
    assert first.observed == pytest.approx(8 / 15)
    assert first.threshold == pytest.approx(0.2)
    assert first.rule_version == 1
    assert first.metric_name == "trace.60s"
    assert first.window_end == T0 + timedelta(seconds=60)
    # Aggregates still count every sample, including those of suppressed windows.
    aggregates = await _aggregates(database, run_id)
    assert [a.sample_count for a in aggregates] == [25, 20, 20, 20]


async def test_latency_rule_uses_histogram_quantile(
    database: db.Database, run_id: uuid.UUID
) -> None:
    durations = [90] * 18 + [2400, 3800]
    envelopes = [
        await _store(database, run_id, T0 + timedelta(seconds=i), duration_ms=duration)
        for i, duration in enumerate(durations)
    ]
    await _process(database, envelopes)

    [aggregate] = await _aggregates(database, run_id)
    assert aggregate.histogram == {"100": 18, "2500": 1, "5000": 1}
    assert aggregate.value_sum == pytest.approx(sum(durations))
    [trigger] = await _triggers(database, replay_scope_for(run_id))
    assert trigger.rule_id == "trace_latency_p95"
    # 18 of 20 samples are at or below 1000 ms, fewer than ceil(0.95 * 20) = 19;
    # the 19th sample lands in the 2500 ms bucket.
    assert trigger.observed == 2500
    assert trigger.threshold == 1000


async def test_windows_are_separate_per_scope_and_service(
    database: db.Database, run_id: uuid.UUID
) -> None:
    other_run = uuid.uuid4()
    checkout = SERVICE_IDS["checkout"]
    envelopes = [await _store(database, run_id, T0, error=True) for _ in range(3)]
    envelopes += [await _store(database, other_run, T0) for _ in range(2)]
    envelopes += [await _store(database, run_id, T0, service_id=checkout) for _ in range(4)]
    await _process(database, envelopes)

    assert [(a.sample_count, a.error_count) for a in await _aggregates(database, run_id)] == [
        (3, 3)
    ]
    assert [(a.sample_count, a.error_count) for a in await _aggregates(database, other_run)] == [
        (2, 0)
    ]
    assert [a.sample_count for a in await _aggregates(database, run_id, checkout)] == [4]


async def test_uncountable_signals_leave_windows_untouched(
    database: db.Database, run_id: uuid.UUID
) -> None:
    envelopes = [
        await _store(database, run_id, T0, kind=SignalKind.DEPLOYMENT),
        await _store(
            database,
            run_id,
            T0,
            error=True,
            status=QualityStatus.DEGRADED,
            flags=(QualityFlag.CLOCK_SKEW,),
        ),
        await _store(
            database,
            run_id,
            T0,
            error=True,
            service_id=None,
            status=QualityStatus.DEGRADED,
            flags=(QualityFlag.MISSING_SERVICE,),
        ),
        await _store(
            database,
            run_id,
            T0,
            kind=SignalKind.LOG,
            service_id=None,
            status=QualityStatus.QUARANTINED,
            flags=(QualityFlag.MALFORMED_PAYLOAD,),
        ),
    ]
    assert set(await _process(database, envelopes)) == {ProcessOutcome.APPLIED}
    assert await _aggregates(database, run_id) == []
    assert await _sample_totals(database, run_id) == (0, 0)


async def test_unknown_signal_is_a_contract_error(database: db.Database, run_id: uuid.UUID) -> None:
    envelope = await _store(database, run_id, T0)
    payload = envelope.payload
    assert isinstance(payload, NormalizedSignalPayload)
    # Same signal ID, another replay scope: the detector must not read across scopes.
    other_scope = envelope.model_copy(
        update={"event_id": uuid.uuid4(), "replay_run_id": uuid.uuid4()}
    )
    missing = envelope.model_copy(
        update={
            "event_id": uuid.uuid4(),
            "payload": payload.model_copy(update={"signal_id": uuid.uuid4()}),
        }
    )
    for bad in (other_scope, missing):
        with pytest.raises(ContractError, match="does not name a stored signal") as caught:
            await _processor(database).process(bad)
        assert caught.value.event_id == bad.event_id
    # The failed transaction kept no receipt, so a redrive is processed normally.
    assert await _processor(database).process(envelope) is ProcessOutcome.APPLIED


async def test_concurrent_writers_count_every_sample_once(
    database: db.Database, run_id: uuid.UUID
) -> None:
    envelopes = [
        await _store(
            database,
            run_id,
            T0 + timedelta(milliseconds=i),
            error=i % 3 == 0,
            duration_ms=40 if i % 2 else 400,
        )
        for i in range(30)
    ]
    processor = _processor(database)

    async def apply(envelope: EventEnvelope[EventPayload]) -> ProcessOutcome:
        return await processor.process(envelope)

    # Each envelope twice, all at once, from separate transactions.
    outcomes = await asyncio.gather(*(apply(e) for e in [*envelopes, *envelopes]))
    assert sorted(outcomes) == sorted(
        [ProcessOutcome.APPLIED] * 30 + [ProcessOutcome.DUPLICATE] * 30
    )

    [aggregate] = await _aggregates(database, run_id)
    assert (aggregate.sample_count, aggregate.error_count) == (30, 10)
    assert aggregate.histogram == {"50": 15, "500": 15}
    assert await _sample_totals(database, run_id) == (30, 10)
    rules = [t.rule_id for t in await _triggers(database, replay_scope_for(run_id))]
    assert sorted(rules) == ["trace_error_rate"]


class CapturingPublisher:
    def __init__(self) -> None:
        self.messages: list[OutgoingMessage] = []

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        self.messages.extend(messages)
        return [DeliveryResult(message, partition=0, offset=0) for message in messages]

    def close(self, timeout_seconds: float = 10.0) -> int:
        return 0


async def test_demo_fault_triggers_only_affected_services(database: db.Database) -> None:
    start = (datetime.now(UTC) - timedelta(minutes=11)).replace(second=0, microsecond=0)
    duration = timedelta(minutes=10)
    dataset = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=5101, start=start, duration=duration)
    labels = dataset.labels
    assert labels.fault_start is not None
    assert labels.fault_end is not None

    raw: list[EventEnvelope[EventPayload]] = []
    for kind, source in SOURCE_BY_KIND.items():
        items = [r.to_json() for r in dataset.records if r.source_id == source.id]
        for item in items:
            del item["source_id"]
        publisher = CapturingPublisher()
        response = await ingest_batch(
            items,
            source=IngestSource(source.id, DEMO_WORKSPACE_ID, kind, enabled=True),
            publisher=publisher,
            policy=IngestPolicy(
                max_future=timedelta(days=1), max_age=timedelta(days=30), redact_fields=()
            ),
            now=start + duration,
            batch_id=uuid.uuid4(),
        )
        assert response.accepted == len(items)
        raw += [decode_event(message.value) for message in publisher.messages]

    normalizer = TransactionalProcessor(
        database,
        NORMALIZER_CONSUMER_GROUP,
        Normalizer(TimingPolicy(timedelta(minutes=5), timedelta(minutes=15))),
    )
    for envelope in raw:
        await normalizer.process(envelope)
    signal_ids = []
    for envelope in raw:
        assert envelope.source_id is not None
        assert isinstance(envelope.payload, RawSignalPayload)
        signal_ids.append(
            signal_id_for(
                envelope.source_id, envelope.replay_scope, envelope.payload.source_event_id
            )
        )
    async with database.transaction() as session:
        rows = await session.scalars(
            select(OutboxEvent).where(OutboxEvent.aggregate_id.in_(signal_ids))
        )
        normalized = [decode_event(json.dumps(row.envelope).encode()) for row in rows]
    assert len(normalized) == len(raw)
    assert set(await _process(database, normalized)) == {ProcessOutcome.APPLIED}

    triggers = [
        t
        for t in await _triggers(database, "live", service_id=None)
        if start <= t.window_start < start + duration
    ]
    fired = {(t.service_id, t.rule_id) for t in triggers}
    names = {service_id: name for name, service_id in SERVICE_IDS.items()}
    assert {names[service] for service, _ in fired} == {"payment", "checkout", "bank-gateway"}
    assert {rule for service, rule in fired if names[service] == "bank-gateway"} == {
        "trace_latency_p95"
    }
    assert {rule for service, rule in fired if names[service] == "payment"} == {
        "trace_error_rate",
        "trace_latency_p95",
        "log_error_rate",
    }
    window = timedelta(seconds=POLICY.window_seconds)
    for trigger in triggers:
        assert labels.fault_start - window <= trigger.window_start < labels.fault_end
    # The fault lasts four minutes, inside one cooldown: each rule fires once per service.
    assert len(triggers) == len(fired)
