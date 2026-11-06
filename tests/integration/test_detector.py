"""The detector against Compose PostgreSQL (P05-T01, P05-T02).

Most tests store signals directly and feed the detector `signal.normalized`
envelopes for them, each test in its own replay scope so windows never
overlap. Live tests share a scope and use their own stretch of time instead.
The last tests run a generated demo dataset through ingestion, the normalizer,
and the detector, using the normalizer's own outbox events.
"""

import asyncio
import json
import random
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource, ingest_batch
from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP, Normalizer, signal_id_for
from signalops.adapters.quality import TimingPolicy
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
from signalops.detection.closure import WindowCloser, flush_replay
from signalops.detection.detector import (
    DETECTOR_CONSUMER_GROUP,
    DetectionPolicy,
    Detector,
    close_windows,
)
from signalops.detection.rules import DEFAULT_RULES, WindowClosure
from signalops.persistence import database as db
from signalops.persistence.models import (
    DetectionClock,
    DetectionSample,
    DetectionTrigger,
    OutboxEvent,
    SignalAggregate,
)
from signalops.persistence.repositories.detection import DetectionRepository
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID
from signalops.streaming.consumer import ProcessOutcome, TransactionalProcessor
from signalops.streaming.producer import DeliveryResult, OutgoingMessage

pytestmark = pytest.mark.integration

SOURCE_BY_KIND = {source.kind: source for source in DEMO_SOURCES}
SERVICE_IDS = {service.name: service.id for service in DEMO_SERVICES}
PAYMENT = SERVICE_IDS["payment"]
POLICY = DetectionPolicy(window_seconds=60, rules=DEFAULT_RULES, allowed_lateness_seconds=30)
# A window boundary well in the past, so live timing never matters.
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
WINDOW = timedelta(seconds=60)
DEADLINE = WINDOW + timedelta(seconds=30)
LIVE = "live"


def _detector() -> Detector:
    return Detector(POLICY)


def _processor(database: db.Database) -> TransactionalProcessor:
    return TransactionalProcessor(database, DETECTOR_CONSUMER_GROUP, _detector())


async def _store(
    database: db.Database,
    run_id: uuid.UUID | None,
    occurred_at: datetime,
    *,
    kind: SignalKind = SignalKind.TRACE,
    service_id: uuid.UUID | None = PAYMENT,
    error: bool = False,
    duration_ms: int = 80,
    status: QualityStatus = QualityStatus.ACCEPTED,
    flags: tuple[QualityFlag, ...] = (),
    ingested_at: datetime | None = None,
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
        ingested_at=ingested_at or occurred_at + timedelta(seconds=1),
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


async def _flush(database: db.Database, run_id: uuid.UUID) -> int:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    return await flush_replay(database, scope, POLICY)


async def _close_live(database: db.Database, now: datetime) -> None:
    closer = WindowCloser(database, POLICY, clock=lambda: now)
    while await closer.close_once():
        pass


async def _aggregates(
    database: db.Database, run_id: uuid.UUID | None, service_id: uuid.UUID = PAYMENT
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
    await _flush(database, run_id)

    [aggregate] = await _aggregates(database, run_id)
    assert aggregate.metric_name == "trace.60s"
    assert (aggregate.sample_count, aggregate.error_count) == (20, 4)
    assert aggregate.window_start == T0
    assert aggregate.window_end == T0 + timedelta(seconds=60)
    assert aggregate.value_sum == pytest.approx(1600)
    assert aggregate.histogram == {"100": 20}
    assert await _sample_totals(database, run_id) == (20, 4)
    # trace_error_rate fired once, on the closed window; the duplicates added nothing.
    [trigger] = await _triggers(database, replay_scope_for(run_id))
    assert trigger.rule_id == "trace_error_rate"
    assert (trigger.sample_count, trigger.error_count) == (20, 4)


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
    await _flush(database, run_id)

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

    await failing_window(T0, 25)  # fires once, when the window closes
    await failing_window(T0 + timedelta(seconds=60), 20)  # within the 300 s cooldown
    await failing_window(T0 + timedelta(seconds=300), 20)  # still within (inclusive)
    await failing_window(T0 + timedelta(seconds=360), 20)  # past the cooldown
    await _flush(database, run_id)

    triggers = await _triggers(database, replay_scope_for(run_id))
    assert [(t.rule_id, t.window_start) for t in triggers] == [
        ("trace_error_rate", T0),
        ("trace_error_rate", T0 + timedelta(seconds=360)),
    ]
    first = triggers[0]
    assert (first.sample_count, first.error_count) == (25, 13)
    assert first.observed == pytest.approx(13 / 25)
    assert first.threshold == pytest.approx(0.2)
    assert first.rule_version == 2
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
    await _flush(database, run_id)

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
    await _flush(database, run_id)

    [aggregate] = await _aggregates(database, run_id)
    assert (aggregate.sample_count, aggregate.error_count) == (30, 10)
    assert aggregate.histogram == {"50": 15, "500": 15}
    assert await _sample_totals(database, run_id) == (30, 10)
    rules = [t.rule_id for t in await _triggers(database, replay_scope_for(run_id))]
    assert sorted(rules) == ["trace_error_rate"]


async def _window_of(
    database: db.Database,
    run_id: uuid.UUID | None,
    start: datetime,
    errors: int,
    count: int = 20,
    service_id: uuid.UUID = PAYMENT,
) -> list[EventEnvelope[EventPayload]]:
    """Signals spread over a window, the first `errors` of them failing."""
    return [
        await _store(
            database,
            run_id,
            start + timedelta(seconds=i * 60 / count),
            error=i < errors,
            service_id=service_id,
        )
        for i in range(count)
    ]


async def test_rules_wait_for_the_window_to_close(database: db.Database, run_id: uuid.UUID) -> None:
    # Five errors open the window: 5 of the first 15 would fire a rule evaluated
    # per sample, but the finished window is 5 of 40, under the 0.2 threshold.
    calm = await _window_of(database, run_id, T0, errors=5, count=40)
    failing = await _window_of(database, run_id, T0 + WINDOW, errors=10)
    await _process(database, [*calm, *failing])
    assert await _triggers(database, replay_scope_for(run_id)) == []

    # The clock has passed the first window's deadline; only the flush can close the last.
    assert await _flush(database, run_id) == 1

    [trigger] = await _triggers(database, replay_scope_for(run_id))
    assert (trigger.window_start, trigger.sample_count) == (T0 + WINDOW, 20)
    aggregates = await _aggregates(database, run_id)
    assert [a.closed_by for a in aggregates] == [
        WindowClosure.SIMULATED_CLOCK,
        WindowClosure.FINAL_FLUSH,
    ]
    assert all(a.closed_at is not None for a in aggregates)
    assert await _flush(database, run_id) == 0


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_shuffled_arrivals_within_the_bound_match_the_ordered_result(
    database: db.Database, seed: int
) -> None:
    async def run(shuffle: bool) -> list[tuple[str, datetime, int, int]]:
        run_id = uuid.uuid4()
        timed: list[tuple[datetime, EventEnvelope[EventPayload]]] = []
        for start, errors, count in (
            (T0, 5, 40),
            (T0 + WINDOW, 12, 30),
            (T0 + 2 * WINDOW, 1, 30),
            (T0 + 8 * WINDOW, 9, 30),
        ):
            envelopes = await _window_of(database, run_id, start, errors=errors, count=count)
            timed += [(e.occurred_at, e) for e in envelopes]
        rng = random.Random(seed)  # noqa: S311 - test data
        # An arrival delayed by up to the bound can be overtaken but never passes the deadline.
        delayed = (
            [(at + timedelta(seconds=rng.uniform(0, 30)), e) for at, e in timed]
            if shuffle
            else timed
        )
        delayed.sort(key=lambda pair: pair[0])
        await _process(database, [e for _, e in delayed])
        await _flush(database, run_id)
        assert all(a.late_count == 0 for a in await _aggregates(database, run_id))
        return [
            (t.rule_id, t.window_start, t.sample_count, t.error_count)
            for t in await _triggers(database, replay_scope_for(run_id))
        ]

    ordered = await run(shuffle=False)
    assert ordered == [
        ("trace_error_rate", T0 + WINDOW, 30, 12),
        ("trace_error_rate", T0 + 8 * WINDOW, 30, 9),
    ]
    assert await run(shuffle=True) == ordered


async def test_simulated_clock_closes_windows_at_the_deadline(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _window_of(database, run_id, T0, errors=10))
    at_deadline = await _store(database, run_id, T0 + DEADLINE, duration_ms=20)
    still_on_time = await _store(database, run_id, T0 + timedelta(seconds=59))
    await _process(database, [at_deadline, still_on_time])

    first, _ = await _aggregates(database, run_id)
    assert (first.closed_at, first.sample_count) == (None, 21)
    assert await _triggers(database, replay_scope_for(run_id)) == []

    past_deadline = await _store(
        database, run_id, T0 + DEADLINE + timedelta(milliseconds=1), duration_ms=20
    )
    await _process(database, [past_deadline])

    first, second = await _aggregates(database, run_id)
    assert first.closed_by == WindowClosure.SIMULATED_CLOCK
    assert second.closed_at is None
    [trigger] = await _triggers(database, replay_scope_for(run_id))
    assert trigger.sample_count == 21


async def test_older_arrivals_are_flagged_and_never_fire(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _window_of(database, run_id, T0, errors=0))
    await _process(database, [await _store(database, run_id, T0 + 3 * WINDOW)])
    [closed, _] = await _aggregates(database, run_id)
    assert closed.closed_by == WindowClosure.SIMULATED_CLOCK

    # A burst of failures for the closed window, and for a window that never had a row.
    old = [await _store(database, run_id, T0 + timedelta(seconds=i), error=True) for i in range(30)]
    untouched = [
        await _store(database, run_id, T0 + WINDOW + timedelta(seconds=i), error=True)
        for i in range(30)
    ]
    assert set(await _process(database, [*old, *untouched])) == {ProcessOutcome.APPLIED}
    # Redelivered under new event IDs: flagged once, counted once.
    await _process(database, [_redelivered(e) for e in old])
    await _flush(database, run_id)

    aggregates = await _aggregates(database, run_id)
    assert [(a.sample_count, a.error_count, a.late_count) for a in aggregates] == [
        (20, 0, 30),
        (0, 0, 30),
        (1, 0, 0),
    ]
    assert all(a.closed_at is not None for a in aggregates)
    assert await _triggers(database, replay_scope_for(run_id)) == []
    async with database.transaction() as session:
        late = await session.scalar(
            select(func.count())
            .select_from(DetectionSample)
            .where(
                DetectionSample.replay_scope == replay_scope_for(run_id),
                DetectionSample.late,
            )
        )
    assert late == 60


async def test_each_service_keeps_its_own_simulated_clock(
    database: db.Database, run_id: uuid.UUID
) -> None:
    checkout = SERVICE_IDS["checkout"]
    payment_window = await _window_of(database, run_id, T0, errors=10)
    # Another partition is further ahead; that closes nothing of this service.
    ahead = await _store(database, run_id, T0 + 10 * WINDOW, service_id=checkout)
    delayed = await _store(database, run_id, T0 + timedelta(seconds=59), error=True)
    await _process(database, [ahead, *payment_window, delayed])

    [aggregate] = await _aggregates(database, run_id)
    assert (aggregate.closed_at, aggregate.sample_count, aggregate.late_count) == (None, 21, 0)
    async with database.transaction() as session:
        clocks = {
            clock.service_id: clock.clock_at
            for clock in await session.scalars(
                select(DetectionClock).where(
                    DetectionClock.replay_scope == replay_scope_for(run_id)
                )
            )
        }
    assert clocks == {checkout: T0 + 10 * WINDOW, PAYMENT: T0 + timedelta(seconds=59)}


async def test_simulated_clock_survives_a_detector_restart(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, [await _store(database, run_id, T0 + 5 * WINDOW)])

    # `_process` builds a new processor and detector: the clock came from PostgreSQL.
    await _process(database, [await _store(database, run_id, T0, error=True)])

    aggregates = await _aggregates(database, run_id)
    assert [(a.sample_count, a.late_count) for a in aggregates] == [(0, 1), (1, 0)]


async def test_live_windows_close_by_wall_clock_after_the_bound(database: db.Database) -> None:
    start = T0 + timedelta(days=1)
    envelopes = [
        await _store(
            database,
            None,
            start + timedelta(seconds=i),
            error=i < 10,
            ingested_at=start + timedelta(seconds=i + 1),
        )
        for i in range(20)
    ]
    # Inside the bound, out of order and not yet closed.
    last = await _store(
        database, None, start + timedelta(seconds=30), ingested_at=start + timedelta(seconds=90)
    )
    await _process(database, [last, *reversed(envelopes)])

    await _close_live(database, start + DEADLINE)
    assert await _live_triggers(database, start) == []
    [aggregate] = await _live_aggregates(database, start)
    assert (aggregate.closed_at, aggregate.sample_count) == (None, 21)

    await _close_live(database, start + DEADLINE + timedelta(milliseconds=1))
    [aggregate] = await _live_aggregates(database, start)
    assert aggregate.closed_by == WindowClosure.WALL_CLOCK
    [trigger] = await _live_triggers(database, start)
    assert (trigger.sample_count, trigger.error_count) == (21, 10)


async def test_live_sample_delayed_by_lag_reopens_evaluation_not_the_window(
    database: db.Database,
) -> None:
    start = T0 + timedelta(days=2)
    arrived = [
        await _store(database, None, start + timedelta(seconds=i), error=True, ingested_at=start)
        for i in range(10)
    ]
    # Ingested in time, but the detector was behind when the closer ran.
    backlog = [
        await _store(database, None, start + timedelta(seconds=i), error=True, ingested_at=start)
        for i in range(10, 20)
    ]
    await _process(database, arrived)
    await _close_live(database, start + timedelta(hours=1))
    assert await _live_triggers(database, start) == []

    await _process(database, backlog)

    [aggregate] = await _live_aggregates(database, start)
    assert (aggregate.sample_count, aggregate.late_count) == (20, 0)
    fired = [t.rule_id for t in await _live_triggers(database, start)]
    assert fired == ["trace_error_rate"]
    # Redelivery after closure changes nothing.
    await _process(database, [_redelivered(e) for e in backlog])
    assert len(await _live_triggers(database, start)) == 1
    assert (await _live_aggregates(database, start))[0].sample_count == 20


async def test_live_sample_ingested_past_the_deadline_is_late(database: db.Database) -> None:
    start = T0 + timedelta(days=3)
    envelopes = [
        await _store(
            database,
            None,
            start + timedelta(seconds=i),
            error=True,
            ingested_at=start + DEADLINE + timedelta(milliseconds=1),
        )
        for i in range(20)
    ]
    await _process(database, envelopes)
    await _close_live(database, start + timedelta(hours=1))

    [aggregate] = await _live_aggregates(database, start)
    assert (aggregate.sample_count, aggregate.late_count) == (0, 20)
    assert aggregate.closed_by == WindowClosure.WALL_CLOCK
    assert await _live_triggers(database, start) == []


async def test_closer_and_flush_stay_in_their_own_scope(
    database: db.Database, run_id: uuid.UUID
) -> None:
    start = T0 + timedelta(days=4)
    await _process(database, [await _store(database, run_id, start)])
    await _process(database, [await _store(database, None, start)])

    await _close_live(database, start + timedelta(hours=1))
    [replay] = await _aggregates(database, run_id)
    assert replay.closed_at is None

    start = T0 + timedelta(days=5)
    await _process(database, [await _store(database, None, start)])
    await _flush(database, run_id)
    [live] = await _live_aggregates(database, start)
    assert live.closed_at is None
    await _close_live(database, start + timedelta(hours=1))


async def test_interrupted_closure_leaves_the_window_open(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _window_of(database, run_id, T0, errors=10))
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))

    class Crash(Exception):
        pass

    async def close_then_crash() -> None:
        async with database.transaction() as session:
            windows = await DetectionRepository(session).open_scope_windows(scope, 10)
            await close_windows(session, POLICY, windows, WindowClosure.FINAL_FLUSH)
            raise Crash

    with pytest.raises(Crash):
        await close_then_crash()

    [aggregate] = await _aggregates(database, run_id)
    assert aggregate.closed_at is None
    assert await _triggers(database, scope.replay_scope) == []

    assert await _flush(database, run_id) == 1
    assert len(await _triggers(database, scope.replay_scope)) == 1


async def test_concurrent_closers_close_a_window_once(database: db.Database) -> None:
    start = T0 + timedelta(days=6)
    envelopes = [
        await _store(database, None, start + timedelta(seconds=i), error=True, ingested_at=start)
        for i in range(20)
    ]
    await _process(database, envelopes)
    now = start + timedelta(hours=1)

    counts = await asyncio.gather(
        *(WindowCloser(database, POLICY, clock=lambda: now).close_once() for _ in range(4))
    )
    await _close_live(database, now)

    assert sum(counts) >= 1
    [aggregate] = await _live_aggregates(database, start)
    assert aggregate.closed_by == WindowClosure.WALL_CLOCK
    assert len(await _live_triggers(database, start)) == 1


async def test_sample_racing_closure_is_always_evaluated(database: db.Database) -> None:
    """Whichever transaction takes the window row first, the final counts get evaluated."""
    start = T0 + timedelta(days=7)
    first = [
        await _store(database, None, start + timedelta(seconds=i), error=True, ingested_at=start)
        for i in range(19)
    ]
    racing = await _store(database, None, start, error=True, ingested_at=start)
    await _process(database, first)
    now = start + timedelta(hours=1)

    await asyncio.gather(
        WindowCloser(database, POLICY, clock=lambda: now).close_once(),
        _processor(database).process(racing),
    )
    await _close_live(database, now)

    [aggregate] = await _live_aggregates(database, start)
    assert aggregate.sample_count == 20
    assert aggregate.closed_at is not None
    assert [t.rule_id for t in await _live_triggers(database, start)] == [
        "trace_error_rate",
    ]


async def _live_aggregates(database: db.Database, start: datetime) -> list[SignalAggregate]:
    return [a for a in await _aggregates(database, None) if a.window_start == start]


async def _live_triggers(database: db.Database, start: datetime) -> list[DetectionTrigger]:
    return [t for t in await _triggers(database, LIVE) if t.window_start == start]


class CapturingPublisher:
    def __init__(self) -> None:
        self.messages: list[OutgoingMessage] = []

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        self.messages.extend(messages)
        return [DeliveryResult(message, partition=0, offset=0) for message in messages]

    def close(self, timeout_seconds: float = 10.0) -> int:
        return 0


async def _normalized_demo(
    database: db.Database,
    start: datetime,
    duration: timedelta,
    *,
    seed: int,
    run_id: uuid.UUID | None,
    scenario: Scenario = Scenario.DOWNSTREAM_TIMEOUT,
) -> list[EventEnvelope[EventPayload]]:
    """A generated dataset through ingestion and the normalizer, in occurred-at order."""
    dataset = generate(scenario, seed=seed, start=start, duration=duration)
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
    # The ingestion API has no replay parameter yet (P10-T02). Raw event IDs derive
    # from the dataset, so each copy needs its own to pass the normalizer's receipt.
    raw = [e.model_copy(update={"event_id": uuid.uuid4(), "replay_run_id": run_id}) for e in raw]

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
    return sorted(normalized, key=lambda envelope: envelope.occurred_at)


async def test_demo_fault_triggers_only_affected_services(database: db.Database) -> None:
    start = (datetime.now(UTC) - timedelta(minutes=11)).replace(second=0, microsecond=0)
    duration = timedelta(minutes=10)
    labels = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=5101, start=start, duration=duration).labels
    assert labels.fault_start is not None
    assert labels.fault_end is not None
    ordered_run, shuffled_run = uuid.uuid4(), uuid.uuid4()

    normalized = await _normalized_demo(database, start, duration, seed=5101, run_id=ordered_run)
    assert set(await _process(database, normalized)) == {ProcessOutcome.APPLIED}
    await _flush(database, ordered_run)

    triggers = await _triggers(database, replay_scope_for(ordered_run), service_id=None)
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
    for trigger in triggers:
        assert labels.fault_start - WINDOW <= trigger.window_start < labels.fault_end
    # The fault lasts four minutes, inside one cooldown: each rule fires once per service.
    assert len(triggers) == len(fired)

    # The same dataset with arrivals shuffled inside the bound gives the same triggers.
    rng = random.Random(5101)  # noqa: S311 - test data
    normalized = await _normalized_demo(database, start, duration, seed=5101, run_id=shuffled_run)
    shuffled = sorted(
        normalized,
        key=lambda envelope: envelope.occurred_at + timedelta(seconds=rng.uniform(0, 30)),
    )
    assert shuffled != normalized
    await _process(database, shuffled)
    await _flush(database, shuffled_run)

    def keys(rows: Sequence[DetectionTrigger]) -> set[tuple[uuid.UUID, str, datetime, int]]:
        return {(t.service_id, t.rule_id, t.window_start, t.sample_count) for t in rows}

    assert keys(await _triggers(database, replay_scope_for(shuffled_run), None)) == keys(triggers)


async def test_demo_backfill_into_live_is_evidence_not_an_incident(database: db.Database) -> None:
    # One whole past fault ingested in a moment, as `demo-post` does without --realtime.
    start = T0 + timedelta(days=10)
    duration = timedelta(minutes=10)
    normalized = await _normalized_demo(database, start, duration, seed=5102, run_id=None)
    await _process(database, normalized)
    await _close_live(database, start + duration + timedelta(hours=1))

    triggers = [
        t
        for t in await _triggers(database, LIVE, service_id=None)
        if start <= t.window_start < start + duration
    ]
    # Only windows still inside the bound when ingestion happened at `start + duration`.
    recent = start + duration - DEADLINE
    assert all(t.window_start >= recent for t in triggers)
    async with database.transaction() as session:
        late = await session.scalar(
            select(func.sum(SignalAggregate.late_count)).where(
                SignalAggregate.replay_scope == LIVE,
                SignalAggregate.window_start >= start,
                SignalAggregate.window_start < start + duration,
            )
        )
    assert late is not None
    assert late > 0.8 * len(normalized)
