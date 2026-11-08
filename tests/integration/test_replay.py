"""Isolated replay orchestration (P10-T02), with the pipeline run in process.

`LocalTransport` runs the real normalizer, detector, and graph projector against
the scratch database. Completion is judged only by PostgreSQL markers, so these
tests also show that a slow pipeline is waited for and a lost record is reported.
"""

import asyncio
import uuid
from collections.abc import Sequence
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from signalops.adapters.ingestion import IngestPolicy, IngestSource
from signalops.adapters.quality import TimingPolicy
from signalops.contracts.common import replay_scope_for
from signalops.contracts.evaluation import ReplayStatus
from signalops.demo.generator import DemoDataset, Scenario, generate
from signalops.detection.detector import DetectionPolicy
from signalops.detection.graph import GraphPolicy
from signalops.detection.rules import DEFAULT_RULES
from signalops.evaluation.replay import LocalTransport, ReplayReport, ReplayTiming, replay
from signalops.persistence import database as db
from signalops.persistence.models import Incident, Signal
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_SOURCES, DEMO_WORKSPACE_ID
from signalops.streaming.producer import DeliveryResult, OutgoingMessage

pytestmark = pytest.mark.integration

SOURCES = {
    source.id: IngestSource(source.id, DEMO_WORKSPACE_ID, source.kind, enabled=True)
    for source in DEMO_SOURCES
}
INGEST = IngestPolicy(max_future=timedelta(days=1), max_age=timedelta(days=30), redact_fields=())
DETECTION = DetectionPolicy(window_seconds=60, rules=DEFAULT_RULES, allowed_lateness_seconds=30)
TIMING = ReplayTiming(timeout=timedelta(seconds=60), poll_interval=timedelta(milliseconds=10))


def _transport(database: db.Database, run_id: uuid.UUID, *, batch: int = 10_000) -> LocalTransport:
    return LocalTransport(
        database,
        EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id)),
        timing_policy=TimingPolicy(timedelta(minutes=5), timedelta(minutes=15)),
        detection_policy=DETECTION,
        graph_policy=GraphPolicy(),
        batch=batch,
    )


async def _replay(
    database: db.Database,
    dataset: DemoDataset,
    run_id: uuid.UUID,
    transport: LocalTransport | None = None,
    timing: ReplayTiming = TIMING,
) -> ReplayReport:
    return await replay(
        database,
        dataset.records,
        replay_run_id=run_id,
        workspace_id=DEMO_WORKSPACE_ID,
        sources=SOURCES,
        transport=transport or _transport(database, run_id),
        ingest_policy=INGEST,
        detection_policy=DETECTION,
        timing=timing,
    )


async def _signals(database: db.Database, run_id: uuid.UUID) -> list[Signal]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(Signal).where(Signal.replay_scope == replay_scope_for(run_id))
        )
        return list(rows)


async def test_identical_replays_do_not_contaminate_each_other(database: db.Database) -> None:
    dataset = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=8101)
    first, second = uuid.uuid4(), uuid.uuid4()

    reports = await asyncio.gather(
        _replay(database, dataset, first), _replay(database, dataset, second)
    )

    records = dataset.labels.record_count
    for report in reports:
        assert report.status is ReplayStatus.COMPLETED
        assert report.accepted == records
        assert report.markers.normalized == records
        assert report.markers.signals_stored == records
        assert report.markers.normalized_events == records
        assert report.markers.detected == report.markers.projected == records
        assert report.windows_flushed > 0
        assert report.unreconciled_count == 0
        assert len(report.incident_ids) == 1
    assert reports[0].markers == reports[1].markers
    assert reports[0].windows_flushed == reports[1].windows_flushed
    assert set(reports[0].incident_ids).isdisjoint(reports[1].incident_ids)

    a, b = await _signals(database, first), await _signals(database, second)
    assert {row.source_event_id for row in a} == {row.source_event_id for row in b}
    assert {row.id for row in a}.isdisjoint({row.id for row in b})
    async with database.transaction() as session:
        titles = {
            row.replay_scope: row.title
            for row in await session.scalars(
                select(Incident).where(
                    Incident.id.in_(reports[0].incident_ids + reports[1].incident_ids)
                )
            )
        }
    assert titles[replay_scope_for(first)] == titles[replay_scope_for(second)]

    # Republishing a finished replay adds nothing and completes at once.
    again = await _replay(database, dataset, first)
    assert again.status is ReplayStatus.COMPLETED
    assert again.markers == reports[0].markers
    assert again.windows_flushed == 0
    assert again.incident_ids == reports[0].incident_ids
    assert len(await _signals(database, first)) == records


async def test_ingestion_runs_on_simulated_time(database: db.Database) -> None:
    dataset = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=8102)
    run_id = uuid.uuid4()
    report = await _replay(database, dataset, run_id)
    assert report.status is ReplayStatus.COMPLETED
    lag = [row.ingested_at - row.occurred_at for row in await _signals(database, run_id)]
    assert min(lag) > timedelta(0)
    assert max(lag) <= TIMING.tick + TIMING.ingest_delay
    # Detection time is event time, so evidence that arrived later stays future evidence.
    async with database.transaction() as session:
        incident = await session.get_one(Incident, report.incident_ids[0])
        visible = await session.scalar(
            select(func.count()).where(
                Signal.replay_scope == replay_scope_for(run_id),
                Signal.ingested_at <= incident.detected_at,
            )
        )
    assert dataset.labels.end > incident.detected_at
    assert 0 < (visible or 0) < dataset.labels.record_count


class CountingTransport(LocalTransport):
    pumps = 0

    async def pump(self) -> None:
        self.pumps += 1
        await super().pump()


async def test_completion_waits_for_the_pipeline_to_drain(database: db.Database) -> None:
    dataset = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=8103)
    run_id = uuid.uuid4()
    # The normalizer here takes 100 records per poll, so draining needs many polls.
    transport = CountingTransport(
        database,
        EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id)),
        timing_policy=TimingPolicy(timedelta(minutes=5), timedelta(minutes=15)),
        detection_policy=DETECTION,
        graph_policy=GraphPolicy(),
        batch=100,
    )

    report = await _replay(database, dataset, run_id, transport)

    assert report.status is ReplayStatus.COMPLETED
    assert transport.pumps >= dataset.labels.record_count // 100
    assert report.markers.detected == dataset.labels.record_count
    assert len(report.incident_ids) == 1


class LosingTransport(LocalTransport):
    """Acknowledges every record but never delivers the first one."""

    lost: uuid.UUID | None = None

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        if self.lost is None and messages:
            self.lost = messages[0].event_id
            super().publish(messages[1:])
            return [DeliveryResult(message) for message in messages]
        return super().publish(messages)


async def test_an_undrained_replay_times_out_with_a_reconciliation_report(
    database: db.Database,
) -> None:
    dataset = generate(Scenario.HEALTHY, seed=8104)
    run_id = uuid.uuid4()
    transport = LosingTransport(
        database,
        EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id)),
        timing_policy=TimingPolicy(timedelta(minutes=5), timedelta(minutes=15)),
        detection_policy=DETECTION,
        graph_policy=GraphPolicy(),
    )
    timing = ReplayTiming(timeout=timedelta(seconds=1), poll_interval=timedelta(milliseconds=50))

    report = await _replay(database, dataset, run_id, transport, timing)

    assert report.status is ReplayStatus.TIMED_OUT
    assert report.accepted == dataset.labels.record_count
    assert report.markers.normalized == report.accepted - 1
    assert report.unreconciled_count == 1
    assert report.unreconciled_event_ids == (transport.lost,)
    # Windows are not flushed on a partial replay, so it cannot fake a final result.
    assert report.windows_flushed == 0
