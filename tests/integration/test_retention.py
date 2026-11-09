"""Retention cleanup against Compose PostgreSQL (P11-T05).

Old evidence is deleted only when nothing cites it; receipts stay for as long as
a duplicate can still arrive, so a redelivered raw event after cleanup is still
a no-op; a dry run changes nothing.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP
from signalops.agents.investigations import end_investigation
from signalops.config import Settings
from signalops.contracts.common import replay_scope_for
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus, RootCause
from signalops.contracts.evidence import NormalizedSignal, QualityStatus, Severity, SignalKind
from signalops.contracts.workflow import IncidentSeverity, InvestigationStatus
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP
from signalops.detection.rules import Window, WindowClosure
from signalops.persistence import database as db
from signalops.persistence.models import (
    ConsumerReceipt,
    IncidentSignal,
    Investigation,
    OutboxEvent,
    ProgressEvent,
    Signal,
    SignalAggregate,
)
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.jobs import JobRepository
from signalops.persistence.repositories.progress import ProgressRepository
from signalops.persistence.repositories.reliability import ReceiptRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.retention import Retention, RetentionPolicy, RetentionPolicyError
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=10)
SETTINGS = Settings(database_url="postgresql+psycopg://unused@localhost/unused")  # type: ignore[arg-type]
CHECKOUT = next(s.id for s in DEMO_SERVICES if s.name == "checkout")
LOGS = DEMO_SOURCES[0].id


def _signal(scope: EvidenceScope, ingested_at: datetime, text: str) -> NormalizedSignal:
    return NormalizedSignal(
        id=uuid.uuid4(),
        workspace_id=scope.workspace_id,
        source_id=LOGS,
        service_id=CHECKOUT,
        source_event_id=f"retention-{uuid.uuid4()}",
        replay_scope=scope.replay_scope,
        kind=SignalKind.LOG,
        occurred_at=ingested_at - timedelta(seconds=1),
        ingested_at=ingested_at,
        severity=Severity.ERROR,
        message=text,
        trace_id=None,
        attributes={},
        quality_status=QualityStatus.ACCEPTED,
        quality_flags=(),
    )


async def _world(database: db.Database, scope: EvidenceScope) -> dict[str, uuid.UUID]:
    """Old uncited, old cited by an incident or a diagnosis, recent, and replayed signals.

    `replayed` has an old simulated `ingested_at` but was stored just now, as a
    replay's signals are; only `stored_at` decides its age.
    """
    names = ("old_uncited", "old_incident", "old_diagnosis", "old_contradiction", "recent")
    signals = {
        name: _signal(scope, NOW - timedelta(hours=1) if name == "recent" else OLD, name)
        for name in (*names, "replayed")
    }
    async with database.transaction() as session:
        evidence = EvidenceRepository(session)
        for item in signals.values():
            assert await evidence.insert_signal(item)
        old = [item.id for name, item in signals.items() if name.startswith("old_")]
        await session.execute(update(Signal).where(Signal.id.in_(old)).values(stored_at=OLD))
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=scope,
                fingerprint="env:retention",
                title="Retention check",
                severity=IncidentSeverity.LOW,
                window=Window(OLD - timedelta(minutes=1), OLD),
                detected_at=OLD,
            )
        )
        session.add(
            IncidentSignal(
                incident_id=incident_id,
                signal_id=signals["old_incident"].id,
                workspace_id=scope.workspace_id,
                replay_scope=scope.replay_scope,
                relevance=1.0,
            )
        )
        investigation = await InvestigationRepository(session).create(
            NewInvestigation(
                scope=scope,
                incident_id=incident_id,
                request_key=f"test:{uuid.uuid4()}",
                workflow_version="v3",
                budget_microusd=1_000_000,
                max_steps=10,
                created_at=OLD,
                deadline_at=OLD + timedelta(minutes=10),
            )
        )
        assert investigation is not None
        await JobRepository(session).start_investigation(investigation.id)
    diagnosis = Diagnosis.model_validate(
        {
            "status": DiagnosisStatus.DIAGNOSED,
            "summary": "checkout errors",
            "root_cause": RootCause(
                service_id=CHECKOUT,
                service_name="checkout",
                failure_type="elevated_errors",
                confidence=0.5,
                evidence_signal_ids=(signals["old_diagnosis"].id,),
            ),
            "contradictions": [
                {
                    "description": "disagree",
                    "evidence_signal_ids": [str(signals["old_contradiction"].id)],
                }
            ],
            "workflow_version": "v3",
        }
    )
    async with database.transaction() as session:
        await end_investigation(session, investigation.id, InvestigationStatus.COMPLETED, diagnosis)
    return {name: item.id for name, item in signals.items()} | {"investigation": investigation.id}


async def _present(database: db.Database, ids: list[uuid.UUID]) -> set[uuid.UUID]:
    async with database.transaction() as session:
        return set(await session.scalars(select(Signal.id).where(Signal.id.in_(ids))))


async def test_cleanup_keeps_cited_evidence_and_a_dry_run_changes_nothing(
    database: db.Database,
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
    ids = await _world(database, scope)
    signal_ids = [ids[n] for n in ids if n != "investigation"]
    retention = Retention(database, RetentionPolicy.from_settings(SETTINGS), batch_size=2)

    dry = await retention.run(NOW, apply=False, scope=scope.replay_scope)
    signals = next(c for c in dry.categories if c.name == "signals")
    assert (signals.eligible, signals.kept, signals.deleted) == (4, 3, 0)
    assert await _present(database, signal_ids) == set(signal_ids)

    applied = await retention.run(NOW, apply=True, scope=scope.replay_scope)
    signals = next(c for c in applied.categories if c.name == "signals")
    assert signals.deleted == 1
    assert await _present(database, signal_ids) == set(signal_ids) - {ids["old_uncited"]}

    # The diagnosis's citations still resolve within its scope and cutoff.
    async with database.transaction() as session:
        row = await session.get_one(Investigation, ids["investigation"])
        known = await EvidenceRepository(session).known_signals(
            scope, (ids["old_diagnosis"],), known_by=row.evidence_cutoff_at
        )
    assert set(known) == {ids["old_diagnosis"]}


async def test_receipts_outlive_every_path_a_duplicate_can_take(database: db.Database) -> None:
    policy = RetentionPolicy.from_settings(SETTINGS)
    bounds = policy.receipt_bounds()
    # Normalizer: ingestion accepts records up to 30 days old plus 1 day ahead.
    assert bounds[NORMALIZER_CONSUMER_GROUP] >= timedelta(days=32)
    assert bounds[DETECTOR_CONSUMER_GROUP] == timedelta(days=8)

    ages = {"n_20d": (NORMALIZER_CONSUMER_GROUP, 20), "n_40d": (NORMALIZER_CONSUMER_GROUP, 40)}
    ages |= {"d_5d": (DETECTOR_CONSUMER_GROUP, 5), "d_10d": (DETECTOR_CONSUMER_GROUP, 10)}
    events = {name: uuid.uuid4() for name in ages}
    async with database.transaction() as session:
        for name, (group, days) in ages.items():
            assert await ReceiptRepository(session).record(group, events[name])
            await session.execute(
                update(ConsumerReceipt)
                .where(ConsumerReceipt.event_id == events[name])
                .values(processed_at=NOW - timedelta(days=days))
            )
    await Retention(database, policy).run(NOW, apply=True, scope="replay:none")
    async with database.transaction() as session:
        left = set(
            await session.scalars(
                select(ConsumerReceipt.event_id).where(
                    ConsumerReceipt.event_id.in_(list(events.values()))
                )
            )
        )
        # A redelivery of a kept event is still a receipt hit.
        assert not await ReceiptRepository(session).record(
            NORMALIZER_CONSUMER_GROUP, events["n_20d"]
        )
    assert left == {events["n_20d"], events["d_5d"]}


async def test_outbox_and_progress_cleanup_leave_pending_and_recent_rows(
    database: db.Database,
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
    ids = await _world(database, scope)
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == ids["investigation"])
            .values(completed_at=NOW - timedelta(days=40))
        )
        progress_before = await session.scalar(
            select(func.count()).where(ProgressEvent.investigation_id == ids["investigation"])
        )
        outbox = await session.scalars(
            select(OutboxEvent.id).where(OutboxEvent.aggregate_id == ids["investigation"])
        )
        rows = list(outbox)
        # One published long ago; any other stays unpublished.
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == rows[0])
            .values(created_at=NOW - timedelta(days=20), published_at=NOW - timedelta(days=20))
        )
    assert progress_before
    report = await Retention(database, RetentionPolicy.from_settings(SETTINGS)).run(
        NOW, apply=True, scope=scope.replay_scope
    )
    by_name = {c.name: c for c in report.categories}
    assert by_name["progress"].deleted >= progress_before
    async with database.transaction() as session:
        assert not await ProgressRepository(session).after(ids["investigation"], 0, limit=10)
        remaining = set(
            await session.scalars(select(OutboxEvent.id).where(OutboxEvent.id.in_(rows)))
        )
        # The investigation itself and its diagnosis stay.
        assert (await session.get_one(Investigation, ids["investigation"])).diagnosis
    assert rows[0] not in remaining
    assert remaining == set(rows[1:])


async def test_aggregates_go_only_when_their_window_and_closure_are_both_old(
    database: db.Database,
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
    old_window = NOW - timedelta(days=40)
    # A replay flushed today closes windows whose event time is weeks old.
    closures = {"closed_long_ago": NOW - timedelta(days=35), "closed_today": NOW, "open": None}
    ids: dict[str, uuid.UUID] = {}
    async with database.transaction() as session:
        for offset, (name, closed_at) in enumerate(closures.items()):
            start = old_window + timedelta(minutes=offset)
            row = SignalAggregate(
                workspace_id=scope.workspace_id,
                service_id=CHECKOUT,
                replay_scope=scope.replay_scope,
                metric_name="errors",
                window_start=start,
                window_end=start + timedelta(minutes=1),
                closed_at=closed_at,
                closed_by=None if closed_at is None else WindowClosure.WALL_CLOCK.value,
            )
            session.add(row)
            await session.flush()
            ids[name] = row.id
    report = await Retention(database, RetentionPolicy.from_settings(SETTINGS)).run(
        NOW, apply=True, scope=scope.replay_scope
    )
    aggregates = next(c for c in report.categories if c.name == "aggregates")
    assert (aggregates.eligible, aggregates.deleted) == (1, 1)
    async with database.transaction() as session:
        left = set(
            await session.scalars(
                select(SignalAggregate.id).where(SignalAggregate.replay_scope == scope.replay_scope)
            )
        )
    assert left == {ids["closed_today"], ids["open"]}


def test_bounds_that_would_drop_needed_history_are_refused() -> None:
    with pytest.raises(RetentionPolicyError, match="reopen"):
        RetentionPolicy.from_settings(SETTINGS.model_copy(update={"retention_aggregates_days": 30}))
    with pytest.raises(RetentionPolicyError, match="deadline"):
        RetentionPolicy.from_settings(
            SETTINGS.model_copy(update={"investigation_deadline_seconds": 86400 * 7})
        )
