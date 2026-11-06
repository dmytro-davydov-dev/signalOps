"""Incident correlation, recovery, and atomicity against Compose PostgreSQL (P05-T03).

Windows and helpers come from the detector tests. Each test has its own replay
scope, so incidents never correlate across tests. Unless a test says otherwise,
windows closed by `_flush` are evaluated oldest first.
"""

import asyncio
import json
import random
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from signalops.contracts.common import replay_scope_for
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    IncidentChange,
    IncidentChangedPayload,
    decode_event,
)
from signalops.contracts.incidents import MAX_INCIDENT_EVIDENCE
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus
from signalops.demo.generator import Scenario, generate
from signalops.detection.closure import flush_replay
from signalops.detection.detector import (
    DETECTOR_CONSUMER_GROUP,
    DetectionPolicy,
    Detector,
    close_windows,
)
from signalops.detection.incidents import EVIDENCE_PER_TRIGGER
from signalops.detection.rules import DEFAULT_RULES, WindowClosure
from signalops.persistence import database as db
from signalops.persistence.models import (
    Incident,
    IncidentService,
    IncidentSignal,
    OutboxEvent,
)
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.detection import DetectionRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.streaming.consumer import TransactionalProcessor
from tests.integration.test_detector import (
    PAYMENT,
    POLICY,
    SERVICE_IDS,
    T0,
    WINDOW,
    _close_live,
    _flush,
    _normalized_demo,
    _process,
    _redelivered,
    _store,
    _triggers,
    _window_of,
)

pytestmark = pytest.mark.integration

CHECKOUT = SERVICE_IDS["checkout"]
GATEWAY = SERVICE_IDS["bank-gateway"]
GAP = timedelta(seconds=POLICY.incidents.correlation_gap_seconds)
RECOVERY = timedelta(seconds=POLICY.incidents.recovery_seconds)
LIVE = "live"


async def _incidents(database: db.Database, run_id: uuid.UUID | None) -> list[Incident]:
    async with database.transaction() as session:
        result = await session.scalars(
            select(Incident)
            .where(Incident.replay_scope == replay_scope_for(run_id))
            .order_by(Incident.first_seen_at)
        )
        return list(result)


async def _services(database: db.Database, incident: Incident) -> dict[uuid.UUID, str]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(IncidentService).where(IncidentService.incident_id == incident.id)
        )
        return {row.service_id: row.role for row in rows}


async def _evidence(database: db.Database, incident: Incident) -> set[uuid.UUID]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(IncidentSignal.signal_id).where(IncidentSignal.incident_id == incident.id)
        )
        return set(rows)


async def _events(database: db.Database, incident: Incident) -> list[IncidentChangedPayload]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.aggregate_id == incident.id)
            .order_by(OutboxEvent.aggregate_version)
        )
        payloads = [decode_event(json.dumps(row.envelope).encode()).payload for row in rows]
    assert all(isinstance(payload, IncidentChangedPayload) for payload in payloads)
    return payloads  # type: ignore[return-value]


async def _breach(
    database: db.Database,
    run_id: uuid.UUID | None,
    start: datetime,
    service_id: uuid.UUID = PAYMENT,
    *,
    errors: int = 10,
    count: int = 20,
) -> list[EventEnvelope[EventPayload]]:
    return await _window_of(
        database, run_id, start, errors=errors, count=count, service_id=service_id
    )


async def _healthy(
    database: db.Database,
    run_id: uuid.UUID | None,
    start: datetime,
    service_id: uuid.UUID = PAYMENT,
    count: int = 20,
) -> list[EventEnvelope[EventPayload]]:
    return await _window_of(database, run_id, start, errors=0, count=count, service_id=service_id)


async def test_repeated_triggers_form_one_logical_incident(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    await _process(database, await _breach(database, run_id, T0, CHECKOUT))
    # Past this rule's cooldown, so it triggers again, and inside the gap.
    later = T0 + 7 * WINDOW
    await _process(database, await _breach(database, run_id, later))
    await _flush(database, run_id)

    triggers = await _triggers(database, replay_scope_for(run_id), service_id=None)
    assert len(triggers) == 3
    [incident] = await _incidents(database, run_id)
    assert {t.incident_id for t in triggers} == {incident.id}
    assert incident.correlation_window_start == T0
    assert (incident.first_seen_at, incident.last_seen_at) == (T0, later + WINDOW)
    assert incident.status == IncidentStatus.OPEN
    assert incident.severity == IncidentSeverity.HIGH
    assert incident.detected_at == T0 + WINDOW + POLICY.lateness
    assert incident.fingerprint == "env:demo"
    assert await _services(database, incident) == {
        PAYMENT: IncidentServiceRole.PRIMARY,
        CHECKOUT: IncidentServiceRole.PRIMARY,
    }

    events = await _events(database, incident)
    assert [e.incident_version for e in events] == list(range(1, incident.version + 1))
    assert events[0].change is IncidentChange.CREATED
    assert {e.change for e in events[1:]} == {IncidentChange.UPDATED}


async def test_a_breach_past_the_gap_is_a_new_incident(
    database: db.Database, run_id: uuid.UUID
) -> None:
    second = T0 + WINDOW + GAP + WINDOW
    await _process(database, await _breach(database, run_id, T0))
    await _process(database, await _breach(database, run_id, second))
    await _flush(database, run_id)

    first, later = await _incidents(database, run_id)
    assert first.id != later.id
    assert (first.correlation_window_start, later.correlation_window_start) == (T0, second)
    assert first.last_seen_at == T0 + WINDOW
    # Evidence stays with the incident it belongs to.
    assert not await _evidence(database, first) & await _evidence(database, later)


async def test_incidents_stay_in_their_scope_and_environment(
    database: db.Database, run_id: uuid.UUID
) -> None:
    other_run = uuid.uuid4()
    async with database.transaction() as session:
        staging = await CatalogRepository(session).ensure_service(
            service_id=uuid.uuid4(),
            workspace_id=DEMO_WORKSPACE_ID,
            name=f"payment-{run_id.hex[:8]}",
            environment="staging",
        )
    await _process(database, await _breach(database, run_id, T0))
    await _process(database, await _breach(database, run_id, T0, staging.id))
    await _process(database, await _breach(database, other_run, T0))
    await _flush(database, run_id)
    await _flush(database, other_run)

    assert {i.fingerprint for i in await _incidents(database, run_id)} == {
        "env:demo",
        "env:staging",
    }
    assert len(await _incidents(database, other_run)) == 1


async def test_a_breach_under_cooldown_extends_the_incident_without_a_trigger(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    await _process(database, await _breach(database, run_id, T0 + 2 * WINDOW))
    await _flush(database, run_id)

    assert len(await _triggers(database, replay_scope_for(run_id))) == 1
    [incident] = await _incidents(database, run_id)
    assert incident.last_seen_at == T0 + 3 * WINDOW
    # The cooled-down window cites nothing; the trigger's evidence is all there is.
    assert len(await _evidence(database, incident)) == 10


async def test_a_fault_outlasting_the_gap_under_cooldown_stays_one_incident(
    database: db.Database, run_id: uuid.UUID
) -> None:
    # Every window breaches for 12 minutes, longer than the gap; triggers are 6 minutes apart.
    for minute in range(12):
        await _process(database, await _breach(database, run_id, T0 + minute * WINDOW))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.last_seen_at == T0 + 12 * WINDOW


async def test_healthy_windows_of_every_service_resolve_the_incident(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    await _process(database, await _breach(database, run_id, T0, CHECKOUT))
    # Too soon: ends less than the recovery time after the last breach.
    soon = T0 + RECOVERY - WINDOW
    await _process(database, await _healthy(database, run_id, soon))
    await _process(database, await _healthy(database, run_id, soon, CHECKOUT))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.OPEN

    enough = T0 + RECOVERY
    await _process(database, await _healthy(database, run_id, enough))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.OPEN  # checkout has not shown health yet

    await _process(database, await _healthy(database, run_id, enough, CHECKOUT))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.RESOLVED
    assert incident.resolved_at == incident.last_seen_at + RECOVERY
    last = (await _events(database, incident))[-1]
    assert (last.change, last.status) == (IncidentChange.RESOLVED, IncidentStatus.RESOLVED)


async def test_silence_and_thin_windows_do_not_show_recovery(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    # Below every trace rule's min_samples: a few healthy requests prove nothing.
    await _process(database, await _healthy(database, run_id, T0 + 20 * WINDOW, count=3))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.OPEN


async def test_a_new_breach_during_recovery_restarts_it(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    relapse = T0 + 4 * WINDOW
    await _process(database, await _breach(database, run_id, relapse))
    await _process(database, await _healthy(database, run_id, T0 + 6 * WINDOW))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.OPEN
    assert incident.last_seen_at == relapse + WINDOW


async def test_a_breach_after_recovery_within_the_gap_reopens(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
    recovered = T0 + RECOVERY
    await _process(database, await _healthy(database, run_id, recovered))
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert incident.status == IncidentStatus.RESOLVED

    relapse = recovered + 2 * WINDOW
    await _process(database, await _breach(database, run_id, relapse))
    await _flush(database, run_id)
    [reopened] = await _incidents(database, run_id)
    assert reopened.id == incident.id
    assert (reopened.status, reopened.resolved_at) == (IncidentStatus.OPEN, None)
    assert (await _events(database, reopened))[-1].change is IncidentChange.REOPENED


async def test_redelivery_changes_nothing(database: db.Database, run_id: uuid.UUID) -> None:
    envelopes = await _breach(database, run_id, T0)
    await _process(database, envelopes)
    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    version, evidence = incident.version, await _evidence(database, incident)

    await _process(database, [_redelivered(e) for e in envelopes])
    await _process(database, envelopes)
    await _flush(database, run_id)

    [same] = await _incidents(database, run_id)
    assert (same.id, same.version) == (incident.id, version)
    assert await _evidence(database, same) == evidence
    assert len(await _events(database, same)) == version


async def test_a_crash_leaves_no_trigger_incident_or_event(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0))
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
    assert await _incidents(database, run_id) == []
    assert await _triggers(database, scope.replay_scope) == []

    await _flush(database, run_id)
    [incident] = await _incidents(database, run_id)
    assert [e.change for e in await _events(database, incident)] == [IncidentChange.CREATED]


@pytest.mark.parametrize("attempt", range(3))
async def test_concurrent_closures_correlate_into_one_incident(
    database: db.Database, run_id: uuid.UUID, attempt: int
) -> None:
    services = (PAYMENT, CHECKOUT, GATEWAY)
    for service in services:
        await _process(database, await _breach(database, run_id, T0, service))
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    async with database.transaction() as session:
        windows = await DetectionRepository(session).open_scope_windows(scope, 10)

    async def close(window_index: int) -> None:
        async with database.transaction() as session:
            await close_windows(session, POLICY, [windows[window_index]], WindowClosure.FINAL_FLUSH)

    await asyncio.gather(*(close(i) for i in range(len(windows))))

    [incident] = await _incidents(database, run_id)
    assert set(await _services(database, incident)) == set(services)
    events = await _events(database, incident)
    assert [e.incident_version for e in events] == [1, 2, 3]


async def test_evidence_cites_breaching_samples_within_its_bounds(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _process(database, await _breach(database, run_id, T0, errors=30, count=40))
    # Evidence for the latency rule: only samples above its threshold.
    slow = [
        await _store(
            database, run_id, T0 + timedelta(seconds=i), service_id=GATEWAY, duration_ms=2_000
        )
        for i in range(5)
    ]
    fast = [
        await _store(database, run_id, T0 + timedelta(seconds=i), service_id=GATEWAY)
        for i in range(25)
    ]
    await _process(database, slow + fast)
    await _flush(database, run_id)

    [incident] = await _incidents(database, run_id)
    cited = await _evidence(database, incident)
    assert len(cited) == EVIDENCE_PER_TRIGGER + 5
    slow_ids = {e.payload.signal_id for e in slow}  # type: ignore[attr-defined]
    fast_ids = {e.payload.signal_id for e in fast}  # type: ignore[attr-defined]
    assert slow_ids <= cited
    assert not fast_ids & cited


async def test_an_incident_stops_citing_at_the_contract_limit(
    database: db.Database, run_id: uuid.UUID
) -> None:
    # Rule cooldowns off, so every window triggers and cites 20 signals.
    rules = tuple(rule.model_copy(update={"cooldown_seconds": 0}) for rule in DEFAULT_RULES)
    policy = replace(POLICY, rules=rules)
    processor = TransactionalProcessor(database, DETECTOR_CONSUMER_GROUP, Detector(policy))
    windows = MAX_INCIDENT_EVIDENCE // EVIDENCE_PER_TRIGGER + 1
    for minute in range(windows):
        for envelope in await _breach(database, run_id, T0 + minute * WINDOW, errors=25, count=25):
            await processor.process(envelope)
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    await flush_replay(database, scope, policy)

    [incident] = await _incidents(database, run_id)
    assert len(await _evidence(database, incident)) == MAX_INCIDENT_EVIDENCE


async def test_live_closure_creates_the_incident_with_its_detection_time(
    database: db.Database,
) -> None:
    start = T0 + timedelta(days=20)
    await _process(database, await _breach(database, None, start))
    await _close_live(database, start + timedelta(minutes=3))

    [incident] = [
        i for i in await _incidents(database, None) if i.correlation_window_start == start
    ]
    # Wall clock, not the event-time deadline replay records.
    assert incident.detected_at > start + timedelta(days=1)
    assert incident.replay_scope == LIVE


async def _demo_policy_process(
    database: db.Database,
    envelopes: Sequence[EventEnvelope[EventPayload]],
    run_id: uuid.UUID,
    policy: DetectionPolicy,
) -> None:
    processor = TransactionalProcessor(database, DETECTOR_CONSUMER_GROUP, Detector(policy))
    for envelope in envelopes:
        await processor.process(envelope)
    await flush_replay(database, EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id)), policy)


async def test_demo_fault_is_one_incident_that_recovers_with_or_without_shuffling(
    database: db.Database,
) -> None:
    start = T0 + timedelta(days=25)
    duration = timedelta(minutes=15)
    labels = generate(Scenario.DOWNSTREAM_TIMEOUT, seed=5103, start=start, duration=duration).labels
    assert labels.fault_start is not None
    assert labels.fault_end is not None
    # The dataset ends three minutes after the fault, so recovery must fit inside that.
    policy = replace(POLICY, incidents=replace(POLICY.incidents, recovery_seconds=120))

    async def run(shuffle: bool) -> tuple[Incident, dict[uuid.UUID, str]]:
        run_id = uuid.uuid4()
        normalized = await _normalized_demo(database, start, duration, seed=5103, run_id=run_id)
        if shuffle:
            rng = random.Random(5103)  # noqa: S311 - test data
            normalized = sorted(
                normalized,
                key=lambda envelope: envelope.occurred_at + timedelta(seconds=rng.uniform(0, 30)),
            )
        await _demo_policy_process(database, normalized, run_id, policy)
        [incident] = await _incidents(database, run_id)
        return incident, await _services(database, incident)

    ordered, services = await run(shuffle=False)
    assert set(services) == {PAYMENT, CHECKOUT, GATEWAY}
    assert ordered.severity == IncidentSeverity.HIGH
    assert labels.fault_start - WINDOW <= ordered.first_seen_at < labels.fault_end
    assert labels.fault_start < ordered.last_seen_at <= labels.fault_end + WINDOW
    assert ordered.status == IncidentStatus.RESOLVED
    assert ordered.resolved_at is not None
    assert ordered.resolved_at > labels.fault_end
    assert len(await _evidence(database, ordered)) > 0

    shuffled, shuffled_services = await run(shuffle=True)
    assert shuffled_services == services
    for field in (
        "fingerprint",
        "severity",
        "status",
        "first_seen_at",
        "last_seen_at",
        "resolved_at",
        "detected_at",
    ):
        assert getattr(shuffled, field) == getattr(ordered, field), field


async def test_demo_healthy_run_opens_no_incident(database: db.Database) -> None:
    start = T0 + timedelta(days=26)
    run_id = uuid.uuid4()
    normalized = await _normalized_demo(
        database, start, timedelta(minutes=10), seed=5104, run_id=run_id, scenario=Scenario.HEALTHY
    )
    await _process(database, normalized)
    await _flush(database, run_id)
    assert await _incidents(database, run_id) == []
