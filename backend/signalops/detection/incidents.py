"""Breaches and operator changes in, incidents and recovery out, in the caller's transaction.

An incident writes with its trigger and its `incident.changed` outbox event, so
a crash leaves all of them or none, and a replayed event finds the trigger and
changes nothing (ADR-017).

**Keys.** The fingerprint is the service's environment, because one fault fires
rules on several dependent services and the catalog does not know dependencies
yet. A correlation window is a session, not a time bucket: a breach joins the
fingerprint's latest incident when it falls within `correlation_gap_seconds`,
and a fault crossing a bucket boundary stays one incident.
`correlation_window_start` is the start of the window that opened the incident.

**Breaches.** Every breached window reaches `record_breach`, including windows
inside a rule's cooldown: cooldown limits triggers, and a fault still running
under it must keep its incident alive. Only triggers cite evidence.

**Recovery.** A window shows its service healthy when a rule had enough samples
to fire and none breached. An incident resolves once every service has such a
window ending at least `recovery_seconds` after the last breach. All of this is
event time, so replay agrees with live. Silence proves nothing: a service with
no traffic keeps its incident open.

A new live incident requests its investigation in the same transaction, so an
incident without one cannot commit. Replay incidents and reopened incidents
request nothing (ADR-020).

A breach after an incident resolved, within the gap, reopens it. A breach in a
window older than the resolution extends it without reopening.

Roles and severity depend only on which rules fired, so they do not depend on
arrival order; the title describes the first breach processed.

**Operators.** `change_status` resolves or reopens a live incident at the version
the operator read (ADR-019). It takes only the row: `lock_scope` would queue a
request behind detector batches. Evidence still wins: a later breach reopens an
incident an operator resolved, and the next healthy window resolves one an
operator reopened if its services already recovered.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from sqlalchemy import ColumnElement
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.agents.investigations import InvestigationPolicy, request_by_detector
from signalops.contracts.common import LIVE_REPLAY_SCOPE, replay_run_id_of
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    IncidentChange,
    IncidentChangedPayload,
)
from signalops.contracts.incidents import MAX_INCIDENT_EVIDENCE
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus
from signalops.detection.rules import DetectionRule, RuleKind, Window
from signalops.observability.metrics import DETECTION_LATENCY, INCIDENTS_OPENED
from signalops.persistence.database import on_commit
from signalops.persistence.models import DetectionSample, Incident
from signalops.persistence.repositories.detection import DetectionRepository, SeriesKey
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

# Fixed namespace: a change's event ID derives from the incident and its version.
INCIDENT_EVENT_NAMESPACE: Final = uuid.UUID("3e7a9d52-6c14-4b8f-a0d3-5f1c8e2b7a96")

EVIDENCE_PER_TRIGGER: Final = 20
# Selected by the rule that fired, not scored: the column is for later rankers.
BREACHING_RELEVANCE: Final = 1.0

SEVERITY_ORDER: Final = tuple(IncidentSeverity)
RULE_TITLES: Final = {
    RuleKind.ERROR_RATE: "High error rate",
    RuleKind.LATENCY_QUANTILE: "High latency",
}


@dataclass(frozen=True)
class IncidentPolicy:
    correlation_gap_seconds: int = 600
    recovery_seconds: int = 300
    # None requests no investigations.
    investigations: InvestigationPolicy | None = None

    @property
    def correlation_gap(self) -> timedelta:
        return timedelta(seconds=self.correlation_gap_seconds)

    @property
    def recovery(self) -> timedelta:
        return timedelta(seconds=self.recovery_seconds)


@dataclass(frozen=True)
class Breach:
    series: SeriesKey
    window: Window
    rule: DetectionRule
    # None when the rule is cooling down or already fired for this window.
    trigger_id: uuid.UUID | None
    # The window's closure deadline: the event-time moment it could first be known.
    deadline: datetime


class IncidentVersionError(Exception):
    """The operator's version is not the incident's current one (409)."""


def fingerprint_for(environment: str) -> str:
    return f"env:{environment}"


def higher_severity(first: IncidentSeverity, second: IncidentSeverity) -> IncidentSeverity:
    return max(first, second, key=SEVERITY_ORDER.index)


def _breaching(rule: DetectionRule) -> ColumnElement[bool]:
    if rule.kind is RuleKind.ERROR_RATE:
        return DetectionSample.is_error.is_(True)
    return DetectionSample.value > rule.threshold


async def _publish(
    session: AsyncSession,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    version: int,
    change: IncidentChange,
    *,
    status: IncidentStatus,
    severity: IncidentSeverity,
    at: datetime,
) -> None:
    envelope = EventEnvelope[EventPayload](
        event_id=uuid.uuid5(INCIDENT_EVENT_NAMESPACE, f"{incident_id}:{version}"),
        event_type=EventType.INCIDENT_CHANGED,
        schema_version=1,
        workspace_id=scope.workspace_id,
        occurred_at=at,
        ingested_at=datetime.now(UTC),
        replay_run_id=replay_run_id_of(scope.replay_scope),
        payload=IncidentChangedPayload(
            incident_id=incident_id,
            incident_version=version,
            change=change,
            status=status,
            severity=severity,
            replay_scope=scope.replay_scope,
        ),
    )
    await OutboxRepository(session).add(
        envelope, aggregate_id=incident_id, aggregate_version=version
    )


async def _extend(
    session: AsyncSession, scope: EvidenceScope, breach: Breach, incident: Incident, *, grew: bool
) -> None:
    """Apply a breach to an existing incident; `grew` when it gained a service or evidence."""
    window = breach.window
    severity = higher_severity(IncidentSeverity(incident.severity), breach.rule.severity)
    reopened = incident.resolved_at is not None and window.end > incident.resolved_at
    changes: dict[str, Any] = {}
    if window.start < incident.first_seen_at:
        changes["first_seen_at"] = window.start
    if window.end > incident.last_seen_at:
        changes["last_seen_at"] = window.end
    if severity != incident.severity:
        changes["severity"] = severity
    if reopened:
        changes |= {"status": IncidentStatus.OPEN, "resolved_at": None}
    if not changes and not grew:
        return
    await _publish(
        session,
        scope,
        incident.id,
        await IncidentRepository(session).apply(incident, changes),
        IncidentChange.REOPENED if reopened else IncidentChange.UPDATED,
        status=IncidentStatus.OPEN if reopened else IncidentStatus(incident.status),
        severity=severity,
        at=window.end,
    )


async def record_breach(session: AsyncSession, policy: IncidentPolicy, breach: Breach) -> None:
    """Create or extend the breach's incident; the caller holds `lock_scope`."""
    series, window, rule = breach.series, breach.window, breach.rule
    incidents = IncidentRepository(session)
    scope = EvidenceScope(series.workspace_id, series.replay_scope)
    service = await incidents.get_service(series)
    fingerprint = fingerprint_for(service.environment)

    incident = await incidents.find_correlated(scope, fingerprint, window, policy.correlation_gap)
    if incident is None:
        incident_id = await incidents.create(
            NewIncident(
                scope=scope,
                fingerprint=fingerprint,
                title=f"{RULE_TITLES[rule.kind]} on {service.name} ({service.environment})",
                severity=rule.severity,
                window=window,
                # Replay keeps event time, so a run's incidents do not depend on when it ran.
                detected_at=(
                    datetime.now(UTC)
                    if series.replay_scope == LIVE_REPLAY_SCOPE
                    else breach.deadline
                ),
            )
        )
    else:
        incident_id = incident.id

    joined = await incidents.add_service(series, incident_id, IncidentServiceRole.PRIMARY)
    cited = 0
    if breach.trigger_id is not None:
        cited = await incidents.attach_breaching_samples(
            series,
            window,
            incident_id,
            _breaching(rule),
            relevance=BREACHING_RELEVANCE,
            limit=EVIDENCE_PER_TRIGGER,
            incident_limit=MAX_INCIDENT_EVIDENCE,
        )
        await DetectionRepository(session).link_trigger(breach.trigger_id, incident_id)

    if incident is None:
        await _count_opened(
            session, incidents, incident_id, live=scope.replay_scope == LIVE_REPLAY_SCOPE
        )
        logger.info(
            "incident opened",
            extra={"incident_id": str(incident_id), "rule": rule.id, "service": service.name},
        )
        await _publish(
            session,
            scope,
            incident_id,
            1,
            IncidentChange.CREATED,
            status=IncidentStatus.OPEN,
            severity=rule.severity,
            at=window.end,
        )
        if policy.investigations is not None and series.replay_scope == LIVE_REPLAY_SCOPE:
            await request_by_detector(
                session, scope, incident_id, policy.investigations, now=datetime.now(UTC)
            )
    else:
        await _extend(session, scope, breach, incident, grew=joined or cited > 0)


async def _count_opened(
    session: AsyncSession, incidents: IncidentRepository, incident_id: uuid.UUID, *, live: bool
) -> None:
    """Count the incident once committed; for live ones, time since its first evidence arrived."""
    earliest = await incidents.earliest_evidence_ingested_at(incident_id) if live else None

    def count() -> None:
        INCIDENTS_OPENED.labels("live" if live else "replay").inc()
        if earliest is not None:
            DETECTION_LATENCY.observe(max(0.0, (datetime.now(UTC) - earliest).total_seconds()))

    on_commit(session, count)


async def record_healthy(
    session: AsyncSession, policy: IncidentPolicy, series: SeriesKey, window: Window
) -> None:
    """Credit a healthy closed window to the service's incidents, resolving the recovered ones."""
    incidents = IncidentRepository(session)
    # Unlocked: most windows belong to no incident. A breach racing this read
    # moves last_seen_at, and the check under the lock sees it.
    candidates = await incidents.recovering(series, window.end - policy.recovery)
    if not candidates:
        return
    await incidents.lock_scope(series.replay_scope)
    scope = EvidenceScope(series.workspace_id, series.replay_scope)
    for incident_id in candidates:
        await incidents.mark_healthy(incident_id, series.service_id, window.end)
        incident = await incidents.locked(scope, incident_id)
        if (
            incident is None
            or incident.status != IncidentStatus.OPEN
            or not await incidents.all_healthy_through(
                incident_id, incident.last_seen_at + policy.recovery
            )
        ):
            continue
        version = await incidents.apply(
            incident, {"status": IncidentStatus.RESOLVED, "resolved_at": window.end}
        )
        await _publish(
            session,
            scope,
            incident_id,
            version,
            IncidentChange.RESOLVED,
            status=IncidentStatus.RESOLVED,
            severity=IncidentSeverity(incident.severity),
            at=window.end,
        )


async def change_status(
    session: AsyncSession,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    status: IncidentStatus,
    *,
    version: int,
    now: datetime,
) -> Incident | None:
    """Apply an operator's status at `version`; None when the scope has no such incident.

    Asking for the status it already has at the current version changes nothing,
    so a retry after a lost response is harmless.
    """
    incidents = IncidentRepository(session)
    incident = await incidents.locked(scope, incident_id)
    if incident is None:
        return None
    if incident.version != version:
        raise IncidentVersionError(f"incident is at version {incident.version}")
    if incident.status == status:
        return incident
    resolving = status is IncidentStatus.RESOLVED
    severity = IncidentSeverity(incident.severity)
    await _publish(
        session,
        scope,
        incident_id,
        await incidents.apply(
            incident, {"status": status, "resolved_at": now if resolving else None}
        ),
        IncidentChange.RESOLVED if resolving else IncidentChange.REOPENED,
        status=status,
        severity=severity,
        at=now,
    )
    return await incidents.locked(scope, incident_id)
