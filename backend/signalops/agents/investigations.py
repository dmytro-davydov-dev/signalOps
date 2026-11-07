"""Investigation requests in, one durable job per investigation out.

A request writes the investigation and its `investigation.requested` outbox row
in the caller's transaction. The API acknowledges after that commit, and the
worker never runs in a request handler. `Dispatcher` consumes the event and
creates the job, so a request survives a broker outage and a redelivered event
finds the job already there (ADR-020).

**Keys.** An operator's `Idempotency-Key` is scoped to the workspace and route.
Tokens carry no identity (ADR-014), so the workspace is the caller. The stored
hash covers the incident and the body, so a key reused on another incident is a
conflict too. The detector's key is `detector:{incident_id}`: one per incident,
so a reopened incident is not investigated again automatically.

**Limits.** Settings set the most an investigation can use. A request may lower
them, never raise them. A repeated request skips this check: it asks for nothing
new, so it still answers after the settings are lowered.

**Replay.** Requests are live only. A replay run's investigations are the
evaluation's to start, under its own budget (P10), so neither an operator nor the
detector can add spend to one.
"""

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError, ContractErrorCategory, replay_run_id_of
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    InvestigationCompletedPayload,
    InvestigationRequestedPayload,
    InvestigationTrigger,
)
from signalops.contracts.investigations import InvestigationRequest
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES, InvestigationStatus
from signalops.persistence.models import Investigation
from signalops.persistence.repositories.incidents import IncidentRepository
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

DISPATCHER_CONSUMER_GROUP: Final = "dispatcher-v1"
WORKFLOW_VERSION: Final = "v1"
# Fixed namespace: a request's event ID derives from its investigation.
INVESTIGATION_EVENT_NAMESPACE: Final = uuid.UUID("c1f0a6d4-8b2e-4f37-9e15-2a7d6b3c9e01")
REQUEST_AGGREGATE_VERSION: Final = 1


@dataclass(frozen=True)
class InvestigationPolicy:
    budget_microusd: int = 1_000_000
    max_steps: int = 20
    deadline_seconds: int = 600

    @classmethod
    def from_settings(cls, settings: Settings) -> "InvestigationPolicy":
        return cls(
            settings.investigation_budget_microusd,
            settings.investigation_max_steps,
            settings.investigation_deadline_seconds,
        )


@dataclass(frozen=True)
class IdempotentRequest:
    route: str
    key: str
    request_hash: str


class IdempotencyConflictError(Exception):
    """The key was used with another request."""


class InvestigationLimitError(Exception):
    """The request asks for more than the configured limit."""


def request_hash_for(incident_id: uuid.UUID, body: InvestigationRequest) -> str:
    """SHA-256 of the canonical request, so field order and spacing change nothing."""
    canonical = {"incident_id": str(incident_id), "body": body.model_dump(mode="json")}
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def operator_request_key(workspace_id: uuid.UUID, route: str, key: str) -> str:
    """`request_key` is unique across workspaces, so it carries the key's whole scope."""
    digest = hashlib.sha256(f"{workspace_id}\0{route}\0{key}".encode()).hexdigest()
    return f"operator:{digest}"


def _resolved(limit: int, requested: int | None, name: str) -> int:
    if requested is None:
        return limit
    if requested > limit:
        raise InvestigationLimitError(f"{name} exceeds the configured limit of {limit}")
    return requested


async def _publish(
    session: AsyncSession, investigation: Investigation, trigger: InvestigationTrigger, at: datetime
) -> None:
    envelope = EventEnvelope[EventPayload](
        event_id=uuid.uuid5(INVESTIGATION_EVENT_NAMESPACE, f"{investigation.id}:requested"),
        event_type=EventType.INVESTIGATION_REQUESTED,
        schema_version=1,
        workspace_id=investigation.workspace_id,
        occurred_at=at,
        ingested_at=at,
        replay_run_id=replay_run_id_of(investigation.replay_scope),
        payload=InvestigationRequestedPayload(
            investigation_id=investigation.id,
            incident_id=investigation.incident_id,
            request_key=investigation.request_key,
            trigger=trigger,
            workflow_version=investigation.workflow_version,
        ),
    )
    await OutboxRepository(session).add(
        envelope, aggregate_id=investigation.id, aggregate_version=REQUEST_AGGREGATE_VERSION
    )


async def request_by_operator(
    session: AsyncSession,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    body: InvestigationRequest,
    idempotency: IdempotentRequest,
    policy: InvestigationPolicy,
    *,
    now: datetime,
) -> Investigation | None:
    """The investigation for this key, created if new; None when the incident is absent."""
    investigations = InvestigationRepository(session)
    route, key = idempotency.route, idempotency.key

    async def repeated() -> Investigation | None:
        stored = await investigations.find_key(scope.workspace_id, route, key)
        if stored is None:
            return None
        if stored.request_hash != idempotency.request_hash:
            raise IdempotencyConflictError("Idempotency-Key was used with another request")
        return await investigations.get(scope, stored.investigation_id)

    if (found := await repeated()) is not None:
        return found
    if await IncidentRepository(session).get(scope, incident_id) is None:
        return None
    deadline_seconds = _resolved(policy.deadline_seconds, body.deadline_seconds, "deadline_seconds")
    created = await investigations.create(
        NewInvestigation(
            scope=scope,
            incident_id=incident_id,
            request_key=operator_request_key(scope.workspace_id, route, key),
            workflow_version=WORKFLOW_VERSION,
            budget_microusd=_resolved(
                policy.budget_microusd, body.budget_microusd, "budget_microusd"
            ),
            max_steps=_resolved(policy.max_steps, body.max_steps, "max_steps"),
            created_at=now,
            deadline_at=now + timedelta(seconds=deadline_seconds),
        )
    )
    if created is None:
        # A concurrent request held the key and has committed by now.
        return await repeated()
    await investigations.add_key(
        scope.workspace_id, route, key, idempotency.request_hash, created.id
    )
    await _publish(session, created, InvestigationTrigger.OPERATOR, now)
    return created


async def request_by_detector(
    session: AsyncSession,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    policy: InvestigationPolicy,
    *,
    now: datetime,
) -> None:
    """Request the incident's automatic investigation, once."""
    created = await InvestigationRepository(session).create(
        NewInvestigation(
            scope=scope,
            incident_id=incident_id,
            request_key=f"detector:{incident_id}",
            workflow_version=WORKFLOW_VERSION,
            budget_microusd=policy.budget_microusd,
            max_steps=policy.max_steps,
            created_at=now,
            deadline_at=now + timedelta(seconds=policy.deadline_seconds),
        )
    )
    if created is not None:
        await _publish(session, created, InvestigationTrigger.DETECTOR, now)


async def fail_investigation(session: AsyncSession, investigation_id: uuid.UUID) -> None:
    """Fail the investigation and stage `investigation.completed`; a terminal one stays as it is.

    The aggregate version is the investigation's `version`, which claiming has
    already moved past the request's version 1, so the events publish in order.
    """
    investigation = await session.get_one(Investigation, investigation_id, with_for_update=True)
    if InvestigationStatus(investigation.status) in TERMINAL_INVESTIGATION_STATUSES:
        return
    at = await session.scalar(select(func.clock_timestamp()))
    assert at is not None  # noqa: S101 - a scalar function always returns a row
    investigation.status = InvestigationStatus.FAILED
    investigation.completed_at = at
    investigation.version += 1
    await session.flush()
    envelope = EventEnvelope[EventPayload](
        event_id=uuid.uuid5(INVESTIGATION_EVENT_NAMESPACE, f"{investigation.id}:completed"),
        event_type=EventType.INVESTIGATION_COMPLETED,
        schema_version=1,
        workspace_id=investigation.workspace_id,
        occurred_at=at,
        ingested_at=at,
        replay_run_id=replay_run_id_of(investigation.replay_scope),
        payload=InvestigationCompletedPayload(
            investigation_id=investigation.id,
            incident_id=investigation.incident_id,
            status=InvestigationStatus.FAILED,
            workflow_version=investigation.workflow_version,
        ),
    )
    await OutboxRepository(session).add(
        envelope, aggregate_id=investigation.id, aggregate_version=investigation.version
    )


class Dispatcher:
    """`EventHandler` for investigations.requested.v1."""

    async def __call__(self, session: AsyncSession, envelope: EventEnvelope[EventPayload]) -> None:
        payload = envelope.payload
        if envelope.event_type is not EventType.INVESTIGATION_REQUESTED or not isinstance(
            payload, InvestigationRequestedPayload
        ):
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                f"dispatcher accepts {EventType.INVESTIGATION_REQUESTED} only, "
                f"not {envelope.event_type}",
                event_id=envelope.event_id,
            )
        scope = EvidenceScope(envelope.workspace_id, envelope.replay_scope)
        investigations = InvestigationRepository(session)
        investigation = await investigations.get(scope, payload.investigation_id)
        if investigation is None:
            raise ContractError(
                ContractErrorCategory.INVALID_ENVELOPE,
                "investigation_id does not name an investigation in this workspace and replay "
                "scope",
                event_id=envelope.event_id,
            )
        if InvestigationStatus(investigation.status) in TERMINAL_INVESTIGATION_STATUSES:
            return
        if await investigations.enqueue(investigation.id):
            logger.info(
                "investigation job queued", extra={"investigation_id": str(investigation.id)}
            )
