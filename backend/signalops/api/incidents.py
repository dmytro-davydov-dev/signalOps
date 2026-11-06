"""Incident list and detail, evidence timeline, status changes, and service graphs.

Every route requires an operator token, and its workspace and the `replay_scope`
parameter (default `live`) bound the query, as in `api/evidence.py`. A status
change has no scope parameter: it applies to live incidents only, so an operator
cannot alter a replay's results, and a replay incident is 404.

Graph reads answer as of `as_of`; a caller looking at an incident passes the
incident's time, so the neighborhood holds nothing observed after it.
"""

import base64
import binascii
import hashlib
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.ingestion import SourceConfigError
from signalops.api.auth import OperatorWorkspace
from signalops.api.evidence import (
    InvalidCursorError,
    InvalidQueryError,
    LimitQuery,
    ReadFailed,
    ReplayScopeQuery,
    check_limit,
    from_micros,
    invalid_query,
    output_redaction,
    read_database,
    to_micros,
)
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, UtcDatetime
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind
from signalops.contracts.incidents import (
    MAX_INCIDENT_EVIDENCE,
    MAX_INCIDENT_TRIGGERS,
    MAX_PAGE_SIZE,
    EvidenceRef,
    IncidentDetail,
    IncidentEvidence,
    IncidentEvidencePage,
    IncidentPage,
    IncidentServiceRef,
    IncidentStatusUpdate,
    IncidentSummary,
    IncidentTrigger,
)
from signalops.contracts.queries import GraphService, ServiceGraph
from signalops.contracts.tools import MAX_TRAVERSAL_HOPS
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus
from signalops.detection.incidents import IncidentVersionError, change_status
from signalops.persistence.models import Incident
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.graph import GraphDirection, GraphRepository
from signalops.persistence.repositories.incidents import (
    IncidentFilter,
    IncidentRepository,
    Position,
)
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["incidents"])

CURSOR_VERSION = 1

CursorQuery = Annotated[str | None, Query(max_length=512)]
NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"description": "Not found in this workspace and replay scope"}
}
BAD_CURSOR: dict[int | str, dict[str, Any]] = {400: {"description": "Invalid or mismatched cursor"}}


@dataclass(frozen=True)
class IncidentQuery:
    scope: EvidenceScope
    criteria: IncidentFilter

    def digest(self) -> str:
        return query_digest({**asdict(self.scope), **asdict(self.criteria)})


def query_digest(fields: dict[str, object]) -> str:
    canonical = json.dumps(fields, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


@dataclass(frozen=True)
class KeysetCursor:
    """A page position bound to the query it came from."""

    position: Position
    digest: str

    def encode(self) -> str:
        document = {
            "v": CURSOR_VERSION,
            "p": [to_micros(self.position.at), self.position.id.hex],
            "q": self.digest,
        }
        raw = json.dumps(document, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @classmethod
    def decode(cls, value: str) -> "KeysetCursor":
        try:
            raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            document = json.loads(raw)
            if not isinstance(document, dict) or document.get("v") != CURSOR_VERSION:
                raise InvalidCursorError("invalid cursor")
            at, row_id = document["p"]
            digest = document["q"]
            if not isinstance(digest, str) or not isinstance(row_id, str):
                raise InvalidCursorError("invalid cursor")
            return cls(Position(from_micros(at), uuid.UUID(hex=row_id)), digest)
        except (
            binascii.Error,
            UnicodeDecodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            OverflowError,
        ):
            raise InvalidCursorError("invalid cursor") from None


def _position(cursor: str | None, digest: str) -> Position | None:
    """The cursor's position; a cursor from another query is rejected."""
    if cursor is None:
        return None
    decoded = KeysetCursor.decode(cursor)
    if decoded.digest != digest:
        raise InvalidCursorError("cursor belongs to a different query")
    return decoded.position


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, "incident not found")


def _summary_fields(row: Incident) -> dict[str, object]:
    return {
        "id": row.id,
        "replay_scope": row.replay_scope,
        "title": row.title,
        "severity": IncidentSeverity(row.severity),
        "status": IncidentStatus(row.status),
        "detected_at": row.detected_at,
        "first_seen_at": row.first_seen_at,
        "last_seen_at": row.last_seen_at,
        "resolved_at": row.resolved_at,
        "version": row.version,
    }


async def _detail(session: AsyncSession, scope: EvidenceScope, row: Incident) -> IncidentDetail:
    incidents = IncidentRepository(session)
    services = await incidents.services_of(row.id)
    evidence = await incidents.evidence_of(scope, row.id, after=None, limit=MAX_INCIDENT_EVIDENCE)
    triggers = await incidents.triggers_of(row.id, limit=MAX_INCIDENT_TRIGGERS + 1)
    return IncidentDetail.model_validate(
        {
            **_summary_fields(row),
            "fingerprint": row.fingerprint,
            "correlation_window_start": row.correlation_window_start,
            "services": [
                IncidentServiceRef(
                    service_id=service.id,
                    name=service.name,
                    environment=service.environment,
                    role=IncidentServiceRole(role),
                )
                for service, role in services
            ],
            "evidence": [
                EvidenceRef(
                    signal_id=signal.id,
                    kind=SignalKind(signal.kind),
                    occurred_at=signal.occurred_at,
                    severity=Severity(signal.severity),
                    quality_status=QualityStatus(signal.quality_status),
                    relevance=relevance,
                )
                for signal, relevance in evidence
            ],
            "triggers": [
                IncidentTrigger(
                    trigger_id=trigger.id,
                    service_id=trigger.service_id,
                    rule_id=trigger.rule_id,
                    metric_name=trigger.metric_name,
                    window_start=trigger.window_start,
                    window_end=trigger.window_end,
                    observed=trigger.observed,
                    threshold=trigger.threshold,
                    sample_count=trigger.sample_count,
                    error_count=trigger.error_count,
                )
                for trigger in triggers[:MAX_INCIDENT_TRIGGERS]
            ],
            "triggers_truncated": len(triggers) > MAX_INCIDENT_TRIGGERS,
        }
    )


@router.get("/incidents", responses=BAD_CURSOR)
async def list_incidents(
    request: Request,
    workspace_id: OperatorWorkspace,
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    service_id: uuid.UUID | None = None,
    severity: IncidentSeverity | None = None,
    incident_status: Annotated[IncidentStatus | None, Query(alias="status")] = None,
    limit: LimitQuery = None,
    cursor: CursorQuery = None,
) -> IncidentPage:
    """Incidents by `detected_at` (newest first) with keyset pagination.

    Pass a cursor with the same filters; a cursor from another query is 400.
    """
    settings: Settings = request.app.state.settings
    query = IncidentQuery(
        scope=EvidenceScope(workspace_id, replay_scope),
        criteria=IncidentFilter(service_id=service_id, severity=severity, status=incident_status),
    )
    try:
        page_size = check_limit(limit, settings, ceiling=MAX_PAGE_SIZE)
        after = _position(cursor, query.digest())
    except InvalidQueryError as error:
        raise invalid_query(error) from None

    try:
        async with read_database(request).transaction() as session:
            rows = await IncidentRepository(session).list_incidents(
                query.scope, query.criteria, after=after, limit=page_size + 1
            )
            items = [IncidentSummary.model_validate(_summary_fields(r)) for r in rows[:page_size]]
    except SQLAlchemyError as error:
        raise ReadFailed("incidents", error) from None

    next_cursor = None
    if len(rows) > page_size:
        last = items[-1]
        next_cursor = KeysetCursor(Position(last.detected_at, last.id), query.digest()).encode()
    return IncidentPage(items=tuple(items), next_cursor=next_cursor)


@router.get("/incidents/{incident_id}", responses=NOT_FOUND)
async def get_incident(
    request: Request,
    workspace_id: OperatorWorkspace,
    incident_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
) -> IncidentDetail:
    """The incident with its services, evidence references, and rule firings."""
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            row = await IncidentRepository(session).get(scope, incident_id)
            if row is None:
                raise _not_found()
            return await _detail(session, scope, row)
    except SQLAlchemyError as error:
        raise ReadFailed("incidents/detail", error) from None


@router.patch(
    "/incidents/{incident_id}",
    responses={**NOT_FOUND, 409: {"description": "`version` is not the current version"}},
)
async def update_incident(
    request: Request,
    workspace_id: OperatorWorkspace,
    incident_id: Annotated[uuid.UUID, Path()],
    update: IncidentStatusUpdate,
) -> IncidentDetail:
    """Resolve or reopen a live incident, at the version the operator last read.

    Another writer's change is 409: read again and decide again. The change and
    its `incident.changed` event commit together.
    """
    scope = EvidenceScope(workspace_id, LIVE_REPLAY_SCOPE)
    try:
        async with read_database(request).transaction() as session:
            row = await change_status(
                session,
                scope,
                incident_id,
                update.status,
                version=update.version,
                now=datetime.now(UTC),
            )
            if row is None:
                raise _not_found()
            return await _detail(session, scope, row)
    except IncidentVersionError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from None
    except SQLAlchemyError as error:
        raise ReadFailed("incidents/update", error) from None


@router.get("/incidents/{incident_id}/evidence", responses={**NOT_FOUND, **BAD_CURSOR})
async def incident_evidence(
    request: Request,
    workspace_id: OperatorWorkspace,
    incident_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    limit: LimitQuery = None,
    cursor: CursorQuery = None,
) -> IncidentEvidencePage:
    """The incident's cited signals, oldest first, redacted as `/v1/signals` is."""
    settings: Settings = request.app.state.settings
    scope = EvidenceScope(workspace_id, replay_scope)
    digest = query_digest({**asdict(scope), "incident_id": incident_id})
    try:
        page_size = check_limit(limit, settings)
        after = _position(cursor, digest)
    except InvalidQueryError as error:
        raise invalid_query(error) from None

    try:
        async with read_database(request).transaction() as session:
            incidents = IncidentRepository(session)
            if await incidents.get(scope, incident_id) is None:
                raise _not_found()
            redaction = await output_redaction(session, settings, workspace_id)
            rows = await incidents.evidence_of(scope, incident_id, after=after, limit=page_size + 1)
            items = [
                IncidentEvidence(signal=redaction.signal(signal), relevance=relevance)
                for signal, relevance in rows[:page_size]
            ]
    except (SQLAlchemyError, SourceConfigError) as error:
        raise ReadFailed("incidents/evidence", error) from None

    next_cursor = None
    if len(rows) > page_size:
        last = items[-1].signal
        next_cursor = KeysetCursor(Position(last.occurred_at, last.id), digest).encode()
    return IncidentEvidencePage(
        incident_id=incident_id,
        replay_scope=replay_scope,
        items=tuple(items),
        next_cursor=next_cursor,
    )


@router.get(
    "/services/{service_id}/graph",
    responses={404: {"description": "No such service in this workspace"}},
)
async def service_graph(
    request: Request,
    workspace_id: OperatorWorkspace,
    service_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    as_of: Annotated[UtcDatetime | None, Query(description="Defaults to now")] = None,
    hops: Annotated[int, Query(ge=1, le=MAX_TRAVERSAL_HOPS)] = MAX_TRAVERSAL_HOPS,
) -> ServiceGraph:
    """Dependencies and dependents of the service as of a time, up to `hops` away.

    A service known to the catalog but not yet observed has an empty neighborhood.
    """
    scope = EvidenceScope(workspace_id, replay_scope)
    instant = as_of or datetime.now(UTC)
    try:
        async with read_database(request).transaction() as session:
            catalog = CatalogRepository(session)
            if not await catalog.get_services(workspace_id, [service_id]):
                raise HTTPException(status.HTTP_404_NOT_FOUND, "service not found")
            graph = GraphRepository(session)
            downstream = await graph.dependencies(scope, service_id, as_of=instant, max_hops=hops)
            upstream = await graph.dependencies(
                scope, service_id, as_of=instant, direction=GraphDirection.UPSTREAM, max_hops=hops
            )
            named = {service_id} | {
                endpoint
                for edge in (*downstream.items, *upstream.items)
                for endpoint in (edge.from_service_id, edge.to_service_id)
            }
            services = await catalog.get_services(workspace_id, named)
            deployments = await graph.deployments_as_of(scope, named, as_of=instant)
    except SQLAlchemyError as error:
        raise ReadFailed("services/graph", error) from None

    return ServiceGraph(
        service_id=service_id,
        replay_scope=replay_scope,
        as_of=instant,
        services=tuple(
            GraphService(
                service_id=service.id,
                name=service.name,
                environment=service.environment,
                deployments=tuple(d for d in deployments if d.service_id == service.id),
            )
            for service in services
        ),
        downstream=downstream.items,
        upstream=upstream.items,
        truncated=downstream.truncated or upstream.truncated,
    )
