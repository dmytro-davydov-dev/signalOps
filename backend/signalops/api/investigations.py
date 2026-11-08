"""Investigation requests, reads, progress history, and cancellation.

A request's response is `202` once the investigation and its outbox request
commit, and a cancellation's once the request commits; the API never waits for
the workflow. Both are live scope only, as for status changes: a replay incident
is 404. See `agents/investigations.py` for the rules.
"""

import logging
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Path, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import SQLAlchemyError

from signalops.adapters.ingestion import SourceConfigError
from signalops.agents.investigations import (
    IdempotencyConflictError,
    IdempotentRequest,
    InvestigationFinishedError,
    InvestigationLimitError,
    InvestigationPolicy,
    cancel_investigation,
    request_by_operator,
    request_hash_for,
)
from signalops.api.auth import OperatorWorkspace
from signalops.api.evidence import (
    InvalidQueryError,
    LimitQuery,
    ReadFailed,
    ReplayScopeQuery,
    check_limit,
    invalid_query,
    output_redaction,
    read_database,
)
from signalops.api.progress_stream import StreamLimits, progress_frames
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, MAX_KEY_LENGTH
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.investigations import (
    MAX_INCIDENT_INVESTIGATIONS,
    IncidentInvestigations,
    InvestigationAccepted,
    InvestigationEvidence,
    InvestigationRequest,
    InvestigationSummary,
    InvestigationUsage,
    InvestigationView,
)
from signalops.contracts.progress import ProgressEventView, ProgressPage
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES, InvestigationStatus
from signalops.dependencies import Resources
from signalops.persistence.database import Database
from signalops.persistence.models import Investigation, ProgressEvent
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.incidents import IncidentRepository
from signalops.persistence.repositories.investigations import InvestigationRepository
from signalops.persistence.repositories.llm_calls import LlmCallRepository, UsageTotals
from signalops.persistence.repositories.progress import ProgressRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.wakeups import subscribe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["investigations"])

# The template, not the path: the stored hash carries the incident.
REQUEST_ROUTE = "POST /v1/incidents/{id}/investigations"

NOT_FOUND: dict[int | str, dict[str, Any]] = {404: {"description": "No such investigation"}}


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, "investigation not found")


def _view(row: Investigation, totals: UsageTotals) -> InvestigationView:
    return InvestigationView(
        id=row.id,
        incident_id=row.incident_id,
        replay_scope=row.replay_scope,
        status=InvestigationStatus(row.status),
        workflow_version=row.workflow_version,
        evidence_cutoff_at=row.evidence_cutoff_at,
        created_at=row.created_at,
        deadline_at=row.deadline_at,
        completed_at=row.completed_at,
        cancel_requested_at=row.cancel_requested_at,
        max_steps=row.max_steps,
        usage=InvestigationUsage(
            budget_microusd=row.budget_microusd,
            reserved_microusd=row.reserved_microusd,
            spent_microusd=row.spent_microusd,
            **asdict(totals),
        ),
        diagnosis=None if row.diagnosis is None else Diagnosis.model_validate(row.diagnosis),
    )


@router.post(
    "/incidents/{incident_id}/investigations",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        404: {"description": "No such live incident in this workspace"},
        409: {"description": "`Idempotency-Key` was used with another request"},
    },
)
async def request_investigation(
    request: Request,
    workspace_id: OperatorWorkspace,
    incident_id: Annotated[uuid.UUID, Path()],
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=MAX_KEY_LENGTH)
    ],
    body: InvestigationRequest | None = None,
) -> InvestigationAccepted:
    """Request an investigation of a live incident.

    The same key and body return the same investigation; the same key with
    another body or incident is 409.
    """
    settings: Settings = request.app.state.settings
    limits = body or InvestigationRequest()
    idempotency = IdempotentRequest(
        REQUEST_ROUTE, idempotency_key, request_hash_for(incident_id, limits)
    )
    try:
        async with read_database(request).transaction() as session:
            investigation = await request_by_operator(
                session,
                EvidenceScope(workspace_id, LIVE_REPLAY_SCOPE),
                incident_id,
                limits,
                idempotency,
                InvestigationPolicy.from_settings(settings),
                now=datetime.now(UTC),
            )
            if investigation is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "incident not found")
            return InvestigationAccepted(
                id=investigation.id,
                incident_id=investigation.incident_id,
                status=InvestigationStatus(investigation.status),
                created_at=investigation.created_at,
            )
    except IdempotencyConflictError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from None
    except InvestigationLimitError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from None
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/request", error) from None


@router.get(
    "/incidents/{incident_id}/investigations",
    responses={404: {"description": "No such incident in this workspace and replay scope"}},
)
async def list_incident_investigations(
    request: Request,
    workspace_id: OperatorWorkspace,
    incident_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
) -> IncidentInvestigations:
    """The incident's newest investigations, operator and automatic alike."""
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            if await IncidentRepository(session).get(scope, incident_id) is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "incident not found")
            rows = await InvestigationRepository(session).for_incident(
                scope, incident_id, limit=MAX_INCIDENT_INVESTIGATIONS + 1
            )
    except SQLAlchemyError as error:
        raise ReadFailed("incidents/investigations", error) from None
    return IncidentInvestigations(
        incident_id=incident_id,
        replay_scope=replay_scope,
        items=tuple(
            InvestigationSummary(
                id=row.id,
                status=InvestigationStatus(row.status),
                created_at=row.created_at,
                completed_at=row.completed_at,
                cancel_requested_at=row.cancel_requested_at,
                diagnosis_status=(
                    None if row.diagnosis is None else DiagnosisStatus(row.diagnosis["status"])
                ),
            )
            for row in rows[:MAX_INCIDENT_INVESTIGATIONS]
        ),
        truncated=len(rows) > MAX_INCIDENT_INVESTIGATIONS,
    )


@router.get("/investigations/{investigation_id}", responses=NOT_FOUND)
async def get_investigation(
    request: Request,
    workspace_id: OperatorWorkspace,
    investigation_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
) -> InvestigationView:
    """An investigation's durable state, in the given scope of the token's workspace."""
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            row = await InvestigationRepository(session).get(scope, investigation_id)
            if row is None:
                raise _not_found()
            return _view(row, await LlmCallRepository(session).usage_of(row.id))
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/detail", error) from None


@router.get("/investigations/{investigation_id}/evidence", responses=NOT_FOUND)
async def get_investigation_evidence(
    request: Request,
    workspace_id: OperatorWorkspace,
    investigation_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
) -> InvestigationEvidence:
    """The diagnosis's cited signals, checked again against scope and cutoff.

    Empty without a root cause. A citation outside the scope or after the cutoff
    is listed as unverified, never returned.
    """
    settings: Settings = request.app.state.settings
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            row = await InvestigationRepository(session).get(scope, investigation_id)
            if row is None:
                raise _not_found()
            diagnosis = None if row.diagnosis is None else Diagnosis.model_validate(row.diagnosis)
            cited = (
                ()
                if diagnosis is None or diagnosis.root_cause is None
                else (diagnosis.root_cause.evidence_signal_ids)
            )
            known = await EvidenceRepository(session).known_signals(
                scope, cited, known_by=row.evidence_cutoff_at
            )
            redaction = await output_redaction(session, settings, workspace_id)
            items = tuple(redaction.signal(known[i]) for i in cited if i in known)
    except (SQLAlchemyError, SourceConfigError) as error:
        raise ReadFailed("investigations/evidence", error) from None
    return InvestigationEvidence(
        investigation_id=investigation_id,
        replay_scope=replay_scope,
        evidence_cutoff_at=row.evidence_cutoff_at,
        items=items,
        unverified_signal_ids=tuple(i for i in cited if i not in known),
    )


@router.get("/investigations/{investigation_id}/progress", responses=NOT_FOUND)
async def get_progress(
    request: Request,
    workspace_id: OperatorWorkspace,
    investigation_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    after: Annotated[int, Query(ge=0, description="The last event ID already read")] = 0,
    limit: LimitQuery = None,
) -> ProgressPage:
    """An investigation's durable progress after `after`, oldest first."""
    settings: Settings = request.app.state.settings
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        page_size = check_limit(limit, settings)
        async with read_database(request).transaction() as session:
            if await InvestigationRepository(session).get(scope, investigation_id) is None:
                raise _not_found()
            rows = await ProgressRepository(session).after(
                investigation_id, after, limit=page_size + 1
            )
            items = [_event_view(row) for row in rows[:page_size]]
    except InvalidQueryError as error:
        raise invalid_query(error) from None
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/progress", error) from None
    return ProgressPage(
        investigation_id=investigation_id,
        replay_scope=replay_scope,
        items=tuple(items),
        next_after=items[-1].id if len(rows) > page_size else None,
    )


def _event_view(row: ProgressEvent) -> ProgressEventView:
    return ProgressEventView.model_validate(
        {
            "id": row.id,
            "event_type": row.event_type,
            "payload": row.payload,
            "created_at": row.created_at,
        }
    )


# Largest cursor PostgreSQL's bigint holds.
MAX_CURSOR = 2**63 - 1


def _cursor(last_event_id: str | None, after: int | None) -> int:
    """`Last-Event-ID`, which a reconnecting client sends, wins over `after`."""
    if last_event_id is None:
        return after or 0
    value = last_event_id.strip()
    if not value.isascii() or not value.isdigit() or int(value) > MAX_CURSOR:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid Last-Event-ID")
    return int(value)


@dataclass(frozen=True)
class DatabaseProgressSource:
    """Each read is its own short transaction; none is open while a stream waits."""

    database: Database
    investigation_id: uuid.UUID

    async def known(self, cursor: int) -> bool:
        async with self.database.transaction() as session:
            return await ProgressRepository(session).exists(self.investigation_id, cursor)

    async def after(self, cursor: int, limit: int) -> Sequence[ProgressEventView]:
        async with self.database.transaction() as session:
            rows = await ProgressRepository(session).after(
                self.investigation_id, cursor, limit=limit
            )
            return [_event_view(row) for row in rows]

    async def terminal(self) -> bool:
        async with self.database.transaction() as session:
            row = await session.get(Investigation, self.investigation_id)
        return row is not None and InvestigationStatus(row.status) in (
            TERMINAL_INVESTIGATION_STATUSES
        )


class StreamCount:
    """Open progress streams in this process."""

    def __init__(self) -> None:
        self.open = 0


def _stream_count(request: Request) -> StreamCount:
    state = request.app.state
    if not hasattr(state, "progress_streams"):
        state.progress_streams = StreamCount()
    count: StreamCount = state.progress_streams
    return count


@router.get(
    "/investigations/{investigation_id}/events",
    response_class=StreamingResponse,
    responses={
        **NOT_FOUND,
        200: {"content": {"text/event-stream": {}}, "description": "Progress events"},
        400: {"description": "Invalid Last-Event-ID"},
        503: {"description": "Too many open streams, or the database is unavailable"},
    },
)
async def stream_progress(
    request: Request,
    workspace_id: OperatorWorkspace,
    investigation_id: Annotated[uuid.UUID, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    after: Annotated[int | None, Query(ge=0, le=MAX_CURSOR)] = None,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID", max_length=32)] = None,
) -> StreamingResponse:
    """Durable progress as server-sent events; see `api/progress_stream.py`.

    Each `id` is a `progress_events` ID. A reconnect with `Last-Event-ID` gets the
    events after it; an unknown cursor gets `reset` and the whole history.
    """
    settings: Settings = request.app.state.settings
    resources: Resources = request.app.state.resources
    cursor = _cursor(last_event_id, after)
    database = read_database(request)
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with database.transaction() as session:
            if await InvestigationRepository(session).get(scope, investigation_id) is None:
                raise _not_found()
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/events", error) from None
    streams = _stream_count(request)
    if streams.open >= settings.sse_max_streams:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "too many open progress streams",
            headers={"Retry-After": "5"},
        )
    limits = StreamLimits(settings.sse_catchup_seconds, settings.sse_max_stream_seconds)
    source = DatabaseProgressSource(database, investigation_id)

    async def body() -> AsyncIterator[str]:
        streams.open += 1
        try:
            async with subscribe(resources.redis, investigation_id) as wakeups:
                async for chunk in progress_frames(
                    source, wakeups, cursor, limits, disconnected=request.is_disconnected
                ):
                    yield chunk
        except SQLAlchemyError as error:
            # The response has started; ending it makes the client reconnect from its cursor.
            logger.warning("progress stream read failed", extra={"error": type(error).__name__})
        finally:
            streams.open -= 1

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post(
    "/investigations/{investigation_id}/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    responses={**NOT_FOUND, 409: {"description": "The investigation already ended another way"}},
)
async def cancel(
    request: Request,
    workspace_id: OperatorWorkspace,
    investigation_id: Annotated[uuid.UUID, Path()],
) -> InvestigationView:
    """Persist a cancellation of a live investigation.

    The response shows `cancelled`, or `cancel_requested_at` on a running
    investigation, whose worker ends it. Repeating the request is safe.
    """
    scope = EvidenceScope(workspace_id, LIVE_REPLAY_SCOPE)
    try:
        async with read_database(request).transaction() as session:
            row = await cancel_investigation(session, scope, investigation_id)
            if row is None:
                raise _not_found()
            return _view(row, await LlmCallRepository(session).usage_of(row.id))
    except InvestigationFinishedError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from None
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/cancel", error) from None
