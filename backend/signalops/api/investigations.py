"""Investigation requests, reads, and cancellation.

A request's response is `202` once the investigation and its outbox request
commit, and a cancellation's once the request commits; the API never waits for
the workflow. Both are live scope only, as for status changes: a replay incident
is 404. See `agents/investigations.py` for the rules.
"""

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Path, Request, status
from sqlalchemy.exc import SQLAlchemyError

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
from signalops.api.evidence import ReadFailed, ReplayScopeQuery, read_database
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, MAX_KEY_LENGTH
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.investigations import (
    InvestigationAccepted,
    InvestigationRequest,
    InvestigationUsage,
    InvestigationView,
)
from signalops.contracts.workflow import InvestigationStatus
from signalops.persistence.models import Investigation
from signalops.persistence.repositories.investigations import InvestigationRepository
from signalops.persistence.repositories.scope import EvidenceScope

router = APIRouter(prefix="/v1", tags=["investigations"])

# The template, not the path: the stored hash carries the incident.
REQUEST_ROUTE = "POST /v1/incidents/{id}/investigations"

NOT_FOUND: dict[int | str, dict[str, Any]] = {404: {"description": "No such investigation"}}


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, "investigation not found")


def _view(row: Investigation) -> InvestigationView:
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
            return _view(row)
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/detail", error) from None


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
            return _view(row)
    except InvestigationFinishedError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from None
    except SQLAlchemyError as error:
        raise ReadFailed("investigations/cancel", error) from None
