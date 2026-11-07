"""Investigation requests.

The response is `202` once the investigation and its outbox request commit; the
API never waits for the workflow. Live scope only, as for status changes: a
replay incident is 404. See `agents/investigations.py` for the key rules.
"""

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Path, Request, status
from sqlalchemy.exc import SQLAlchemyError

from signalops.agents.investigations import (
    IdempotencyConflictError,
    IdempotentRequest,
    InvestigationLimitError,
    InvestigationPolicy,
    request_by_operator,
    request_hash_for,
)
from signalops.api.auth import OperatorWorkspace
from signalops.api.evidence import ReadFailed, read_database
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, MAX_KEY_LENGTH
from signalops.contracts.investigations import InvestigationAccepted, InvestigationRequest
from signalops.contracts.workflow import InvestigationStatus
from signalops.persistence.repositories.scope import EvidenceScope

router = APIRouter(prefix="/v1", tags=["investigations"])

# The template, not the path: the stored hash carries the incident.
REQUEST_ROUTE = "POST /v1/incidents/{id}/investigations"


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
