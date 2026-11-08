"""Operator authentication for every route but ingestion and health (ADR-014, ADR-038).

The presented bearer token must match an operator credential
(`api/credentials.py`); its workspace bounds every query the request makes, and
no route takes a workspace parameter. Source credentials are a disjoint set, so
a source token never opens an operator route. The matching credential's ID is
kept on the request for the audit line (`api/request_ids.py`).
"""

import uuid
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from signalops.api.credentials import CredentialStore

operator_bearer = HTTPBearer(
    scheme_name="OperatorBearer", auto_error=False, description="Operator token for one workspace"
)


def operator_workspace(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(operator_bearer)],
) -> uuid.UUID:
    """The workspace of the presented operator credential; 401 without a valid one."""
    store: CredentialStore = request.app.state.credentials
    match = None if credentials is None else store.operator(credentials.credentials)
    if match is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing or invalid operator credential",
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.state.credential_id = match.credential_id
    return match.principal


OperatorWorkspace = Annotated[uuid.UUID, Depends(operator_workspace)]
