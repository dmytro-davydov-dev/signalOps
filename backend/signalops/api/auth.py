"""Operator credentials for the read APIs (interim mechanism, ADR-014).

Each configured token belongs to one workspace, and that workspace bounds
every query the request makes. Source ingestion tokens are a separate set:
settings refuse a token present in both, so a source credential never opens
an operator route.
"""

import hashlib
import hmac
import uuid
from collections.abc import Mapping
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import SecretStr

operator_bearer = HTTPBearer(
    scheme_name="OperatorBearer", auto_error=False, description="Operator token for one workspace"
)


class OperatorTokens:
    """Workspace per operator token, compared as SHA-256 digests in constant time."""

    def __init__(self, tokens: Mapping[uuid.UUID, SecretStr]) -> None:
        self._digests = [
            (workspace_id, _digest(token.get_secret_value()))
            for workspace_id, token in tokens.items()
        ]

    def workspace_for(self, presented: str) -> uuid.UUID | None:
        presented_digest = _digest(presented)
        found: uuid.UUID | None = None
        # Compare against every token, so timing does not reveal which one matched.
        for workspace_id, expected in self._digests:
            if hmac.compare_digest(presented_digest, expected):
                found = workspace_id
        return found


def _digest(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


def operator_workspace(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(operator_bearer)],
) -> uuid.UUID:
    """The workspace of the presented operator token; 401 without a valid one."""
    tokens: OperatorTokens = request.app.state.operator_tokens
    workspace_id = None if credentials is None else tokens.workspace_for(credentials.credentials)
    if workspace_id is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing or invalid operator credential",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return workspace_id


OperatorWorkspace = Annotated[uuid.UUID, Depends(operator_workspace)]
