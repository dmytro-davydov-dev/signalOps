"""Workspace and replay-scope boundaries shared by evidence reads.

Every query over scoped evidence filters with `EvidenceScope.clause`, so a
read never crosses into another workspace or replay dataset. Callers build the
scope from the authenticated principal, never from request parameters alone.
"""

import uuid
from dataclasses import dataclass

from sqlalchemy import ColumnElement, and_

from signalops.contracts.common import LIVE_REPLAY_SCOPE
from signalops.persistence.models import Deployment, Signal

ScopedModel = type[Signal] | type[Deployment]


@dataclass(frozen=True)
class EvidenceScope:
    workspace_id: uuid.UUID
    replay_scope: str = LIVE_REPLAY_SCOPE

    def clause(self, model: ScopedModel) -> ColumnElement[bool]:
        return and_(
            model.workspace_id == self.workspace_id,
            model.replay_scope == self.replay_scope,
        )
