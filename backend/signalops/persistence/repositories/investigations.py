"""Investigation requests, their idempotency keys, and durable jobs.

`create` and `enqueue` insert with ON CONFLICT DO NOTHING. A concurrent insert of
the same key waits for the other transaction and then reports a duplicate, so
two callers never both publish a request or queue a job.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.persistence.models import IdempotencyKey, Investigation, Job
from signalops.persistence.repositories.scope import EvidenceScope


@dataclass(frozen=True)
class NewInvestigation:
    scope: EvidenceScope
    incident_id: uuid.UUID
    request_key: str
    workflow_version: str
    budget_microusd: int
    max_steps: int
    created_at: datetime
    deadline_at: datetime
    # Defaults to `created_at`. An evaluation replay sets the incident's detection
    # time, in event time, so tools see what was known when it was detected.
    evidence_cutoff_at: datetime | None = None


class InvestigationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, new: NewInvestigation) -> Investigation | None:
        """Insert a queued investigation; None when `request_key` already has one.

        Tools read nothing after the evidence cutoff, `created_at` unless given.
        """
        investigation_id = await self._session.scalar(
            insert(Investigation)
            .values(
                workspace_id=new.scope.workspace_id,
                replay_scope=new.scope.replay_scope,
                incident_id=new.incident_id,
                request_key=new.request_key,
                workflow_version=new.workflow_version,
                evidence_cutoff_at=new.evidence_cutoff_at or new.created_at,
                budget_microusd=new.budget_microusd,
                max_steps=new.max_steps,
                created_at=new.created_at,
                deadline_at=new.deadline_at,
            )
            .on_conflict_do_nothing(index_elements=[Investigation.request_key])
            .returning(Investigation.id)
        )
        if investigation_id is None:
            return None
        return await self._session.get_one(Investigation, investigation_id)

    async def get(self, scope: EvidenceScope, investigation_id: uuid.UUID) -> Investigation | None:
        return await self._session.scalar(
            select(Investigation).where(
                scope.clause(Investigation), Investigation.id == investigation_id
            )
        )

    async def for_incident(
        self, scope: EvidenceScope, incident_id: uuid.UUID, *, limit: int
    ) -> Sequence[Investigation]:
        """The incident's investigations in scope, newest first, ties by ID descending."""
        rows = await self._session.scalars(
            select(Investigation)
            .where(scope.clause(Investigation), Investigation.incident_id == incident_id)
            .order_by(Investigation.created_at.desc(), Investigation.id.desc())
            .limit(limit)
        )
        return rows.all()

    async def locked(
        self, scope: EvidenceScope, investigation_id: uuid.UUID
    ) -> Investigation | None:
        """The row, freshly read and locked. Lock its job first, as workers do."""
        return await self._session.scalar(
            select(Investigation)
            .where(scope.clause(Investigation), Investigation.id == investigation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def find_key(
        self, workspace_id: uuid.UUID, route: str, key: str
    ) -> IdempotencyKey | None:
        return await self._session.get(IdempotencyKey, (workspace_id, route, key))

    async def add_key(
        self,
        workspace_id: uuid.UUID,
        route: str,
        key: str,
        request_hash: str,
        investigation_id: uuid.UUID,
    ) -> None:
        self._session.add(
            IdempotencyKey(
                workspace_id=workspace_id,
                route=route,
                key=key,
                request_hash=request_hash,
                investigation_id=investigation_id,
            )
        )
        await self._session.flush()

    async def enqueue(self, investigation_id: uuid.UUID) -> bool:
        """Create the investigation's job; False when it already has one."""
        job_id = await self._session.scalar(
            insert(Job)
            .values(investigation_id=investigation_id)
            .on_conflict_do_nothing(index_elements=[Job.investigation_id])
            .returning(Job.id)
        )
        return job_id is not None
