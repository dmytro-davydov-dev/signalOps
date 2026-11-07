"""Claiming and fencing durable jobs.

Lease times come from PostgreSQL's clock, so skew between workers cannot decide
who owns a job.

`hold` is the fence: it locks the job row after checking owner and generation,
and the write it guards commits in the same transaction. A claim skips a locked
row, so a takeover cannot slip between the check and the write. A lease past
`lease_until` that nobody has taken stays valid: its generation is still current.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.workflow import AgentRole, InvestigationStatus, JobStatus, StepStatus
from signalops.persistence.models import AgentStep, Investigation, Job


class LeaseLostError(Exception):
    """The job is no longer held at this owner and generation."""


@dataclass(frozen=True)
class JobLease:
    job_id: uuid.UUID
    investigation_id: uuid.UUID
    owner: str
    generation: int
    # Which claim of the job this is, from 1.
    attempt: int


def _in_seconds(seconds: float) -> Any:
    return func.clock_timestamp() + timedelta(seconds=seconds)


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def next_claimable(self) -> Job | None:
        """Lock the job waiting longest: queued and due, or running on an expired lease."""
        now = func.clock_timestamp()
        return await self._session.scalar(
            select(Job)
            .where(
                or_(
                    and_(Job.status == JobStatus.QUEUED, Job.available_at <= now),
                    and_(Job.status == JobStatus.RUNNING, Job.lease_until <= now),
                )
            )
            .order_by(func.coalesce(Job.lease_until, Job.available_at))
            .limit(1)
            .with_for_update(skip_locked=True)
        )

    async def take(
        self, job: Job, owner: str, lease_seconds: float, last_error: dict[str, Any] | None
    ) -> JobLease:
        """Give a locked job to `owner`, spending one attempt."""
        generation, attempt = (
            await self._session.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    status=JobStatus.RUNNING,
                    lease_owner=owner,
                    lease_until=_in_seconds(lease_seconds),
                    lease_generation=Job.lease_generation + 1,
                    attempts=Job.attempts + 1,
                    last_error=last_error,
                )
                .returning(Job.lease_generation, Job.attempts)
            )
        ).one()
        return JobLease(job.id, job.investigation_id, owner, generation, attempt)

    async def heartbeat(self, lease: JobLease, lease_seconds: float) -> None:
        extended = await self._session.scalar(
            update(Job)
            .where(
                Job.id == lease.job_id,
                Job.status == JobStatus.RUNNING,
                Job.lease_owner == lease.owner,
                Job.lease_generation == lease.generation,
            )
            .values(lease_until=_in_seconds(lease_seconds))
            .returning(Job.id)
        )
        if extended is None:
            raise LeaseLostError(f"job {lease.job_id} generation {lease.generation}")

    async def hold(self, lease: JobLease) -> Job:
        """Lock the job for a fenced write in this transaction, or raise `LeaseLostError`."""
        job = await self._session.scalar(
            select(Job)
            .where(
                Job.id == lease.job_id,
                Job.status == JobStatus.RUNNING,
                Job.lease_owner == lease.owner,
                Job.lease_generation == lease.generation,
            )
            .with_for_update()
        )
        if job is None:
            raise LeaseLostError(f"job {lease.job_id} generation {lease.generation}")
        return job

    async def finish(
        self, job_id: uuid.UUID, status: JobStatus, last_error: dict[str, Any] | None = None
    ) -> None:
        """Move a locked job to a terminal status, keeping the error of its last attempt."""
        values: dict[str, Any] = {"status": status, "lease_owner": None, "lease_until": None}
        if last_error is not None:
            values["last_error"] = last_error
        await self._session.execute(update(Job).where(Job.id == job_id).values(**values))

    async def requeue(
        self,
        job_id: uuid.UUID,
        delay_seconds: float,
        last_error: dict[str, Any] | None = None,
        *,
        refund_attempt: bool = False,
    ) -> None:
        """Queue a held job again, due after the delay."""
        values: dict[str, Any] = {
            "status": JobStatus.QUEUED,
            "lease_owner": None,
            "lease_until": None,
            "available_at": _in_seconds(delay_seconds),
            "attempts": Job.attempts - 1 if refund_attempt else Job.attempts,
        }
        if last_error is not None:
            values["last_error"] = last_error
        await self._session.execute(update(Job).where(Job.id == job_id).values(**values))

    async def start_investigation(self, investigation_id: uuid.UUID) -> None:
        """Mark a queued investigation running; a retried one is running already."""
        await self._session.execute(
            update(Investigation)
            .where(
                Investigation.id == investigation_id,
                Investigation.status == InvestigationStatus.QUEUED,
            )
            .values(status=InvestigationStatus.RUNNING, version=Investigation.version + 1)
        )

    async def save_checkpoint(
        self,
        lease: JobLease,
        step_key: str,
        agent_role: AgentRole,
        step_input: dict[str, Any],
        checkpoint: dict[str, Any],
    ) -> bool:
        """Fenced upsert of a running step; False when the step has already finished."""
        await self.hold(lease)
        statement = insert(AgentStep).values(
            investigation_id=lease.investigation_id,
            step_key=step_key,
            agent_role=agent_role,
            status=StepStatus.RUNNING,
            attempt=lease.attempt,
            input=step_input,
            checkpoint=checkpoint,
            lease_generation=lease.generation,
            started_at=func.clock_timestamp(),
        )
        saved = await self._session.scalar(
            statement.on_conflict_do_update(
                index_elements=[AgentStep.investigation_id, AgentStep.step_key],
                set_={
                    "attempt": statement.excluded.attempt,
                    "checkpoint": statement.excluded.checkpoint,
                    "lease_generation": statement.excluded.lease_generation,
                },
                where=AgentStep.status == StepStatus.RUNNING,
            ).returning(AgentStep.id)
        )
        return saved is not None
