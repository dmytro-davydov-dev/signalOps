"""Claiming and fencing durable jobs.

Lease times come from PostgreSQL's clock, so skew between workers cannot decide
who owns a job.

`hold` is the fence: it locks the job row after checking owner and generation,
and the write it guards commits in the same transaction. A claim skips a locked
row, so a takeover cannot slip between the check and the write. A lease past
`lease_until` that nobody has taken stays valid: its generation is still current.

Writers lock a job before its investigation, so they cannot deadlock.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.workflow import (
    TERMINAL_INVESTIGATION_STATUSES,
    AgentRole,
    InvestigationStatus,
    JobStatus,
    StepStatus,
)
from signalops.persistence.models import AgentStep, Investigation, Job


class LeaseLostError(Exception):
    """The job is no longer held at this owner and generation."""


class JobStop(StrEnum):
    """Why a job must end whatever its handler is doing."""

    CANCELLED = "cancelled"
    DEADLINE_EXCEEDED = "deadline_exceeded"


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

    async def locked_for(self, investigation_id: uuid.UUID) -> Job | None:
        return await self._session.scalar(
            select(Job)
            .where(Job.investigation_id == investigation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def stop_reason(self, investigation_id: uuid.UUID) -> JobStop | None:
        """Why the investigation's job must stop, by PostgreSQL's clock; cancellation first.

        Both reasons are permanent. Under the job lock a cancellation cannot land
        between this read and the write it decides.
        """
        status, cancel_requested_at, overdue = (
            await self._session.execute(
                select(
                    Investigation.status,
                    Investigation.cancel_requested_at,
                    Investigation.deadline_at <= func.clock_timestamp(),
                ).where(Investigation.id == investigation_id)
            )
        ).one()
        if cancel_requested_at is not None:
            return JobStop.CANCELLED
        # A cancellation that raced the dispatcher leaves a job with nothing to do.
        if InvestigationStatus(status) in TERMINAL_INVESTIGATION_STATUSES:
            return JobStop.CANCELLED
        return JobStop.DEADLINE_EXCEEDED if overdue else None

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
        """Fenced open of a step, or of a retry of one; False when it has completed."""
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
                    "status": StepStatus.RUNNING,
                    "attempt": statement.excluded.attempt,
                    "checkpoint": statement.excluded.checkpoint,
                    "lease_generation": statement.excluded.lease_generation,
                    "completed_at": None,
                },
                where=AgentStep.status.in_((StepStatus.RUNNING, StepStatus.FAILED)),
            ).returning(AgentStep.id)
        )
        return saved is not None

    async def steps_of(self, investigation_id: uuid.UUID) -> Sequence[AgentStep]:
        rows = await self._session.scalars(
            select(AgentStep).where(AgentStep.investigation_id == investigation_id)
        )
        return rows.all()

    async def complete_step(
        self,
        lease: JobLease,
        step_key: str,
        output: dict[str, Any],
        checkpoint: dict[str, Any],
    ) -> bool:
        """Fenced end of a running step, with the output a resume reuses."""
        return await self._end_step(
            lease,
            step_key,
            status=StepStatus.COMPLETED,
            output=output,
            checkpoint=checkpoint,
        )

    async def fail_step(self, lease: JobLease, step_key: str, checkpoint: dict[str, Any]) -> bool:
        """Fenced end of a running step that raised; a retry reopens it."""
        return await self._end_step(
            lease, step_key, status=StepStatus.FAILED, checkpoint=checkpoint
        )

    async def abandon_steps(self, investigation_id: uuid.UUID) -> None:
        """Fail steps left running by an investigation that has ended."""
        await self._session.execute(
            update(AgentStep)
            .where(
                AgentStep.investigation_id == investigation_id,
                AgentStep.status == StepStatus.RUNNING,
            )
            .values(status=StepStatus.FAILED, completed_at=func.clock_timestamp())
        )

    async def _end_step(self, lease: JobLease, step_key: str, **values: Any) -> bool:
        await self.hold(lease)
        ended = await self._session.scalar(
            update(AgentStep)
            .where(
                AgentStep.investigation_id == lease.investigation_id,
                AgentStep.step_key == step_key,
                AgentStep.status == StepStatus.RUNNING,
            )
            .values(
                lease_generation=lease.generation,
                completed_at=func.clock_timestamp(),
                **values,
            )
            .returning(AgentStep.id)
        )
        return ended is not None
