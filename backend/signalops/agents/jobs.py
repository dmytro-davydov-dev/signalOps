"""Worker side of durable jobs: claim, heartbeat, and settle (ADR-021).

**Claims.** A claim is its own short transaction, committed before the handler
runs. Each claim spends an attempt, including a takeover after a crash, so a job
that kills its workers stops at `max_attempts`.

**Settling.** A handler that returns completes the job. `PermanentJobError` fails
it. Any other exception retries it after a capped, jittered delay kept in
`available_at`. A job out of attempts fails its investigation, in one transaction
with the `investigation.completed` outbox row.

**Lease loss.** A worker that loses its lease, or cannot reach PostgreSQL before
the lease runs out, cancels the handler and writes nothing more. A write that
races the takeover is rejected by the fence in `JobRepository.hold`.

**Shutdown.** A stopping worker releases its job with the attempt refunded, since
a restart says nothing about the job.

**Stops.** A cancellation or a passed deadline ends the job (ADR-023). A cancelled
one ends `cancelled`; a deadline ends it and its investigation `failed`. The claim,
each heartbeat, and each settling write check, and the engine checks before each
node. A heartbeat that finds a stop interrupts the handler, so a hung call cannot
outlive the check. A job that finished its work past its deadline still succeeds.
"""

import asyncio
import contextlib
import logging
import os
import socket
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Self

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.agents.investigations import end_investigation
from signalops.config import Settings
from signalops.contracts.common import MAX_ERROR_DETAIL_LENGTH
from signalops.contracts.workflow import InvestigationStatus, JobStatus
from signalops.persistence.database import Database
from signalops.persistence.models import Job
from signalops.persistence.repositories.jobs import (
    JobLease,
    JobRepository,
    JobStop,
    LeaseLostError,
)
from signalops.streaming.retry import RetryPolicy

logger = logging.getLogger(__name__)

JobHandler = Callable[[JobLease], Awaitable[None]]


class JobErrorCategory(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNEXPECTED = "unexpected"
    LEASE_EXPIRED = "lease_expired"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    CANCELLED = "cancelled"
    DEADLINE_EXCEEDED = "deadline_exceeded"


class TransientJobError(Exception):
    """Worth another attempt. The message is stored, so it must hold no evidence or secrets."""


class PermanentJobError(Exception):
    """Another attempt cannot help. The message is stored, as for `TransientJobError`."""


class JobStoppedError(Exception):
    """The handler reached a check and found a stop; the job settles by re-reading it."""


@dataclass(frozen=True)
class JobPolicy:
    lease_seconds: float = 30.0
    poll_interval_seconds: float = 1.0
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            lease_seconds=settings.job_lease_seconds,
            poll_interval_seconds=settings.job_poll_interval_ms / 1000,
            retry=RetryPolicy.from_settings(settings),
        )

    @property
    def heartbeat_seconds(self) -> float:
        """Three beats fit in a lease, so one slow or lost beat costs nothing."""
        return self.lease_seconds / 3


def worker_owner() -> str:
    """Unique per process start, so a restarted worker never matches its old leases."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def error_record(error: Exception) -> dict[str, Any]:
    """`last_error` for an exception. An unexpected one's text may quote data: type only."""
    if isinstance(error, TransientJobError | PermanentJobError):
        category = (
            JobErrorCategory.TRANSIENT
            if isinstance(error, TransientJobError)
            else JobErrorCategory.PERMANENT
        )
        return {
            "category": category,
            "type": type(error).__name__,
            "detail": str(error)[:MAX_ERROR_DETAIL_LENGTH],
        }
    return {"category": JobErrorCategory.UNEXPECTED, "type": type(error).__name__}


async def _exhaust(session: AsyncSession, job: Job, cause: dict[str, Any] | None) -> None:
    logger.warning("job out of attempts", extra={"job_id": str(job.id), "attempts": job.attempts})
    await JobRepository(session).finish(
        job.id,
        JobStatus.FAILED,
        {"category": JobErrorCategory.ATTEMPTS_EXHAUSTED, "attempts": job.attempts, "cause": cause},
    )
    await end_investigation(session, job.investigation_id, InvestigationStatus.FAILED)


async def _stop(
    session: AsyncSession, job_id: uuid.UUID, investigation_id: uuid.UUID, reason: JobStop
) -> None:
    """End a locked job and its investigation for this reason."""
    logger.info("job stopped", extra={"job_id": str(job_id), "reason": str(reason)})
    jobs = JobRepository(session)
    if reason is JobStop.CANCELLED:
        await jobs.finish(job_id, JobStatus.CANCELLED, {"category": JobErrorCategory.CANCELLED})
        await end_investigation(session, investigation_id, InvestigationStatus.CANCELLED)
    else:
        await jobs.finish(
            job_id, JobStatus.FAILED, {"category": JobErrorCategory.DEADLINE_EXCEEDED}
        )
        await end_investigation(session, investigation_id, InvestigationStatus.FAILED)


async def claim_job(database: Database, owner: str, policy: JobPolicy) -> JobLease | None:
    """Claim the oldest available job, ending any stopped or out of attempts on the way."""
    while True:
        async with database.transaction() as session:
            jobs = JobRepository(session)
            job = await jobs.next_claimable()
            if job is None:
                return None
            expired = job.status == JobStatus.RUNNING
            error = (
                {
                    "category": JobErrorCategory.LEASE_EXPIRED,
                    "lease_owner": job.lease_owner,
                    "lease_generation": job.lease_generation,
                }
                if expired
                else job.last_error
            )
            reason = await jobs.stop_reason(job.investigation_id)
            if reason is not None:
                await _stop(session, job.id, job.investigation_id, reason)
            elif job.attempts < job.max_attempts:
                lease = await jobs.take(job, owner, policy.lease_seconds, error)
                await jobs.start_investigation(job.investigation_id)
                return lease
            else:
                await _exhaust(session, job, error)


async def complete_job(database: Database, lease: JobLease) -> None:
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.hold(lease)
        if await jobs.stop_reason(lease.investigation_id) is JobStop.CANCELLED:
            await _stop(session, lease.job_id, lease.investigation_id, JobStop.CANCELLED)
        else:
            await jobs.finish(lease.job_id, JobStatus.SUCCEEDED)


async def retry_job(
    database: Database, lease: JobLease, error: Exception, policy: JobPolicy
) -> None:
    """Queue another attempt after a backoff, or fail the job if none remain."""
    record = error_record(error)
    async with database.transaction() as session:
        jobs = JobRepository(session)
        job = await jobs.hold(lease)
        reason = await jobs.stop_reason(lease.investigation_id)
        if reason is not None:
            await _stop(session, job.id, lease.investigation_id, reason)
        elif job.attempts >= job.max_attempts:
            await _exhaust(session, job, record)
        else:
            await jobs.requeue(job.id, policy.retry.delay(job.attempts), record)


async def fail_job(database: Database, lease: JobLease, error: Exception) -> None:
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.hold(lease)
        if await jobs.stop_reason(lease.investigation_id) is JobStop.CANCELLED:
            await _stop(session, lease.job_id, lease.investigation_id, JobStop.CANCELLED)
        else:
            await jobs.finish(lease.job_id, JobStatus.FAILED, error_record(error))
            await end_investigation(session, lease.investigation_id, InvestigationStatus.FAILED)


async def stop_job(database: Database, lease: JobLease) -> None:
    """End a job whose handler found a stop."""
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.hold(lease)
        reason = await jobs.stop_reason(lease.investigation_id)
        if reason is None:
            raise AssertionError("a stop reason, once seen, never clears")
        await _stop(session, lease.job_id, lease.investigation_id, reason)


async def release_job(database: Database, lease: JobLease) -> None:
    """Hand a job back at once, without spending the attempt."""
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.hold(lease)
        await jobs.requeue(lease.job_id, 0, refund_attempt=True)


class JobRunner:
    """Run one job at a time. A failed claim propagates, so the process restarts."""

    def __init__(
        self,
        database: Database,
        handler: JobHandler,
        policy: JobPolicy,
        *,
        owner: str | None = None,
    ) -> None:
        self._database = database
        self._handler = handler
        self._policy = policy
        self._owner = owner or worker_owner()

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("job runner started", extra={"owner": self._owner})
        while not stop.is_set():
            lease = await claim_job(self._database, self._owner, self._policy)
            if lease is not None:
                await self.execute(lease, stop)
                continue
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self._policy.poll_interval_seconds)
        logger.info("job runner stopped", extra={"owner": self._owner})

    async def execute(self, lease: JobLease, stop: asyncio.Event) -> None:
        """Run the handler under heartbeats until it ends, the lease goes, or `stop` is set."""
        loop = asyncio.get_running_loop()
        work = asyncio.ensure_future(self._handler(lease))
        beats = loop.create_task(self._keep_lease(lease))
        stopping = loop.create_task(stop.wait())
        try:
            await asyncio.wait({work, beats, stopping}, return_when=asyncio.FIRST_COMPLETED)
            finished = work.done()
            beat_ended = beats.done()
        finally:
            beats.cancel()
            stopping.cancel()
            work.cancel()
            await asyncio.gather(work, beats, stopping, return_exceptions=True)
        try:
            if finished:
                await self._settle(lease, work)
            elif beat_ended and beats.exception() is None and beats.result() is not None:
                await stop_job(self._database, lease)
            elif beat_ended:
                logger.warning("job lease lost", extra={"job_id": str(lease.job_id)})
            else:
                await release_job(self._database, lease)
        except LeaseLostError:
            logger.warning("job lease lost before settling", extra={"job_id": str(lease.job_id)})

    async def _settle(self, lease: JobLease, work: "asyncio.Future[None]") -> None:
        error = None if work.cancelled() else work.exception()
        if work.cancelled():
            await release_job(self._database, lease)
        elif error is None:
            await complete_job(self._database, lease)
        elif not isinstance(error, Exception):
            raise error
        elif isinstance(error, JobStoppedError):
            await stop_job(self._database, lease)
        elif isinstance(error, PermanentJobError):
            await fail_job(self._database, lease, error)
        else:
            logger.warning(
                "job attempt failed",
                exc_info=error,
                extra={"job_id": str(lease.job_id), "attempt": lease.attempt},
            )
            await retry_job(self._database, lease, error, self._policy)

    async def _keep_lease(self, lease: JobLease) -> JobStop | None:
        """Beat until the job must stop, or the lease is lost or runs out unrenewed (None).

        An unreachable database is not yet a lost lease, so beats keep trying
        until the last renewal's time is up. Another worker may take over from then on.
        """
        loop = asyncio.get_running_loop()
        lease_seconds = self._policy.lease_seconds
        expires = loop.time() + lease_seconds
        while True:
            await asyncio.sleep(self._policy.heartbeat_seconds)
            sent = loop.time()
            try:
                async with asyncio.timeout_at(expires):
                    async with self._database.transaction() as session:
                        jobs = JobRepository(session)
                        await jobs.heartbeat(lease, lease_seconds)
                        reason = await jobs.stop_reason(lease.investigation_id)
            except LeaseLostError:
                return None
            except (SQLAlchemyError, OSError, TimeoutError) as error:
                logger.warning(
                    "job heartbeat failed",
                    extra={"job_id": str(lease.job_id), "error": type(error).__name__},
                )
                if loop.time() >= expires:
                    return None
            else:
                expires = sent + lease_seconds
                if reason is not None:
                    return reason
