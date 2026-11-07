"""Job claiming, fencing, and recovery against Compose PostgreSQL (P06-T02).

Each test empties `jobs`, because a claim takes any job in the database. Lease
expiry moves `lease_until` directly instead of sleeping; the runner tests use
real short leases.
"""

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import OperationalError

from signalops.agents.investigations import INVESTIGATION_EVENT_NAMESPACE
from signalops.agents.jobs import (
    JobErrorCategory,
    JobPolicy,
    JobRunner,
    PermanentJobError,
    TransientJobError,
    claim_job,
    complete_job,
    fail_job,
    release_job,
    retry_job,
)
from signalops.contracts.events import EventType
from signalops.contracts.workflow import (
    AgentRole,
    IncidentSeverity,
    InvestigationStatus,
    JobStatus,
    StepStatus,
)
from signalops.detection.rules import Window
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep, Investigation, Job, OutboxEvent
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.streaming.retry import RetryPolicy

pytestmark = pytest.mark.integration

LIVE = EvidenceScope(DEMO_WORKSPACE_ID)
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
POLICY = JobPolicy(lease_seconds=30, retry=RetryPolicy(base_delay_seconds=60, max_delay_seconds=60))
FAST = JobPolicy(lease_seconds=0.6, poll_interval_seconds=0.05)
A, B = "worker-a", "worker-b"


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def add_job(database: db.Database) -> uuid.UUID:
    """A queued investigation with its job; returns the investigation."""
    now = datetime.now(UTC)
    async with database.transaction() as session:
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=LIVE,
                fingerprint=f"env:{uuid.uuid4().hex[:8]}",
                title="High error rate",
                severity=IncidentSeverity.HIGH,
                window=Window(T0, T0 + timedelta(minutes=1)),
                detected_at=T0 + timedelta(minutes=1),
            )
        )
        repository = InvestigationRepository(session)
        investigation = await repository.create(
            NewInvestigation(
                scope=LIVE,
                incident_id=incident_id,
                request_key=f"test:{uuid.uuid4()}",
                workflow_version="v1",
                budget_microusd=1_000,
                max_steps=5,
                created_at=now,
                deadline_at=now + timedelta(minutes=10),
            )
        )
        assert investigation is not None
        assert await repository.enqueue(investigation.id)
        return investigation.id


async def job_of(database: db.Database, investigation_id: uuid.UUID) -> Job:
    async with database.transaction() as session:
        rows = await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        return rows.one()


async def investigation_of(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    async with database.transaction() as session:
        return await session.get_one(Investigation, investigation_id)


async def expire(database: db.Database, investigation_id: uuid.UUID) -> None:
    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.investigation_id == investigation_id)
            .values(lease_until=func.clock_timestamp() - timedelta(seconds=1))
        )


async def claim(database: db.Database, owner: str = A) -> JobLease:
    lease = await claim_job(database, owner, POLICY)
    assert lease is not None
    return lease


async def checkpoint(database: db.Database, lease: JobLease, value: str) -> bool:
    async with database.transaction() as session:
        return await JobRepository(session).save_checkpoint(
            lease, "investigate", AgentRole.INVESTIGATOR, {}, {"value": value}
        )


async def heartbeat(database: db.Database, lease: JobLease) -> None:
    async with database.transaction() as session:
        await JobRepository(session).heartbeat(lease, POLICY.lease_seconds)


async def takeover(database: db.Database, investigation_id: uuid.UUID) -> tuple[JobLease, JobLease]:
    stale = await claim(database, A)
    await expire(database, investigation_id)
    return stale, await claim(database, B)


async def completed_events(database: db.Database, investigation_id: uuid.UUID) -> list[OutboxEvent]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id == investigation_id,
                OutboxEvent.id
                == uuid.uuid5(INVESTIGATION_EVENT_NAMESPACE, f"{investigation_id}:completed"),
            )
        )
        return list(rows.all())


async def test_a_claim_commits_a_running_lease_and_starts_the_investigation(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)

    lease = await claim(database)

    job = await job_of(database, investigation_id)
    assert (job.status, job.lease_owner, job.lease_generation, job.attempts) == (
        JobStatus.RUNNING,
        A,
        1,
        1,
    )
    assert job.lease_until is not None
    assert job.lease_until > datetime.now(UTC)
    assert (lease.generation, lease.attempt, lease.job_id) == (1, 1, job.id)
    investigation = await investigation_of(database, investigation_id)
    assert (investigation.status, investigation.version) == (InvestigationStatus.RUNNING, 2)


async def test_concurrent_workers_claim_one_job_once(database: db.Database) -> None:
    await add_job(database)

    leases = await asyncio.gather(*(claim_job(database, f"worker-{n}", POLICY) for n in range(8)))

    assert sum(lease is not None for lease in leases) == 1


async def test_concurrent_workers_share_jobs_without_overlap(database: db.Database) -> None:
    ids = [await add_job(database) for _ in range(4)]

    leases = await asyncio.gather(*(claim_job(database, f"worker-{n}", POLICY) for n in range(8)))

    claimed = [lease for lease in leases if lease is not None]
    assert sorted(str(lease.investigation_id) for lease in claimed) == sorted(map(str, ids))
    assert len({lease.owner for lease in claimed}) == 4


async def test_concurrent_workers_take_over_an_expired_job_once(database: db.Database) -> None:
    investigation_id = await add_job(database)
    await claim(database, A)
    await expire(database, investigation_id)

    leases = await asyncio.gather(*(claim_job(database, f"worker-{n}", POLICY) for n in range(8)))

    taken = [lease for lease in leases if lease is not None]
    assert [lease.generation for lease in taken] == [2]
    assert (await job_of(database, investigation_id)).lease_owner == taken[0].owner


async def test_a_live_lease_is_not_claimable(database: db.Database) -> None:
    await add_job(database)
    await claim(database, A)

    assert await claim_job(database, B, POLICY) is None


async def test_a_job_is_not_claimable_before_available_at(database: db.Database) -> None:
    investigation_id = await add_job(database)
    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.investigation_id == investigation_id)
            .values(available_at=func.clock_timestamp() + timedelta(minutes=5))
        )

    assert await claim_job(database, A, POLICY) is None


async def test_a_takeover_advances_generation_and_records_why(database: db.Database) -> None:
    investigation_id = await add_job(database)

    _, lease = await takeover(database, investigation_id)

    assert (lease.owner, lease.generation, lease.attempt) == (B, 2, 2)
    job = await job_of(database, investigation_id)
    assert job.last_error == {
        "category": JobErrorCategory.LEASE_EXPIRED,
        "lease_owner": A,
        "lease_generation": 1,
    }
    investigation = await investigation_of(database, investigation_id)
    assert (investigation.status, investigation.version) == (InvestigationStatus.RUNNING, 2)


StaleWrite = Callable[[db.Database, JobLease], Awaitable[Any]]
STALE_WRITES: dict[str, StaleWrite] = {
    "heartbeat": heartbeat,
    "checkpoint": lambda database, lease: checkpoint(database, lease, "stale"),
    "complete": complete_job,
    "retry": lambda database, lease: retry_job(database, lease, TransientJobError("x"), POLICY),
    "fail": lambda database, lease: fail_job(database, lease, PermanentJobError("x")),
    "release": release_job,
}


@pytest.mark.parametrize("write", STALE_WRITES.values(), ids=STALE_WRITES.keys())
async def test_a_stale_worker_cannot_write_after_a_takeover(
    database: db.Database, write: StaleWrite
) -> None:
    investigation_id = await add_job(database)
    stale, current = await takeover(database, investigation_id)
    await checkpoint(database, current, "current")
    before = await job_of(database, investigation_id)

    with pytest.raises(LeaseLostError):
        await write(database, stale)

    job = await job_of(database, investigation_id)
    assert (job.status, job.lease_owner, job.lease_generation, job.lease_until) == (
        before.status,
        before.lease_owner,
        before.lease_generation,
        before.lease_until,
    )
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.RUNNING
    async with database.transaction() as session:
        step = await session.scalar(
            select(AgentStep).where(AgentStep.investigation_id == investigation_id)
        )
    assert step is not None
    assert (step.checkpoint, step.lease_generation) == ({"value": "current"}, 2)


@pytest.mark.parametrize("write", STALE_WRITES.values(), ids=STALE_WRITES.keys())
async def test_the_generation_fences_a_worker_that_reclaims_its_own_job(
    database: db.Database, write: StaleWrite
) -> None:
    investigation_id = await add_job(database)
    stale = await claim(database, A)
    await expire(database, investigation_id)
    await claim(database, A)

    with pytest.raises(LeaseLostError):
        await write(database, stale)


@pytest.mark.parametrize("write", STALE_WRITES.values(), ids=STALE_WRITES.keys())
async def test_the_owner_fences_a_lease_with_the_current_generation(
    database: db.Database, write: StaleWrite
) -> None:
    await add_job(database)
    lease = await claim(database, A)

    with pytest.raises(LeaseLostError):
        await write(database, replace(lease, owner=B))


async def test_the_new_owner_can_finish_after_a_takeover(database: db.Database) -> None:
    investigation_id = await add_job(database)
    _, current = await takeover(database, investigation_id)

    assert await checkpoint(database, current, "current")
    await heartbeat(database, current)
    await complete_job(database, current)

    job = await job_of(database, investigation_id)
    assert (job.status, job.lease_owner, job.lease_until) == (JobStatus.SUCCEEDED, None, None)
    assert await claim_job(database, B, POLICY) is None


async def test_a_fenced_write_in_progress_blocks_the_takeover(database: db.Database) -> None:
    """The row lock orders the write against the claim, so neither is lost."""
    investigation_id = await add_job(database)
    lease = await claim(database, A)
    await expire(database, investigation_id)

    async with database.transaction() as session:
        await JobRepository(session).save_checkpoint(
            lease, "investigate", AgentRole.INVESTIGATOR, {}, {"value": "first"}
        )
        assert await claim_job(database, B, POLICY) is None

    assert (await claim(database, B)).generation == 2


async def test_an_expired_lease_nobody_took_can_still_be_renewed(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database, A)
    await expire(database, investigation_id)

    await heartbeat(database, lease)

    assert await claim_job(database, B, POLICY) is None
    assert await checkpoint(database, lease, "still-mine")


async def test_a_heartbeat_extends_the_lease(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    first = (await job_of(database, investigation_id)).lease_until
    assert first is not None

    await heartbeat(database, lease)

    second = (await job_of(database, investigation_id)).lease_until
    assert second is not None
    assert second > first


async def test_a_checkpoint_does_not_change_a_finished_step(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    assert await checkpoint(database, lease, "first")
    async with database.transaction() as session:
        await session.execute(
            update(AgentStep)
            .where(AgentStep.investigation_id == investigation_id)
            .values(
                status=StepStatus.COMPLETED,
                output={"done": True},
                completed_at=func.clock_timestamp(),
            )
        )

    assert not await checkpoint(database, lease, "second")

    async with database.transaction() as session:
        step = await session.scalar(
            select(AgentStep).where(AgentStep.investigation_id == investigation_id)
        )
    assert step is not None
    assert step.checkpoint == {"value": "first"}


async def test_a_retry_delays_the_next_attempt(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)

    class FixedDelay(RetryPolicy):
        def delay(self, failures: int, rng: random.Random | None = None) -> float:
            return self.ceiling(failures)

    policy = JobPolicy(retry=FixedDelay(base_delay_seconds=3600, max_delay_seconds=3600))
    await retry_job(database, lease, TransientJobError("provider throttled"), policy)

    job = await job_of(database, investigation_id)
    assert (job.status, job.lease_owner, job.lease_until, job.attempts) == (
        JobStatus.QUEUED,
        None,
        None,
        1,
    )
    assert job.last_error == {
        "category": JobErrorCategory.TRANSIENT,
        "type": "TransientJobError",
        "detail": "provider throttled",
    }
    assert job.available_at > datetime.now(UTC) + timedelta(minutes=59)
    assert await claim_job(database, B, POLICY) is None
    investigation = await investigation_of(database, investigation_id)
    assert (investigation.status, investigation.version) == (InvestigationStatus.RUNNING, 2)


async def test_a_retried_job_waits_for_its_delay(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.hold(lease)
        await jobs.requeue(lease.job_id, 60)

    assert await claim_job(database, B, POLICY) is None

    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.investigation_id == investigation_id)
            .values(available_at=func.clock_timestamp())
        )
    next_attempt = await claim(database, B)
    assert (next_attempt.generation, next_attempt.attempt) == (2, 2)


async def test_retries_stop_at_max_attempts_and_fail_the_investigation(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    policy = JobPolicy(retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001))

    for _ in range(3):
        await asyncio.sleep(0.01)
        lease = await claim(database)
        await retry_job(database, lease, TransientJobError("still throttled"), policy)

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts, job.lease_owner) == (JobStatus.FAILED, 3, None)
    assert job.last_error is not None
    assert job.last_error["category"] == JobErrorCategory.ATTEMPTS_EXHAUSTED
    assert job.last_error["cause"]["category"] == JobErrorCategory.TRANSIENT
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.FAILED
    assert investigation.completed_at is not None
    assert await claim_job(database, B, POLICY) is None


async def test_a_crash_loop_stops_at_max_attempts(database: db.Database) -> None:
    investigation_id = await add_job(database)
    for attempt in range(1, 4):
        lease = await claim(database, f"crashed-{attempt}")
        assert lease.attempt == attempt
        await expire(database, investigation_id)

    assert await claim_job(database, B, POLICY) is None

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts, job.lease_owner) == (JobStatus.FAILED, 3, None)
    assert job.last_error is not None
    assert job.last_error["cause"]["lease_owner"] == "crashed-3"
    events = await completed_events(database, investigation_id)
    assert [(event.topic, event.aggregate_version) for event in events] == [
        ("investigations.completed.v1", 3)
    ]
    assert events[0].envelope["event_type"] == EventType.INVESTIGATION_COMPLETED
    assert events[0].envelope["payload"]["status"] == InvestigationStatus.FAILED


async def test_claiming_moves_past_an_exhausted_job(database: db.Database) -> None:
    exhausted = await add_job(database)
    for _ in range(3):
        await claim(database)
        await expire(database, exhausted)
    waiting = await add_job(database)

    lease = await claim(database, B)

    assert lease.investigation_id == waiting
    assert (await job_of(database, exhausted)).status == JobStatus.FAILED


async def test_exhausting_a_job_keeps_a_terminal_investigation(database: db.Database) -> None:
    investigation_id = await add_job(database)
    for _ in range(3):
        await claim(database)
        await expire(database, investigation_id)
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(status=InvestigationStatus.CANCELLED, completed_at=func.clock_timestamp())
        )

    await claim_job(database, B, POLICY)

    assert (await job_of(database, investigation_id)).status == JobStatus.FAILED
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED
    assert await completed_events(database, investigation_id) == []


async def test_a_permanent_failure_ends_the_job_at_once(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)

    await fail_job(database, lease, PermanentJobError("workflow version is unknown"))

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts) == (JobStatus.FAILED, 1)
    assert job.last_error is not None
    assert job.last_error["category"] == JobErrorCategory.PERMANENT
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.FAILED
    assert len(await completed_events(database, investigation_id)) == 1


async def test_a_release_refunds_the_attempt(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database, A)

    await release_job(database, lease)

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts, job.lease_owner) == (JobStatus.QUEUED, 0, None)
    next_lease = await claim(database, B)
    assert (next_lease.generation, next_lease.attempt) == (2, 1)
    with pytest.raises(LeaseLostError):
        await complete_job(database, lease)


async def run_until(
    database: db.Database,
    handler: Callable[[JobLease], Awaitable[None]],
    done: Callable[[], Awaitable[bool]],
    policy: JobPolicy = FAST,
    owner: str = A,
) -> None:
    stop = asyncio.Event()
    runner = asyncio.create_task(JobRunner(database, handler, policy, owner=owner).run(stop))
    try:
        async with asyncio.timeout(10):
            while not await done():  # noqa: ASYNC110 - polling PostgreSQL, not an in-process flag
                await asyncio.sleep(0.02)
    finally:
        stop.set()
        await runner


def job_status_is(
    database: db.Database, investigation_id: uuid.UUID, status: JobStatus
) -> Callable[[], Awaitable[bool]]:
    async def done() -> bool:
        return (await job_of(database, investigation_id)).status == status

    return done


async def test_the_runner_completes_a_job(database: db.Database) -> None:
    investigation_id = await add_job(database)
    seen: list[JobLease] = []

    async def handler(lease: JobLease) -> None:
        seen.append(lease)

    await run_until(
        database, handler, job_status_is(database, investigation_id, JobStatus.SUCCEEDED)
    )

    assert [lease.investigation_id for lease in seen] == [investigation_id]


async def test_the_runner_retries_an_unexpected_error_without_storing_its_text(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    policy = JobPolicy(
        lease_seconds=FAST.lease_seconds,
        poll_interval_seconds=FAST.poll_interval_seconds,
        retry=RetryPolicy(base_delay_seconds=3600, max_delay_seconds=3600),
    )

    async def handler(_lease: JobLease) -> None:
        raise ValueError("token=hunter2 from a quoted log line")

    async def retried() -> bool:
        return (await job_of(database, investigation_id)).last_error is not None

    await run_until(database, handler, retried, policy)

    job = await job_of(database, investigation_id)
    assert job.last_error == {"category": JobErrorCategory.UNEXPECTED, "type": "ValueError"}


async def test_the_runner_fails_a_permanent_error(database: db.Database) -> None:
    investigation_id = await add_job(database)

    async def handler(_lease: JobLease) -> None:
        raise PermanentJobError("no such workflow")

    await run_until(database, handler, job_status_is(database, investigation_id, JobStatus.FAILED))

    assert (await investigation_of(database, investigation_id)).status == (
        InvestigationStatus.FAILED
    )


async def test_heartbeats_keep_a_long_job_from_another_worker(database: db.Database) -> None:
    investigation_id = await add_job(database)
    rival_claims: list[JobLease | None] = []

    async def handler(_lease: JobLease) -> None:
        for _ in range(4):
            await asyncio.sleep(FAST.lease_seconds / 2)
            rival_claims.append(await claim_job(database, B, FAST))

    await run_until(
        database, handler, job_status_is(database, investigation_id, JobStatus.SUCCEEDED)
    )

    assert rival_claims == [None, None, None, None]


async def test_losing_the_lease_cancels_the_handler_and_writes_nothing(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def handler(_lease: JobLease) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def taken_over() -> bool:
        return cancelled.is_set()

    async def steal() -> JobLease:
        await started.wait()
        await expire(database, investigation_id)
        return await claim(database, B)

    thief = asyncio.create_task(steal())
    await run_until(database, handler, taken_over)
    current = await thief

    job = await job_of(database, investigation_id)
    assert (job.status, job.lease_owner, job.lease_generation) == (
        JobStatus.RUNNING,
        B,
        current.generation,
    )


async def test_a_stopping_runner_releases_its_job(database: db.Database) -> None:
    investigation_id = await add_job(database)
    started = asyncio.Event()

    async def handler(_lease: JobLease) -> None:
        started.set()
        await asyncio.sleep(60)

    async def is_started() -> bool:
        return started.is_set()

    await run_until(database, handler, is_started)

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts, job.lease_owner) == (JobStatus.QUEUED, 0, None)


async def test_a_worker_cut_off_from_postgres_stops_when_its_lease_runs_out(
    database: db.Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    await add_job(database)
    loop = asyncio.get_running_loop()
    started_at: list[float] = []
    cancelled_at: list[float] = []

    async def unreachable(*_args: object) -> None:
        raise OperationalError("heartbeat", {}, Exception("connection refused"))

    async def handler(_lease: JobLease) -> None:
        started_at.append(loop.time())
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled_at.append(loop.time())
            raise

    async def stopped() -> bool:
        return bool(cancelled_at)

    monkeypatch.setattr(JobRepository, "heartbeat", unreachable)

    await run_until(database, handler, stopped)

    # Failed beats alone do not stop it, and it does not outlive the lease by a beat.
    held = cancelled_at[0] - started_at[0]
    assert FAST.lease_seconds * 0.9 <= held < FAST.lease_seconds + FAST.heartbeat_seconds
