"""Cancellation and deadlines against Compose PostgreSQL (P06-T04).

The HTTP routes are in `test_investigation_requests.py`, and a real worker process
is cancelled in `test_workflow_recovery.py`.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import delete, func, update

from signalops.agents.investigations import (
    InvestigationFinishedError,
    cancel_investigation,
    end_investigation,
)
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
from signalops.agents.workflow import (
    CONCLUDE,
    INVESTIGATE,
    LOAD_CONTEXT,
    Node,
    NodeContext,
    Workflow,
    WorkflowEngine,
)
from signalops.contracts.common import ContractModel
from signalops.contracts.steps import InvestigatorFindings
from signalops.contracts.workflow import InvestigationStatus, JobStatus, StepStatus
from signalops.persistence import database as db
from signalops.persistence.models import Investigation, Job
from signalops.persistence.repositories.investigations import InvestigationRepository
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError
from tests.integration.job_support import ABSTENTION, LIVE, add_job
from tests.integration.test_job_leases import completed_events, expire, investigation_of, job_of
from tests.integration.test_workflow_engine import claim, counted, run_once, steps_of

pytestmark = pytest.mark.integration

FAST = JobPolicy(lease_seconds=0.6, poll_interval_seconds=0.02)
FINDINGS = InvestigatorFindings(missing_evidence=("Nothing found",))
WAIT_SECONDS = 10.0


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def cancel(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    async with database.transaction() as session:
        row = await cancel_investigation(session, LIVE, investigation_id)
    assert row is not None
    return row


async def pass_deadline(database: db.Database, investigation_id: uuid.UUID) -> None:
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(
                created_at=func.clock_timestamp() - timedelta(hours=2),
                deadline_at=func.clock_timestamp() - timedelta(hours=1),
            )
        )


def engine_with(database: db.Database, *nodes: Node[ContractModel]) -> WorkflowEngine:
    return WorkflowEngine(database, {"v1": Workflow("v1", nodes, CONCLUDE)})


def hanging(entered: asyncio.Event, stopped: list[bool]) -> Node[InvestigatorFindings]:
    async def hang(_context: NodeContext) -> InvestigatorFindings:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stopped.append(True)
            raise
        raise AssertionError("unreachable")

    return replace(INVESTIGATE, run=hang)


async def test_no_job_yet_cancels_at_once_and_a_late_job_never_runs(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    async with database.transaction() as session:
        await session.execute(delete(Job))

    row = await cancel(database, investigation_id)
    assert row.status == InvestigationStatus.CANCELLED
    assert row.completed_at is not None

    # The dispatcher read the investigation before the cancellation committed.
    async with database.transaction() as session:
        assert await InvestigationRepository(session).enqueue(investigation_id)

    assert await claim_job(database, "worker-a", FAST) is None
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    assert [
        event.envelope["payload"]["status"]
        for event in await completed_events(database, investigation_id)
    ] == ["cancelled"]


async def test_a_queued_job_is_cancelled_with_its_investigation(database: db.Database) -> None:
    investigation_id = await add_job(database)

    row = await cancel(database, investigation_id)

    assert row.status == InvestigationStatus.CANCELLED
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    assert await claim_job(database, "worker-a", FAST) is None


async def test_a_job_waiting_to_retry_is_cancelled_at_once(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await retry_job(database, lease, TransientJobError("throttled"), FAST)
    assert (await job_of(database, investigation_id)).status == JobStatus.QUEUED

    row = await cancel(database, investigation_id)

    assert row.status == InvestigationStatus.CANCELLED
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED


async def test_a_running_job_keeps_running_until_its_worker_acts(database: db.Database) -> None:
    investigation_id = await add_job(database)
    await claim(database)

    first = await cancel(database, investigation_id)

    assert first.status == InvestigationStatus.RUNNING
    assert first.cancel_requested_at is not None
    assert first.completed_at is None
    assert (await job_of(database, investigation_id)).status == JobStatus.RUNNING
    assert await completed_events(database, investigation_id) == []

    again = await cancel(database, investigation_id)
    assert (again.cancel_requested_at, again.version) == (first.cancel_requested_at, first.version)


async def test_a_cancellation_during_a_call_starts_no_later_node(database: db.Database) -> None:
    investigation_id = await add_job(database)

    async def cancelled_mid_call(context: NodeContext) -> InvestigatorFindings:
        await cancel(database, context.run.investigation_id)
        return FINDINGS

    downstream, body = counted(CONCLUDE)
    engine = engine_with(
        database, LOAD_CONTEXT, replace(INVESTIGATE, run=cancelled_mid_call), downstream
    )

    await run_once(database, engine)

    assert body.runs == 0
    steps = await steps_of(database, investigation_id)
    assert set(steps) == {"load_context", "investigate"}
    assert steps["investigate"].status == StepStatus.COMPLETED
    job = await job_of(database, investigation_id)
    assert job.status == JobStatus.CANCELLED
    assert job.lease_owner is None
    assert job.last_error == {"category": JobErrorCategory.CANCELLED}
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED
    events = await completed_events(database, investigation_id)
    assert [event.aggregate_version for event in events] == [investigation.version]


async def test_a_heartbeat_interrupts_a_hung_call_on_cancellation(database: db.Database) -> None:
    investigation_id = await add_job(database)
    entered, stopped = asyncio.Event(), list[bool]()
    downstream, body = counted(CONCLUDE)
    engine = engine_with(database, LOAD_CONTEXT, hanging(entered, stopped), downstream)
    lease = await claim_job(database, "worker-a", FAST)
    assert lease is not None

    run = asyncio.ensure_future(
        JobRunner(database, engine, FAST, owner=lease.owner).execute(lease, asyncio.Event())
    )
    await asyncio.wait_for(entered.wait(), WAIT_SECONDS)
    await cancel(database, investigation_id)
    await asyncio.wait_for(run, WAIT_SECONDS)

    assert stopped == [True]
    assert body.runs == 0
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    steps = await steps_of(database, investigation_id)
    assert steps["load_context"].status == StepStatus.COMPLETED
    assert steps["investigate"].status == StepStatus.FAILED
    assert steps["investigate"].completed_at is not None
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED


async def test_a_heartbeat_interrupts_a_hung_call_at_the_deadline(database: db.Database) -> None:
    investigation_id = await add_job(database)
    entered, stopped = asyncio.Event(), list[bool]()
    engine = engine_with(database, LOAD_CONTEXT, hanging(entered, stopped), CONCLUDE)
    lease = await claim_job(database, "worker-a", FAST)
    assert lease is not None

    run = asyncio.ensure_future(
        JobRunner(database, engine, FAST, owner=lease.owner).execute(lease, asyncio.Event())
    )
    await asyncio.wait_for(entered.wait(), WAIT_SECONDS)
    await pass_deadline(database, investigation_id)
    await asyncio.wait_for(run, WAIT_SECONDS)

    assert stopped == [True]
    job = await job_of(database, investigation_id)
    assert job.status == JobStatus.FAILED
    assert job.last_error == {"category": JobErrorCategory.DEADLINE_EXCEEDED}
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.FAILED
    assert [
        event.envelope["payload"]["status"]
        for event in await completed_events(database, investigation_id)
    ] == ["failed"]


async def test_a_passed_deadline_starts_no_later_node(database: db.Database) -> None:
    investigation_id = await add_job(database)

    async def slow(context: NodeContext) -> InvestigatorFindings:
        await pass_deadline(database, context.run.investigation_id)
        return FINDINGS

    downstream, body = counted(CONCLUDE)
    engine = engine_with(database, LOAD_CONTEXT, replace(INVESTIGATE, run=slow), downstream)

    await run_once(database, engine)

    assert body.runs == 0
    job = await job_of(database, investigation_id)
    assert job.status == JobStatus.FAILED
    assert job.last_error == {"category": JobErrorCategory.DEADLINE_EXCEEDED}


async def test_work_finished_past_the_deadline_still_succeeds(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await pass_deadline(database, investigation_id)

    await complete_job(database, lease, ABSTENTION)

    assert (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED
    assert (await investigation_of(database, investigation_id)).status == (
        InvestigationStatus.INSUFFICIENT_EVIDENCE
    )


async def test_a_claim_ends_a_job_past_its_deadline_without_running_it(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    await pass_deadline(database, investigation_id)

    assert await claim_job(database, "worker-a", FAST) is None

    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts) == (JobStatus.FAILED, 0)
    assert job.last_error == {"category": JobErrorCategory.DEADLINE_EXCEEDED}
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.FAILED


async def test_a_claim_finishes_the_cancellation_of_a_dead_worker(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    async with database.transaction() as session:
        await JobRepository(session).save_checkpoint(lease, "investigate", INVESTIGATE.role, {}, {})
    await cancel(database, investigation_id)
    await expire(database, investigation_id)

    assert await claim_job(database, "worker-b", FAST) is None

    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    steps = await steps_of(database, investigation_id)
    assert steps["investigate"].status == StepStatus.FAILED
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED


async def test_a_cancellation_outranks_exhausted_attempts(database: db.Database) -> None:
    investigation_id = await add_job(database)
    for _ in range(3):
        await claim(database)
        await expire(database, investigation_id)
    await cancel(database, investigation_id)

    assert await claim_job(database, "worker-b", FAST) is None

    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED


async def test_a_stale_worker_cannot_write_after_a_cancellation(database: db.Database) -> None:
    investigation_id = await add_job(database)
    stale = await claim(database)
    async with database.transaction() as session:
        await JobRepository(session).save_checkpoint(stale, "investigate", INVESTIGATE.role, {}, {})
    await cancel(database, investigation_id)
    await expire(database, investigation_id)
    assert await claim_job(database, "worker-b", FAST) is None

    await assert_fenced(database, stale)

    step = (await steps_of(database, investigation_id))["investigate"]
    assert (step.status, step.output) == (StepStatus.FAILED, None)
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    assert len(await completed_events(database, investigation_id)) == 1


async def assert_fenced(database: db.Database, stale: JobLease) -> None:
    with pytest.raises(LeaseLostError):
        await complete_job(database, stale, ABSTENTION)
    with pytest.raises(LeaseLostError):
        await fail_job(database, stale, PermanentJobError("late"))
    with pytest.raises(LeaseLostError):
        await retry_job(database, stale, TransientJobError("late"), FAST)
    with pytest.raises(LeaseLostError):
        await release_job(database, stale)
    with pytest.raises(LeaseLostError):
        async with database.transaction() as session:
            await JobRepository(session).complete_step(stale, "investigate", {"late": True}, {})
    with pytest.raises(LeaseLostError):
        async with database.transaction() as session:
            await JobRepository(session).heartbeat(stale, 30)


@pytest.mark.parametrize(
    "settle",
    [
        pytest.param(
            lambda database, lease: complete_job(database, lease, ABSTENTION), id="complete"
        ),
        pytest.param(
            lambda database, lease: fail_job(database, lease, PermanentJobError("no")), id="fail"
        ),
        pytest.param(
            lambda database, lease: retry_job(database, lease, TransientJobError("no"), FAST),
            id="retry",
        ),
    ],
)
async def test_a_pending_cancellation_wins_however_the_attempt_ends(
    database: db.Database, settle: Callable[[db.Database, JobLease], Awaitable[None]]
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await cancel(database, investigation_id)

    await settle(database, lease)

    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED


async def test_a_permanent_failure_past_the_deadline_keeps_its_own_error(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await pass_deadline(database, investigation_id)

    await fail_job(database, lease, PermanentJobError("unknown workflow"))

    job = await job_of(database, investigation_id)
    assert job.last_error is not None
    assert job.last_error["category"] == JobErrorCategory.PERMANENT


async def test_a_stopping_worker_leaves_the_cancellation_to_the_next_claim(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await cancel(database, investigation_id)

    await release_job(database, lease)

    assert (await job_of(database, investigation_id)).status == JobStatus.QUEUED
    assert await claim_job(database, "worker-b", FAST) is None
    assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED


async def test_a_step_left_running_is_closed_when_attempts_run_out(database: db.Database) -> None:
    investigation_id = await add_job(database)
    for _ in range(3):
        lease = await claim(database)
        async with database.transaction() as session:
            await JobRepository(session).save_checkpoint(
                lease, "investigate", INVESTIGATE.role, {}, {}
            )
        await expire(database, investigation_id)

    assert await claim_job(database, "worker-b", FAST) is None

    assert (await steps_of(database, investigation_id))["investigate"].status == StepStatus.FAILED


async def test_ending_an_investigation_twice_writes_one_event(database: db.Database) -> None:
    investigation_id = await add_job(database)
    for status in (InvestigationStatus.CANCELLED, InvestigationStatus.FAILED):
        async with database.transaction() as session:
            await end_investigation(session, investigation_id, status)

    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED
    assert len(await completed_events(database, investigation_id)) == 1


async def test_concurrent_cancellations_end_the_investigation_once(database: db.Database) -> None:
    investigation_id = await add_job(database)

    rows = await asyncio.gather(*(cancel(database, investigation_id) for _ in range(8)))

    assert {row.status for row in rows} == {InvestigationStatus.CANCELLED}
    assert len(await completed_events(database, investigation_id)) == 1


@pytest.mark.parametrize("attempt", range(12))
async def test_a_cancellation_racing_completion_has_one_outcome(
    database: db.Database, attempt: int
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)

    calls = [cancel(database, investigation_id), complete_job(database, lease, ABSTENTION)]
    results = await asyncio.gather(
        *(calls if attempt % 2 else reversed(calls)), return_exceptions=True
    )

    # Whichever committed first decides both rows. A cancellation that lost is refused.
    job = await job_of(database, investigation_id)
    investigation = await investigation_of(database, investigation_id)
    refused = [r for r in results if isinstance(r, InvestigationFinishedError)]
    if job.status == JobStatus.SUCCEEDED:
        assert investigation.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
        assert investigation.diagnosis is not None
        assert len(refused) == 1
    else:
        assert job.status == JobStatus.CANCELLED
        assert investigation.status == InvestigationStatus.CANCELLED
        assert investigation.diagnosis is None
        assert refused == []
    assert len(await completed_events(database, investigation_id)) == 1


@pytest.mark.parametrize("attempt", range(12))
async def test_a_cancellation_racing_a_claim_never_loses_the_request(
    database: db.Database, attempt: int
) -> None:
    investigation_id = await add_job(database)

    calls = [cancel(database, investigation_id), claim_job(database, "worker-a", FAST)]
    results = await asyncio.gather(*(calls if attempt % 2 else reversed(calls)))
    lease = next((result for result in results if isinstance(result, JobLease)), None)

    investigation = await investigation_of(database, investigation_id)
    if lease is None:
        assert investigation.status == InvestigationStatus.CANCELLED
        assert (await job_of(database, investigation_id)).status == JobStatus.CANCELLED
    else:
        assert investigation.status == InvestigationStatus.RUNNING
        assert investigation.cancel_requested_at is not None


async def test_steps_keep_their_completed_output_after_a_cancellation(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    async with database.transaction() as session:
        jobs = JobRepository(session)
        await jobs.save_checkpoint(lease, "investigate", INVESTIGATE.role, {}, {})
        await jobs.complete_step(lease, "investigate", {"kept": True}, {})
    await cancel(database, investigation_id)
    await stop_with_lease_gone(database, investigation_id)

    step = (await steps_of(database, investigation_id))["investigate"]
    assert (step.status, step.output) == (StepStatus.COMPLETED, {"kept": True})


async def stop_with_lease_gone(database: db.Database, investigation_id: uuid.UUID) -> None:
    await expire(database, investigation_id)
    assert await claim_job(database, "worker-b", FAST) is None
