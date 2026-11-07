"""The workflow engine against Compose PostgreSQL (P06-T03)."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace

import pytest
from sqlalchemy import delete, select, update

from signalops.agents.jobs import (
    JobErrorCategory,
    JobPolicy,
    JobRunner,
    PermanentJobError,
    TransientJobError,
    claim_job,
)
from signalops.agents.workflow import (
    CONCLUDE,
    INVESTIGATE,
    LOAD_CONTEXT,
    WORKFLOWS,
    Node,
    NodeContext,
    Workflow,
    WorkflowEngine,
)
from signalops.contracts.common import ContractModel
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.steps import IncidentBrief, InvestigatorFindings
from signalops.contracts.workflow import InvestigationStatus, JobStatus, StepStatus
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep, Investigation, Job
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError
from signalops.streaming.retry import RetryPolicy
from tests.integration.job_support import add_job
from tests.integration.test_job_leases import expire

pytestmark = pytest.mark.integration

POLICY = JobPolicy(
    lease_seconds=30,
    poll_interval_seconds=0.02,
    retry=RetryPolicy(base_delay_seconds=0.001, max_delay_seconds=0.001),
)
FINDINGS = InvestigatorFindings(missing_evidence=("Nothing found",))


class Counted[T: ContractModel]:
    """A node body that counts runs and fails as told."""

    def __init__(self, result: Callable[[NodeContext], Awaitable[T]]) -> None:
        self.runs = 0
        self.failures: list[Exception] = []
        self._result = result

    async def __call__(self, context: NodeContext) -> T:
        self.runs += 1
        if self.failures:
            raise self.failures.pop(0)
        return await self._result(context)


def counted[T: ContractModel](node: Node[T]) -> tuple[Node[T], Counted[T]]:
    body = Counted(node.run)
    return replace(node, run=body), body


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def claim(database: db.Database, owner: str = "worker-a") -> JobLease:
    lease = await claim_job(database, owner, POLICY)
    assert lease is not None
    return lease


async def steps_of(database: db.Database, investigation_id: uuid.UUID) -> dict[str, AgentStep]:
    async with database.transaction() as session:
        steps = await JobRepository(session).steps_of(investigation_id)
        return {step.step_key: step for step in steps}


async def job_of(database: db.Database, investigation_id: uuid.UUID) -> Job:
    async with database.transaction() as session:
        return (
            await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        ).one()


async def investigation_of(database: db.Database, investigation_id: uuid.UUID) -> Investigation:
    async with database.transaction() as session:
        return await session.get_one(Investigation, investigation_id)


async def run_once(database: db.Database, engine: WorkflowEngine) -> None:
    """Claim the next job and run it to one settled outcome."""
    lease = await claim(database)
    await JobRunner(database, engine, POLICY, owner=lease.owner).execute(lease, asyncio.Event())


async def test_the_sequence_runs_each_node_in_order_and_stores_validated_output(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)

    await run_once(database, WorkflowEngine(database))

    steps = await steps_of(database, investigation_id)
    assert set(steps) == {"load_context", "investigate", "conclude"}
    assert all(step.status == StepStatus.COMPLETED for step in steps.values())
    assert steps["load_context"].input == {"after": []}
    assert steps["conclude"].input == {"after": ["load_context", "investigate"]}
    assert all(step.lease_generation == 1 and step.attempt == 1 for step in steps.values())
    brief = IncidentBrief.model_validate(steps["load_context"].output)
    assert brief.incident_id == (await investigation_of(database, investigation_id)).incident_id
    diagnosis = Diagnosis.model_validate(steps["conclude"].output)
    assert diagnosis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE
    assert diagnosis.summary == "No diagnosis for: High error rate"
    assert (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED


async def test_a_transient_failure_retries_only_the_unfinished_nodes(
    database: db.Database,
) -> None:
    first, first_body = counted(LOAD_CONTEXT)
    second, second_body = counted(INVESTIGATE)
    third, third_body = counted(CONCLUDE)
    second_body.failures.append(TransientJobError("throttled"))
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (first, second, third))})
    investigation_id = await add_job(database)

    await run_once(database, engine)

    steps = await steps_of(database, investigation_id)
    assert steps["investigate"].status == StepStatus.FAILED
    assert steps["investigate"].completed_at is not None
    assert steps["investigate"].checkpoint is not None
    assert steps["investigate"].checkpoint["state"]["error"]["detail"] == "throttled"
    assert (await job_of(database, investigation_id)).status == JobStatus.QUEUED

    await asyncio.sleep(0.01)
    await run_once(database, engine)

    steps = await steps_of(database, investigation_id)
    assert (first_body.runs, second_body.runs, third_body.runs) == (1, 2, 1)
    assert steps["load_context"].attempt == 1
    assert (steps["investigate"].status, steps["investigate"].attempt) == (
        StepStatus.COMPLETED,
        2,
    )
    checkpoint = steps["investigate"].checkpoint
    assert checkpoint is not None
    assert (checkpoint["state"], checkpoint["attempt"], checkpoint["lease_generation"]) == (
        {},
        2,
        2,
    )
    assert (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED


async def test_a_retried_failure_does_not_leak_the_exception_text(database: db.Database) -> None:
    node, body = counted(INVESTIGATE)
    body.failures.append(RuntimeError("log line with a secret"))
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (LOAD_CONTEXT, node))})
    investigation_id = await add_job(database)

    await run_once(database, engine)

    checkpoint = (await steps_of(database, investigation_id))["investigate"].checkpoint
    assert checkpoint is not None
    assert checkpoint["state"]["error"] == {
        "category": JobErrorCategory.UNEXPECTED,
        "type": "RuntimeError",
    }


async def test_a_permanent_failure_fails_the_step_job_and_investigation(
    database: db.Database,
) -> None:
    node, body = counted(INVESTIGATE)
    body.failures.append(PermanentJobError("no such tool"))
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (LOAD_CONTEXT, node, CONCLUDE))})
    investigation_id = await add_job(database)

    await run_once(database, engine)

    steps = await steps_of(database, investigation_id)
    assert steps["investigate"].status == StepStatus.FAILED
    assert "conclude" not in steps
    assert (await job_of(database, investigation_id)).status == JobStatus.FAILED
    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.FAILED


async def test_a_job_with_every_step_complete_runs_nothing(database: db.Database) -> None:
    """A crash between the last step and `complete_job`."""
    first, first_body = counted(LOAD_CONTEXT)
    second, second_body = counted(INVESTIGATE)
    third, third_body = counted(CONCLUDE)
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (first, second, third))})
    investigation_id = await add_job(database)

    await engine(await claim(database))
    await expire(database, investigation_id)
    await run_once(database, engine)

    assert (first_body.runs, second_body.runs, third_body.runs) == (1, 1, 1)
    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts) == (JobStatus.SUCCEEDED, 2)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"output": {"incident_id": "not-a-uuid"}}, "step load_context cannot be reused"),
        ({"checkpoint": {"schema_version": 99}}, "step load_context cannot be reused"),
        ({"checkpoint": None}, "step load_context cannot be reused"),
    ],
)
async def test_unusable_completed_output_fails_instead_of_running_again(
    database: db.Database, changes: dict[str, object], message: str
) -> None:
    node, body = counted(LOAD_CONTEXT)
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (node, INVESTIGATE))})
    investigation_id = await add_job(database)
    await engine(await claim(database))
    async with database.transaction() as session:
        await session.execute(
            update(AgentStep)
            .where(AgentStep.investigation_id == investigation_id)
            .where(AgentStep.step_key == "load_context")
            .values(**changes)
        )
    await expire(database, investigation_id)

    with pytest.raises(PermanentJobError, match=message):
        await engine(await claim(database, "worker-b"))

    assert body.runs == 1


async def test_a_step_from_another_workflow_version_is_not_reused(database: db.Database) -> None:
    investigation_id = await add_job(database)
    await WorkflowEngine(database)(await claim(database))
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(workflow_version="v2")
        )
    await expire(database, investigation_id)
    engine = WorkflowEngine(database, {"v2": replace(WORKFLOWS["v1"], version="v2")})

    with pytest.raises(PermanentJobError, match="written by workflow v1"):
        await engine(await claim(database, "worker-b"))


async def test_an_unknown_workflow_version_fails_the_job(database: db.Database) -> None:
    investigation_id = await add_job(database, workflow_version="v99")

    await run_once(database, WorkflowEngine(database))

    job = await job_of(database, investigation_id)
    assert job.status == JobStatus.FAILED
    assert job.last_error is not None
    assert job.last_error["detail"] == "unknown workflow version v99"


async def test_max_steps_stops_before_the_next_node_runs(database: db.Database) -> None:
    node, body = counted(CONCLUDE)
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (LOAD_CONTEXT, INVESTIGATE, node))})
    investigation_id = await add_job(database, max_steps=2)

    await run_once(database, engine)

    assert body.runs == 0
    assert set(await steps_of(database, investigation_id)) == {"load_context", "investigate"}
    job = await job_of(database, investigation_id)
    assert job.status == JobStatus.FAILED
    assert job.last_error is not None
    assert job.last_error["detail"] == "max_steps 2 reached before conclude"


async def test_a_stale_worker_cannot_complete_a_step_after_a_takeover(
    database: db.Database,
) -> None:
    """The stale node finishes after a new owner took the job: its output is dropped."""
    entered, released = asyncio.Event(), asyncio.Event()

    async def slow(_context: NodeContext) -> InvestigatorFindings:
        entered.set()
        await released.wait()
        return InvestigatorFindings(missing_evidence=("From the stale worker",))

    stale_engine = WorkflowEngine(
        database, {"v1": Workflow("v1", (LOAD_CONTEXT, replace(INVESTIGATE, run=slow)))}
    )
    investigation_id = await add_job(database)
    stale = asyncio.create_task(stale_engine(await claim(database)))
    await entered.wait()
    await expire(database, investigation_id)

    await WorkflowEngine(database)(await claim(database, "worker-b"))
    released.set()

    with pytest.raises(LeaseLostError):
        await stale

    step = (await steps_of(database, investigation_id))["investigate"]
    assert step.output == {"missing_evidence": ["No model-backed investigator is configured"]}
    assert step.lease_generation == 2


async def test_a_stale_worker_cannot_open_a_step_after_a_takeover(database: db.Database) -> None:
    investigation_id = await add_job(database)
    stale = await claim(database)
    await expire(database, investigation_id)
    await claim(database, "worker-b")

    with pytest.raises(LeaseLostError):
        await WorkflowEngine(database)(stale)

    assert await steps_of(database, investigation_id) == {}


async def test_a_stale_failure_is_rejected_too(database: db.Database) -> None:
    entered, released = asyncio.Event(), asyncio.Event()

    async def failing(_context: NodeContext) -> InvestigatorFindings:
        entered.set()
        await released.wait()
        raise TransientJobError("too late")

    engine = WorkflowEngine(
        database, {"v1": Workflow("v1", (LOAD_CONTEXT, replace(INVESTIGATE, run=failing)))}
    )
    investigation_id = await add_job(database)
    stale = asyncio.create_task(engine(await claim(database)))
    await entered.wait()
    await expire(database, investigation_id)
    await claim(database, "worker-b")
    released.set()

    with pytest.raises(LeaseLostError):
        await stale

    step = (await steps_of(database, investigation_id))["investigate"]
    assert step.status == StepStatus.RUNNING


async def test_a_stopped_worker_leaves_the_step_for_the_next_one(database: db.Database) -> None:
    entered = asyncio.Event()

    async def hang(_context: NodeContext) -> InvestigatorFindings:
        entered.set()
        await asyncio.Event().wait()
        return FINDINGS

    hanging = WorkflowEngine(
        database,
        {"v1": Workflow("v1", (LOAD_CONTEXT, replace(INVESTIGATE, run=hang), CONCLUDE))},
    )
    investigation_id = await add_job(database)
    stop = asyncio.Event()
    lease = await claim(database)
    running = asyncio.create_task(JobRunner(database, hanging, POLICY).execute(lease, stop))
    await entered.wait()
    stop.set()
    await running

    assert (await steps_of(database, investigation_id))["investigate"].status == StepStatus.RUNNING
    job = await job_of(database, investigation_id)
    assert (job.status, job.attempts) == (JobStatus.QUEUED, 0)

    await run_once(database, WorkflowEngine(database))

    steps = await steps_of(database, investigation_id)
    assert all(step.status == StepStatus.COMPLETED for step in steps.values())
    assert steps["load_context"].attempt == 1
    assert steps["investigate"].attempt == 1


async def test_completed_output_cannot_be_overwritten(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await WorkflowEngine(database)(lease)
    before = (await steps_of(database, investigation_id))["conclude"]

    async with database.transaction() as session:
        jobs = JobRepository(session)
        assert not await jobs.complete_step(lease, "conclude", {"other": 1}, {})
        assert not await jobs.fail_step(lease, "conclude", {})
        assert not await jobs.complete_step(lease, "never_opened", {"other": 1}, {})

    after = (await steps_of(database, investigation_id))["conclude"]
    assert (after.status, after.output, after.completed_at) == (
        StepStatus.COMPLETED,
        before.output,
        before.completed_at,
    )


async def test_the_lease_that_ends_a_step_is_recorded(database: db.Database) -> None:
    investigation_id = await add_job(database)
    stale = await claim(database)
    async with database.transaction() as session:
        await JobRepository(session).save_checkpoint(stale, "investigate", INVESTIGATE.role, {}, {})
    await expire(database, investigation_id)
    lease = await claim(database, "worker-b")

    async with database.transaction() as session:
        assert await JobRepository(session).complete_step(lease, "investigate", {"done": 1}, {})

    assert (await steps_of(database, investigation_id))["investigate"].lease_generation == 2
