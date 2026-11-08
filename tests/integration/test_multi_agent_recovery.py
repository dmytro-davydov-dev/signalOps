"""Workflow `v3` survives killed workers and stale writers (P09-T04).

Real worker processes are killed with SIGKILL at three points: after branches have
completed, while another branch's model call is in flight, and after the conclusion
is stored but before the job ends. `signalops worker` then reclaims each job with a
newer lease generation. An in-process takeover shows a stale worker's writes fail.
"""

import asyncio
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, delete, select, update

from signalops.agents.jobs import JobErrorCategory, JobRunner, claim_job, complete_job
from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.roles import AgentEnding, SpecialistFindings
from signalops.contracts.workflow import (
    AgentRole,
    InvestigationStatus,
    JobStatus,
    LlmCallStatus,
    StepStatus,
)
from signalops.gateway.fake import FakeProvider
from signalops.gateway.fake_roles import role_step
from signalops.gateway.types import ModelRequest, ModelResponse
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep, Investigation, Job, LlmCall
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError
from signalops.persistence.repositories.llm_calls import LlmCallRepository
from signalops.persistence.repositories.progress import ProgressRepository
from tests.integration.multi_agent_support import (
    V3_STEPS,
    queued_v3,
    steps_by_key,
    timeout_incident,
    v3_engine,
)
from tests.integration.test_investigator import JOBS, SERVICE_IDS, row_of
from tests.integration.test_workflow_recovery import job_of, wait_for

pytestmark = pytest.mark.integration

LEASE_SECONDS = 3
BRANCHES = ("telemetry", "deployment", "dependency")


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


@contextmanager
def process(settings: Settings, *command: str, **extra: str) -> Iterator[subprocess.Popen[bytes]]:
    environment = {
        **os.environ,
        "SIGNALOPS_DATABASE_URL": settings.validated_database_url(),
        "SIGNALOPS_REDIS_URL": str(settings.redis_url),
        "SIGNALOPS_JOB_LEASE_SECONDS": str(LEASE_SECONDS),
        "SIGNALOPS_JOB_POLL_INTERVAL_MS": "100",
        "SIGNALOPS_MODEL_PROVIDER": "fake",
        "SIGNALOPS_FAKE_MODEL_DELAY_SECONDS": "0",
        **extra,
    }
    child = subprocess.Popen(  # noqa: S603 - the test's own interpreter and modules
        [sys.executable, "-m", *command], env=environment
    )
    try:
        yield child
    finally:
        child.kill()
        child.wait()


def kill(child: subprocess.Popen[bytes]) -> None:
    child.send_signal(signal.SIGKILL)
    assert child.wait() == -signal.SIGKILL


async def resume(settings: Settings, database: db.Database, investigation_id: uuid.UUID) -> None:
    async def succeeded() -> bool:
        return (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED

    with process(settings, "signalops.cli", "worker") as worker:
        await wait_for(succeeded)
        worker.send_signal(signal.SIGTERM)
        assert worker.wait(30) == 0


async def calls_by_step(
    database: db.Database, investigation_id: uuid.UUID
) -> dict[str, list[LlmCall]]:
    async with database.transaction() as session:
        rows = await session.execute(
            select(AgentStep.step_key, LlmCall)
            .join(LlmCall, LlmCall.agent_step_id == AgentStep.id)
            .where(AgentStep.investigation_id == investigation_id)
            .order_by(LlmCall.attempt)
        )
        found: dict[str, list[LlmCall]] = {}
        for step_key, call in rows:
            found.setdefault(step_key, []).append(call)
        return found


async def history(database: db.Database, investigation_id: uuid.UUID) -> list[tuple[str, ...]]:
    async with database.transaction() as session:
        rows = await ProgressRepository(session).after(investigation_id, 0, limit=200)
        return [
            (row.event_type, str(row.payload.get("step_key")), str(row.payload.get("attempt")))
            for row in rows
        ]


def summary(calls: dict[str, list[LlmCall]]) -> dict[str, list[tuple[uuid.UUID, str]]]:
    return {key: [(call.id, call.status) for call in rows] for key, rows in calls.items()}


def stale(job: Job, generation: int = 1) -> JobLease:
    assert job.lease_owner is not None
    return JobLease(job.id, job.investigation_id, job.lease_owner, generation, generation)


async def test_completed_branches_are_reused_and_the_hung_call_stays_counted(
    seeded: tuple[Settings, Engine], database: db.Database
) -> None:
    settings = seeded[0]
    scope, incident_id = await timeout_incident(database, 9401)
    investigation_id = await queued_v3(database, scope, incident_id)

    async def two_branches_done_and_one_call_in_flight() -> bool:
        steps = await steps_by_key(database, investigation_id)
        calls = await calls_by_step(database, investigation_id)
        done = all(
            key in steps and steps[key].status == StepStatus.COMPLETED
            for key in ("telemetry", "dependency")
        )
        pending = [c for c in calls.get("deployment", []) if c.status == LlmCallStatus.PENDING]
        return done and bool(pending)

    with process(settings, "tests.integration.multi_agent_victim", VICTIM_MODE="branch") as victim:
        await wait_for(two_branches_done_and_one_call_in_flight)
        before = await steps_by_key(database, investigation_id)
        dead = await job_of(database, investigation_id)
        kill(victim)

    assert (dead.status, dead.attempts, dead.lease_generation) == (JobStatus.RUNNING, 1, 1)
    assert before["deployment"].status == StepStatus.RUNNING
    assert "synthesize" not in before
    [hung] = (await calls_by_step(database, investigation_id))["deployment"]
    assert hung.status == LlmCallStatus.PENDING
    assert hung.reserved_microusd > 0

    await resume(settings, database, investigation_id)

    job = await job_of(database, investigation_id)
    assert (job.attempts, job.lease_generation) == (2, 2)
    assert job.last_error is not None
    assert job.last_error["category"] == JobErrorCategory.LEASE_EXPIRED
    steps = await steps_by_key(database, investigation_id)
    assert sorted(steps) == sorted(V3_STEPS)
    # Completed branches are reused exactly as stored by the dead worker.
    for key in ("load_context", "plan", "telemetry", "dependency"):
        kept, earlier = steps[key], before[key]
        assert (kept.output, kept.completed_at, kept.attempt, kept.lease_generation) == (
            earlier.output,
            earlier.completed_at,
            1,
            1,
        )
    redone = steps["deployment"]
    assert (redone.status, redone.attempt, redone.lease_generation) == (StepStatus.COMPLETED, 2, 2)

    # The hung call may have run and been billed: it is closed as unknown and keeps
    # its reservation, and the retry was reserved separately.
    calls = await calls_by_step(database, investigation_id)
    first, *retried = calls["deployment"]
    assert first.id == hung.id
    assert (first.status, first.error_category) == (LlmCallStatus.UNKNOWN, "abandoned")
    assert first.reserved_microusd == hung.reserved_microusd
    assert retried
    assert {call.lease_generation for call in retried} == {2}
    assert {call.lease_generation for call in calls["telemetry"]} == {1}
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    assert row.reserved_microusd == hung.reserved_microusd
    async with database.transaction() as session:
        usage = await LlmCallRepository(session).usage_of(investigation_id)
    assert (usage.unknown_calls, usage.unknown_reserved_microusd) == (1, hung.reserved_microusd)
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_id == SERVICE_IDS["bank-gateway"]

    # The dead worker's lease can no longer write anything.
    old = stale(dead)
    async with database.transaction() as session:
        with pytest.raises(LeaseLostError):
            await JobRepository(session).hold(old)
    with pytest.raises(LeaseLostError):
        await complete_job(database, old, diagnosis)

    events = await history(database, investigation_id)
    assert events.count(("step.started", "telemetry", "1")) == 1
    assert ("step.started", "telemetry", "2") not in events
    assert [e for e in events if e[1] == "deployment"] == [
        ("step.started", "deployment", "1"),
        ("step.started", "deployment", "2"),
        ("step.completed", "deployment", "2"),
    ]
    assert events[-1][0] == "investigation.ended"


async def test_a_crash_after_the_conclusion_is_stored_reuses_every_step(
    seeded: tuple[Settings, Engine], database: db.Database
) -> None:
    settings = seeded[0]
    scope, incident_id = await timeout_incident(database, 9402)
    investigation_id = await queued_v3(database, scope, incident_id)

    async def concluded() -> bool:
        steps = await steps_by_key(database, investigation_id)
        return "conclude" in steps and steps["conclude"].status == StepStatus.COMPLETED

    with process(settings, "tests.integration.multi_agent_victim", VICTIM_MODE="persist") as victim:
        await wait_for(concluded)
        dead = await job_of(database, investigation_id)
        kill(victim)

    unsettled = await row_of(database, investigation_id)
    assert unsettled.status == InvestigationStatus.RUNNING
    assert unsettled.diagnosis is None
    before = await steps_by_key(database, investigation_id)
    calls_before = await calls_by_step(database, investigation_id)

    await resume(settings, database, investigation_id)

    steps = await steps_by_key(database, investigation_id)
    for key, step in steps.items():
        assert (step.attempt, step.lease_generation, step.completed_at) == (
            1,
            1,
            before[key].completed_at,
        ), key
    # Nothing ran again: no new model call, and the stored conclusion is the outcome.
    assert summary(await calls_by_step(database, investigation_id)) == summary(calls_before)
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    assert row.diagnosis == before["conclude"].output
    assert Diagnosis.model_validate(row.diagnosis).status is DiagnosisStatus.DIAGNOSED
    job = await job_of(database, investigation_id)
    assert (job.attempts, job.lease_generation) == (2, 2)
    with pytest.raises(LeaseLostError):
        await complete_job(database, stale(dead), Diagnosis.model_validate(row.diagnosis))
    events = await history(database, investigation_id)
    assert [e for e in events if e[0] == "investigation.ended"] == [
        ("investigation.ended", "None", "None")
    ]
    assert not [e for e in events if e[2] == "2"]


class GatedDeployment:
    """Fake roles; the deployment branch's first call waits until the test opens the gate."""

    name = "fake"

    def __init__(self) -> None:
        self.inner = FakeProvider(responder=role_step)
        self.waiting = asyncio.Event()
        self.gate = asyncio.Event()
        self._held = False

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if request.prompt_version == "deployment-v1" and not self._held:
            self._held = True
            self.waiting.set()
            await self.gate.wait()
        return await self.inner.complete(request)


async def test_a_stale_worker_cannot_write_after_a_newer_lease_takes_over(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9403)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = GatedDeployment()
    engine = v3_engine(database, provider)
    old = await claim_job(database, "worker-a", JOBS)
    assert old is not None
    stale_run = asyncio.create_task(engine(old))
    await asyncio.wait_for(provider.waiting.wait(), 30)

    async def other_branches_done() -> bool:
        steps = await steps_by_key(database, investigation_id)
        return all(
            k in steps and steps[k].status == StepStatus.COMPLETED
            for k in ("telemetry", "dependency")
        )

    await wait_for(other_branches_done)
    # The old lease runs out while its call is in flight, and another worker claims the job.
    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.id == old.job_id)
            .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    new = await claim_job(database, "worker-b", JOBS)
    assert new is not None
    assert (new.job_id, new.generation) == (old.job_id, old.generation + 1)

    # The old worker's answer arrives now; its next fenced write fails.
    provider.gate.set()
    with pytest.raises(LeaseLostError):
        await stale_run
    steps = await steps_by_key(database, investigation_id)
    assert steps["deployment"].status == StepStatus.RUNNING
    assert steps["deployment"].lease_generation == old.generation
    assert "synthesize" not in steps

    await JobRunner(database, engine, JOBS, owner=new.owner).execute(new, asyncio.Event())

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    steps = await steps_by_key(database, investigation_id)
    assert steps["telemetry"].lease_generation == old.generation
    assert steps["deployment"].lease_generation == new.generation
    for key in ("synthesize", "verify", "conclude"):
        assert steps[key].lease_generation == new.generation
    stored = SpecialistFindings.model_validate(steps["deployment"].output)
    assert stored.report.findings
    assert stored.provenance.ended_by is AgentEnding.SUBMITTED
    assert steps["deployment"].agent_role == AgentRole.DEPLOYMENT_INVESTIGATOR
    # The stale call settled as a fact about money; no call of the old lease is pending.
    calls = await calls_by_step(database, investigation_id)
    assert all(call.status != LlmCallStatus.PENDING for key in calls for call in calls[key])
    old_calls = [c for c in calls["deployment"] if c.lease_generation == old.generation]
    assert len(old_calls) == 1
    assert old_calls[0].status in (LlmCallStatus.SUCCEEDED, LlmCallStatus.UNKNOWN)
    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
        held = sum(
            c.reserved_microusd
            for key in calls
            for c in calls[key]
            if c.status == LlmCallStatus.UNKNOWN
        )
    assert investigation.reserved_microusd == held
