"""A real worker process killed between nodes, resumed by `signalops worker` (P06-T03)."""

import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy import Engine, select

from signalops.agents.investigations import cancel_investigation
from signalops.agents.jobs import JobErrorCategory
from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.steps import IncidentBrief
from signalops.contracts.workflow import InvestigationStatus, JobStatus, StepStatus
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep, Investigation, Job
from signalops.persistence.repositories.progress import ProgressRepository
from tests.integration.job_support import LIVE, add_job

pytestmark = pytest.mark.integration

LEASE_SECONDS = 3
WAIT_SECONDS = 30.0


@contextmanager
def worker(settings: Settings, *command: str) -> Iterator[subprocess.Popen[bytes]]:
    environment = {
        **os.environ,
        "SIGNALOPS_DATABASE_URL": settings.validated_database_url(),
        "SIGNALOPS_JOB_LEASE_SECONDS": str(LEASE_SECONDS),
        "SIGNALOPS_JOB_POLL_INTERVAL_MS": "100",
    }
    process = subprocess.Popen(  # noqa: S603 - the test's own interpreter and modules
        [sys.executable, "-m", *command], env=environment
    )
    try:
        yield process
    finally:
        process.kill()
        process.wait()


async def steps_of(database: db.Database, investigation_id: uuid.UUID) -> dict[str, AgentStep]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(AgentStep).where(AgentStep.investigation_id == investigation_id)
        )
        return {step.step_key: step for step in rows}


async def job_of(database: db.Database, investigation_id: uuid.UUID) -> Job:
    async with database.transaction() as session:
        return (
            await session.scalars(select(Job).where(Job.investigation_id == investigation_id))
        ).one()


async def wait_for(condition: Callable[[], Awaitable[bool]]) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not await condition():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.05)


async def test_a_killed_worker_resumes_at_the_next_unfinished_node(
    seeded: tuple[Settings, Engine],
    database: db.Database,
) -> None:
    settings = seeded[0]
    investigation_id = await add_job(database)

    async def hanging_in_investigate() -> bool:
        steps = await steps_of(database, investigation_id)
        return (
            "load_context" in steps
            and steps["load_context"].status == StepStatus.COMPLETED
            and "investigate" in steps
        )

    with worker(settings, "tests.integration.hanging_worker") as victim:
        await wait_for(hanging_in_investigate)
        before = (await steps_of(database, investigation_id))["load_context"]
        victim.send_signal(signal.SIGKILL)
        assert victim.wait() == -signal.SIGKILL

    dead = await job_of(database, investigation_id)
    assert (dead.status, dead.attempts) == (JobStatus.RUNNING, 1)
    steps = await steps_of(database, investigation_id)
    assert steps["investigate"].status == StepStatus.RUNNING
    assert "conclude" not in steps

    async def succeeded() -> bool:
        return (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED

    with worker(settings, "signalops.cli", "worker") as resumed:
        await wait_for(succeeded)
        resumed.send_signal(signal.SIGTERM)
        assert resumed.wait(WAIT_SECONDS) == 0

    job = await job_of(database, investigation_id)
    assert (job.attempts, job.lease_generation) == (2, 2)
    assert job.last_error is not None
    assert job.last_error["category"] == JobErrorCategory.LEASE_EXPIRED

    steps = await steps_of(database, investigation_id)
    kept = steps["load_context"]
    assert (kept.output, kept.completed_at, kept.attempt, kept.lease_generation) == (
        before.output,
        before.completed_at,
        1,
        1,
    )
    assert IncidentBrief.model_validate(kept.output).title == "High error rate"
    assert steps["investigate"].attempt == 2
    assert steps["investigate"].lease_generation == 2
    assert steps["investigate"].status == StepStatus.COMPLETED
    diagnosis = Diagnosis.model_validate(steps["conclude"].output)
    assert diagnosis.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE

    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
        rows = await ProgressRepository(session).after(investigation_id, 0, limit=100)
        history = [
            (row.event_type, row.payload.get("step_key"), row.payload.get("attempt"))
            for row in rows
        ]
    assert investigation.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert investigation.diagnosis == steps["conclude"].output

    # Both workers are gone: what each committed reads as one history.
    assert history == [
        ("step.started", "load_context", 1),
        ("step.completed", "load_context", 1),
        ("step.started", "investigate", 1),
        ("step.started", "investigate", 2),
        ("step.completed", "investigate", 2),
        ("step.started", "conclude", 2),
        ("step.completed", "conclude", 2),
        ("investigation.ended", None, None),
    ]


async def test_cancelling_a_worker_mid_call_leaves_no_downstream_call(
    seeded: tuple[Settings, Engine],
    database: db.Database,
) -> None:
    settings = seeded[0]
    investigation_id = await add_job(database)

    async def hanging_in_investigate() -> bool:
        return "investigate" in await steps_of(database, investigation_id)

    async def job_cancelled() -> bool:
        return (await job_of(database, investigation_id)).status == JobStatus.CANCELLED

    with worker(settings, "tests.integration.hanging_worker") as running:
        await wait_for(hanging_in_investigate)
        async with database.transaction() as session:
            requested = await cancel_investigation(session, LIVE, investigation_id)
        assert requested is not None
        assert requested.status == InvestigationStatus.RUNNING

        await wait_for(job_cancelled)

        assert running.poll() is None, "the worker should outlive the job it stopped"

    steps = await steps_of(database, investigation_id)
    assert steps["load_context"].status == StepStatus.COMPLETED
    assert steps["investigate"].status == StepStatus.FAILED
    assert "conclude" not in steps
    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
    assert investigation.status == InvestigationStatus.CANCELLED
    assert investigation.completed_at is not None
