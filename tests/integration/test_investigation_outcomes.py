"""Progress history and terminal outcomes against Compose PostgreSQL (P06-T05)."""

import asyncio
import json
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from sqlalchemy import Engine, delete, select, text, update
from sqlalchemy.exc import IntegrityError

from signalops.agents.investigations import INVESTIGATION_EVENT_NAMESPACE, end_investigation
from signalops.agents.jobs import (
    PermanentJobError,
    TransientJobError,
    claim_job,
    complete_job,
)
from signalops.agents.workflow import (
    CONCLUDE,
    INVESTIGATE,
    LOAD_CONTEXT,
    NodeContext,
    Workflow,
    WorkflowEngine,
)
from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.events import InvestigationCompletedPayload, decode_event
from signalops.contracts.progress import ProgressEventType
from signalops.contracts.workflow import AgentRole, InvestigationStatus, JobStatus, StepStatus
from signalops.persistence import database as db
from signalops.persistence.migrations import alembic_config
from signalops.persistence.models import Job, OutboxEvent, ProgressEvent
from signalops.persistence.repositories.jobs import JobRepository, LeaseLostError
from signalops.persistence.repositories.progress import ProgressRepository
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.persistence.seed import seed_demo
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE
from tests.integration.job_support import ABSTENTION, add_job
from tests.integration.test_cancellation import cancel, pass_deadline
from tests.integration.test_job_leases import completed_events, expire, investigation_of, job_of
from tests.integration.test_workflow_engine import POLICY, claim, counted, run_once, steps_of

pytestmark = pytest.mark.integration

DIAGNOSED = Diagnosis.model_validate(
    json.loads(
        Path(__file__)
        .resolve()
        .parents[2]
        .joinpath("fixtures/contracts/v1/diagnoses/diagnosed.json")
        .read_text()
    )
)

History = list[tuple[str, dict[str, Any]]]


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def history_of(database: db.Database, investigation_id: uuid.UUID) -> History:
    async with database.transaction() as session:
        rows = await ProgressRepository(session).after(investigation_id, 0, limit=100)
        return [(row.event_type, row.payload) for row in rows]


def step(key: str, role: AgentRole, attempt: int = 1, **extra: Any) -> dict[str, Any]:
    return {"step_key": key, "agent_role": role, "attempt": attempt, **extra}


LOAD = step("load_context", AgentRole.COORDINATOR)
INVESTIGATING = step("investigate", AgentRole.INVESTIGATOR)
CONCLUDING = step("conclude", AgentRole.SYNTHESIZER)
ABSTAINED = {"status": "insufficient_evidence", "diagnosis_status": "insufficient_evidence"}
FULL_RUN: History = [
    ("step.started", LOAD),
    ("step.completed", LOAD),
    ("step.started", INVESTIGATING),
    ("step.completed", INVESTIGATING),
    ("step.started", CONCLUDING),
    ("step.completed", CONCLUDING),
    ("investigation.ended", ABSTAINED),
]


def engine_ending_in(database: db.Database, diagnosis: Diagnosis) -> WorkflowEngine:
    async def conclude(_context: NodeContext) -> Diagnosis:
        return diagnosis

    return WorkflowEngine(
        database,
        {"v1": Workflow("v1", (LOAD_CONTEXT, INVESTIGATE), replace(CONCLUDE, run=conclude))},
    )


async def test_a_run_records_each_step_and_the_ending_in_order(database: db.Database) -> None:
    investigation_id = await add_job(database)

    await run_once(database, WorkflowEngine(database))

    assert await history_of(database, investigation_id) == FULL_RUN


async def test_an_abstention_ends_the_investigation_with_its_diagnosis(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)

    await run_once(database, WorkflowEngine(database))

    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert investigation.completed_at is not None
    assert investigation.diagnosis is not None
    assert investigation.diagnosis["status"] == DiagnosisStatus.INSUFFICIENT_EVIDENCE
    assert (await job_of(database, investigation_id)).status == JobStatus.SUCCEEDED


async def test_a_diagnosis_completes_the_investigation(database: db.Database) -> None:
    investigation_id = await add_job(database)

    await run_once(database, engine_ending_in(database, DIAGNOSED))

    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.COMPLETED
    assert Diagnosis.model_validate(investigation.diagnosis) == DIAGNOSED
    assert (await history_of(database, investigation_id))[-1] == (
        "investigation.ended",
        {"status": "completed", "diagnosis_status": "diagnosed"},
    )
    [event] = await completed_events(database, investigation_id)
    payload = decode_event(json.dumps(event.envelope).encode()).payload
    assert isinstance(payload, InvestigationCompletedPayload)
    assert payload.status is InvestigationStatus.COMPLETED
    assert payload.diagnosis_status is DiagnosisStatus.DIAGNOSED
    assert event.aggregate_version == investigation.version


async def test_a_retry_shows_the_failure_and_the_new_attempt(database: db.Database) -> None:
    node, body = counted(INVESTIGATE)
    body.failures.append(TransientJobError("throttled by host 10.0.0.1"))
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (LOAD_CONTEXT, node), CONCLUDE)})
    investigation_id = await add_job(database)

    await run_once(database, engine)
    await asyncio.sleep(0.01)
    await run_once(database, engine)

    history = await history_of(database, investigation_id)
    assert history[2:] == [
        ("step.started", INVESTIGATING),
        ("step.failed", {**INVESTIGATING, "error_category": "transient"}),
        ("step.started", {**INVESTIGATING, "attempt": 2}),
        ("step.completed", {**INVESTIGATING, "attempt": 2}),
        ("step.started", {**CONCLUDING, "attempt": 2}),
        ("step.completed", {**CONCLUDING, "attempt": 2}),
        ("investigation.ended", ABSTAINED),
    ]
    # The step keeps the detail; the shared history holds only the category.
    assert "10.0.0.1" not in json.dumps(history)


async def test_a_permanent_failure_ends_without_a_diagnosis(database: db.Database) -> None:
    node, body = counted(INVESTIGATE)
    body.failures.append(PermanentJobError("no such tool"))
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (LOAD_CONTEXT, node), CONCLUDE)})
    investigation_id = await add_job(database)

    await run_once(database, engine)

    assert (await history_of(database, investigation_id))[-2:] == [
        ("step.failed", {**INVESTIGATING, "error_category": "permanent"}),
        ("investigation.ended", {"status": "failed", "diagnosis_status": None}),
    ]
    assert (await investigation_of(database, investigation_id)).diagnosis is None


async def test_a_step_abandoned_by_a_cancellation_is_closed_in_the_history(
    database: db.Database,
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    async with database.transaction() as session:
        await JobRepository(session).save_checkpoint(lease, "investigate", INVESTIGATE.role, {}, {})
    await cancel(database, investigation_id)
    await expire(database, investigation_id)
    assert await claim_job(database, "worker-b", POLICY) is None

    assert await history_of(database, investigation_id) == [
        ("step.started", INVESTIGATING),
        ("step.failed", {**INVESTIGATING, "error_category": "abandoned"}),
        ("investigation.ended", {"status": "cancelled", "diagnosis_status": None}),
    ]


async def test_a_passed_deadline_records_a_failed_ending(database: db.Database) -> None:
    investigation_id = await add_job(database)
    await pass_deadline(database, investigation_id)

    assert await claim_job(database, "worker-a", POLICY) is None

    assert await history_of(database, investigation_id) == [
        ("investigation.ended", {"status": "failed", "diagnosis_status": None}),
    ]


async def test_a_crash_before_the_terminal_write_replays_to_the_stored_outcome(
    database: db.Database,
) -> None:
    """The last step committed, then the worker died before ending the investigation."""
    first, first_body = counted(LOAD_CONTEXT)
    second, second_body = counted(INVESTIGATE)
    third, third_body = counted(CONCLUDE)
    engine = WorkflowEngine(database, {"v1": Workflow("v1", (first, second), third)})
    investigation_id = await add_job(database)

    await engine(await claim(database))

    assert len(await history_of(database, investigation_id)) == 6
    assert (await investigation_of(database, investigation_id)).status == (
        InvestigationStatus.RUNNING
    )

    await expire(database, investigation_id)
    await run_once(database, engine)

    assert (first_body.runs, second_body.runs, third_body.runs) == (1, 1, 1)
    # Reused steps add nothing, so the replay reads as one run.
    assert await history_of(database, investigation_id) == FULL_RUN
    investigation = await investigation_of(database, investigation_id)
    conclude = (await steps_of(database, investigation_id))["conclude"]
    assert investigation.diagnosis == conclude.output
    assert len(await completed_events(database, investigation_id)) == 1


async def test_a_replay_with_other_conclusions_cannot_change_the_outcome(
    database: db.Database,
) -> None:
    """A changed model answer on the replay still reaches the stored conclusion."""
    investigation_id = await add_job(database)
    await engine_ending_in(database, DIAGNOSED)(await claim(database))
    await expire(database, investigation_id)

    await run_once(database, WorkflowEngine(database))

    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.COMPLETED
    assert Diagnosis.model_validate(investigation.diagnosis) == DIAGNOSED


async def test_a_stale_worker_cannot_conclude_after_a_takeover(database: db.Database) -> None:
    entered, released = asyncio.Event(), asyncio.Event()

    async def slow(_context: NodeContext) -> Diagnosis:
        entered.set()
        await released.wait()
        return DIAGNOSED

    stale_engine = WorkflowEngine(
        database, {"v1": Workflow("v1", (LOAD_CONTEXT, INVESTIGATE), replace(CONCLUDE, run=slow))}
    )
    investigation_id = await add_job(database)
    stale_lease = await claim(database)
    stale = asyncio.create_task(stale_engine(stale_lease))
    await entered.wait()
    await expire(database, investigation_id)

    await run_once(database, WorkflowEngine(database))
    released.set()

    with pytest.raises(LeaseLostError):
        await stale
    with pytest.raises(LeaseLostError):
        await complete_job(database, stale_lease, DIAGNOSED)

    investigation = await investigation_of(database, investigation_id)
    assert investigation.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    history = await history_of(database, investigation_id)
    assert [event for event in history if event[0] == "investigation.ended"] == [
        ("investigation.ended", ABSTAINED)
    ]


async def test_a_finished_job_cannot_run_or_end_again(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    diagnosis = await WorkflowEngine(database)(lease)
    await complete_job(database, lease, diagnosis)
    before = await investigation_of(database, investigation_id)
    history = await history_of(database, investigation_id)

    assert await claim_job(database, "worker-b", POLICY) is None
    with pytest.raises(LeaseLostError):
        await complete_job(database, lease, DIAGNOSED)
    for status, other in (
        (InvestigationStatus.FAILED, None),
        (InvestigationStatus.COMPLETED, DIAGNOSED),
    ):
        async with database.transaction() as session:
            await end_investigation(session, investigation_id, status, other)

    after = await investigation_of(database, investigation_id)
    assert (after.status, after.diagnosis, after.completed_at, after.version) == (
        before.status,
        before.diagnosis,
        before.completed_at,
        before.version,
    )
    assert await history_of(database, investigation_id) == history
    assert len(await completed_events(database, investigation_id)) == 1


@pytest.mark.parametrize("attempt", range(6))
async def test_concurrent_completions_end_the_investigation_once(
    database: db.Database, attempt: int
) -> None:
    assert attempt >= 0
    investigation_id = await add_job(database)
    lease = await claim(database)

    results = await asyncio.gather(
        *(complete_job(database, lease, ABSTENTION) for _ in range(4)), return_exceptions=True
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, LeaseLostError) for result in results) == 3
    assert await history_of(database, investigation_id) == [("investigation.ended", ABSTAINED)]
    assert len(await completed_events(database, investigation_id)) == 1


async def test_a_failed_outbox_write_leaves_the_job_to_finish_later(
    database: db.Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Status, diagnosis, event, and history commit together or not at all."""
    investigation_id = await add_job(database)
    lease = await claim(database)

    async def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("cannot stage the event")

    with monkeypatch.context() as patched:
        patched.setattr(OutboxRepository, "add", broken)
        with pytest.raises(RuntimeError):
            await complete_job(database, lease, ABSTENTION)

    investigation = await investigation_of(database, investigation_id)
    assert (investigation.status, investigation.diagnosis) == (InvestigationStatus.RUNNING, None)
    assert (await job_of(database, investigation_id)).status == JobStatus.RUNNING
    assert await history_of(database, investigation_id) == []

    await complete_job(database, lease, ABSTENTION)

    assert await history_of(database, investigation_id) == [("investigation.ended", ABSTAINED)]


async def test_a_step_event_rolls_back_with_its_step(
    database: db.Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)

    async def broken(*_args: object) -> None:
        raise RuntimeError("cannot write progress")

    with monkeypatch.context() as patched:
        patched.setattr(ProgressRepository, "add", broken)
        with pytest.raises(RuntimeError):
            async with database.transaction() as session:
                await JobRepository(session).save_checkpoint(
                    lease, "investigate", INVESTIGATE.role, {}, {}
                )

    assert await steps_of(database, investigation_id) == {}


async def test_a_stale_step_write_adds_no_progress(database: db.Database) -> None:
    investigation_id = await add_job(database)
    stale = await claim(database)
    await expire(database, investigation_id)
    await claim(database, "worker-b")

    with pytest.raises(LeaseLostError):
        async with database.transaction() as session:
            await JobRepository(session).save_checkpoint(
                stale, "investigate", INVESTIGATE.role, {}, {}
            )

    assert await history_of(database, investigation_id) == []


async def test_reopening_a_step_requires_it_to_be_unfinished(database: db.Database) -> None:
    investigation_id = await add_job(database)
    lease = await claim(database)
    await WorkflowEngine(database)(lease)
    history = await history_of(database, investigation_id)

    async with database.transaction() as session:
        jobs = JobRepository(session)
        assert not await jobs.save_checkpoint(lease, "conclude", CONCLUDE.role, {}, {})
        assert not await jobs.complete_step(lease, "conclude", {"other": 1}, {})
        assert not await jobs.fail_step(lease, "conclude", {}, "permanent")

    assert await history_of(database, investigation_id) == history
    assert (await steps_of(database, investigation_id))["conclude"].status == StepStatus.COMPLETED


async def test_the_database_refuses_a_second_ending(database: db.Database) -> None:
    investigation_id = await add_job(database)
    row = {
        "investigation_id": investigation_id,
        "event_type": ProgressEventType.INVESTIGATION_ENDED,
    }
    async with database.transaction() as session:
        session.add(ProgressEvent(**row))

    with pytest.raises(IntegrityError, match="uq_progress_events_investigation_ended"):
        async with database.transaction() as session:
            session.add(ProgressEvent(**row))


async def test_one_workers_events_read_in_commit_order(database: db.Database) -> None:
    investigation_id = await add_job(database)
    await run_once(database, WorkflowEngine(database))

    async with database.transaction() as session:
        repository = ProgressRepository(session)
        ids = [row.id for row in await repository.after(investigation_id, 0, limit=100)]
        tail = await repository.after(investigation_id, ids[3], limit=2)
        none_left = await repository.after(investigation_id, ids[-1], limit=100)

    assert ids == sorted(ids)
    assert [row.id for row in tail] == ids[4:6]
    assert none_left == []


async def stranded_row(database: db.Database, *, stranded: bool = True) -> uuid.UUID:
    """Every step done: left running by old code, or still awaiting its worker's terminal write."""
    investigation_id = await add_job(database)
    lease = await claim_job(database, f"worker-{uuid.uuid4().hex[:6]}", POLICY)
    assert lease is not None
    await WorkflowEngine(database)(lease)
    if not stranded:
        return investigation_id
    async with database.transaction() as session:
        await session.execute(delete(ProgressEvent))
        await session.execute(
            update(Job)
            .where(Job.id == lease.job_id)
            .values(status=JobStatus.SUCCEEDED, lease_owner=None, lease_until=None)
        )
    return investigation_id


def test_the_migration_settles_investigations_left_running(scratch_database: Settings) -> None:
    config = alembic_config(scratch_database)
    command.upgrade(config, "0014_investigation_cancellation")

    async def prepare() -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
        engine = db.create_engine(scratch_database)
        database = db.Database.from_engine(engine)
        try:
            await seed_demo(database)
            stranded = await stranded_row(database)
            job_running = await stranded_row(database, stranded=False)
            queued = await add_job(database)
            return stranded, job_running, queued
        finally:
            await engine.dispose()

    stranded, job_running, queued = asyncio.run(prepare())

    command.upgrade(config, "head")

    async def check() -> None:
        engine = db.create_engine(scratch_database)
        database = db.Database.from_engine(engine)
        try:
            investigation = await investigation_of(database, stranded)
            conclude = (await steps_of(database, stranded))["conclude"]
            assert investigation.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
            assert investigation.diagnosis == conclude.output
            assert investigation.completed_at == conclude.completed_at
            assert await history_of(database, stranded) == [("investigation.ended", ABSTAINED)]

            [event] = await completed_events(database, stranded)
            assert event.id == uuid.uuid5(INVESTIGATION_EVENT_NAMESPACE, f"{stranded}:completed")
            assert event.aggregate_version == investigation.version
            assert event.message_key == str(stranded)
            envelope = decode_event(json.dumps(event.envelope).encode())
            assert event.topic == TOPIC_BY_EVENT_TYPE[envelope.event_type]
            assert isinstance(envelope.payload, InvestigationCompletedPayload)
            assert envelope.payload.status is InvestigationStatus.INSUFFICIENT_EVIDENCE
            assert envelope.payload.diagnosis_status is DiagnosisStatus.INSUFFICIENT_EVIDENCE

            untouched = [
                await investigation_of(database, job_running),
                await investigation_of(database, queued),
            ]
            assert [row.status for row in untouched] == [
                InvestigationStatus.RUNNING,
                InvestigationStatus.QUEUED,
            ]
            async with database.transaction() as session:
                others = await session.scalars(
                    select(OutboxEvent.id).where(
                        OutboxEvent.aggregate_id.in_([job_running, queued])
                    )
                )
                assert others.all() == []
        finally:
            await engine.dispose()

    asyncio.run(check())

    command.downgrade(config, "0014_investigation_cancellation")
    command.upgrade(config, "head")


def test_the_migrated_index_matches_the_model(seeded: tuple[Settings, Engine]) -> None:
    with seeded[1].connect() as connection:
        definition = connection.execute(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE indexname = 'uq_progress_events_investigation_ended'"
            )
        ).scalar_one()
    assert "UNIQUE" in definition
    assert "investigation.ended" in definition
