"""Run evaluation runs in process with a chosen workflow engine (Phase 13 tests)."""

import asyncio
import contextlib
import uuid
from collections.abc import Mapping
from datetime import timedelta

from sqlalchemy import delete, select

from signalops.agents.investigator import AgentRuntime
from signalops.agents.jobs import JobRunner
from signalops.agents.nodes import Workflow
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import WORKFLOWS, WorkflowEngine
from signalops.config import Settings
from signalops.contracts.evaluation import ScenarioSplit, SystemVariant
from signalops.evaluation.replay import ReplayTiming
from signalops.evaluation.runner import RunExecutor, RunnerContext, RunRequest, queue_run
from signalops.gateway.fake import FAKE_MODEL
from signalops.gateway.gateway import ModelGateway
from signalops.gateway.provider import ModelProvider
from signalops.persistence import database as db
from signalops.persistence.models import EvaluationCase, EvaluationRun, Job, LlmCall
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.multi_agent_support import V3_LIMITS
from tests.integration.test_investigator import JOBS, RETRIES
from tests.integration.test_replay import DETECTION, INGEST, SOURCES, _transport

SETTINGS = Settings(database_url="postgresql+psycopg://unused@localhost/unused")  # type: ignore[arg-type]
AGENTS = (SystemVariant.SINGLE_AGENT, SystemVariant.MULTI_AGENT)


def engine_for(
    database: db.Database,
    provider: ModelProvider,
    workflows: Mapping[str, Workflow] = WORKFLOWS,
) -> WorkflowEngine:
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider, policy=RETRIES),
        tools=ToolExecutor(database, TextRedactor(("password", "token", "api_key"))),
        model=FAKE_MODEL,
        limits=V3_LIMITS,
    )
    return WorkflowEngine(database, workflows, runtime=runtime)


def runner_context(database: db.Database) -> RunnerContext:
    return RunnerContext(
        database=database,
        sources=SOURCES,
        ingest_policy=INGEST,
        detection_policy=DETECTION,
        transport_for=lambda scope: _transport(database, uuid.UUID(scope.replay_scope[7:])),
        replay_timing=ReplayTiming(
            timeout=timedelta(seconds=120), poll_interval=timedelta(milliseconds=10)
        ),
        poll_interval=timedelta(milliseconds=100),
    )


async def run_with(
    database: db.Database,
    engine: WorkflowEngine,
    *,
    scenarios: tuple[str, ...],
    split: ScenarioSplit = ScenarioSplit.DEVELOPMENT,
    variants: tuple[SystemVariant, ...] = AGENTS,
    trials: int = 1,
    label: str = "stage test",
    workers: int = 3,
) -> tuple[EvaluationRun, list[EvaluationCase]]:
    """Queue and execute one run with in-process investigation workers using `engine`."""
    async with database.transaction() as session:
        await session.execute(delete(Job))
    stop = asyncio.Event()
    runners = [JobRunner(database, engine, JOBS, owner=f"stage-{i}") for i in range(workers)]
    tasks = [asyncio.create_task(runner.run(stop)) for runner in runners]
    try:
        request = RunRequest(
            label=label,
            split=split,
            variants=variants,
            trials=trials,
            budget_microusd=2_000_000,
            max_steps=40,
            deadline_seconds=300,
            scenarios=scenarios,
        )
        run = await queue_run(database, request, workspace_id=DEMO_WORKSPACE_ID, settings=SETTINGS)
        executor = RunExecutor(runner_context(database), owner=f"runner-{uuid.uuid4().hex[:6]}")
        while (claimed := await executor.claim_and_execute()) is not None:
            if claimed == run.id:
                break
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks)
    async with database.transaction() as session:
        stored = await session.get_one(EvaluationRun, run.id, populate_existing=True)
        cases = list(
            await session.scalars(
                select(EvaluationCase)
                .where(EvaluationCase.run_id == run.id)
                .order_by(EvaluationCase.case_key)
            )
        )
    return stored, cases


async def model_call_count(database: db.Database) -> int:
    async with database.transaction() as session:
        return len(list(await session.scalars(select(LlmCall.id))))
