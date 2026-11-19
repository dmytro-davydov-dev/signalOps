"""Component evaluations of the synthesizer and verifier on the fake provider (P13-T04).

Every corpus case runs as a holder investigation through the ordinary worker, the
gateway, and `AgentLoop`. Reservations and spending land on each holder's budget,
and a case whose budget cannot fit a call ends `budget_exceeded` on its own.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import delete, select

from signalops.agents.jobs import JobRunner
from signalops.contracts.evaluation_components import ComponentRole, CorpusCase
from signalops.evaluation.components import (
    COMPONENT_STEPS,
    ComponentLimits,
    corpus_dir,
    load_corpus,
    run_components,
)
from signalops.gateway.fake import FakeProvider
from signalops.gateway.fake_roles import verifier_step
from signalops.persistence import database as db
from signalops.persistence.models import (
    EvaluationStageCase,
    EvaluationStageRun,
    Investigation,
    Job,
)
from signalops.persistence.repositories.llm_calls import LlmCallRepository
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.evaluation_support import engine_for
from tests.integration.multi_agent_support import roles_with
from tests.integration.test_investigator import JOBS
from tests.integration.test_stage_scores import accept_everything, edited

pytestmark = pytest.mark.integration

CORPUS = corpus_dir(Path(__file__).resolve().parents[2])
FAKE = {"provider": "fake", "measures_model_quality": False}
LIMITS = ComponentLimits(budget_microusd=2_000_000, max_steps=COMPONENT_STEPS + 2)


@contextlib.asynccontextmanager
async def workers(database: db.Database, provider: FakeProvider) -> AsyncIterator[None]:
    async with database.transaction() as session:
        await session.execute(delete(Job))
    engine = engine_for(database, provider)
    stop = asyncio.Event()
    runners = [JobRunner(database, engine, JOBS, owner=f"component-{i}") for i in range(3)]
    tasks = [asyncio.create_task(runner.run(stop)) for runner in runners]
    try:
        yield
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks)


async def run_role(
    database: db.Database,
    role: ComponentRole,
    provider: FakeProvider,
    *,
    cases: list[CorpusCase] | None = None,
    budget_for: Callable[[CorpusCase, int], int | None] | None = None,
) -> tuple[EvaluationStageRun, list[EvaluationStageCase]]:
    loaded, digest = load_corpus(CORPUS, role)
    async with workers(database, provider):
        run_id = await run_components(
            database,
            workspace_id=DEMO_WORKSPACE_ID,
            role=role,
            cases=cases or loaded,
            corpus_hash=digest,
            trials=1,
            model_config=FAKE,
            limits=LIMITS,
            budget_for=budget_for or (lambda _case, _trial: None),
            poll_interval=timedelta(milliseconds=100),
        )
    async with database.transaction() as session:
        run = await session.get_one(EvaluationStageRun, run_id)
        rows = list(
            await session.scalars(
                select(EvaluationStageCase).where(EvaluationStageCase.stage_run_id == run_id)
            )
        )
    return run, rows


@pytest.mark.parametrize("role", list(ComponentRole))
async def test_every_corpus_case_runs_through_the_gateway_on_the_fake_provider(
    database: db.Database, role: ComponentRole
) -> None:
    run, rows = await run_role(database, role, FakeProvider(responder=roles_with()))
    cases, _ = load_corpus(CORPUS, role)
    assert run.status == "completed"
    assert len(rows) == len(cases)
    assert {row.status for row in rows} == {"scored"}, [row.error for row in rows]
    metrics = run.metrics
    assert metrics is not None
    assert metrics["measures_model_quality"] is False
    assert metrics["cases"] == len(cases)
    assert all(item["ci95"] is not None for item in metrics["by_defect"].values())
    if role is ComponentRole.VERIFIER:
        assert {"verdict_accuracy", "false_accept_rate", "citation_agreement"} <= set(metrics)
    else:
        assert {"leading_accuracy", "contradiction_recall", "abstention_correctness"} <= set(
            metrics
        )

    # Each case's model calls were reserved and settled against its own holder.
    ids = [row.investigation_id for row in rows]
    async with database.transaction() as session:
        holders = list(
            await session.scalars(select(Investigation).where(Investigation.id.in_(ids)))
        )
        usage = {
            holder.id: await LlmCallRepository(session).usage_of(holder.id) for holder in holders
        }
    assert len({holder.replay_scope for holder in holders}) == len(holders)
    for holder in holders:
        assert holder.workflow_version == f"s1-{role.value}"
        assert holder.reserved_microusd == 0
        assert usage[holder.id].model_calls >= 1
        assert usage[holder.id].unknown_reserved_microusd == 0
        assert 0 < holder.spent_microusd <= holder.budget_microusd


async def test_a_verifier_accepting_everything_accepts_every_defect(
    database: db.Database,
) -> None:
    provider = FakeProvider(
        responder=roles_with({"verifier-v2": edited(verifier_step, accept_everything)})
    )
    run, _ = await run_role(database, ComponentRole.VERIFIER, provider)
    assert run.metrics is not None
    false_accepts = run.metrics["false_accept_rate"]
    assert false_accepts["denominator"] == 6
    assert false_accepts["rate"] == 1.0
    assert run.metrics["false_reject_rate"]["rate"] == 0.0


async def test_a_case_whose_budget_cannot_fit_a_call_fails_alone(database: db.Database) -> None:
    cases, _ = load_corpus(CORPUS, ComponentRole.VERIFIER)
    starved, fed = cases[0], cases[1]
    run, rows = await run_role(
        database,
        ComponentRole.VERIFIER,
        FakeProvider(responder=roles_with()),
        cases=[starved, fed],
        budget_for=lambda case, _trial: 1 if case.key == starved.key else None,
    )
    by_key = {row.case_key: row for row in rows}
    assert by_key[starved.key].status == "failed"
    assert by_key[starved.key].investigation_status == "budget_exceeded"
    assert by_key[fed.key].status == "scored"
    assert run.metrics is not None
    assert run.metrics["failed_by_status"] == {"budget_exceeded": 1}
    async with database.transaction() as session:
        starved_holder = await session.get_one(Investigation, by_key[starved.key].investigation_id)
    assert starved_holder.spent_microusd == 0
