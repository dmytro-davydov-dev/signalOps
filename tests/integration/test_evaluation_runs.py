"""Evaluation runs with three comparable baselines (P10-T04).

A run replays each scenario once (in process) and investigates its incident with
the deterministic, single-agent, and multi-agent variants, on the fake provider,
with an in-process worker standing in for the investigation workers.
"""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from sqlalchemy import delete, select, update

from signalops.agents.jobs import JobRunner
from signalops.config import Settings
from signalops.contracts.evaluation import (
    CaseStatus,
    ObservedOutcome,
    ReplayStatus,
    RunStatus,
    ScenarioSplit,
    SystemVariant,
)
from signalops.evaluation.replay import ReplayTiming
from signalops.evaluation.runner import RunExecutor, RunnerContext, RunRequest, queue_run
from signalops.gateway.fake import FakeProvider
from signalops.persistence import database as db
from signalops.persistence.models import (
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
    Investigation,
    Job,
)
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.multi_agent_support import roles_with, v3_engine
from tests.integration.test_investigator import JOBS
from tests.integration.test_replay import DETECTION, INGEST, SOURCES, _transport

pytestmark = pytest.mark.integration

SCENARIOS = ("downstream-timeout/dev", "unexplained-errors/dev", "healthy/dev")
SETTINGS = Settings(database_url="postgresql+psycopg://unused@localhost/unused")  # type: ignore[arg-type]


@pytest.fixture
async def worker(database: db.Database) -> AsyncIterator[None]:
    async with database.transaction() as session:
        await session.execute(delete(Job))
    engine = v3_engine(database, FakeProvider(responder=roles_with()))
    stop = asyncio.Event()
    runners = [JobRunner(database, engine, JOBS, owner=f"eval-worker-{i}") for i in range(3)]
    tasks = [asyncio.create_task(runner.run(stop)) for runner in runners]
    try:
        yield
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks)


def _context(database: db.Database) -> RunnerContext:
    return RunnerContext(
        database=database,
        sources=SOURCES,
        ingest_policy=INGEST,
        detection_policy=DETECTION,
        transport_for=lambda scope: _transport(database, uuid.UUID(scope.replay_scope[7:])),
        replay_timing=ReplayTiming(
            timeout=timedelta(seconds=60), poll_interval=timedelta(milliseconds=10)
        ),
        poll_interval=timedelta(milliseconds=100),
    )


async def test_every_variant_investigates_identical_evidence_under_equal_limits(
    database: db.Database, worker: None
) -> None:
    request = RunRequest(
        label="baselines",
        split=ScenarioSplit.DEVELOPMENT,
        trials=2,
        budget_microusd=2_000_000,
        max_steps=40,
        deadline_seconds=300,
        scenarios=SCENARIOS,
    )
    run = await queue_run(database, request, workspace_id=DEMO_WORKSPACE_ID, settings=SETTINGS)
    executor = RunExecutor(_context(database), owner="runner-a")
    assert await executor.claim_and_execute() == run.id

    async with database.transaction() as session:
        stored = await session.get_one(EvaluationRun, run.id, populate_existing=True)
        cases = list(
            await session.scalars(select(EvaluationCase).where(EvaluationCase.run_id == run.id))
        )
        replays = list(
            await session.scalars(select(EvaluationReplay).where(EvaluationReplay.run_id == run.id))
        )
        ids = [case.investigation_id for case in cases if case.investigation_id]
        investigations = {
            row.id: row
            for row in await session.scalars(select(Investigation).where(Investigation.id.in_(ids)))
        }
    assert stored.status == RunStatus.COMPLETED, stored.error
    assert stored.workflow_versions == {
        "deterministic": "d1",
        "single_agent": "v2",
        "multi_agent": "v3",
    }
    assert stored.prompt_versions["single_agent"] == ["investigator-v1"]
    assert stored.model_config["measures_model_quality"] is False
    assert stored.pricing_config["version"]
    assert len(stored.dataset_hash) == 64
    assert {replay.status for replay in replays} == {ReplayStatus.COMPLETED}
    # One deterministic case and two trials per agent variant, per scenario.
    assert len(cases) == len(SCENARIOS) * 5
    assert {case.status for case in cases} == {CaseStatus.SCORED}

    by_scenario: dict[uuid.UUID, list[Investigation]] = {}
    for case in cases:
        if case.investigation_id is None:
            # Healthy traffic opened no incident, so there was nothing to investigate.
            assert case.expected_outcome["incident_expected"] is False
            assert case.scores is not None
            assert case.scores["outcome_correct"] is True
            continue
        by_scenario.setdefault(case.replay_id, []).append(investigations[case.investigation_id])
    assert len(by_scenario) == 2
    for rows in by_scenario.values():
        # Same incident, scope, cutoff, budget, steps, and deadline length for every variant.
        assert len({(r.incident_id, r.replay_scope, r.evidence_cutoff_at) for r in rows}) == 1
        assert {(r.budget_microusd, r.max_steps) for r in rows} == {(2_000_000, 40)}
        assert {round((r.deadline_at - r.created_at).total_seconds()) for r in rows} == {300}
        assert {r.workflow_version for r in rows} == {"d1", "v2", "v3"}

    timeout = {
        c.variant: c for c in cases if c.case_key.startswith("downstream-timeout") and c.trial == 1
    }
    observed = {k: ObservedOutcome.model_validate(c.observed_outcome) for k, c in timeout.items()}
    assert observed["multi_agent"].root_cause_service == "bank-gateway"
    assert observed["multi_agent"].cited_event_ids
    # The deterministic rule blames the alerting service and makes no model call.
    assert observed["deterministic"].model_calls == 0
    assert observed["deterministic"].spent_microusd == 0
    assert observed["deterministic"].root_cause_service != "bank-gateway"

    metrics = stored.metrics
    assert metrics is not None
    assert metrics["budget_per_case"] == {
        "budget_microusd": 2_000_000,
        "max_steps": 40,
        "deadline_seconds": 300,
    }
    for variant in SystemVariant:
        summary = metrics["variants"][variant.value]
        assert summary["samples"] == summary["cases"]
        assert {"abstentions", "accuracy_stdev", "trial_accuracy", "false_diagnoses"} <= set(
            summary
        )
    assert metrics["variants"]["deterministic"]["trials"] == 1
    assert metrics["variants"]["multi_agent"]["trials"] == 2
    detection = metrics["detection"]
    assert (detection["replays_scored"], detection["true_positives"]) == (3, 2)
    assert (detection["false_positives"], detection["false_negatives"]) == (0, 0)
    assert detection["delay_clock"] == "event time"
    multi = metrics["variants"]["multi_agent"]
    assert multi["evidence"]["source"] == "automatic"
    assert multi["evidence"]["cited"] > 0
    assert multi["latency_seconds_p95"] is not None
    assert multi["human_audit"]["audited_citations"] == 0
    assert metrics["judge_model_scores"] is None
    # The fake provider is deterministic, so trials agree.
    assert metrics["variants"]["multi_agent"]["accuracy_stdev"] == 0.0

    # A second execution repeats nothing.
    async with database.transaction() as session:
        await session.execute(
            update(EvaluationRun)
            .where(EvaluationRun.id == run.id)
            .values(status="queued", completed_at=None, metrics=None)
        )
    assert await executor.claim_and_execute() == run.id
    async with database.transaction() as session:
        again = list(
            await session.scalars(select(EvaluationCase).where(EvaluationCase.run_id == run.id))
        )
        count = len(
            list(await session.scalars(select(Investigation.id).where(Investigation.id.in_(ids))))
        )
    assert {(c.id, c.investigation_id, c.completed_at) for c in again} == {
        (c.id, c.investigation_id, c.completed_at) for c in cases
    }
    assert count == len(ids)
