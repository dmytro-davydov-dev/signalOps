"""The run-level spend ceiling during execution (P13-T06).

Settings name a non-fake provider, so the run goes through the paid path, while the
in-process workers answer with the fake provider: no network, real accounting.
"""

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr
from sqlalchemy import delete, select, update

from signalops.agents.jobs import JobRunner
from signalops.contracts.common import replay_scope_for
from signalops.contracts.evaluation import ScenarioSplit, SystemVariant
from signalops.contracts.workflow import IncidentSeverity
from signalops.detection.rules import Window
from signalops.evaluation.runner import (
    PaidRunRefusedError,
    RunExecutor,
    RunRequest,
    queue_run,
    run_commitment,
)
from signalops.gateway.fake import FakeProvider
from signalops.persistence import database as db
from signalops.persistence.models import EvaluationCase, EvaluationRun, Incident, Investigation, Job
from signalops.persistence.repositories.evaluation import EvaluationRepository
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.evaluation_support import SETTINGS, engine_for, runner_context
from tests.integration.multi_agent_support import roles_with
from tests.integration.test_investigator import JOBS

pytestmark = pytest.mark.integration

PAID = SETTINGS.model_copy(
    update={"model_provider": "anthropic", "model_api_key": SecretStr("unused")}
)
BUDGET = 2_000_000
SCENARIOS = ("downstream-timeout/dev", "mixed-timeout-deploy/dev")


def paid_request(**fields: object) -> RunRequest:
    base: dict[str, object] = {
        "label": "ceiling",
        "split": ScenarioSplit.DEVELOPMENT,
        "variants": (SystemVariant.DETERMINISTIC, SystemVariant.MULTI_AGENT),
        "trials": 1,
        "budget_microusd": BUDGET,
        "max_steps": 40,
        "deadline_seconds": 300,
        "scenarios": SCENARIOS,
        "allow_paid": True,
        "max_spend_microusd": len(SCENARIOS) * BUDGET,
        "approval_ref": "test approval",
    }
    return RunRequest.model_validate(base | fields)


async def test_a_paid_request_without_its_guards_is_refused_before_queueing(
    database: db.Database,
) -> None:
    async with database.transaction() as session:
        before = len(list(await session.scalars(select(EvaluationRun.id))))
    for bad in (
        paid_request(allow_paid=False),
        paid_request(max_spend_microusd=None),
        paid_request(approval_ref=None),
        paid_request(max_spend_microusd=len(SCENARIOS) * BUDGET - 1),
    ):
        with pytest.raises(PaidRunRefusedError):
            await queue_run(database, bad, workspace_id=DEMO_WORKSPACE_ID, settings=PAID)
    async with database.transaction() as session:
        assert len(list(await session.scalars(select(EvaluationRun.id)))) == before


async def test_a_run_that_reaches_its_ceiling_stops_requesting_and_completes(
    database: db.Database,
) -> None:
    run = await queue_run(database, paid_request(), workspace_id=DEMO_WORKSPACE_ID, settings=PAID)
    assert run.max_spend_microusd == len(SCENARIOS) * BUDGET
    assert run.approval_ref == "test approval"
    assert run.limits["worst_case_microusd"] == len(SCENARIOS) * BUDGET
    # Lower the stored ceiling to one case's budget plus one microUSD: the first
    # investigation fits, and once its recorded spend counts, the next cannot.
    async with database.transaction() as session:
        await session.execute(
            update(EvaluationRun)
            .where(EvaluationRun.id == run.id)
            .values(max_spend_microusd=BUDGET + 1)
        )
        await session.execute(delete(Job))
    stop = asyncio.Event()
    engine = engine_for(database, FakeProvider(responder=roles_with()))
    tasks = [
        asyncio.create_task(JobRunner(database, engine, JOBS, owner=f"ceiling-{i}").run(stop))
        for i in range(2)
    ]
    try:
        executor = RunExecutor(runner_context(database), owner="ceiling-runner")
        while (claimed := await executor.claim_and_execute()) is not None:
            if claimed == run.id:
                break
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks)
    async with database.transaction() as session:
        stored = await session.get_one(EvaluationRun, run.id, populate_existing=True)
        cases = {
            case.case_key: case
            for case in await session.scalars(
                select(EvaluationCase).where(EvaluationCase.run_id == run.id)
            )
        }
        committed = await run_commitment(session, run.id)
    assert stored.status == "completed", stored.error
    first = cases["downstream-timeout/dev@2/multi_agent/1"]
    second = cases["mixed-timeout-deploy/dev@2/multi_agent/1"]
    assert first.status == "scored"
    assert second.status == "failed"
    assert second.error is not None
    assert second.error.startswith("run_budget")
    assert second.investigation_id is None
    # The deterministic baseline costs nothing, so the ceiling never stops it.
    assert {cases[k].status for k in cases if "/deterministic/" in k} == {"scored"}
    assert committed <= BUDGET + 1
    metrics = stored.metrics
    assert metrics is not None
    assert metrics["variants"]["multi_agent"]["run_budget_stops"] == 1
    assert metrics["spend"]["max_spend_microusd"] == BUDGET + 1
    assert metrics["spend"]["approval_ref"] == "test approval"


async def test_concurrent_requests_cannot_jointly_pass_the_ceiling(database: db.Database) -> None:
    request = paid_request(
        variants=(SystemVariant.MULTI_AGENT,),
        trials=5,
        scenarios=("downstream-timeout/dev",),
        max_spend_microusd=5 * BUDGET,
    )
    run = await queue_run(database, request, workspace_id=DEMO_WORKSPACE_ID, settings=PAID)
    async with database.transaction() as session:
        await session.execute(
            update(EvaluationRun)
            .where(EvaluationRun.id == run.id)
            .values(max_spend_microusd=2 * BUDGET)
        )
        await session.execute(delete(Job))
        repository = EvaluationRepository(session)
        scenario = (await repository.scenarios(ScenarioSplit.DEVELOPMENT))[0]
        replay = await repository.ensure_replay(run.id, scenario.id)
        cases = [
            await repository.ensure_case(
                replay,
                case_key=f"parallel/{trial}",
                variant=SystemVariant.MULTI_AGENT,
                trial=trial,
                expected_outcome={"incident_expected": False, "diagnosable": False},
            )
            for trial in range(1, 6)
        ]
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
    now = datetime.now(UTC)
    async with database.transaction() as session:
        incident_id = await IncidentRepository(session).create(
            NewIncident(
                scope=scope,
                fingerprint=f"ceiling:{uuid.uuid4().hex[:8]}",
                title="High error rate on payment",
                severity=IncidentSeverity.HIGH,
                window=Window(now - timedelta(minutes=5), now - timedelta(minutes=4)),
                detected_at=now - timedelta(minutes=4),
            )
        )
        incident = await session.get_one(Incident, incident_id)
        stored = await session.get_one(EvaluationRun, run.id, populate_existing=True)
    # Separate executors stand in for racing runners; the run row lock serializes them.
    results = await asyncio.gather(
        *(
            RunExecutor(runner_context(database), owner=f"racer-{i}").request_investigation(
                stored, case, scope, incident
            )
            for i, case in enumerate(cases)
        )
    )
    granted = [result for result in results if result is not None]
    assert len(granted) == 2
    async with database.transaction() as session:
        assert await run_commitment(session, run.id) == 2 * BUDGET
        # An unknown call's reservation stays counted after its investigation ends.
        await session.execute(
            update(Investigation)
            .where(Investigation.id == granted[0])
            .values(status="failed", completed_at=now, spent_microusd=100, reserved_microusd=250)
        )
    async with database.transaction() as session:
        assert await run_commitment(session, run.id) == BUDGET + 350
