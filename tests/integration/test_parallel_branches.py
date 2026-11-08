"""Parallel specialist branches: per-investigation and global limits, and shared budgets."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, update

from signalops.agents.jobs import JobRunner, claim_job
from signalops.agents.multi_agent import downstream_call_reserve
from signalops.agents.nodes import MAX_PARALLEL_BRANCHES, Node, NodeContext, Parallel
from signalops.contracts.roles import AgentEnding, InvestigationPlan
from signalops.contracts.steps import IncidentBrief
from signalops.contracts.workflow import AgentRole, InvestigationStatus, StepStatus
from signalops.gateway.errors import (
    BudgetExceededError,
    BudgetShareExhaustedError,
    ProviderErrorCategory,
)
from signalops.gateway.fake import FakeFailure, FakeProvider, FakeStep
from signalops.gateway.gateway import BudgetLimit
from signalops.gateway.types import ModelRequest, ModelResponse
from signalops.persistence import database as db
from signalops.persistence.models import Investigation, Job
from signalops.persistence.repositories.llm_calls import LlmCallRepository
from tests.integration.multi_agent_support import (
    findings_of,
    queued_v3,
    roles_with,
    steps_by_key,
    timeout_incident,
    v3_engine,
)
from tests.integration.test_investigator import JOBS, llm_calls_of, row_of, run_once
from tests.integration.test_model_gateway import (
    MAX_COST,
    REQUEST,
    STEP,
    budget_of,
    gateway,
    running_step,
)
from tests.integration.test_model_gateway import OK as OK_ANSWER

pytestmark = pytest.mark.integration

SPECIALIST_PROMPTS = frozenset({"telemetry-v1", "deployment-v1", "dependency-v1"})


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


class Probe:
    """A fake provider that holds each call open and counts specialist calls in flight."""

    name = "fake"

    def __init__(self, delay: float = 0.15) -> None:
        self.inner = FakeProvider(responder=roles_with())
        self.delay = delay
        self.active = 0
        self.peak = 0

    async def complete(self, request: ModelRequest) -> ModelResponse:
        specialist = request.prompt_version in SPECIALIST_PROMPTS
        if specialist:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(self.delay)
            return await self.inner.complete(request)
        finally:
            if specialist:
                self.active -= 1


def test_a_parallel_stage_holds_at_most_three_branches() -> None:
    async def run(_context: NodeContext) -> IncidentBrief:
        raise AssertionError

    nodes = tuple(Node(f"n{i}", AgentRole.COORDINATOR, IncidentBrief, run) for i in range(4))
    assert MAX_PARALLEL_BRANCHES == 3
    with pytest.raises(ValueError, match="1 to 3"):
        Parallel(nodes)
    with pytest.raises(ValueError, match="1 to 3"):
        Parallel(())


async def test_specialist_branches_run_at_once_each_as_its_own_step(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9201)
    investigation_id = await queued_v3(database, scope, incident_id)
    probe = Probe()

    await run_once(database, v3_engine(database, probe))

    assert probe.peak == MAX_PARALLEL_BRANCHES
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    steps = await steps_by_key(database, investigation_id)
    branches = [steps[key] for key in ("telemetry", "deployment", "dependency")]
    assert {step.status for step in branches} == {StepStatus.COMPLETED}
    # Every branch opened before any finished.
    started = [moment(step.started_at) for step in branches]
    ended = [moment(step.completed_at) for step in branches]
    assert max(started) < min(ended)
    assert moment(steps["plan"].completed_at) <= min(started)
    assert moment(steps["synthesize"].started_at) >= max(ended)


def moment(value: datetime | None) -> datetime:
    assert value is not None
    return value


async def test_global_slots_queue_branches_of_several_jobs_without_dropping_any(
    database: db.Database,
) -> None:
    first_scope, first_incident = await timeout_incident(database, 9202)
    second_scope, second_incident = await timeout_incident(database, 9203)
    first = await queued_v3(database, first_scope, first_incident)
    second = await queued_v3(database, second_scope, second_incident)
    probe = Probe()
    engine = v3_engine(database, probe)
    engine._slots = asyncio.Semaphore(2)

    leases = [await claim_job(database, owner, JOBS) for owner in ("worker-a", "worker-b")]
    assert all(lease is not None for lease in leases)
    await asyncio.gather(
        *(
            JobRunner(database, engine, JOBS, owner=lease.owner).execute(lease, asyncio.Event())
            for lease in leases
            if lease is not None
        )
    )

    # Six branches wanted to run; two slots let two run at a time, and all six finished.
    assert probe.peak == 2
    for investigation_id in (first, second):
        assert (await row_of(database, investigation_id)).status == InvestigationStatus.COMPLETED
        steps = await steps_by_key(database, investigation_id)
        for key in ("telemetry", "deployment", "dependency"):
            assert steps[key].status == StepStatus.COMPLETED


async def test_branch_shares_and_the_downstream_reserve_bound_every_call(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9204)
    investigation_id = await queued_v3(database, scope, incident_id, budget=2_000_000)
    probe = Probe(delay=0.05)

    await run_once(database, v3_engine(database, probe))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    assert row.reserved_microusd == 0
    assert row.spent_microusd <= row.budget_microusd
    steps = await steps_by_key(database, investigation_id)
    plan = InvestigationPlan.model_validate(steps["plan"].output)
    async with database.transaction() as session:
        calls = LlmCallRepository(session)
        for branch in plan.branches:
            usage = await calls.step_usage(steps[branch.step_key].id)
            assert 0 < usage <= branch.budget_share_microusd
    assert (
        sum(b.budget_share_microusd for b in plan.branches) + (plan.downstream_reserve_microusd)
        <= row.budget_microusd
    )


async def test_branches_out_of_share_report_it_and_leave_the_reserve_for_later_roles(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9205)
    probe = Probe(delay=0)
    engine = v3_engine(database, probe)
    assert engine._runtime is not None
    reserve = 2 * downstream_call_reserve(engine._runtime)
    # Shares far below any specialist call's maximum.
    investigation_id = await queued_v3(database, scope, incident_id, budget=reserve + 3_000)

    await run_once(database, engine)

    row = await row_of(database, investigation_id)
    # The branches stopped; the investigation still ended on its own terms.
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    assert row.diagnosis is not None
    assert any("budget share" in item for item in row.diagnosis["missing_evidence"])
    steps = await steps_by_key(database, investigation_id)
    for key in ("telemetry", "deployment", "dependency"):
        stored = findings_of(steps[key])
        assert stored.provenance.ended_by is AgentEnding.BUDGET_SHARE
        assert stored.provenance.model_turns == 0
    assert await llm_calls_of(database, investigation_id) == []
    assert row.reserved_microusd == row.spent_microusd == 0


async def test_a_call_over_its_step_cap_or_into_the_reserve_reserves_nothing(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database, budget=10 * MAX_COST)
    model, _provider = gateway(database, OK_ANSWER)

    await model.complete(lease, STEP, REQUEST, budget=BudgetLimit(step_cap=2 * MAX_COST))
    async with database.transaction() as session:
        calls = LlmCallRepository(session)
        used = await calls.step_usage(await calls.step_id(investigation_id, STEP))
    assert 0 < used < MAX_COST

    # The step's earlier spend counts against its cap.
    with pytest.raises(BudgetShareExhaustedError):
        await model.complete(lease, STEP, REQUEST, budget=BudgetLimit(step_cap=MAX_COST))
    # Money held back for later roles is not available to this call.
    spent = (await budget_of(database, investigation_id))[1]
    held_back = 10 * MAX_COST - spent - MAX_COST + 1
    with pytest.raises(BudgetShareExhaustedError):
        await model.complete(lease, STEP, REQUEST, budget=BudgetLimit(held_back=held_back))
    assert (await budget_of(database, investigation_id))[0] == 0

    # The same call with the whole budget fits.
    await model.complete(lease, STEP, REQUEST, budget=BudgetLimit(held_back=held_back - 1))
    # Without a limit, a call that does not fit ends the investigation as before.
    async with database.transaction() as session:
        await session.execute(
            update(Investigation)
            .where(Investigation.id == investigation_id)
            .values(budget_microusd=Investigation.spent_microusd)
        )
    with pytest.raises(BudgetExceededError):
        await model.complete(lease, STEP, REQUEST)


async def test_an_unknown_outcome_keeps_counting_against_the_step_cap(
    database: db.Database,
) -> None:
    investigation_id, lease = await running_step(database)
    model, _provider = gateway(database, FakeFailure(fail=ProviderErrorCategory.REJECTED))
    timed_out, _ = gateway(database, FakeFailure(fail=ProviderErrorCategory.TIMEOUT))
    with pytest.raises(Exception, match="model call failed"):
        await timed_out.complete(lease, STEP, REQUEST)
    async with database.transaction() as session:
        calls = LlmCallRepository(session)
        used = await calls.step_usage(await calls.step_id(investigation_id, STEP))
    # Three timed-out attempts, each reservation held because each may have been billed.
    assert used == 3 * MAX_COST
    with pytest.raises(BudgetShareExhaustedError):
        await model.complete(lease, STEP, REQUEST, budget=BudgetLimit(step_cap=4 * MAX_COST - 1))


async def test_a_failed_branch_keeps_its_siblings_work_for_the_retry(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9206)
    investigation_id = await queued_v3(database, scope, incident_id)
    failing = {"left": 3}

    def deployment(request: ModelRequest) -> FakeStep:
        if failing["left"]:
            failing["left"] -= 1
            return FakeFailure(fail=ProviderErrorCategory.THROTTLED)
        from signalops.gateway.fake_roles import deployment_step

        return deployment_step(request)

    provider = FakeProvider(responder=roles_with({"deployment-v1": deployment}))
    engine = v3_engine(database, provider)

    await run_once(database, engine)
    steps = await steps_by_key(database, investigation_id)
    assert steps["deployment"].status == StepStatus.FAILED
    assert steps["telemetry"].status == StepStatus.COMPLETED
    assert steps["dependency"].status == StepStatus.COMPLETED
    first_requests = len(provider.requests)

    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.investigation_id == investigation_id)
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    await run_once(database, engine)

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    later = provider.requests[first_requests:]
    # The retry ran only the failed branch and what follows it.
    assert {r.prompt_version for r in later} == {
        "deployment-v1",
        "synthesizer-v1",
        "verifier-v1",
    }
    steps = await steps_by_key(database, investigation_id)
    assert steps["telemetry"].attempt == 1
    assert steps["deployment"].attempt == 2
    assert uuid.UUID(str(steps["deployment"].id))
