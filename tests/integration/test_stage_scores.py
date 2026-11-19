"""Stage scores stored by evaluation runs on the fake provider (P13-T02).

A development run stores stage scores for every agent case without a model call.
Mutated fake roles, run through the real workflow, move the metric of the stage they
change: a synthesizer leading with its second hypothesis loses the lead, a verifier
accepting everything accepts a wrong lead, and a specialist given no turns ends on a
limit and finds nothing.
"""

import copy
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import select, update

from signalops.agents.multi_agent import PLAN, WORKFLOW_V3, plan_investigation
from signalops.agents.nodes import Node, NodeContext, Workflow
from signalops.agents.workflow import WORKFLOWS
from signalops.contracts.common import replay_scope_for
from signalops.contracts.evaluation import ExpectedOutcome, SystemVariant
from signalops.contracts.evaluation_stages import StageScores, VerdictCell
from signalops.contracts.roles import AgentEnding, InvestigationPlan
from signalops.contracts.workflow import AgentRole
from signalops.evaluation.runner import stage_scores_of
from signalops.gateway.fake import FakeAnswer, FakeProvider, FakeStep
from signalops.gateway.fake_roles import synthesizer_step, verifier_step
from signalops.gateway.types import ModelRequest, ToolUseBlock
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep, EvaluationCase, EvaluationScenario
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_SERVICES, DEMO_WORKSPACE_ID
from tests.integration.evaluation_support import engine_for, model_call_count, run_with
from tests.integration.multi_agent_support import roles_with

pytestmark = pytest.mark.integration

DEV = (
    "downstream-timeout/dev",
    "deployment-regression/dev",
    "mixed-timeout-deploy/dev",
    "unexplained-errors/dev",
)
TIMEOUT = ("downstream-timeout/dev",)
ORDERS_DB = next(service.id for service in DEMO_SERVICES if service.name == "orders-db")
Answer = Callable[[ModelRequest], FakeStep]


def edited(step: Answer, edit: Callable[[dict[str, Any]], None]) -> Answer:
    """A fake role whose submission is changed by `edit`."""

    def respond(request: ModelRequest) -> FakeStep:
        answer = step(request)
        assert isinstance(answer, FakeAnswer)
        blocks = []
        for block in answer.content:
            if isinstance(block, ToolUseBlock) and block.name.startswith("submit"):
                submitted = copy.deepcopy(block.input)
                edit(submitted)
                block = block.model_copy(update={"input": submitted})
            blocks.append(block)
        return answer.model_copy(update={"content": tuple(blocks)})

    return respond


def lead_with_second(report: dict[str, Any]) -> None:
    hypotheses = report["hypotheses"]
    assert len(hypotheses) >= 2
    hypotheses[0], hypotheses[1] = hypotheses[1], hypotheses[0]


def misattribute(report: dict[str, Any]) -> None:
    # Same citations, another service: a wrong lead the fake verifier can see through.
    report["hypotheses"][0]["service_id"] = str(ORDERS_DB)


def accept_everything(report: dict[str, Any]) -> None:
    report["verdict"] = "supported"
    report.pop("follow_up", None)
    report["missing_evidence"] = []
    for check in report["citation_checks"]:
        check["supports"] = True


def no_telemetry_turns() -> dict[str, Workflow]:
    async def plan(context: NodeContext) -> InvestigationPlan:
        planned = await plan_investigation(context)
        return planned.model_copy(
            update={
                "branches": tuple(
                    branch.model_copy(update={"max_turns": 0})
                    if branch.role is AgentRole.TELEMETRY_INVESTIGATOR
                    else branch
                    for branch in planned.branches
                )
            }
        )

    v3 = WORKFLOWS[WORKFLOW_V3]
    nodes = tuple(
        Node(PLAN.key, PLAN.role, PLAN.output, plan) if n is PLAN else n for n in v3.nodes
    )
    return {**WORKFLOWS, WORKFLOW_V3: Workflow(WORKFLOW_V3, nodes, v3.conclusion)}


async def v3_stages(database: db.Database, responder: Answer, **engine: Any) -> StageScores:
    _, cases = await run_with(
        database,
        engine_for(database, FakeProvider(responder=responder), **engine),
        scenarios=TIMEOUT,
        variants=(SystemVariant.MULTI_AGENT,),
    )
    (case,) = cases
    assert case.stage_scores is not None, case.error
    return StageScores.model_validate(case.stage_scores)


def unrelated(stages: StageScores) -> tuple[object, ...]:
    return (
        stages.lead_found,
        stages.lead_found_strict,
        tuple((s.role, s.named_service, s.cited, s.cited_supported) for s in stages.specialists),
        stages.limit_endings,
        stages.synthesis_violations,
    )


async def test_a_development_run_stores_stage_scores_without_model_calls(
    database: db.Database,
) -> None:
    run, cases = await run_with(
        database, engine_for(database, FakeProvider(responder=roles_with())), scenarios=DEV
    )
    assert run.status == "completed", run.error
    assert len(cases) == 2 * len(DEV)
    for case in cases:
        assert case.stage_scores is not None, case.case_key
        stages = StageScores.model_validate(case.stage_scores)
        assert stages.workflow_version == {"single_agent": "v2", "multi_agent": "v3"}[case.variant]
        assert stages.synthesis_violations == ()
    by_key = {case.case_key: StageScores.model_validate(case.stage_scores) for case in cases}
    mixed = by_key["mixed-timeout-deploy/dev@2/multi_agent/1"]
    assert (mixed.expected_contradiction, mixed.contradiction_listed) == ("checkout", True)
    assert by_key["downstream-timeout/dev@2/multi_agent/1"].lead_retained is True
    assert by_key["downstream-timeout/dev@2/single_agent/1"].proposal_correct is True
    assert run.metrics is not None
    multi = run.metrics["variants"]["multi_agent"]["stages"]
    assert multi["cases_with_stage_scores"] == len(DEV)
    assert multi["specialist_lead_recall"]["denominator"] == 3
    assert run.metrics["variants"]["single_agent"]["stages"]["single_agent"]["proposal_correct"][
        "denominator"
    ] == len(DEV)

    # Scoring again reads stored rows only: the gateway is never called.
    calls = await model_call_count(database)
    async with database.transaction() as session:
        scenarios = {
            row.id: row
            for row in await session.scalars(
                select(EvaluationScenario).where(EvaluationScenario.version == "2")
            )
        }
        replay_scopes = dict(
            (
                await session.execute(
                    select(EvaluationCase.id, EvaluationCase.replay_id).where(
                        EvaluationCase.run_id == run.id
                    )
                )
            ).all()
        )
    for case in cases:
        assert case.investigation_id is not None
        expected = ExpectedOutcome.model_validate(scenarios[case.scenario_id].expected_outcome)
        scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(replay_scopes[case.id]))
        again = await stage_scores_of(database, scope, expected, case.investigation_id)
        assert again == case.stage_scores
    assert await model_call_count(database) == calls


async def test_a_synthesizer_leading_with_its_second_hypothesis_loses_the_lead(
    database: db.Database,
) -> None:
    baseline = await v3_stages(database, roles_with())
    mutated = await v3_stages(
        database, roles_with({"synthesizer-v1": edited(synthesizer_step, lead_with_second)})
    )
    assert (baseline.lead_retained, mutated.lead_retained) == (True, False)
    assert unrelated(mutated) == unrelated(baseline)


async def test_a_verifier_accepting_everything_accepts_a_wrong_lead(
    database: db.Database,
) -> None:
    wrong = {"synthesizer-v1": edited(synthesizer_step, misattribute)}
    baseline = await v3_stages(database, roles_with(wrong))
    mutated = await v3_stages(
        database,
        roles_with(wrong | {"verifier-v2": edited(verifier_step, accept_everything)}),
    )
    assert baseline.leading_correct is False
    assert baseline.verdict_cell in {VerdictCell.WRONG_REJECTED, VerdictCell.WRONG_FOLLOW_UP}
    assert mutated.verdict_cell is VerdictCell.WRONG_ACCEPTED
    assert unrelated(mutated) == unrelated(baseline)
    assert mutated.lead_retained == baseline.lead_retained is False


async def test_a_specialist_with_no_turns_ends_on_a_limit_and_finds_no_lead(
    database: db.Database,
) -> None:
    baseline = await v3_stages(database, roles_with())
    mutated = await v3_stages(database, roles_with(), workflows=no_telemetry_turns())
    assert baseline.limit_endings == ()
    assert [(e.step_key, e.ended_by) for e in mutated.limit_endings] == [
        ("telemetry", AgentEnding.NO_TURNS)
    ]
    assert (baseline.lead_found, mutated.lead_found) == (True, False)


async def test_a_planted_synthesis_violation_is_reported_as_a_defect(
    database: db.Database,
) -> None:
    _, (case,) = await run_with(
        database,
        engine_for(database, FakeProvider(responder=roles_with())),
        scenarios=TIMEOUT,
        variants=(SystemVariant.MULTI_AGENT,),
    )
    assert case.investigation_id is not None
    async with database.transaction() as session:
        step = await session.scalar(
            select(AgentStep).where(
                AgentStep.investigation_id == case.investigation_id,
                AgentStep.step_key == "synthesize",
            )
        )
        assert step is not None
        assert step.output is not None
        output = copy.deepcopy(step.output)
        output["report"]["hypotheses"][0]["evidence_signal_ids"].append(str(uuid.uuid4()))
        await session.execute(
            update(AgentStep).where(AgentStep.id == step.id).values(output=output)
        )
        scenario = await session.get_one(EvaluationScenario, case.scenario_id)
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(case.replay_id))
    stages = await stage_scores_of(
        database,
        scope,
        ExpectedOutcome.model_validate(scenario.expected_outcome),
        case.investigation_id,
    )
    assert stages is not None
    assert StageScores.model_validate(stages).synthesis_violations == (
        "hypotheses[0] cites a signal no specialist cited",
    )
