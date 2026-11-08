"""Workflow `v3` roles and handoffs on real evidence, with the deterministic fake roles."""

import json
import uuid
from typing import Any

import pytest
from sqlalchemy import delete

from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.roles import (
    AgentEnding,
    InvestigationPlan,
    SynthesisBrief,
    SynthesisOutput,
    Verdict,
    VerificationBrief,
    VerificationOutput,
)
from signalops.contracts.workflow import (
    AgentRole,
    CostBasis,
    InvestigationStatus,
    LlmCallStatus,
    StepStatus,
    ToolName,
)
from signalops.demo.generator import Scenario
from signalops.gateway.fake import FakeAnswer, FakeProvider
from signalops.gateway.fake_investigator import submit, turn_of
from signalops.gateway.fake_roles import synthesizer_step, verifier_step
from signalops.gateway.types import ModelRequest, StopReason, ToolUseBlock
from signalops.persistence import database as db
from signalops.persistence.models import Job
from tests.integration.multi_agent_support import (
    V3_STEPS,
    brief_of,
    findings_of,
    offered,
    queued_v3,
    roles_with,
    steps_by_key,
    timeout_incident,
    timeout_incident_with_graph,
    v3_engine,
)
from tests.integration.test_investigator import (
    SERVICE_IDS,
    llm_calls_of,
    replayed_incident,
    row_of,
    run_once,
    tool_calls_of,
)

pytestmark = pytest.mark.integration

ROLE_TOOLS = {
    "telemetry-v1": {"query_logs", "get_trace_summary", "submit_findings"},
    "deployment-v1": {"get_deployments", "query_logs", "submit_findings"},
    "dependency-v1": {"get_service_dependencies", "retrieve_runbooks", "submit_findings"},
    "synthesizer-v1": {"submit_synthesis"},
    "verifier-v2": {"submit_verification"},
}


@pytest.fixture(autouse=True)
async def no_other_jobs(database: db.Database) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))


async def test_specialists_synthesis_and_verification_name_the_faulty_service(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9101)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = FakeProvider(responder=roles_with())

    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.status is DiagnosisStatus.DIAGNOSED
    assert diagnosis.workflow_version == "v3"
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_name == "bank-gateway"

    steps = await steps_by_key(database, investigation_id)
    assert sorted(steps) == sorted(V3_STEPS)
    assert {key: step.agent_role for key, step in steps.items()} == {
        "load_context": AgentRole.COORDINATOR,
        "plan": AgentRole.COORDINATOR,
        "telemetry": AgentRole.TELEMETRY_INVESTIGATOR,
        "deployment": AgentRole.DEPLOYMENT_INVESTIGATOR,
        "dependency": AgentRole.DEPENDENCY_INVESTIGATOR,
        "synthesize": AgentRole.SYNTHESIZER,
        "verify": AgentRole.VERIFIER,
        "follow_up": AgentRole.COORDINATOR,
        "reverify": AgentRole.VERIFIER,
        "conclude": AgentRole.SYNTHESIZER,
    }
    assert {step.status for step in steps.values()} == {StepStatus.COMPLETED}

    # Every specialist returns typed findings with provenance and explicit uncertainty.
    for key, prompt in (
        ("telemetry", "telemetry-v1"),
        ("deployment", "deployment-v1"),
        ("dependency", "dependency-v1"),
    ):
        stored = findings_of(steps[key])
        assert stored.provenance.prompt_version == prompt
        assert stored.provenance.ended_by is AgentEnding.SUBMITTED
        assert stored.provenance.model == "fake-1"
        assert stored.provenance.model_turns >= 2
        assert stored.provenance.tool_calls
        assert {ref.tool.value for ref in stored.provenance.tool_calls} <= ROLE_TOOLS[prompt]
        assert stored.report.findings or stored.report.missing_evidence
        assert all(finding.uncertainty for finding in stored.report.findings)
    # This replay has no projected graph or runbooks, so the dependency role says so.
    assert findings_of(steps["dependency"]).report.missing_evidence
    telemetry = findings_of(steps["telemetry"]).report.findings[0]
    assert telemetry.service_id == SERVICE_IDS["bank-gateway"]
    deployment = findings_of(steps["deployment"]).report.findings[0]
    assert deployment.service_id == SERVICE_IDS["inventory"]
    assert deployment.failure_type == "deployment_regression"

    synthesis = SynthesisOutput.model_validate(steps["synthesize"].output)
    assert synthesis.provenance.prompt_version == "synthesizer-v1"
    assert synthesis.report.hypotheses[0].service_id == SERVICE_IDS["bank-gateway"]
    assert synthesis.report.hypotheses[0].supporting_roles == (AgentRole.TELEMETRY_INVESTIGATOR,)
    verification = VerificationOutput.model_validate(steps["verify"].output)
    assert verification.report.verdict is Verdict.SUPPORTED
    assert set(diagnosis.root_cause.evidence_signal_ids) <= set(verification.report.supporting())
    # The deployment lead stays visible as an alternative, not hidden by the merge.
    assert any("inventory" in item.hypothesis for item in diagnosis.alternatives)

    calls = await llm_calls_of(database, investigation_id)
    assert {call.prompt_version for call in calls} == set(ROLE_TOOLS)
    assert {call.status for call in calls} == {LlmCallStatus.SUCCEEDED}
    assert {call.cost_basis for call in calls} == {CostBasis.ESTIMATED}
    assert row.reserved_microusd == 0


async def test_each_role_receives_only_its_own_context_and_tools(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9102)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = FakeProvider(responder=roles_with())

    await run_once(database, v3_engine(database, provider))

    seen: set[str] = set()
    for request in provider.requests:
        prompt = request.prompt_version
        seen.add(prompt)
        assert offered(request) == ROLE_TOOLS[prompt], prompt
        brief = brief_of(request)
        if prompt == "telemetry-v1":
            assert brief["detector_signal_ids"]
            assert brief["incident_service_ids"] == []
        elif prompt in ("deployment-v1", "dependency-v1"):
            assert brief["detector_signal_ids"] == []
            assert SERVICE_IDS["payment"] in {uuid.UUID(i) for i in brief["incident_service_ids"]}
        elif prompt == "synthesizer-v1":
            parsed = SynthesisBrief.model_validate(brief)
            # Reports only: no tool results, provenance, or raw evidence text.
            assert {report.role for report in parsed.reports} == {
                AgentRole.TELEMETRY_INVESTIGATOR,
                AgentRole.DEPLOYMENT_INVESTIGATOR,
                AgentRole.DEPENDENCY_INVESTIGATOR,
            }
            assert "tool_calls" not in str(brief)
            assert "items" not in brief
        else:
            parsed_verification = VerificationBrief.model_validate(brief)
            assert parsed_verification.cited
            assert not parsed_verification.unreadable_signal_ids
            leading = parsed_verification.synthesis.hypotheses[0]
            assert {c.signal_id for c in parsed_verification.cited} == set(
                leading.evidence_signal_ids
            )
    assert seen == set(ROLE_TOOLS)

    steps = await steps_by_key(database, investigation_id)
    plan = InvestigationPlan.model_validate(steps["plan"].output)
    # 40 steps less 10 nodes: synthesis and verifications first, 5 per branch, 3 to follow up.
    assert (plan.synthesis_turns, plan.verification_turns, plan.follow_up_turns) == (2, 2, 3)
    assert {branch.max_turns for branch in plan.branches} == {5}
    assert sum(branch.budget_share_microusd for branch in plan.branches) <= (
        5_000_000 - plan.downstream_reserve_microusd
    )
    assert plan.downstream_reserve_microusd > 0
    assert {branch.brief.role for branch in plan.branches} == {
        branch.role for branch in plan.branches
    }


def _input_of(answer: FakeAnswer) -> dict[str, Any]:
    block = answer.content[0]
    assert isinstance(block, ToolUseBlock)
    loaded: dict[str, Any] = json.loads(json.dumps(block.input))
    return loaded


def _submitting(name: str, payload: dict[str, Any]) -> FakeAnswer:
    block = ToolUseBlock(id="toolu_bad", name=name, input=payload)
    return FakeAnswer(content=(block,), stop_reason=StopReason.TOOL_USE)


async def test_a_specialist_cannot_cite_what_it_did_not_read(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9103)
    investigation_id = await queued_v3(database, scope, incident_id)
    unread = uuid.uuid4()

    def telemetry(request: ModelRequest) -> FakeAnswer:
        if turn_of(request) == 0:
            return _submitting(
                "submit_findings",
                {
                    "findings": [
                        {
                            "claim": "Payment is broken.",
                            "service_id": str(SERVICE_IDS["payment"]),
                            "failure_type": "elevated_errors",
                            "confidence": 0.9,
                            "uncertainty": "none",
                            "evidence_signal_ids": [str(unread)],
                        }
                    ]
                },
            )
        return _submitting("submit_findings", {"findings": [], "missing_evidence": []})

    provider = FakeProvider(responder=roles_with({"telemetry-v1": telemetry}))
    await run_once(database, v3_engine(database, provider))

    steps = await steps_by_key(database, investigation_id)
    stored = findings_of(steps["telemetry"])
    assert stored.report.findings == ()
    assert stored.provenance.ended_by is AgentEnding.REPAIR_LIMIT
    telemetry_requests = [r for r in provider.requests if r.prompt_version == "telemetry-v1"]
    assert "were not returned by your tool results" in str(telemetry_requests[1].messages[-1])
    diagnosis = Diagnosis.model_validate((await row_of(database, investigation_id)).diagnosis)
    assert diagnosis.root_cause is None or unread not in diagnosis.root_cause.evidence_signal_ids


async def test_a_specialist_cannot_call_another_roles_tools(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9104)
    investigation_id = await queued_v3(database, scope, incident_id)

    def dependency(request: ModelRequest) -> FakeAnswer:
        if turn_of(request) == 0:
            block = ToolUseBlock(id="toolu_x", name="query_logs", input={})
            return FakeAnswer(content=(block,), stop_reason=StopReason.TOOL_USE)
        return submit(1, _no_findings())

    provider = FakeProvider(responder=roles_with({"dependency-v1": dependency}))
    await run_once(database, v3_engine(database, provider))

    dependency_requests = [r for r in provider.requests if r.prompt_version == "dependency-v1"]
    assert "There is no such tool." in str(dependency_requests[1].messages[-1])
    steps = await steps_by_key(database, investigation_id)
    tools = await tool_calls_of(database, investigation_id)
    assert not [c for c in tools if c.agent_step_id == steps["dependency"].id]
    assert {c.tool_name for c in tools if c.agent_step_id == steps["telemetry"].id} <= {
        ToolName.QUERY_LOGS,
        ToolName.GET_TRACE_SUMMARY,
    }


def _no_findings() -> Any:
    from signalops.contracts.roles import SpecialistReport

    return SpecialistReport(missing_evidence=("Nothing read",))


async def test_the_synthesizer_cannot_cite_evidence_no_specialist_cited(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9105)
    investigation_id = await queued_v3(database, scope, incident_id)
    invented = uuid.uuid4()

    def synthesizer(request: ModelRequest) -> FakeAnswer:
        payload = _input_of(synthesizer_step(request))
        hypothesis = payload["hypotheses"][0]
        hypothesis["evidence_signal_ids"].append(str(invented))
        payload["hypotheses"] = [hypothesis]
        return _submitting("submit_synthesis", payload)

    provider = FakeProvider(responder=roles_with({"synthesizer-v1": synthesizer}))
    await run_once(database, v3_engine(database, provider))

    steps = await steps_by_key(database, investigation_id)
    synthesis = SynthesisOutput.model_validate(steps["synthesize"].output)
    assert synthesis.provenance.ended_by is AgentEnding.REPAIR_LIMIT
    assert synthesis.report.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE
    requests = [r for r in provider.requests if r.prompt_version == "synthesizer-v1"]
    assert "no specialist cited" in str(requests[1].messages[-1])
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    # No hypothesis, so the verifier had nothing to check and made no call.
    verification = VerificationOutput.model_validate(steps["verify"].output)
    assert verification.provenance.ended_by is AgentEnding.SKIPPED
    assert "verifier-v2" not in {r.prompt_version for r in provider.requests}


async def test_citations_the_verifier_rejects_never_reach_the_diagnosis(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident(database, 9106)
    investigation_id = await queued_v3(database, scope, incident_id)

    def verifier(request: ModelRequest) -> FakeAnswer:
        payload = _input_of(verifier_step(request))
        for check in payload["citation_checks"][1:]:
            check["supports"] = False
        return _submitting("submit_verification", payload)

    provider = FakeProvider(responder=roles_with({"verifier-v2": verifier}))
    await run_once(database, v3_engine(database, provider))

    steps = await steps_by_key(database, investigation_id)
    verification = VerificationOutput.model_validate(steps["verify"].output)
    accepted = verification.report.supporting()
    assert len(accepted) == 1
    diagnosis = Diagnosis.model_validate((await row_of(database, investigation_id)).diagnosis)
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.evidence_signal_ids == accepted
    assert any("did not accept" in item for item in diagnosis.limitations)


async def test_the_verifier_must_check_every_cited_signal(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9107)
    investigation_id = await queued_v3(database, scope, incident_id)

    def verifier(request: ModelRequest) -> FakeAnswer:
        payload = _input_of(verifier_step(request))
        payload["citation_checks"] = payload["citation_checks"][:1]
        return _submitting("submit_verification", payload)

    provider = FakeProvider(responder=roles_with({"verifier-v2": verifier}))
    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    steps = await steps_by_key(database, investigation_id)
    verification = VerificationOutput.model_validate(steps["verify"].output)
    assert verification.provenance.ended_by is AgentEnding.REPAIR_LIMIT
    assert verification.report.verdict is Verdict.UNSUPPORTED


async def test_no_findings_skip_synthesis_and_verification_calls(database: db.Database) -> None:
    scope, incident_id = await replayed_incident(database, Scenario.DOWNSTREAM_TIMEOUT, 9108)
    assert incident_id is not None
    investigation_id = await queued_v3(database, scope, incident_id)

    def nothing(request: ModelRequest) -> FakeAnswer:
        return submit(turn_of(request), _no_findings())

    provider = FakeProvider(
        responder=roles_with(
            {prompt: nothing for prompt in ("telemetry-v1", "deployment-v1", "dependency-v1")}
        )
    )
    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.missing_evidence
    assert {r.prompt_version for r in provider.requests} == {
        "telemetry-v1",
        "deployment-v1",
        "dependency-v1",
    }
    steps = await steps_by_key(database, investigation_id)
    synthesis = SynthesisOutput.model_validate(steps["synthesize"].output)
    assert synthesis.provenance.ended_by is AgentEnding.SKIPPED
    assert synthesis.provenance.model is None


async def test_too_few_steps_leave_branches_no_turns_and_abstain(database: db.Database) -> None:
    scope, incident_id = await timeout_incident(database, 9109)
    # 10 nodes leave 4 turns, all for synthesis and the two verifications.
    investigation_id = await queued_v3(database, scope, incident_id, max_steps=14)
    provider = FakeProvider(responder=roles_with())

    await run_once(database, v3_engine(database, provider))

    steps = await steps_by_key(database, investigation_id)
    for key in ("telemetry", "deployment", "dependency"):
        assert findings_of(steps[key]).provenance.ended_by is AgentEnding.NO_TURNS
    assert provider.requests == []
    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.INSUFFICIENT_EVIDENCE


async def test_the_dependency_role_reads_the_graph_and_cites_runbooks(
    database: db.Database,
) -> None:
    scope, incident_id = await timeout_incident_with_graph(database, 9110)
    investigation_id = await queued_v3(database, scope, incident_id)
    provider = FakeProvider(responder=roles_with())

    await run_once(database, v3_engine(database, provider))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    steps = await steps_by_key(database, investigation_id)
    stored = findings_of(steps["dependency"])
    assert stored.provenance.ended_by is AgentEnding.SUBMITTED
    [finding] = stored.report.findings
    assert "downstream-timeouts@v1" in finding.references
    assert finding.evidence_signal_ids
    assert finding.service_id is None
    tools = {ref.tool for ref in stored.provenance.tool_calls}
    assert tools == {ToolName.GET_SERVICE_DEPENDENCIES, ToolName.RETRIEVE_RUNBOOKS}
