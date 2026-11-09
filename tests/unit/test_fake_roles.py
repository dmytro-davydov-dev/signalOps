"""The fake synthesizer's wording: it is shown in demonstrations and the UI."""

import json
import uuid
from datetime import UTC, datetime

from signalops.contracts.diagnosis import FailureType
from signalops.contracts.roles import (
    AgentEnding,
    Finding,
    IncidentFacts,
    RoleReport,
    SpecialistReport,
    SynthesisBrief,
)
from signalops.contracts.workflow import AgentRole
from signalops.gateway.fake_roles import synthesizer_step
from signalops.gateway.types import (
    ModelMessage,
    ModelRequest,
    ModelRole,
    TextBlock,
    ToolUseBlock,
)

AT = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
GATEWAY = uuid.UUID("57988918-06d9-4f40-82a2-48ed0c9c0adf")
INVENTORY = uuid.UUID("4a1d9b2e-3c4f-4e5a-8b6c-7d8e9f0a1b2c")


def _finding(service: uuid.UUID, failure: FailureType, claim: str) -> Finding:
    return Finding(
        claim=claim,
        service_id=service,
        failure_type=failure,
        confidence=0.7,
        uncertainty="Synthetic.",
        evidence_signal_ids=(uuid.uuid4(),),
    )


def _synthesize(*reports: RoleReport) -> dict[str, object]:
    brief = SynthesisBrief(
        incident=IncidentFacts(
            incident_id=uuid.uuid4(), title="t", detected_at=AT, evidence_cutoff_at=AT
        ),
        reports=reports,
    )
    request = ModelRequest(
        model="fake-1",
        prompt_version="synthesizer-v1",
        system="s",
        messages=(
            ModelMessage(role=ModelRole.USER, content=(TextBlock(text=brief.model_dump_json()),)),
        ),
        max_output_tokens=1000,
    )
    block = synthesizer_step(request).content[0]
    assert isinstance(block, ToolUseBlock)
    loaded: dict[str, object] = json.loads(json.dumps(block.input))
    return loaded


def _report(role: AgentRole, *findings: Finding) -> RoleReport:
    return RoleReport(
        role=role, ended_by=AgentEnding.SUBMITTED, report=SpecialistReport(findings=findings)
    )


def test_identical_claims_from_two_roles_appear_once_in_the_assessment() -> None:
    claim = "Revision inventory-2 was deployed shortly before the incident window."
    regression = "deployment_regression"
    payload = _synthesize(
        _report(AgentRole.DEPLOYMENT_INVESTIGATOR, _finding(INVENTORY, regression, claim)),
        _report(AgentRole.DEPENDENCY_INVESTIGATOR, _finding(INVENTORY, regression, claim)),
    )
    hypotheses = payload["hypotheses"]
    assert isinstance(hypotheses, list)
    assert hypotheses[0]["assessment"] == claim


def test_a_contradiction_from_one_role_uses_the_singular_verb() -> None:
    payload = _synthesize(
        _report(
            AgentRole.TELEMETRY_INVESTIGATOR,
            _finding(GATEWAY, "downstream_timeout", "Calls end at bank-gateway."),
        ),
        _report(
            AgentRole.DEPLOYMENT_INVESTIGATOR,
            _finding(INVENTORY, "deployment_regression", "Inventory was deployed."),
        ),
    )
    contradictions = payload["contradictions"]
    assert isinstance(contradictions, list)
    assert contradictions[0]["description"] == (
        "telemetry investigator points to one service and deployment investigator to another."
    )
