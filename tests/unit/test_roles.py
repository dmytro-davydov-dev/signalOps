"""Role contracts, turn allotment, and what an agent loop counts as read."""

import json
import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from signalops.agents.agent_loop import Reads
from signalops.agents.multi_agent import (
    SPECIALISTS,
    Room,
    TurnPlan,
    allot_turns,
    check_synthesis,
    check_verification,
    detect_conflicts,
    follow_up_fits,
)
from signalops.contracts.diagnosis import Contradiction, DiagnosisStatus
from signalops.contracts.roles import (
    AgentEnding,
    BranchPlan,
    CitationCheck,
    Finding,
    FollowUpOutput,
    FollowUpRequest,
    Hypothesis,
    IncidentFacts,
    Provenance,
    SpecialistBrief,
    SpecialistFindings,
    SpecialistReport,
    SynthesisOutput,
    SynthesisReport,
    Verdict,
    VerificationReport,
)
from signalops.contracts.workflow import AgentRole, ToolName

NOW = datetime(2026, 11, 8, 12, tzinfo=UTC)
FACTS = IncidentFacts(incident_id=uuid.uuid4(), title="t", detected_at=NOW, evidence_cutoff_at=NOW)
SERVICE = uuid.uuid4()
SIGNALS = (uuid.uuid4(), uuid.uuid4(), uuid.uuid4())
TELEMETRY = AgentRole.TELEMETRY_INVESTIGATOR


def finding(*ids: uuid.UUID, service: uuid.UUID | None = SERVICE) -> Finding:
    return Finding(
        claim="c",
        service_id=service,
        failure_type="elevated_errors" if service else None,
        confidence=0.6,
        uncertainty="u",
        evidence_signal_ids=ids,
    )


def stored(role: AgentRole, report: SpecialistReport) -> SpecialistFindings:
    return SpecialistFindings(
        role=role,
        report=report,
        provenance=Provenance(role=role, prompt_version="p", ended_by=AgentEnding.SUBMITTED),
    )


def hypothesis(*ids: uuid.UUID, roles: tuple[AgentRole, ...] = (TELEMETRY,)) -> Hypothesis:
    return Hypothesis(
        service_id=SERVICE,
        failure_type="elevated_errors",
        confidence=0.7,
        evidence_signal_ids=ids,
        supporting_roles=roles,
        assessment="a",
    )


def test_a_finding_states_uncertainty_and_names_a_service_for_a_failure() -> None:
    with pytest.raises(ValidationError, match="uncertainty"):
        Finding.model_validate({"claim": "c", "confidence": 0.5})
    with pytest.raises(ValidationError, match="needs a service_id"):
        Finding(claim="c", failure_type="x", confidence=0.5, uncertainty="u")
    with pytest.raises(ValidationError, match="less than or equal to 1"):
        Finding(claim="c", confidence=1.5, uncertainty="u")
    with pytest.raises(ValidationError, match="duplicates"):
        finding(SIGNALS[0], SIGNALS[0])


def test_a_report_with_nothing_found_says_what_is_missing() -> None:
    with pytest.raises(ValidationError, match="missing_evidence"):
        SpecialistReport()
    assert SpecialistReport(missing_evidence=("x",)).cited() == frozenset()
    report = SpecialistReport(findings=(finding(*SIGNALS[:2]), finding(SIGNALS[1])))
    assert report.cited() == frozenset(SIGNALS[:2])


def test_unknown_fields_cannot_reach_a_role_contract() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        SpecialistReport.model_validate({"missing_evidence": ["x"], "reasoning": "secret"})
    with pytest.raises(ValidationError, match="Extra inputs"):
        Finding.model_validate(
            {"claim": "c", "confidence": 0.5, "uncertainty": "u", "workspace_id": "w"}
        )


def test_only_specialist_roles_take_specialist_contracts() -> None:
    with pytest.raises(ValidationError, match="not a specialist role"):
        SpecialistBrief(role=AgentRole.VERIFIER, incident=FACTS)
    with pytest.raises(ValidationError, match="not a specialist role"):
        hypothesis(SIGNALS[0], roles=(AgentRole.SYNTHESIZER,))
    brief = SpecialistBrief(role=TELEMETRY, incident=FACTS)
    with pytest.raises(ValidationError, match=r"brief\.role"):
        BranchPlan(
            role=AgentRole.DEPLOYMENT_INVESTIGATOR,
            step_key="deployment",
            max_turns=1,
            budget_share_microusd=1,
            brief=brief,
        )
    with pytest.raises(ValidationError, match=r"provenance\.role"):
        SpecialistFindings(
            role=TELEMETRY,
            report=SpecialistReport(missing_evidence=("x",)),
            provenance=Provenance(
                role=AgentRole.VERIFIER, prompt_version="p", ended_by=AgentEnding.SKIPPED
            ),
        )


def test_synthesis_and_verification_need_reasons_to_abstain() -> None:
    with pytest.raises(ValidationError, match="needs a hypothesis"):
        SynthesisReport(status=DiagnosisStatus.DIAGNOSED, summary="s")
    with pytest.raises(ValidationError, match="missing_evidence"):
        SynthesisReport(status=DiagnosisStatus.INSUFFICIENT_EVIDENCE, summary="s")
    with pytest.raises(ValidationError, match="missing_evidence"):
        VerificationReport(verdict=Verdict.UNSUPPORTED)
    abstaining = SynthesisReport(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="s",
        hypotheses=(hypothesis(SIGNALS[0]),),
        missing_evidence=("x",),
    )
    assert abstaining.leading is None


def test_the_synthesis_may_cite_and_credit_only_what_specialists_reported() -> None:
    findings = (
        stored(TELEMETRY, SpecialistReport(findings=(finding(*SIGNALS[:2]),))),
        stored(AgentRole.DEPLOYMENT_INVESTIGATOR, SpecialistReport(missing_evidence=("x",))),
    )

    def synthesis(*hypotheses: Hypothesis) -> SynthesisReport:
        return SynthesisReport(status=DiagnosisStatus.DIAGNOSED, summary="s", hypotheses=hypotheses)

    assert check_synthesis(synthesis(hypothesis(*SIGNALS[:2])), findings) is None
    problem = check_synthesis(synthesis(hypothesis(SIGNALS[2])), findings)
    assert problem is not None
    assert "no specialist cited" in problem
    credited = hypothesis(SIGNALS[0], roles=(TELEMETRY, AgentRole.DEPLOYMENT_INVESTIGATOR))
    problem = check_synthesis(synthesis(credited), findings)
    assert problem is not None
    assert "no findings" in problem


def test_the_verifier_judges_exactly_the_leading_citations() -> None:
    def report(*checks: tuple[uuid.UUID, bool]) -> VerificationReport:
        return VerificationReport(
            verdict=Verdict.SUPPORTED,
            citation_checks=tuple(
                CitationCheck(signal_id=i, supports=s, note="n") for i, s in checks
            ),
        )

    cited = SIGNALS[:2]
    assert check_verification(report((SIGNALS[0], True), (SIGNALS[1], False)), cited, ()) is None
    assert "misses 1" in str(check_verification(report((SIGNALS[0], True)), cited, ()))
    extra = report((SIGNALS[0], True), (SIGNALS[1], True), (SIGNALS[2], True))
    assert "not cited" in str(check_verification(extra, cited, ()))
    unreadable = report((SIGNALS[0], True), (SIGNALS[1], True))
    assert "unreadable" in str(check_verification(unreadable, cited, (SIGNALS[1],)))


@pytest.mark.parametrize(
    ("available", "expected"),
    [
        (30, TurnPlan(5, 2, 2, 3)),
        (20, TurnPlan(4, 2, 2, 2)),
        (13, TurnPlan(2, 2, 2, 0)),
        (6, TurnPlan(0, 2, 2, 0)),
        (3, TurnPlan(0, 2, 0, 0)),
        (0, TurnPlan(0, 0, 0, 0)),
        (-4, TurnPlan(0, 0, 0, 0)),
    ],
)
def test_turns_go_to_downstream_roles_then_branches_then_a_follow_up(
    available: int, expected: TurnPlan
) -> None:
    assert allot_turns(available, len(SPECIALISTS)) == expected


def test_specialists_have_disjoint_purposes_and_only_read_only_tools() -> None:
    assert [spec.role for spec in SPECIALISTS] == [
        AgentRole.TELEMETRY_INVESTIGATOR,
        AgentRole.DEPLOYMENT_INVESTIGATOR,
        AgentRole.DEPENDENCY_INVESTIGATOR,
    ]
    assert len({spec.prompt_version for spec in SPECIALISTS}) == 3
    for spec in SPECIALISTS:
        assert set(spec.tools) <= set(ToolName)
        assert len(spec.tools) == 2


def test_reads_keep_signal_ids_and_runbook_versions_from_results_only() -> None:
    reads = Reads()
    first, second = uuid.uuid4(), uuid.uuid4()
    reads.take(
        ToolName.QUERY_LOGS,
        "query_logs:a",
        json.dumps({"items": [{"signal_id": str(first), "message": f"see {second}"}]}),
    )
    reads.take(
        ToolName.RETRIEVE_RUNBOOKS,
        "retrieve_runbooks:b",
        json.dumps({"items": [{"runbook_key": "pool", "version": "v2"}]}),
    )
    reads.take(ToolName.QUERY_LOGS, "query_logs:a", "not json")
    reads.take(ToolName.QUERY_LOGS, "query_logs:c", json.dumps({"items": [{"signal_id": "x"}]}))
    # An ID quoted in a message is not a signal the tool returned.
    assert reads.signal_ids == {first}
    assert reads.references == {"pool@v2"}
    assert [ref.call_key for ref in reads.tool_calls] == [
        "query_logs:a",
        "retrieve_runbooks:b",
        "query_logs:c",
    ]


def test_a_follow_up_verdict_needs_a_request_and_an_offer() -> None:
    request = FollowUpRequest(role=TELEMETRY, question="Look again")
    with pytest.raises(ValidationError, match="follow_up"):
        VerificationReport(verdict=Verdict.NEEDS_FOLLOW_UP)
    with pytest.raises(ValidationError, match="follow_up"):
        VerificationReport(verdict=Verdict.SUPPORTED, follow_up=request)
    with pytest.raises(ValidationError, match="not a specialist role"):
        FollowUpRequest(role=AgentRole.VERIFIER, question="q")
    asking = VerificationReport(verdict=Verdict.NEEDS_FOLLOW_UP, follow_up=request)
    assert check_verification(asking, (), (), follow_up_allowed=True) is None
    refused = check_verification(asking, (), ())
    assert refused is not None
    assert "not available" in refused


def test_a_round_that_did_not_run_says_why_and_holds_no_findings() -> None:
    with pytest.raises(ValidationError, match="reason"):
        FollowUpOutput(ran=False)
    with pytest.raises(ValidationError, match="findings"):
        FollowUpOutput(ran=True)
    with pytest.raises(ValidationError, match="findings"):
        FollowUpOutput(
            ran=False,
            reason="r",
            findings=stored(TELEMETRY, SpecialistReport(missing_evidence=("x",))),
        )


@pytest.mark.parametrize(
    ("room", "turns", "fits"),
    [
        (Room(1_000, 100.0), 3, True),
        (Room(999, 100.0), 3, False),
        (Room(1_000, 99.0), 3, False),
        (Room(1_000, 100.0), 1, False),
        (Room(10_000, -5.0), 3, False),
    ],
)
def test_a_follow_up_needs_room_for_two_calls_and_turns(room: Room, turns: int, fits: bool) -> None:
    assert follow_up_fits(room, turns=turns, call_reserve=500, call_seconds=50.0) is fits


def test_code_records_roles_naming_different_failing_services() -> None:
    other = uuid.uuid4()
    telemetry = finding(SIGNALS[0])
    deployment = Finding(
        claim="deployed",
        service_id=other,
        failure_type="deployment_regression",
        confidence=0.3,
        uncertainty="timing",
        evidence_signal_ids=(SIGNALS[1],),
    )
    weak = deployment.model_copy(update={"confidence": 0.2})
    structural = finding(SIGNALS[2], service=None)
    findings = (
        stored(TELEMETRY, SpecialistReport(findings=(telemetry,))),
        stored(AgentRole.DEPLOYMENT_INVESTIGATOR, SpecialistReport(findings=(deployment,))),
        stored(AgentRole.DEPENDENCY_INVESTIGATOR, SpecialistReport(findings=(structural,))),
    )
    [conflict] = detect_conflicts(findings, {SERVICE: "bank-gateway", other: "inventory"})
    assert conflict.roles == (TELEMETRY, AgentRole.DEPLOYMENT_INVESTIGATOR)
    assert set(conflict.evidence_signal_ids) == set(SIGNALS[:2])
    assert "bank-gateway" in conflict.description
    assert "inventory" in conflict.description
    below = (
        findings[0],
        stored(AgentRole.DEPLOYMENT_INVESTIGATOR, SpecialistReport(findings=(weak,))),
    )
    assert detect_conflicts(below, {}) == ()
    # Two findings of one role are that role's own uncertainty, not a contradiction.
    alone = (stored(TELEMETRY, SpecialistReport(findings=(telemetry, deployment))),)
    assert detect_conflicts(alone, {}) == ()


def test_the_diagnosis_keeps_the_synthesizers_and_codes_contradictions() -> None:
    def conflict(roles: tuple[AgentRole, ...], *ids: uuid.UUID) -> Contradiction:
        return Contradiction(description="d", roles=roles, evidence_signal_ids=ids)

    pair = (TELEMETRY, AgentRole.DEPLOYMENT_INVESTIGATOR)
    listed = conflict(pair, SIGNALS[0])
    output = SynthesisOutput(
        report=SynthesisReport(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
            summary="s",
            missing_evidence=("x",),
            contradictions=(listed,),
        ),
        provenance=Provenance(
            role=AgentRole.SYNTHESIZER, prompt_version="p", ended_by=AgentEnding.SUBMITTED
        ),
        detected_contradictions=(
            conflict(pair, SIGNALS[0], SIGNALS[1]),
            conflict((TELEMETRY, AgentRole.DEPENDENCY_INVESTIGATOR), SIGNALS[2]),
        ),
    )
    # The first found conflict is the one the synthesizer already listed.
    assert [c.roles for c in output.contradictions()] == [
        pair,
        (TELEMETRY, AgentRole.DEPENDENCY_INVESTIGATOR),
    ]
