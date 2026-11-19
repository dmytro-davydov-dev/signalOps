"""Stage scores over constructed step outputs (P13-T02).

Each metric's numerator and denominator come from what stages stored; `stage_summary`
aggregates them per variant. A mutation of one stage moves its metric and no other.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from signalops.contracts.diagnosis import (
    Contradiction,
    Diagnosis,
    DiagnosisStatus,
    RootCause,
)
from signalops.contracts.evaluation import ExpectedOutcome, FaultWindow
from signalops.contracts.evaluation_stages import (
    Conclusion,
    FollowUpEffect,
    StageScores,
    VerdictCell,
)
from signalops.contracts.roles import (
    AgentEnding,
    CitationCheck,
    Finding,
    FollowUpOutput,
    FollowUpRequest,
    Hypothesis,
    Provenance,
    SpecialistFindings,
    SpecialistReport,
    SynthesisOutput,
    SynthesisReport,
    Verdict,
    VerificationOutput,
    VerificationReport,
)
from signalops.contracts.steps import ProposedCause, ProposedDiagnosis
from signalops.contracts.workflow import AgentRole
from signalops.evaluation.metrics import stage_summary
from signalops.evaluation.stages import StageInputs, score_stages
from signalops.persistence.models import EvaluationCase

T, D, P = (
    AgentRole.TELEMETRY_INVESTIGATOR,
    AgentRole.DEPLOYMENT_INVESTIGATOR,
    AgentRole.DEPENDENCY_INVESTIGATOR,
)
BANK, INVENTORY, CHECKOUT = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
NAMES = {BANK: "bank-gateway", INVENTORY: "inventory", CHECKOUT: "checkout"}
FAULT = [uuid.uuid4() for _ in range(4)]
NOISE = [uuid.uuid4() for _ in range(3)]
EVENTS = {signal: f"fault-{i}" for i, signal in enumerate(FAULT)} | {
    signal: f"noise-{i}" for i, signal in enumerate(NOISE)
}
START = datetime(2026, 10, 16, 10, 34, tzinfo=UTC)
TIMEOUT = ExpectedOutcome(
    incident_expected=True,
    diagnosable=True,
    root_cause_service="bank-gateway",
    failure_type="downstream_timeout",
    affected_services=("bank-gateway", "checkout", "payment"),
    fault_window=FaultWindow(start=START, end=START.replace(minute=38)),
    supporting_event_ids=tuple(f"fault-{i}" for i in range(4)),
    lead_roles=(T,),
    expected_contradiction="checkout",
)
UNEXPLAINED = ExpectedOutcome(
    incident_expected=True,
    diagnosable=False,
    affected_services=("checkout",),
    fault_window=FaultWindow(start=START, end=START.replace(minute=38)),
)


def provenance(role: AgentRole, ending: AgentEnding = AgentEnding.SUBMITTED) -> Provenance:
    return Provenance(role=role, prompt_version="test-v1", ended_by=ending)


def finding(service: uuid.UUID, failure: str, ids: list[uuid.UUID]) -> Finding:
    return Finding(
        claim="c",
        service_id=service,
        failure_type=failure,
        confidence=0.7,
        uncertainty="u",
        evidence_signal_ids=tuple(ids),
    )


def specialist(
    role: AgentRole, *found: Finding, ending: AgentEnding = AgentEnding.SUBMITTED
) -> dict[str, Any]:
    report = SpecialistReport(findings=found, missing_evidence=() if found else ("none",))
    return SpecialistFindings(
        role=role, report=report, provenance=provenance(role, ending)
    ).model_dump(mode="json")


def hypothesis(
    service: uuid.UUID, failure: str, ids: list[uuid.UUID], roles: tuple[AgentRole, ...]
) -> Hypothesis:
    return Hypothesis(
        service_id=service,
        failure_type=failure,
        confidence=0.7,
        evidence_signal_ids=tuple(ids),
        supporting_roles=roles,
        assessment="a",
    )


def synthesis(*hypotheses: Hypothesis) -> dict[str, Any]:
    report = SynthesisReport(
        status=DiagnosisStatus.DIAGNOSED if hypotheses else DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="s",
        hypotheses=hypotheses,
        missing_evidence=() if hypotheses else ("none",),
    )
    return SynthesisOutput(report=report, provenance=provenance(AgentRole.SYNTHESIZER)).model_dump(
        mode="json"
    )


def verification(
    verdict: Verdict, ids: list[uuid.UUID], *, skipped: bool = False
) -> dict[str, Any]:
    report = VerificationReport(
        verdict=verdict,
        citation_checks=tuple(
            CitationCheck(signal_id=i, supports=verdict is Verdict.SUPPORTED, note="n") for i in ids
        ),
        missing_evidence=("m",) if verdict is Verdict.UNSUPPORTED else (),
        follow_up=(
            FollowUpRequest(role=T, question="q") if verdict is Verdict.NEEDS_FOLLOW_UP else None
        ),
    )
    ending = AgentEnding.SKIPPED if skipped else AgentEnding.SUBMITTED
    return VerificationOutput(
        report=report, provenance=provenance(AgentRole.VERIFIER, ending)
    ).model_dump(mode="json")


def diagnosis(
    service: uuid.UUID | None, contradictions: tuple[Contradiction, ...] = ()
) -> Diagnosis:
    cause = None
    if service is not None:
        cause = RootCause(
            service_id=service,
            service_name=NAMES[service],
            failure_type="downstream_timeout",
            confidence=0.7,
            evidence_signal_ids=(FAULT[0],),
        )
    return Diagnosis(
        status=DiagnosisStatus.DIAGNOSED if cause else DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="s",
        root_cause=cause,
        missing_evidence=() if cause else ("m",),
        contradictions=contradictions,
        workflow_version="v3",
    )


def v3_steps(**overrides: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """A correct `v3` run on the timeout scenario; overrides replace single steps."""
    steps = {
        "telemetry": specialist(T, finding(BANK, "downstream_timeout", FAULT[:3])),
        "deployment": specialist(D, finding(INVENTORY, "deployment_regression", [NOISE[0]])),
        "dependency": specialist(P),
        "synthesize": synthesis(
            hypothesis(BANK, "downstream_timeout", FAULT[:3], (T,)),
            hypothesis(INVENTORY, "deployment_regression", [NOISE[0]], (D,)),
        ),
        "verify": verification(Verdict.SUPPORTED, FAULT[:3]),
        "follow_up": FollowUpOutput(ran=False, reason="final").model_dump(mode="json"),
        "reverify": verification(Verdict.UNSUPPORTED, [], skipped=True),
    }
    return steps | overrides


def scored(
    steps: dict[str, dict[str, Any]],
    expected: ExpectedOutcome = TIMEOUT,
    outcome: Diagnosis | None = None,
    workflow: str = "v3",
) -> StageScores:
    contradiction = Contradiction(
        description="The deployment investigator points to checkout.",
        roles=(T, D),
        evidence_signal_ids=(NOISE[1],),
    )
    stages = score_stages(
        expected,
        StageInputs(
            workflow_version=workflow,
            steps=steps,
            diagnosis=outcome or diagnosis(BANK, (contradiction,)),
            service_names=NAMES,
            event_ids=EVENTS,
            signal_services={NOISE[1]: "checkout"},
        ),
    )
    assert stages is not None
    return stages


def case(stages: StageScores, *, abstained: bool = False) -> EvaluationCase:
    return EvaluationCase(
        investigation_id=uuid.uuid4(),
        stage_scores=stages.model_dump(mode="json"),
        scores={"abstained": abstained},
    )


def test_a_correct_multi_agent_run_scores_every_stage() -> None:
    stages = scored(v3_steps())
    telemetry, deployment, dependency = stages.specialists
    assert (telemetry.named_service, telemetry.named_cause) == (True, True)
    assert (telemetry.cited, telemetry.cited_supported) == (3, 3)
    assert (deployment.named_service, deployment.cited, deployment.cited_supported) == (False, 1, 0)
    assert dependency.findings == 0
    assert (stages.lead_found, stages.lead_found_strict, stages.lead_role_found) == (True,) * 3
    assert (stages.leading_service, stages.leading_correct, stages.lead_retained) == (
        "bank-gateway",
        True,
        True,
    )
    assert stages.synthesis_violations == ()
    assert stages.contradiction_listed is True
    assert stages.verdict_cell is VerdictCell.CORRECT_ACCEPTED
    assert stages.follow_up_ran is False
    assert stages.limit_hit is False


def test_metrics_aggregate_numerators_and_denominators() -> None:
    good = scored(v3_steps())
    lost = scored(
        v3_steps(
            synthesize=synthesis(
                hypothesis(INVENTORY, "deployment_regression", [NOISE[0]], (D,)),
                hypothesis(BANK, "downstream_timeout", FAULT[:3], (T,)),
            )
        )
    )
    unexplained = scored(
        v3_steps(
            telemetry=specialist(T, finding(CHECKOUT, "elevated_errors", NOISE[:2])),
            synthesize=synthesis(hypothesis(CHECKOUT, "elevated_errors", NOISE[:2], (T,))),
            verify=verification(Verdict.UNSUPPORTED, NOISE[:2]),
        ),
        expected=UNEXPLAINED,
        outcome=diagnosis(None),
    )
    summary = stage_summary([case(good), case(lost), case(unexplained, abstained=True)])
    # Lead recall counts diagnosable cases only.
    assert summary["specialist_lead_recall"] == {"numerator": 2, "denominator": 2, "rate": 1.0}
    assert summary["synthesis_lead_retention"] == {"numerator": 1, "denominator": 2, "rate": 0.5}
    telemetry = summary["per_role"]["telemetry_investigator"]
    assert telemetry["evidence_precision"] == {"numerator": 6, "denominator": 8, "rate": 0.75}
    assert summary["verifier_matrix"]["correct_accepted"] == 1
    assert summary["verifier_matrix"]["wrong_accepted"] == 1
    assert summary["verifier_matrix"]["wrong_rejected"] == 1
    # The contradiction label exists only on the timeout outcome.
    assert summary["contradiction_recall"] == {"numerator": 2, "denominator": 2, "rate": 1.0}
    assert summary["abstentions_with_limit"] == {"numerator": 0, "denominator": 1, "rate": 0.0}
    assert summary["synthesis_invariant_violations"] == 0


def _unrelated(stages: StageScores) -> tuple[object, ...]:
    return (
        stages.lead_found,
        stages.lead_found_strict,
        tuple((s.cited, s.cited_supported) for s in stages.specialists),
        stages.limit_endings,
        stages.synthesis_violations,
    )


def test_a_synthesizer_leading_with_its_second_hypothesis_loses_the_lead() -> None:
    baseline = scored(v3_steps())
    mutated = scored(
        v3_steps(
            synthesize=synthesis(
                hypothesis(INVENTORY, "deployment_regression", [NOISE[0]], (D,)),
                hypothesis(BANK, "downstream_timeout", FAULT[:3], (T,)),
            )
        )
    )
    assert (baseline.lead_retained, mutated.lead_retained) == (True, False)
    assert _unrelated(mutated) == _unrelated(baseline)


def test_a_verifier_accepting_everything_accepts_wrong_leads() -> None:
    wrong_lead = synthesis(hypothesis(CHECKOUT, "elevated_errors", FAULT[:3], (T,)))
    baseline = scored(
        v3_steps(synthesize=wrong_lead, verify=verification(Verdict.UNSUPPORTED, FAULT[:3]))
    )
    mutated = scored(
        v3_steps(synthesize=wrong_lead, verify=verification(Verdict.SUPPORTED, FAULT[:3]))
    )
    assert baseline.verdict_cell is VerdictCell.WRONG_REJECTED
    assert mutated.verdict_cell is VerdictCell.WRONG_ACCEPTED
    assert _unrelated(mutated) == _unrelated(baseline)
    assert mutated.lead_retained == baseline.lead_retained


def test_a_specialist_with_no_turns_is_a_limit_ending_and_loses_the_lead() -> None:
    baseline = scored(v3_steps())
    mutated = scored(v3_steps(telemetry=specialist(T, ending=AgentEnding.NO_TURNS)))
    assert baseline.limit_endings == ()
    assert [e.ended_by for e in mutated.limit_endings] == [AgentEnding.NO_TURNS]
    assert (baseline.lead_found, mutated.lead_found) == (True, False)
    assert mutated.lead_role_found is False
    # The lead was never found, so retention does not apply.
    assert mutated.lead_retained is None
    summary = stage_summary([case(mutated, abstained=True)])
    assert summary["limit_endings"] == {"no_turns": 1}
    assert summary["abstentions_with_limit"] == {"numerator": 1, "denominator": 1, "rate": 1.0}


def test_a_planted_synthesis_violation_is_reported() -> None:
    foreign = uuid.uuid4()
    stages = scored(
        v3_steps(synthesize=synthesis(hypothesis(BANK, "downstream_timeout", [foreign], (T, P))))
    )
    assert stages.synthesis_violations == (
        "hypotheses[0] cites a signal no specialist cited",
        "hypotheses[0] credits a role with no findings",
    )
    assert stage_summary([case(stages)])["synthesis_invariant_violations"] == 2


def test_follow_up_effect_compares_the_two_verdicts() -> None:
    stages = scored(
        v3_steps(
            verify=verification(Verdict.NEEDS_FOLLOW_UP, FAULT[:3]),
            follow_up=FollowUpOutput(
                ran=True,
                findings=SpecialistFindings.model_validate(
                    specialist(T, finding(BANK, "downstream_timeout", [FAULT[3]]))
                ),
            ).model_dump(mode="json"),
            reverify=verification(Verdict.SUPPORTED, FAULT),
        )
    )
    assert stages.verdict_cell is VerdictCell.CORRECT_FOLLOW_UP
    assert stages.follow_up_ran is True
    assert stages.final_verdict is Verdict.SUPPORTED
    assert stages.follow_up_effect is FollowUpEffect.WRONG_TO_RIGHT
    summary = stage_summary([case(stages)])
    assert summary["follow_up"]["rounds"] == 1
    assert summary["follow_up"]["wrong_to_right"] == 1


def test_no_lead_metrics_apply_without_a_true_cause() -> None:
    stages = scored(v3_steps(), expected=UNEXPLAINED)
    assert (stages.lead_found, stages.lead_retained, stages.leading_correct) == (None, None, False)
    assert stages.verdict_cell is VerdictCell.WRONG_ACCEPTED


def _v2(
    proposal: ProposedDiagnosis, outcome: Diagnosis, expected: ExpectedOutcome = TIMEOUT
) -> StageScores:
    return scored(
        {"investigate": proposal.model_dump(mode="json")}, expected, outcome, workflow="v2"
    )


def test_single_agent_proposal_and_conclusion() -> None:
    proposal = ProposedDiagnosis(
        status=DiagnosisStatus.DIAGNOSED,
        summary="s",
        root_cause=ProposedCause(
            service_id=BANK,
            failure_type="downstream_timeout",
            confidence=0.8,
            evidence_signal_ids=(FAULT[0],),
        ),
    )
    kept = _v2(proposal, diagnosis(BANK))
    assert (kept.proposal_service, kept.proposal_correct, kept.conclusion) == (
        "bank-gateway",
        True,
        Conclusion.KEPT,
    )
    dropped = _v2(proposal, diagnosis(None))
    assert dropped.conclusion is Conclusion.DROPPED
    abstention = ProposedDiagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="s",
        missing_evidence=("The step limit ended the investigation before a conclusion",),
    )
    limited = _v2(abstention, diagnosis(None))
    assert limited.conclusion is Conclusion.ABSTAINED
    assert [e.ended_by for e in limited.limit_endings] == [AgentEnding.TURN_LIMIT]
    assert limited.proposal_correct is False
    correct_abstention = _v2(abstention, diagnosis(None), UNEXPLAINED)
    assert correct_abstention.proposal_correct is True
    summary = stage_summary([case(kept), case(dropped), case(limited, abstained=True)])
    assert summary["single_agent"]["proposal_correct"] == {
        "numerator": 2,
        "denominator": 3,
        "rate": 0.6667,
    }
    assert summary["single_agent"]["conclusions"]["dropped"] == 1


def test_the_deterministic_baseline_has_no_stages() -> None:
    inputs = StageInputs(workflow_version="d1", steps={}, diagnosis=None, service_names=NAMES)
    assert score_stages(TIMEOUT, inputs) is None
