"""Attribute each incorrect case to the first stage that failed (P13-T03, ADR-040).

The checks run in a fixed order and the first match wins, so every incorrect case gets
exactly one stage and a correct case gets none:

1. `harness_failure`: the harness produced no outcome.
2. `detection_miss`: no incident matched the fault, the investigated incident is not the
   matched one, or an incident opened where none was expected.
3. `no_lead`: no specialist (`v3`), or the agent's proposal (`v2`), named the true service.
4. `lead_lost_in_synthesis`: a specialist named the true service (or cause) and the
   leading hypothesis did not.
5. `verifier_false_reject`: the leading hypothesis was the true cause and the final
   verification did not accept it.
6. `verifier_false_accept`: the leading hypothesis was wrong and verification accepted it.
7. `conclude_dropped`: a correct, accepted lead (or `v2` proposal) ended in an abstention.
8. `wrong_failure_type`: the right service with the wrong failure type.
9. `false_diagnosis_on_underdetermined`: a cause asserted where abstaining was correct,
   with the step that first asserted it.
10. `rule_error`: any other error of the deterministic baseline.

"First failing stage" is a convention: later stages may also have failed, and every
stage score stays visible on the case. `limit_hit` is a separate flag: some role ended
on a turn, repair, or budget limit, a refusal, or no turns. An agent case without stage
scores is `unattributed` rather than guessed.
"""

import uuid

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import (
    CaseScores,
    CaseStatus,
    ExpectedOutcome,
    ObservedOutcome,
    SystemVariant,
)
from signalops.contracts.evaluation_stages import (
    Attribution,
    Conclusion,
    FailureStage,
    StageScores,
)
from signalops.contracts.roles import Verdict


def attribute(
    *,
    variant: SystemVariant,
    status: CaseStatus,
    expected: ExpectedOutcome,
    observed: ObservedOutcome | None,
    scores: CaseScores | None,
    stages: StageScores | None,
    matched_incident_id: uuid.UUID | None,
) -> Attribution | None:
    """The failing stage of an incorrect case; None for a correct one."""
    if status is CaseStatus.FAILED or scores is None or observed is None:
        return Attribution(stage=FailureStage.HARNESS_FAILURE)
    if scores.outcome_correct:
        return None
    limit_hit = stages.limit_hit if stages is not None else False

    def found(stage: FailureStage, origin: str | None = None) -> Attribution:
        return Attribution(stage=stage, origin=origin, limit_hit=limit_hit)

    investigated = observed.incident_ids[0] if observed.incident_ids else None
    if expected.incident_expected:
        if matched_incident_id is None or investigated != matched_incident_id:
            return found(FailureStage.DETECTION_MISS)
    elif observed.incident_ids:
        return found(FailureStage.DETECTION_MISS)
    if variant is SystemVariant.DETERMINISTIC:
        return found(FailureStage.RULE_ERROR)
    if stages is None:
        return found(FailureStage.UNATTRIBUTED)
    if stages.workflow_version == "v3":
        return found(*_multi_agent(expected, observed, stages))
    return found(*_single_agent(expected, observed, stages))


def _right_service_wrong_type(expected: ExpectedOutcome, observed: ObservedOutcome) -> bool:
    return (
        observed.diagnosis_status == DiagnosisStatus.DIAGNOSED
        and observed.root_cause_service == expected.root_cause_service
        and observed.failure_type != expected.failure_type
    )


def _multi_agent(
    expected: ExpectedOutcome, observed: ObservedOutcome, stages: StageScores
) -> tuple[FailureStage, str | None]:
    leading_named = expected.diagnosable and stages.leading_service == expected.root_cause_service
    if expected.diagnosable:
        if not stages.lead_found:
            return FailureStage.NO_LEAD, None
        if not leading_named or (stages.lead_found_strict and not stages.leading_correct):
            return FailureStage.LEAD_LOST_IN_SYNTHESIS, None
    accepted = stages.final_verdict is Verdict.SUPPORTED
    if stages.leading_correct and not accepted:
        return FailureStage.VERIFIER_FALSE_REJECT, None
    if stages.leading_correct is False and accepted:
        return FailureStage.VERIFIER_FALSE_ACCEPT, None
    if stages.leading_correct and observed.diagnosis_status != DiagnosisStatus.DIAGNOSED:
        return FailureStage.CONCLUDE_DROPPED, None
    if expected.diagnosable and (leading_named or _right_service_wrong_type(expected, observed)):
        return FailureStage.WRONG_FAILURE_TYPE, None
    if not expected.diagnosable and observed.diagnosis_status == DiagnosisStatus.DIAGNOSED:
        return FailureStage.FALSE_DIAGNOSIS_ON_UNDERDETERMINED, "synthesize"
    return FailureStage.UNATTRIBUTED, None


def _single_agent(
    expected: ExpectedOutcome, observed: ObservedOutcome, stages: StageScores
) -> tuple[FailureStage, str | None]:
    if expected.diagnosable:
        if not stages.proposal_service_correct:
            return FailureStage.NO_LEAD, None
        if stages.proposal_correct and stages.conclusion is Conclusion.DROPPED:
            return FailureStage.CONCLUDE_DROPPED, None
        if _right_service_wrong_type(expected, observed) or not stages.proposal_correct:
            return FailureStage.WRONG_FAILURE_TYPE, None
        return FailureStage.UNATTRIBUTED, None
    if observed.diagnosis_status == DiagnosisStatus.DIAGNOSED:
        return FailureStage.FALSE_DIAGNOSIS_ON_UNDERDETERMINED, "investigate"
    return FailureStage.UNATTRIBUTED, None
