"""Each incorrect case gets exactly one first failing stage (P13-T03)."""

import uuid
from collections.abc import Callable
from typing import Any

import pytest

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import (
    CaseScores,
    CaseStatus,
    ExpectedOutcome,
    ObservedOutcome,
    SystemVariant,
)
from signalops.contracts.evaluation_stages import FailureStage, StageScores
from signalops.contracts.roles import AgentEnding, Verdict
from signalops.contracts.steps import ProposedCause, ProposedDiagnosis
from signalops.evaluation.attribution import attribute
from signalops.evaluation.metrics import failure_summary
from signalops.evaluation.scoring import score_case
from signalops.persistence.models import EvaluationCase
from tests.unit.test_stage_scores import (
    BANK,
    CHECKOUT,
    FAULT,
    INVENTORY,
    NOISE,
    TIMEOUT,
    UNEXPLAINED,
    D,
    T,
    diagnosis,
    finding,
    hypothesis,
    scored,
    specialist,
    synthesis,
    v3_steps,
    verification,
)

INCIDENT = uuid.uuid4()
OTHER = uuid.uuid4()
HEALTHY = ExpectedOutcome(incident_expected=False, diagnosable=False)


def observed(
    service: str | None,
    failure: str = "downstream_timeout",
    incidents: tuple[uuid.UUID, ...] = (INCIDENT,),
) -> ObservedOutcome:
    return ObservedOutcome(
        incident_ids=incidents,
        investigation_id=uuid.uuid4() if incidents else None,
        diagnosis_status=(
            None
            if not incidents
            else DiagnosisStatus.DIAGNOSED
            if service
            else DiagnosisStatus.INSUFFICIENT_EVIDENCE
        ),
        root_cause_service=service,
        failure_type=failure if service else None,
    )


def stage_of(
    expected: ExpectedOutcome,
    seen: ObservedOutcome,
    stages: StageScores | None,
    *,
    variant: SystemVariant = SystemVariant.MULTI_AGENT,
    matched: uuid.UUID | None = INCIDENT,
) -> FailureStage | None:
    scores = score_case(expected, seen)
    attribution = attribute(
        variant=variant,
        status=CaseStatus.SCORED,
        expected=expected,
        observed=seen,
        scores=scores,
        stages=stages,
        matched_incident_id=matched,
    )
    assert (attribution is None) == scores.outcome_correct
    return None if attribution is None else attribution.stage


WRONG_LEAD = synthesis(hypothesis(CHECKOUT, "elevated_errors", NOISE[:2], (T,)))
V3_CASES: list[
    tuple[
        str, Callable[[], tuple[ExpectedOutcome, ObservedOutcome, StageScores]], FailureStage | None
    ]
] = [
    ("correct", lambda: (TIMEOUT, observed("bank-gateway"), scored(v3_steps())), None),
    (
        "no specialist named the service",
        lambda: (
            TIMEOUT,
            observed("checkout", "elevated_errors"),
            scored(
                v3_steps(
                    telemetry=specialist(T, finding(CHECKOUT, "elevated_errors", NOISE[:2])),
                    synthesize=WRONG_LEAD,
                )
            ),
        ),
        FailureStage.NO_LEAD,
    ),
    (
        "synthesis led with another cause",
        lambda: (
            TIMEOUT,
            observed("inventory", "deployment_regression"),
            scored(
                v3_steps(
                    synthesize=synthesis(
                        hypothesis(INVENTORY, "deployment_regression", [NOISE[0]], (D,)),
                        hypothesis(BANK, "downstream_timeout", FAULT[:3], (T,)),
                    )
                )
            ),
        ),
        FailureStage.LEAD_LOST_IN_SYNTHESIS,
    ),
    (
        "verifier rejected the true cause",
        lambda: (
            TIMEOUT,
            observed(None),
            scored(
                v3_steps(verify=verification(Verdict.UNSUPPORTED, FAULT[:3])),
                outcome=diagnosis(None),
            ),
        ),
        FailureStage.VERIFIER_FALSE_REJECT,
    ),
    (
        "verifier accepted a wrong lead on underdetermined evidence",
        lambda: (
            UNEXPLAINED,
            observed("checkout", "elevated_errors"),
            scored(
                v3_steps(synthesize=WRONG_LEAD, verify=verification(Verdict.SUPPORTED, NOISE[:2])),
                expected=UNEXPLAINED,
            ),
        ),
        FailureStage.VERIFIER_FALSE_ACCEPT,
    ),
    (
        "conclude abstained on an accepted true cause",
        lambda: (TIMEOUT, observed(None), scored(v3_steps(), outcome=diagnosis(None))),
        FailureStage.CONCLUDE_DROPPED,
    ),
    (
        "right service, wrong type, rejected",
        lambda: (
            TIMEOUT,
            observed(None),
            scored(
                v3_steps(
                    telemetry=specialist(T, finding(BANK, "elevated_errors", FAULT[:3])),
                    synthesize=synthesis(hypothesis(BANK, "elevated_errors", FAULT[:3], (T,))),
                    verify=verification(Verdict.UNSUPPORTED, FAULT[:3]),
                ),
                outcome=diagnosis(None),
            ),
        ),
        FailureStage.WRONG_FAILURE_TYPE,
    ),
]


@pytest.mark.parametrize(("name", "build", "stage"), V3_CASES, ids=[c[0] for c in V3_CASES])
def test_multi_agent_cases_are_attributed_to_their_first_failing_stage(
    name: str,
    build: Callable[[], tuple[ExpectedOutcome, ObservedOutcome, StageScores]],
    stage: FailureStage | None,
) -> None:
    expected, seen, stages = build()
    assert stage_of(expected, seen, stages) is stage, name


def test_detection_comes_before_every_agent_stage() -> None:
    wrong = scored(v3_steps(synthesize=WRONG_LEAD))
    seen = observed("checkout", "elevated_errors")
    # No match, another incident investigated, or an incident on healthy traffic.
    assert stage_of(TIMEOUT, seen, wrong, matched=None) is FailureStage.DETECTION_MISS
    assert stage_of(TIMEOUT, seen, wrong, matched=OTHER) is FailureStage.DETECTION_MISS
    assert stage_of(HEALTHY, seen, wrong, matched=None) is FailureStage.DETECTION_MISS
    # Healthy traffic with no incident is correct and gets no stage.
    assert stage_of(HEALTHY, observed(None, incidents=()), None, matched=None) is None


def test_no_lead_takes_precedence_over_a_verifier_error() -> None:
    # Nobody found bank-gateway and the verifier accepted the wrong lead: the first
    # failure is upstream.
    stages = scored(
        v3_steps(
            telemetry=specialist(T, finding(CHECKOUT, "elevated_errors", NOISE[:2])),
            synthesize=WRONG_LEAD,
            verify=verification(Verdict.SUPPORTED, NOISE[:2]),
        )
    )
    assert (
        stage_of(TIMEOUT, observed("checkout", "elevated_errors"), stages) is FailureStage.NO_LEAD
    )


def test_a_harness_failure_has_no_outcome() -> None:
    attribution = attribute(
        variant=SystemVariant.MULTI_AGENT,
        status=CaseStatus.FAILED,
        expected=TIMEOUT,
        observed=None,
        scores=None,
        stages=None,
        matched_incident_id=INCIDENT,
    )
    assert attribution is not None
    assert attribution.stage is FailureStage.HARNESS_FAILURE


def test_the_deterministic_baseline_fails_by_rule_and_agents_without_stages_are_unattributed() -> (
    None
):
    seen = observed("checkout", "elevated_errors")
    assert (
        stage_of(TIMEOUT, seen, None, variant=SystemVariant.DETERMINISTIC)
        is FailureStage.RULE_ERROR
    )
    assert stage_of(TIMEOUT, seen, None) is FailureStage.UNATTRIBUTED


def _proposal(service: uuid.UUID | None, failure: str = "downstream_timeout") -> dict[str, Any]:
    if service is None:
        return ProposedDiagnosis(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE, summary="s", missing_evidence=("m",)
        ).model_dump(mode="json")
    return ProposedDiagnosis(
        status=DiagnosisStatus.DIAGNOSED,
        summary="s",
        root_cause=ProposedCause(
            service_id=service,
            failure_type=failure,
            confidence=0.8,
            evidence_signal_ids=(FAULT[0],),
        ),
    ).model_dump(mode="json")


def test_single_agent_stages() -> None:
    def v2(
        proposal: dict[str, Any], expected: ExpectedOutcome = TIMEOUT, dropped: bool = False
    ) -> StageScores:
        cause = proposal.get("root_cause")
        outcome = diagnosis(None if dropped or cause is None else uuid.UUID(cause["service_id"]))
        return scored({"investigate": proposal}, expected, outcome, workflow="v2")

    single = SystemVariant.SINGLE_AGENT
    assert (
        stage_of(
            TIMEOUT,
            observed("checkout", "elevated_errors"),
            v2(_proposal(CHECKOUT, "elevated_errors")),
            variant=single,
        )
        is FailureStage.NO_LEAD
    )
    assert (
        stage_of(TIMEOUT, observed(None), v2(_proposal(None)), variant=single)
        is FailureStage.NO_LEAD
    )
    assert (
        stage_of(TIMEOUT, observed(None), v2(_proposal(BANK), dropped=True), variant=single)
        is FailureStage.CONCLUDE_DROPPED
    )
    assert (
        stage_of(
            TIMEOUT,
            observed("bank-gateway", "elevated_errors"),
            v2(_proposal(BANK, "elevated_errors")),
            variant=single,
        )
        is FailureStage.WRONG_FAILURE_TYPE
    )
    underdetermined = v2(_proposal(CHECKOUT, "elevated_errors"), UNEXPLAINED)
    seen = observed("checkout", "elevated_errors")
    attribution = attribute(
        variant=single,
        status=CaseStatus.SCORED,
        expected=UNEXPLAINED,
        observed=seen,
        scores=score_case(UNEXPLAINED, seen),
        stages=underdetermined,
        matched_incident_id=INCIDENT,
    )
    assert attribution is not None
    assert (attribution.stage, attribution.origin) == (
        FailureStage.FALSE_DIAGNOSIS_ON_UNDERDETERMINED,
        "investigate",
    )


def test_limit_hit_is_orthogonal_to_the_stage() -> None:
    stages = scored(
        v3_steps(telemetry=specialist(T, ending=AgentEnding.NO_TURNS), synthesize=synthesis())
    )
    seen = observed(None)
    attribution = attribute(
        variant=SystemVariant.MULTI_AGENT,
        status=CaseStatus.SCORED,
        expected=TIMEOUT,
        observed=seen,
        scores=score_case(TIMEOUT, seen),
        stages=stages,
        matched_incident_id=INCIDENT,
    )
    assert attribution is not None
    assert (attribution.stage, attribution.limit_hit) == (FailureStage.NO_LEAD, True)


def test_stage_counts_sum_to_the_incorrect_count() -> None:
    def row(
        status: str, correct: bool | None, stage: FailureStage | None, limit: bool | None = None
    ) -> EvaluationCase:
        return EvaluationCase(
            status=status,
            scores=None
            if correct is None
            else CaseScores(
                outcome_correct=correct,
                root_cause_correct=None,
                abstained=False,
                false_diagnosis=False,
            ).model_dump(mode="json"),
            failure_stage=None if stage is None else stage.value,
            limit_hit=limit,
        )

    rows = [
        row("scored", True, None),
        row("scored", False, FailureStage.NO_LEAD, True),
        row("scored", False, FailureStage.NO_LEAD, False),
        row("scored", False, FailureStage.VERIFIER_FALSE_ACCEPT, False),
        row("failed", None, FailureStage.HARNESS_FAILURE, False),
    ]
    summary = failure_summary(rows)
    assert summary["incorrect"] == 4
    assert sum(summary["by_stage"].values()) == 4
    assert summary["by_stage"]["no_lead"] == 2
    assert summary["limit_hit"] == 1
