"""Scoring and evidence auditing (P10-T05): deterministic labels, one-to-one matching."""

import uuid
from datetime import UTC, datetime, timedelta

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import (
    CaseStatus,
    DetectionScore,
    ExpectedOutcome,
    FaultWindow,
    IncidentRef,
    ObservedOutcome,
)
from signalops.evaluation.metrics import summarize
from signalops.evaluation.scoring import match_detection, score_case
from signalops.persistence.models import (
    EvaluationAudit,
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
)

START = datetime(2026, 10, 16, 10, 34, tzinfo=UTC)
WINDOW = FaultWindow(start=START, end=START + timedelta(minutes=4))
TIMEOUT = ExpectedOutcome(
    incident_expected=True,
    diagnosable=True,
    root_cause_service="bank-gateway",
    failure_type="downstream_timeout",
    affected_services=("bank-gateway", "checkout", "payment"),
    fault_window=WINDOW,
    supporting_event_ids=("gw-1", "gw-2", "pay-1"),
)
UNEXPLAINED = ExpectedOutcome(
    incident_expected=True,
    diagnosable=False,
    affected_services=("checkout",),
    fault_window=WINDOW,
    supporting_event_ids=("co-1",),
)
HEALTHY = ExpectedOutcome(incident_expected=False, diagnosable=False)


def _diagnosis(
    service: str, cited: tuple[str, ...], failure: str = "downstream_timeout"
) -> ObservedOutcome:
    return ObservedOutcome(
        incident_ids=(uuid.uuid4(),),
        investigation_id=uuid.uuid4(),
        diagnosis_status=DiagnosisStatus.DIAGNOSED,
        root_cause_service=service,
        failure_type=failure,
        cited_event_ids=cited,
        started_at=START,
        completed_at=START + timedelta(seconds=12),
    )


def _abstention() -> ObservedOutcome:
    return ObservedOutcome(
        incident_ids=(uuid.uuid4(),),
        investigation_id=uuid.uuid4(),
        diagnosis_status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
    )


def test_a_supported_correct_diagnosis_scores_fully() -> None:
    scores = score_case(TIMEOUT, _diagnosis("bank-gateway", ("gw-1", "pay-1")))
    assert scores.outcome_correct
    assert scores.root_cause_correct
    assert scores.evidence_precision == 1.0
    assert not scores.unsupported_diagnosis
    assert scores.latency_seconds == 12.0


def test_a_known_false_diagnosis_loses_accuracy() -> None:
    wrong_service = score_case(TIMEOUT, _diagnosis("payment", ("pay-1",)))
    wrong_type = score_case(TIMEOUT, _diagnosis("bank-gateway", ("gw-1",), "elevated_errors"))
    for scores in (wrong_service, wrong_type):
        assert not scores.outcome_correct
        assert scores.root_cause_correct is False
        assert scores.false_diagnosis
    # Citing real fault evidence does not rescue a wrong cause, and is scored apart.
    assert wrong_service.evidence_precision == 1.0


def test_an_unsupported_citation_lowers_evidence_precision() -> None:
    partly = score_case(TIMEOUT, _diagnosis("bank-gateway", ("gw-1", "noise-7")))
    assert partly.outcome_correct
    assert partly.evidence_precision == 0.5
    assert not partly.unsupported_diagnosis
    none = score_case(TIMEOUT, _diagnosis("bank-gateway", ("noise-7", "noise-8")))
    assert none.evidence_precision == 0.0
    assert none.unsupported_diagnosis


def test_abstention_is_correct_only_when_the_cause_is_not_in_the_evidence() -> None:
    assert score_case(UNEXPLAINED, _abstention()).correct_abstention is True
    assert score_case(UNEXPLAINED, _abstention()).outcome_correct
    asserted = score_case(UNEXPLAINED, _diagnosis("checkout", ("co-1",), "elevated_errors"))
    assert asserted.false_diagnosis
    assert asserted.correct_abstention is False
    over = score_case(TIMEOUT, _abstention())
    assert not over.outcome_correct
    assert over.correct_abstention is None
    assert score_case(HEALTHY, ObservedOutcome()).outcome_correct


def _incident(minutes: float, *services: str) -> IncidentRef:
    return IncidentRef(
        id=uuid.uuid4(), detected_at=START + timedelta(minutes=minutes), services=services
    )


def test_detection_matches_one_to_one() -> None:
    first, second = _incident(1, "payment"), _incident(2, "checkout")
    twice = match_detection(TIMEOUT, [second, first])
    # Two incidents for one fault: one true positive, one false positive, never two hits.
    assert (twice.true_positives, twice.false_positives, twice.false_negatives) == (1, 1, 0)
    assert twice.matched_incident_id == first.id
    assert twice.detection_delay_seconds == 60.0
    late = match_detection(TIMEOUT, [_incident(30, "payment")])
    assert (late.true_positives, late.false_positives, late.false_negatives) == (0, 1, 1)
    elsewhere = match_detection(TIMEOUT, [_incident(1, "inventory")])
    assert (elsewhere.true_positives, elsewhere.false_negatives) == (0, 1)
    missed = match_detection(TIMEOUT, [])
    assert (missed.true_positives, missed.false_positives, missed.false_negatives) == (0, 0, 1)
    noise = match_detection(HEALTHY, [_incident(1, "payment")])
    assert (noise.true_positives, noise.false_positives, noise.false_negatives) == (0, 1, 0)


def _run() -> EvaluationRun:
    return EvaluationRun(
        id=uuid.uuid4(),
        variants=["multi_agent", "deterministic"],
        workflow_versions={"multi_agent": "v3", "deterministic": "d1"},
        limits={"budget_microusd": 1_000, "max_steps": 40, "deadline_seconds": 600},
        model_config={"measures_model_quality": False},
    )


def _case(
    variant: str, expected: ExpectedOutcome, observed: ObservedOutcome, trial: int = 1
) -> EvaluationCase:
    return EvaluationCase(
        id=uuid.uuid4(),
        variant=variant,
        trial=trial,
        status=CaseStatus.SCORED,
        expected_outcome=expected.summary().model_dump(mode="json"),
        observed_outcome=observed.model_dump(mode="json"),
        scores=score_case(expected, observed).model_dump(mode="json"),
    )


def test_the_summary_separates_accuracy_evidence_detection_and_human_audits() -> None:
    good = _case("multi_agent", TIMEOUT, _diagnosis("bank-gateway", ("gw-1", "gw-2")))
    unsupported = _case("multi_agent", TIMEOUT, _diagnosis("bank-gateway", ("noise-1",)), trial=2)
    false = _case("deterministic", TIMEOUT, _diagnosis("payment", ("pay-1", "noise-2")))
    failed = EvaluationCase(
        id=uuid.uuid4(),
        variant="deterministic",
        trial=1,
        status=CaseStatus.FAILED,
        expected_outcome=UNEXPLAINED.summary().model_dump(mode="json"),
    )
    replay = EvaluationReplay(
        report={
            "detection": DetectionScore(
                true_positives=1, false_positives=1, false_negatives=0, detection_delay_seconds=60
            ).model_dump(mode="json")
        }
    )
    audit = EvaluationAudit(
        case_id=good.id, signal_id=uuid.uuid4(), supported=False, auditor="reviewer"
    )

    metrics = summarize(_run(), [good, unsupported, false, failed], [replay], [audit])

    multi = metrics["variants"]["multi_agent"]
    assert multi["root_cause_accuracy"] == 1.0
    assert multi["evidence"] == {
        "source": "automatic",
        "cited": 3,
        "supported": 2,
        "precision": 0.6667,
    }
    assert multi["unsupported_diagnoses"] == 1
    assert multi["unsupported_rate"] == 0.5
    assert multi["trial_accuracy"] == [1.0, 1.0]
    # People's judgements stay apart from the automatic labels.
    assert multi["human_audit"] == {
        "source": "human",
        "audited_citations": 1,
        "supported": 0,
        "precision": 0.0,
    }
    deterministic = metrics["variants"]["deterministic"]
    assert deterministic["false_diagnoses"] == 1
    assert deterministic["root_cause_accuracy"] == 0.0
    assert deterministic["harness_failures"] == 1
    # A harness failure counts as incorrect rather than vanishing from the sample.
    assert deterministic["outcome_accuracy"] == 0.0
    assert deterministic["evidence"]["precision"] == 0.5
    assert metrics["detection"]["precision"] == 0.5
    assert metrics["detection"]["recall"] == 1.0
    assert metrics["judge_model_scores"] is None
