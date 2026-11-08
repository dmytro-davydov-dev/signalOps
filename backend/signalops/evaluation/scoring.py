"""Score observed outcomes against ground truth, using deterministic labels only.

Nothing here asks a model to judge. A score is a comparison of what the system
stored with the scenario's labels.
"""

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import CaseScores, ExpectedOutcome, ObservedOutcome


def score_case(expected: ExpectedOutcome, observed: ObservedOutcome) -> CaseScores:
    diagnosed = observed.diagnosis_status == DiagnosisStatus.DIAGNOSED
    abstained = observed.diagnosis_status == DiagnosisStatus.INSUFFICIENT_EVIDENCE
    matches = (
        diagnosed
        and observed.root_cause_service == expected.root_cause_service
        and observed.failure_type == expected.failure_type
    )
    if expected.diagnosable:
        return CaseScores(
            outcome_correct=matches,
            root_cause_correct=matches,
            abstained=abstained,
            false_diagnosis=diagnosed and not matches,
        )
    # With no incident, there is nothing to investigate and nothing asserted.
    quiet = not expected.incident_expected and not observed.incident_ids
    return CaseScores(
        outcome_correct=abstained or quiet,
        root_cause_correct=None,
        abstained=abstained,
        false_diagnosis=diagnosed,
    )
