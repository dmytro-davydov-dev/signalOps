"""Score observed outcomes against ground truth, using deterministic labels only.

Nothing here asks a model to judge. A score compares what the system stored with
the scenario's labels: the expected service and failure type, and the records the
fault produced (`supporting_event_ids`). A citation outside those records counts
as unsupported. Semantic support beyond that comes from human audits, which are
reported separately and never mixed into these scores.

**Detection matching.** An incident matches the expected fault when it was
detected (event time) between the fault's start and its end plus
`MATCH_GRACE`, and names at least one affected service. Matching is one-to-one:
the earliest matching incident is the true positive, every other incident is a
false positive, and a fault with no match is a false negative. So two incidents
for one fault count once, not twice.
"""

from collections.abc import Sequence
from datetime import timedelta
from typing import Final

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evaluation import (
    CaseScores,
    DetectionScore,
    ExpectedOutcome,
    IncidentRef,
    ObservedOutcome,
)

MATCH_GRACE: Final = timedelta(minutes=5)


def score_case(expected: ExpectedOutcome, observed: ObservedOutcome) -> CaseScores:
    diagnosed = observed.diagnosis_status == DiagnosisStatus.DIAGNOSED
    abstained = observed.diagnosis_status == DiagnosisStatus.INSUFFICIENT_EVIDENCE
    matches = (
        diagnosed
        and observed.root_cause_service == expected.root_cause_service
        and observed.failure_type == expected.failure_type
    )
    supporting = set(expected.supporting_event_ids)
    cited = len(observed.cited_event_ids)
    supported = sum(event_id in supporting for event_id in observed.cited_event_ids)
    if expected.diagnosable:
        outcome = CaseScores(
            outcome_correct=matches,
            root_cause_correct=matches,
            abstained=abstained,
            false_diagnosis=diagnosed and not matches,
        )
    else:
        # With no incident, there is nothing to investigate and nothing asserted.
        quiet = not expected.incident_expected and not observed.incident_ids
        outcome = CaseScores(
            outcome_correct=abstained or quiet,
            root_cause_correct=None,
            abstained=abstained,
            false_diagnosis=diagnosed,
            correct_abstention=abstained if expected.incident_expected else None,
        )
    return outcome.model_copy(
        update={
            "cited": cited,
            "cited_supported": supported,
            "evidence_precision": round(supported / cited, 4) if cited else None,
            "unsupported_diagnosis": diagnosed and supported == 0,
            "latency_seconds": _latency(observed),
        }
    )


def _latency(observed: ObservedOutcome) -> float | None:
    if observed.started_at is None or observed.completed_at is None:
        return None
    return round((observed.completed_at - observed.started_at).total_seconds(), 3)


def match_detection(expected: ExpectedOutcome, incidents: Sequence[IncidentRef]) -> DetectionScore:
    window = expected.fault_window
    if not expected.incident_expected or window is None:
        return DetectionScore(true_positives=0, false_positives=len(incidents), false_negatives=0)
    affected = set(expected.affected_services)
    matching = sorted(
        (
            incident
            for incident in incidents
            if window.start <= incident.detected_at <= window.end + MATCH_GRACE
            and affected & set(incident.services)
        ),
        key=lambda incident: (incident.detected_at, incident.id),
    )
    if not matching:
        return DetectionScore(true_positives=0, false_positives=len(incidents), false_negatives=1)
    first = matching[0]
    return DetectionScore(
        true_positives=1,
        false_positives=len(incidents) - 1,
        false_negatives=0,
        matched_incident_id=first.id,
        detection_delay_seconds=round((first.detected_at - window.start).total_seconds(), 3),
    )
