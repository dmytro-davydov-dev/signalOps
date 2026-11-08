"""Aggregate case scores into the comparison a run reports (P10-T04, P10-T05).

Every variant's figures come with their sample count, the spread across trials,
and the budget it was allowed, which is the same for all of them. A harness
failure counts against its variant rather than being dropped.

**Sources.** `automatic` figures compare stored outcomes with deterministic labels.
`human_audit` figures come only from people's judgements of sampled citations and
stay separate. No figure comes from a judge model.

**Detection** is per replay, shared by every variant, and uses the one-to-one
matching in `scoring.match_detection`. Its delay is in event time. Investigation
latency is wall-clock time from request to stored outcome.
"""

import statistics
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from signalops.contracts.evaluation import CaseScores, CaseStatus, DetectionScore, ObservedOutcome
from signalops.persistence.models import (
    EvaluationAudit,
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
)


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 4)


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    # Nearest rank, so a reported p95 is a latency that actually happened.
    rank = max(1, -(-len(ordered) * q // 1))
    return round(ordered[int(rank) - 1], 3)


def detection_summary(replays: Sequence[EvaluationReplay]) -> dict[str, Any]:
    scores = [
        DetectionScore.model_validate(row.report["detection"])
        for row in replays
        if row.report and "detection" in row.report
    ]
    tp = sum(score.true_positives for score in scores)
    fp = sum(score.false_positives for score in scores)
    fn = sum(score.false_negatives for score in scores)
    delays = [s.detection_delay_seconds for s in scores if s.detection_delay_seconds is not None]
    return {
        "replays_scored": len(scores),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "precision": _rate(tp, tp + fp),
        "recall": _rate(tp, tp + fn),
        "delay_seconds_p50": _quantile(delays, 0.5),
        "delay_seconds_p95": _quantile(delays, 0.95),
        "delay_clock": "event time",
    }


def _audit_summary(audits: Sequence[EvaluationAudit]) -> dict[str, Any]:
    supported = sum(audit.supported for audit in audits)
    return {
        "source": "human",
        "audited_citations": len(audits),
        "supported": supported,
        "precision": _rate(supported, len(audits)),
    }


def variant_summary(
    run: EvaluationRun,
    variant: str,
    rows: Sequence[EvaluationCase],
    audits: Sequence[EvaluationAudit],
) -> dict[str, Any]:
    scored = [row for row in rows if row.status == CaseStatus.SCORED and row.scores]
    scores = [CaseScores.model_validate(row.scores) for row in scored]
    observed = [ObservedOutcome.model_validate(row.observed_outcome) for row in scored]
    per_trial: dict[int, list[bool]] = defaultdict(list)
    for row in rows:
        per_trial[row.trial].append(bool(row.scores and row.scores.get("outcome_correct")))
    trial_accuracy = [round(sum(v) / len(v), 4) for _, v in sorted(per_trial.items())]
    costs = [item.spent_microusd for item in observed if item.investigation_id is not None]
    diagnosable = [row for row in rows if row.expected_outcome.get("diagnosable")]
    root_correct = sum(
        bool(row.scores and row.scores.get("root_cause_correct")) for row in diagnosable
    )
    diagnosed = sum(item.diagnosis_status == "diagnosed" for item in observed)
    abstention_chances = [s for s in scores if s.correct_abstention is not None]
    cited = sum(score.cited for score in scores)
    latencies = [s.latency_seconds for s in scores if s.latency_seconds is not None]
    return {
        "workflow_version": run.workflow_versions.get(variant),
        "cases": len(rows),
        "samples": len(scored),
        "trials": len(per_trial),
        "harness_failures": sum(row.status == CaseStatus.FAILED for row in rows),
        "outcome_correct": sum(score.outcome_correct for score in scores),
        # Failed cases count as incorrect.
        "outcome_accuracy": _rate(sum(score.outcome_correct for score in scores), len(rows)),
        "trial_accuracy": trial_accuracy,
        "accuracy_stdev": (
            round(statistics.pstdev(trial_accuracy), 4) if len(trial_accuracy) > 1 else None
        ),
        "root_cause_accuracy": _rate(root_correct, len(diagnosable)),
        "diagnosable_cases": len(diagnosable),
        "diagnoses": diagnosed,
        "false_diagnoses": sum(score.false_diagnosis for score in scores),
        "unsupported_diagnoses": sum(score.unsupported_diagnosis for score in scores),
        "unsupported_rate": _rate(sum(s.unsupported_diagnosis for s in scores), diagnosed),
        "abstentions": sum(score.abstained for score in scores),
        "correct_abstentions": sum(bool(s.correct_abstention) for s in abstention_chances),
        "abstention_opportunities": len(abstention_chances),
        # Abstained although the evidence supports the labelled cause.
        "over_abstentions": sum(s.abstained for s in scores if s.root_cause_correct is not None),
        "evidence": {
            "source": "automatic",
            "cited": cited,
            "supported": sum(score.cited_supported for score in scores),
            "precision": _rate(sum(score.cited_supported for score in scores), cited),
        },
        "human_audit": _audit_summary(audits),
        "latency_seconds_p50": _quantile(latencies, 0.5),
        "latency_seconds_p95": _quantile(latencies, 0.95),
        "cost_microusd_total": sum(costs),
        "cost_microusd_mean": round(statistics.fmean(costs)) if costs else None,
        "unknown_reserved_microusd": sum(item.unknown_reserved_microusd for item in observed),
        "model_calls": sum(item.model_calls for item in observed),
    }


def summarize(
    run: EvaluationRun,
    cases: Sequence[EvaluationCase],
    replays: Sequence[EvaluationReplay],
    audits: Sequence[EvaluationAudit] = (),
) -> dict[str, Any]:
    by_variant: dict[str, list[EvaluationCase]] = defaultdict(list)
    variant_of = {case.id: case.variant for case in cases}
    for case in cases:
        by_variant[case.variant].append(case)
    audits_by_variant: dict[str, list[EvaluationAudit]] = defaultdict(list)
    for audit in audits:
        audits_by_variant[variant_of.get(audit.case_id, "")].append(audit)
    return {
        "variants": {
            variant: variant_summary(
                run, variant, by_variant.get(variant, []), audits_by_variant.get(variant, [])
            )
            for variant in run.variants
        },
        "detection": detection_summary(replays),
        "budget_per_case": {
            key: run.limits.get(key) for key in ("budget_microusd", "max_steps", "deadline_seconds")
        },
        "replays": {
            "total": len(replays),
            "completed": sum(row.status == "completed" for row in replays),
        },
        "measures_model_quality": run.model_config.get("measures_model_quality"),
        "judge_model_scores": None,
    }
