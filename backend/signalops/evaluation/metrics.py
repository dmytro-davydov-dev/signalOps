"""Aggregate case scores into the comparison a run reports (P10-T04, P10-T05).

Every variant's figures come with their sample count and the spread across
trials, and with the budget it was allowed, which is the same for all of them.
A harness failure counts against its variant rather than being dropped.
"""

import statistics
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from signalops.contracts.evaluation import CaseScores, CaseStatus, ObservedOutcome
from signalops.persistence.models import EvaluationCase, EvaluationReplay, EvaluationRun


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 4)


def summarize(
    run: EvaluationRun, cases: Sequence[EvaluationCase], replays: Sequence[EvaluationReplay]
) -> dict[str, Any]:
    by_variant: dict[str, list[EvaluationCase]] = defaultdict(list)
    for case in cases:
        by_variant[case.variant].append(case)
    variants = {}
    for variant in run.variants:
        rows = by_variant.get(variant, [])
        scored = [row for row in rows if row.status == CaseStatus.SCORED and row.scores]
        scores = [CaseScores.model_validate(row.scores) for row in scored]
        observed = [ObservedOutcome.model_validate(row.observed_outcome) for row in scored]
        per_trial: dict[int, list[bool]] = defaultdict(list)
        for row in rows:
            correct = bool(row.scores and row.scores.get("outcome_correct"))
            per_trial[row.trial].append(correct)
        trial_accuracy = [
            round(sum(values) / len(values), 4) for _, values in sorted(per_trial.items())
        ]
        costs = [item.spent_microusd for item in observed if item.investigation_id is not None]
        variants[variant] = {
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
            "abstentions": sum(score.abstained for score in scores),
            "false_diagnoses": sum(score.false_diagnosis for score in scores),
            "cost_microusd_total": sum(costs),
            "cost_microusd_mean": round(statistics.fmean(costs)) if costs else None,
            "unknown_reserved_microusd": sum(item.unknown_reserved_microusd for item in observed),
            "model_calls": sum(item.model_calls for item in observed),
        }
    return {
        "variants": variants,
        "budget_per_case": {
            key: run.limits.get(key) for key in ("budget_microusd", "max_steps", "deadline_seconds")
        },
        "replays": {
            "total": len(replays),
            "completed": sum(row.status == "completed" for row in replays),
        },
        "measures_model_quality": run.model_config.get("measures_model_quality"),
    }
