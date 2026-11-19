"""Report v2 additions: intervals, paired comparisons, and calibration (P13-T05).

`evaluation-report/v2` is additive over the metrics a run stored before: every v1
field keeps its meaning, and these sections are added. See `statistics.py` for the
methods and `docs/evaluation.md` for the fields.
"""

import itertools
import uuid
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Final

from signalops.contracts.evaluation import CaseScores, CaseStatus, ObservedOutcome
from signalops.evaluation.statistics import (
    ALPHA,
    RESAMPLES,
    brier,
    cluster_bootstrap,
    detectable_difference,
    mcnemar_exact,
    reliability_bins,
    wilson,
)
from signalops.persistence.models import EvaluationCase, EvaluationRun

REPORT_VERSION: Final = "evaluation-report/v2"
NO_DIFFERENCE: Final = "no detectable difference"


def interval(successes: int, trials: int) -> dict[str, Any]:
    """A rate, its counts, and its Wilson 95% interval."""
    bounds = wilson(successes, trials)
    return {
        "numerator": successes,
        "denominator": trials,
        "rate": None if trials == 0 else round(successes / trials, 4),
        "ci95": None if bounds is None else list(bounds),
    }


def _correct(row: EvaluationCase) -> bool:
    return row.status == CaseStatus.SCORED and bool((row.scores or {}).get("outcome_correct"))


def _observed(row: EvaluationCase) -> ObservedOutcome | None:
    if row.status != CaseStatus.SCORED or row.observed_outcome is None:
        return None
    return ObservedOutcome.model_validate(row.observed_outcome)


def variant_intervals(rows: Sequence[EvaluationCase]) -> dict[str, Any]:
    """Wilson intervals for the variant's headline rates."""
    scored = [row for row in rows if row.status == CaseStatus.SCORED and row.scores]
    scores = [CaseScores.model_validate(row.scores) for row in scored]
    diagnosable = [row for row in rows if row.expected_outcome.get("diagnosable")]
    diagnosed = [
        score
        for score, row in zip(scores, scored, strict=True)
        if (row.observed_outcome or {}).get("diagnosis_status") == "diagnosed"
    ]
    chances = [score for score in scores if score.correct_abstention is not None]
    cited = sum(score.cited for score in scores)
    return {
        "outcome_accuracy": interval(sum(_correct(row) for row in rows), len(rows)),
        "root_cause_accuracy": interval(
            sum(bool((row.scores or {}).get("root_cause_correct")) for row in diagnosable),
            len(diagnosable),
        ),
        "unsupported_rate": interval(
            sum(score.unsupported_diagnosis for score in diagnosed), len(diagnosed)
        ),
        "correct_abstention_rate": interval(
            sum(bool(score.correct_abstention) for score in chances), len(chances)
        ),
        "evidence_precision": interval(sum(score.cited_supported for score in scores), cited),
        "harness_failure_rate": interval(
            sum(row.status == CaseStatus.FAILED for row in rows), len(rows)
        ),
    }


def cost_per_correct(rows: Sequence[EvaluationCase]) -> int | None:
    observed = [item for row in rows if (item := _observed(row)) is not None]
    correct = sum(_correct(row) for row in rows)
    if correct == 0:
        return None
    return round(sum(item.spent_microusd for item in observed) / correct)


def calibration(rows: Sequence[EvaluationCase], meaningful: bool) -> dict[str, Any]:
    """Stated root-cause confidence against `root_cause_correct`, on asserted causes."""
    confidences: list[float] = []
    outcomes: list[bool] = []
    for row in rows:
        observed = _observed(row)
        correct = (row.scores or {}).get("root_cause_correct")
        if observed is None or observed.root_cause_confidence is None or correct is None:
            continue
        confidences.append(observed.root_cause_confidence)
        outcomes.append(bool(correct))
    return {
        "meaningful": meaningful,
        "note": None
        if meaningful
        else "The provider does not measure model quality, so its confidences mean nothing.",
        "samples": len(confidences),
        "brier": brier(confidences, outcomes),
        "bins": reliability_bins(confidences, outcomes),
    }


def _pairs(
    a: Sequence[EvaluationCase], b: Sequence[EvaluationCase]
) -> dict[uuid.UUID, list[tuple[EvaluationCase, EvaluationCase]]]:
    """Cases of two variants on the same scenario, matched by trial.

    A variant run once per scenario (the deterministic one) is matched with every
    trial of the other, so each pair compares the same evidence.
    """
    by_a: dict[uuid.UUID, dict[int, EvaluationCase]] = defaultdict(dict)
    by_b: dict[uuid.UUID, dict[int, EvaluationCase]] = defaultdict(dict)
    for row in a:
        by_a[row.scenario_id][row.trial] = row
    for row in b:
        by_b[row.scenario_id][row.trial] = row
    paired: dict[uuid.UUID, list[tuple[EvaluationCase, EvaluationCase]]] = {}
    for scenario in sorted(set(by_a) & set(by_b), key=str):
        trials_a, trials_b = by_a[scenario], by_b[scenario]
        trials = sorted(set(trials_a) | set(trials_b))
        matched = []
        for trial in trials:
            left = trials_a.get(trial) or (trials_a[1] if len(trials_a) == 1 else None)
            right = trials_b.get(trial) or (trials_b[1] if len(trials_b) == 1 else None)
            if left is not None and right is not None:
                matched.append((left, right))
        if matched:
            paired[scenario] = matched
    return paired


def _difference(
    pairs: dict[uuid.UUID, list[tuple[EvaluationCase, EvaluationCase]]],
    value: Any,
    seed: int,
    digits: int = 4,
) -> dict[str, Any] | None:
    differences: dict[uuid.UUID, list[float]] = {}
    for scenario, matched in pairs.items():
        values = [
            float(left_value - right_value)
            for left, right in matched
            if (left_value := value(left)) is not None and (right_value := value(right)) is not None
        ]
        if values:
            differences[scenario] = values
    result = cluster_bootstrap(differences, seed=seed, digits=digits)
    if result is None:
        return None
    return {
        "estimate": result.estimate,
        "ci95": [result.low, result.high],
        "clusters": result.clusters,
        "pairs": result.pairs,
        "excludes_zero": result.excludes_zero,
    }


def _cost(row: EvaluationCase) -> int | None:
    observed = _observed(row)
    return (
        None if observed is None or observed.investigation_id is None else observed.spent_microusd
    )


def _latency(row: EvaluationCase) -> float | None:
    value = (row.scores or {}).get("latency_seconds")
    return None if value is None else float(value)


def compare(
    run: EvaluationRun, left: str, right: str, by_variant: dict[str, list[EvaluationCase]]
) -> dict[str, Any]:
    """Paired difference `left - right` on identical cases, with its tests."""
    pairs = _pairs(by_variant.get(left, []), by_variant.get(right, []))
    flat = [pair for matched in pairs.values() for pair in matched]
    accuracy = _difference(pairs, lambda row: int(_correct(row)), run.seed)
    single_trial = all(len(matched) == 1 for matched in pairs.values())
    mcnemar = None
    if flat and single_trial:
        only_left = sum(_correct(a) and not _correct(b) for a, b in flat)
        only_right = sum(_correct(b) and not _correct(a) for a, b in flat)
        mcnemar = {
            "only_left_correct": only_left,
            "only_right_correct": only_right,
            "p_value": round(mcnemar_exact(only_left, only_right), 4),
        }
    if mcnemar is not None:
        significant = mcnemar["p_value"] < ALPHA
        method = "exact McNemar"
    else:
        significant = bool(accuracy and accuracy["excludes_zero"])
        method = "cluster bootstrap over scenarios"
    estimate = accuracy["estimate"] if accuracy else 0.0
    verdict = NO_DIFFERENCE
    if significant:
        verdict = f"{left} more accurate" if estimate > 0 else f"{right} more accurate"
    return {
        "left": left,
        "right": right,
        "scenarios": len(pairs),
        "pairs": len(flat),
        "accuracy_difference": accuracy,
        "mcnemar": mcnemar,
        "test": method,
        "significant": significant,
        "verdict": verdict,
        "min_detectable_difference": detectable_difference(len(pairs)),
        "cost_difference_microusd": _difference(pairs, _cost, run.seed, digits=0),
        "latency_difference_seconds": _difference(pairs, _latency, run.seed, digits=3),
    }


def comparisons(
    run: EvaluationRun, by_variant: dict[str, list[EvaluationCase]]
) -> list[dict[str, Any]]:
    return [
        compare(run, left, right, by_variant)
        for left, right in itertools.combinations(run.variants, 2)
    ]


def reproducibility(run: EvaluationRun) -> dict[str, Any]:
    return {
        "catalog_version": str(run.limits.get("catalog_version", "1")),
        "dataset_hash": run.dataset_hash,
        "seed": run.seed,
        "bootstrap": {"seed": run.seed, "resamples": RESAMPLES, "interval": "percentile 95%"},
        "intervals": "Wilson score 95%",
    }
