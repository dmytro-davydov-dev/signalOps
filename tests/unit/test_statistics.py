"""Statistics against independently computed reference values (P13-T05)."""

import json
import uuid
from pathlib import Path

import pytest

from signalops.contracts.evaluation import CaseScores, ObservedOutcome
from signalops.contracts.evaluation_api import EvaluationRunDetail
from signalops.evaluation.export import export_documents, markdown
from signalops.evaluation.report import NO_DIFFERENCE, calibration, compare
from signalops.evaluation.statistics import (
    brier,
    cluster_bootstrap,
    detectable_difference,
    mcnemar_exact,
    reliability_bins,
    smallest_significant_discordance,
    wilson,
)
from signalops.persistence.models import EvaluationCase, EvaluationRun

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("successes", "trials", "expected"),
    [
        # Newcombe (1998), Statistics in Medicine 17:857-872, Table I, method 3.
        (81, 263, (0.2553, 0.3662)),
        (15, 148, (0.0624, 0.1605)),
        (0, 20, (0.0, 0.1611)),
        (1, 29, (0.0061, 0.1718)),
    ],
)
def test_wilson_matches_published_intervals(
    successes: int, trials: int, expected: tuple[float, float]
) -> None:
    assert wilson(successes, trials) == expected


def test_wilson_bounds_and_empty_samples() -> None:
    assert wilson(0, 0) is None
    assert wilson(20, 20) == (0.8389, 1.0)
    with pytest.raises(ValueError, match="between"):
        wilson(3, 2)


def test_exact_mcnemar_matches_published_and_hand_values() -> None:
    # Fagerland, Lydersen, Laake (2013), BMC Med Res Methodol 13:91: b=1, c=5 -> 0.219.
    assert round(mcnemar_exact(1, 5), 3) == 0.219
    assert mcnemar_exact(5, 1) == mcnemar_exact(1, 5)
    # 2 * 0.5**7 by hand.
    assert mcnemar_exact(7, 0) == 0.015625
    assert mcnemar_exact(0, 0) == 1.0
    assert smallest_significant_discordance() == 6
    assert detectable_difference(37) == round(6 / 37, 4)
    assert detectable_difference(5) is None
    assert detectable_difference(0) is None


def test_cluster_bootstrap_matches_enumeration_on_a_tiny_sample() -> None:
    # Two clusters with differences 1 and 0: resampled means are 1, 0.5, 0.5, 0 with
    # probabilities 1/4, 1/2, 1/4, so the 2.5th and 97.5th percentiles are 0 and 1.
    result = cluster_bootstrap({"a": [1.0], "b": [0.0]}, seed=7)
    assert result is not None
    assert (result.estimate, result.low, result.high) == (0.5, 0.0, 1.0)
    assert not result.excludes_zero
    # Trials stay with their cluster: the estimate is the mean over pairs.
    weighted = cluster_bootstrap({"a": [1.0, 1.0, 1.0], "b": [0.0]}, seed=7)
    assert weighted is not None
    assert weighted.estimate == 0.75
    # The same seed gives the same interval.
    assert cluster_bootstrap({"a": [1.0], "b": [0.0], "c": [1.0]}, seed=3) == cluster_bootstrap(
        {"c": [1.0], "b": [0.0], "a": [1.0]}, seed=3
    )
    assert cluster_bootstrap({}, seed=0) is None


def test_brier_and_reliability_bins_by_hand() -> None:
    confidences = [0.9, 0.8, 0.3, 0.1]
    outcomes = [True, False, False, False]
    # (0.01 + 0.64 + 0.09 + 0.01) / 4
    assert brier(confidences, outcomes) == 0.1875
    bins = reliability_bins(confidences, outcomes)
    assert [b["count"] for b in bins] == [1, 1, 0, 0, 2]
    assert bins[4] == {
        "low": 0.8,
        "high": 1.0,
        "count": 2,
        "mean_confidence": 0.85,
        "accuracy": 0.5,
    }
    assert brier([], []) is None


SCENARIOS = [uuid.uuid4() for _ in range(10)]


def _run(variants: list[str], *, trials: int = 1) -> EvaluationRun:
    return EvaluationRun(
        id=uuid.uuid4(),
        variants=variants,
        trials=trials,
        seed=0,
        model_config={"measures_model_quality": False},
        limits={},
        dataset_hash="0" * 64,
    )


def _case(scenario: uuid.UUID, variant: str, correct: bool, trial: int = 1) -> EvaluationCase:
    observed = ObservedOutcome(
        investigation_id=uuid.uuid4(), spent_microusd=1000 if variant != "deterministic" else 0
    )
    return EvaluationCase(
        scenario_id=scenario,
        variant=variant,
        trial=trial,
        status="scored",
        expected_outcome={"diagnosable": True},
        observed_outcome=observed.model_dump(mode="json"),
        scores=CaseScores(
            outcome_correct=correct,
            root_cause_correct=correct,
            abstained=False,
            false_diagnosis=not correct,
            latency_seconds=2.0,
        ).model_dump(mode="json"),
    )


def test_identical_outcomes_show_no_detectable_difference() -> None:
    run = _run(["single_agent", "multi_agent"])
    outcomes = [i % 2 == 0 for i in range(len(SCENARIOS))]
    by_variant = {
        variant: [_case(s, variant, ok) for s, ok in zip(SCENARIOS, outcomes, strict=True)]
        for variant in run.variants
    }
    result = compare(run, "single_agent", "multi_agent", by_variant)
    assert result["verdict"] == NO_DIFFERENCE
    assert result["significant"] is False
    assert result["test"] == "exact McNemar"
    assert result["mcnemar"]["p_value"] == 1.0
    assert result["accuracy_difference"]["estimate"] == 0.0
    assert result["min_detectable_difference"] == 0.6


def test_one_variant_right_where_the_other_is_wrong_is_significant() -> None:
    run = _run(["single_agent", "multi_agent"])
    by_variant = {
        "single_agent": [_case(s, "single_agent", False) for s in SCENARIOS],
        "multi_agent": [_case(s, "multi_agent", True) for s in SCENARIOS],
    }
    result = compare(run, "single_agent", "multi_agent", by_variant)
    assert result["significant"] is True
    assert result["verdict"] == "multi_agent more accurate"
    assert result["mcnemar"] == {
        "only_left_correct": 0,
        "only_right_correct": 10,
        "p_value": round(2 * 0.5**10, 4),
    }
    assert result["accuracy_difference"]["estimate"] == -1.0
    assert result["cost_difference_microusd"]["estimate"] == 0


def test_trials_use_the_cluster_bootstrap_and_broadcast_a_single_baseline_run() -> None:
    run = _run(["deterministic", "multi_agent"], trials=3)
    by_variant = {
        "deterministic": [_case(s, "deterministic", False) for s in SCENARIOS],
        "multi_agent": [
            _case(s, "multi_agent", True, trial) for s in SCENARIOS for trial in (1, 2, 3)
        ],
    }
    result = compare(run, "deterministic", "multi_agent", by_variant)
    assert result["test"] == "cluster bootstrap over scenarios"
    assert result["mcnemar"] is None
    assert (result["scenarios"], result["pairs"]) == (10, 30)
    assert result["accuracy_difference"]["ci95"] == [-1.0, -1.0]
    assert result["verdict"] == "multi_agent more accurate"
    assert result["cost_difference_microusd"]["estimate"] == -1000


def test_calibration_is_flagged_when_the_provider_measures_nothing() -> None:
    case = _case(SCENARIOS[0], "multi_agent", True)
    observed = ObservedOutcome.model_validate(case.observed_outcome)
    case.observed_outcome = observed.model_copy(update={"root_cause_confidence": 0.7}).model_dump(
        mode="json"
    )
    result = calibration([case], meaningful=False)
    assert result["meaningful"] is False
    assert result["note"]
    assert (result["samples"], result["brier"]) == (1, 0.09)


def test_a_v1_report_still_reads_and_exports_deterministically() -> None:
    retained = json.loads(
        (ROOT / "docs" / "evaluation-runs" / "7ac381af-heldout-fake.json").read_text()
    )
    detail = EvaluationRunDetail.model_validate(retained)
    first = export_documents(detail, "abc123")
    assert first == export_documents(detail, "abc123")
    text = markdown(detail, retained["metrics"])
    assert "Fake provider" in text
    assert "## Paired comparisons" in text
