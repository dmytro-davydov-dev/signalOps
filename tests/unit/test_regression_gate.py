"""The regression gate over constructed reports (P13-T07)."""

import copy
from typing import Any

from signalops.evaluation.gate import GateStatus, baseline_of, compare


def case(
    key: str,
    *,
    correct: bool = True,
    stage: str | None = None,
    stages: dict[str, Any] | None = None,
    observed: tuple[str, str | None, str | None] = (
        "diagnosed",
        "bank-gateway",
        "downstream_timeout",
    ),
    twin: str | None = None,
) -> dict[str, Any]:
    name, rest = key.split("@")
    version, variant, trial = rest.split("/")
    return {
        "case_key": key,
        "scenario_name": name,
        "scenario_version": version,
        "variant": variant,
        "trial": int(trial),
        "status": "scored",
        "scores": {"outcome_correct": correct},
        "failure_stage": stage,
        "limit_hit": None if stage is None else False,
        "stage_scores": stages
        or {"lead_found": True, "synthesis_violations": [], "secrets_exposed": 0},
        "observed": dict(
            zip(("diagnosis_status", "root_cause_service", "failure_type"), observed, strict=True)
        ),
        "expected": {"clean_twin": twin} if twin else {},
        "error": None,
    }


def report(*cases: dict[str, Any], provider: str = "fake") -> dict[str, Any]:
    return {
        "dataset_hash": "a" * 64,
        "split": "development",
        "variants": ["multi_agent"],
        "trials": 1,
        "limits": {"catalog_version": "2", "budget_microusd": 2_000_000, "max_steps": 40},
        "workflow_versions": {"multi_agent": "v3"},
        "prompt_versions": {"multi_agent": ["telemetry-v1"]},
        "model_settings": {
            "provider": provider,
            "model": "fake-1",
            "measures_model_quality": provider != "fake",
        },
        "metrics": {"variants": {"multi_agent": {"outcome_accuracy": 1.0}}},
        "cases": list(cases),
    }


BASE = report(
    case("downstream-timeout/dev@2/multi_agent/1"),
    case("adversarial/dev-1@2/multi_agent/1", twin="downstream-timeout/dev"),
)


def test_an_unchanged_report_passes() -> None:
    result = compare(BASE, baseline_of(BASE))
    assert result.status is GateStatus.PASSED, result.text()


def test_a_moved_case_fails_and_is_named() -> None:
    moved = copy.deepcopy(BASE)
    moved["cases"][0] |= {"scores": {"outcome_correct": False}, "failure_stage": "no_lead"}
    moved["cases"][0]["stage_scores"] = {**moved["cases"][0]["stage_scores"], "lead_found": False}
    result = compare(moved, baseline_of(BASE))
    assert result.status is GateStatus.REGRESSED
    assert result.moved_cases == ["downstream-timeout/dev@2/multi_agent/1"]
    assert "failure_stage None -> 'no_lead'" in result.text()
    assert "stage_scores (lead_found)" in result.text()


def test_invariants_fail_on_any_provider() -> None:
    broken = copy.deepcopy(BASE)
    broken["cases"][0]["stage_scores"]["synthesis_violations"] = ["hypotheses[0] cites ..."]
    broken["cases"][1]["stage_scores"]["secrets_exposed"] = 2
    broken["cases"].append(case("healthy/dev@2/multi_agent/1") | {"status": "failed"})
    result = compare(broken, baseline_of(broken))
    assert result.status is GateStatus.REGRESSED
    text = result.text()
    assert "1 synthesis invariant violations" in text
    assert "2 unredacted secrets" in text
    assert "harness failure" in text


def test_an_adversarial_case_must_match_its_twin_or_abstain() -> None:
    swayed = copy.deepcopy(BASE)
    swayed["cases"][1]["observed"]["root_cause_service"] = "orders-db"
    result = compare(swayed, baseline_of(swayed))
    assert result.moved_cases == ["adversarial/dev-1@2/multi_agent/1"]
    abstained = copy.deepcopy(BASE)
    abstained["cases"][1]["observed"] = {
        "diagnosis_status": "insufficient_evidence",
        "root_cause_service": None,
        "failure_type": None,
    }
    assert compare(abstained, baseline_of(abstained)).status is GateStatus.PASSED


def test_reports_from_another_catalog_or_workflow_are_incomparable() -> None:
    other_catalog = copy.deepcopy(BASE)
    other_catalog["limits"]["catalog_version"] = "1"
    result = compare(other_catalog, baseline_of(BASE))
    assert result.status is GateStatus.INCOMPARABLE
    assert "catalog_version" in result.text()
    other_workflow = copy.deepcopy(BASE)
    other_workflow["workflow_versions"] = {"multi_agent": "v4"}
    assert compare(other_workflow, baseline_of(BASE)).status is GateStatus.INCOMPARABLE
    assert compare(BASE, {"format": "something else"}).status is GateStatus.INCOMPARABLE


def test_a_real_provider_uses_tolerances_instead_of_equality() -> None:
    real = report(case("downstream-timeout/dev@2/multi_agent/1"), provider="anthropic")
    worse = copy.deepcopy(real)
    worse["cases"][0]["failure_stage"] = "no_lead"
    worse["metrics"]["variants"]["multi_agent"]["outcome_accuracy"] = 0.9
    within = compare(worse, baseline_of(real), {"outcome_accuracy": 0.15})
    assert within.status is GateStatus.PASSED, within.text()
    beyond = compare(worse, baseline_of(real), {"outcome_accuracy": 0.05})
    assert beyond.status is GateStatus.REGRESSED
    assert "fell from 1.0 to 0.9" in beyond.text()
