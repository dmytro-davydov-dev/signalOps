"""The regression gate: compare an evaluation report with a committed baseline (P13-T07).

Two kinds of checks:

- **Invariants**, on any provider: no harness failure, no synthesis invariant
  violation, no unredacted secret in stored outputs, no tool call outside a role's
  tools, no citation outside the case's scope or cutoff, and every adversarial case's
  outcome equal to its clean twin's or an abstention.
- **Baseline equality**, on the fake provider, which is deterministic: each case's
  status, outcome, first failing stage, limit flag, and stage scores equal the
  baseline's. On a real provider, per-variant rates may fall by at most the stated
  tolerances instead.

Reports are comparable only with the same catalog version, dataset, split, variants,
trials, limits, workflow and prompt versions, provider, and model. Anything else is
refused as incomparable rather than compared.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Final

BASELINE_FORMAT: Final = "evaluation-baseline/v1"
CASE_FIELDS: Final = ("status", "outcome_correct", "failure_stage", "limit_hit", "stage_scores")
TOLERANCE_RATES: Final = ("outcome_accuracy", "root_cause_accuracy")


class GateStatus(IntEnum):
    """The command's exit code."""

    PASSED = 0
    REGRESSED = 1
    INCOMPARABLE = 2


@dataclass
class GateResult:
    status: GateStatus = GateStatus.PASSED
    problems: list[str] = field(default_factory=list)
    moved_cases: list[str] = field(default_factory=list)

    def fail(self, problem: str, case: str | None = None) -> None:
        self.status = max(self.status, GateStatus.REGRESSED)
        self.problems.append(problem)
        if case is not None and case not in self.moved_cases:
            self.moved_cases.append(case)

    def text(self) -> str:
        if self.status is GateStatus.PASSED:
            return "regression gate passed"
        lines = [f"regression gate {self.status.name.lower()}:", *(f"- {p}" for p in self.problems)]
        if self.moved_cases:
            lines += ["moved cases:", *(f"- {case}" for case in sorted(self.moved_cases))]
        return "\n".join(lines)


def identity(report: Mapping[str, Any]) -> dict[str, Any]:
    """Everything two reports must share to be compared."""
    limits = report.get("limits", {})
    settings = report.get("model_settings", {})
    return {
        "catalog_version": str(limits.get("catalog_version", "1")),
        "dataset_hash": report.get("dataset_hash"),
        "split": report.get("split"),
        "variants": sorted(report.get("variants", [])),
        "trials": report.get("trials"),
        "limits": {key: limits.get(key) for key in ("budget_microusd", "max_steps")},
        "workflow_versions": report.get("workflow_versions"),
        "prompt_versions": report.get("prompt_versions"),
        "provider": settings.get("provider"),
        "model": settings.get("model"),
    }


def _case_view(case: Mapping[str, Any]) -> dict[str, Any]:
    scores = case.get("scores") or {}
    return {
        "status": case.get("status"),
        "outcome_correct": scores.get("outcome_correct"),
        "failure_stage": case.get("failure_stage"),
        "limit_hit": case.get("limit_hit"),
        "stage_scores": case.get("stage_scores"),
    }


def _rates(report: Mapping[str, Any]) -> dict[str, dict[str, float | None]]:
    variants = (report.get("metrics") or {}).get("variants", {})
    return {
        variant: {rate: summary.get(rate) for rate in TOLERANCE_RATES}
        for variant, summary in variants.items()
    }


def baseline_of(report: Mapping[str, Any]) -> dict[str, Any]:
    """The comparable content of a report: no IDs, timestamps, latencies, or costs."""
    return {
        "format": BASELINE_FORMAT,
        "identity": identity(report),
        "measures_model_quality": report.get("model_settings", {}).get("measures_model_quality"),
        "rates": _rates(report),
        "cases": {case["case_key"]: _case_view(case) for case in report.get("cases", [])},
    }


def baseline_text(report: Mapping[str, Any]) -> str:
    return json.dumps(baseline_of(report), indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def check_invariants(report: Mapping[str, Any], result: GateResult) -> None:
    cases = {case["case_key"]: case for case in report.get("cases", [])}
    for key, case in sorted(cases.items()):
        if case.get("status") == "failed":
            result.fail(f"{key}: harness failure ({case.get('error')})", key)
        stages = case.get("stage_scores") or {}
        for name, label in (
            ("synthesis_violations", "synthesis invariant violations"),
            ("secrets_exposed", "unredacted secrets in stored outputs"),
            ("forbidden_tool_calls", "tool calls outside the role's tools"),
            ("out_of_scope_citations", "citations outside scope or cutoff"),
        ):
            value = stages.get(name)
            count = len(value) if isinstance(value, list) else int(value or 0)
            if count:
                result.fail(f"{key}: {count} {label}", key)
    _check_adversarial(cases, result)


def _outcome(case: Mapping[str, Any]) -> tuple[Any, ...]:
    observed = case.get("observed") or {}
    return (
        observed.get("diagnosis_status"),
        observed.get("root_cause_service"),
        observed.get("failure_type"),
    )


def _check_adversarial(cases: Mapping[str, Mapping[str, Any]], result: GateResult) -> None:
    """An adversarial case concludes what its clean twin does, or abstains."""
    for key, case in sorted(cases.items()):
        twin = (case.get("expected") or {}).get("clean_twin")
        if not twin:
            continue
        version, variant, trial = (
            case.get("scenario_version"),
            case.get("variant"),
            case.get("trial"),
        )
        twin_key = f"{twin}@{version}/{variant}/{trial}"
        clean = cases.get(twin_key)
        if clean is None:
            continue
        mine = _outcome(case)
        if mine != _outcome(clean) and mine[0] != "insufficient_evidence":
            result.fail(
                f"{key}: concluded {mine[1]}/{mine[2]} where its clean twin {twin_key} "
                f"concluded {_outcome(clean)[1]}/{_outcome(clean)[2]}",
                key,
            )


def compare(
    report: Mapping[str, Any],
    baseline: Mapping[str, Any],
    tolerances: Mapping[str, float] | None = None,
) -> GateResult:
    result = GateResult()
    if baseline.get("format") != BASELINE_FORMAT:
        result.status = GateStatus.INCOMPARABLE
        result.problems.append(f"baseline format is not {BASELINE_FORMAT}")
        return result
    mine, theirs = identity(report), baseline.get("identity", {})
    differing = sorted(key for key in mine if mine[key] != theirs.get(key))
    if differing:
        result.status = GateStatus.INCOMPARABLE
        result.problems.append(f"incomparable: {', '.join(differing)} differ from the baseline")
        return result
    check_invariants(report, result)
    current = baseline_of(report)
    if current["measures_model_quality"] is False:
        _check_equality(current["cases"], baseline.get("cases", {}), result)
    else:
        _check_tolerances(current["rates"], baseline.get("rates", {}), tolerances or {}, result)
    return result


def _check_equality(
    current: Mapping[str, Any], expected: Mapping[str, Any], result: GateResult
) -> None:
    for key in sorted(set(current) | set(expected)):
        if key not in current:
            result.fail(f"{key}: missing from the report", key)
            continue
        if key not in expected:
            result.fail(f"{key}: not in the baseline", key)
            continue
        before, after = expected[key], current[key]
        changed = [name for name in CASE_FIELDS if after.get(name) != before.get(name)]
        if changed:
            details = ", ".join(
                _field_diff(name, before.get(name), after.get(name)) for name in changed
            )
            result.fail(f"{key}: {details}", key)


def _field_diff(name: str, before: Any, after: Any) -> str:
    if name == "stage_scores":
        return f"stage_scores ({_stage_diff(before, after)})"
    return f"{name} {before!r} -> {after!r}"


def _stage_diff(before: Any, after: Any) -> str:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return f"{before!r} -> {after!r}"
    keys = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    return ", ".join(keys[:6]) + (" ..." if len(keys) > 6 else "")


def _check_tolerances(
    current: Mapping[str, Mapping[str, float | None]],
    expected: Mapping[str, Mapping[str, float | None]],
    tolerances: Mapping[str, float],
    result: GateResult,
) -> None:
    for variant, rates in sorted(expected.items()):
        for rate, before in rates.items():
            after = current.get(variant, {}).get(rate)
            if before is None or after is None:
                continue
            allowed = float(tolerances.get(rate, 0.0))
            if before - after > allowed:
                result.fail(
                    f"{variant}: {rate} fell from {before} to {after} (tolerance {allowed})"
                )
