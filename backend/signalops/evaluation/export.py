"""Run views shared by the API, and `evaluation-export` (P13-T05).

`run_detail` is what `GET /v1/evaluation-runs/{id}` returns. `export_documents` turns
it into the retained report: the same JSON with the metrics recomputed from the
stored cases, so a run that finished before report v2 gains its intervals and
comparisons, plus where it came from, and a Markdown summary. Both are deterministic:
exporting one run twice from the same tree gives identical files.
"""

import json
import subprocess
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.evaluation import CaseScores, ExpectedOutcome, ObservedOutcome
from signalops.contracts.evaluation_api import (
    EvaluationCaseRow,
    EvaluationRunDetail,
    EvaluationRunSummary,
)
from signalops.contracts.evaluation_stages import StageScores
from signalops.evaluation.metrics import summarize
from signalops.evaluation.report import NO_DIFFERENCE
from signalops.persistence.models import (
    EvaluationCase,
    EvaluationReplay,
    EvaluationRun,
    EvaluationScenario,
)
from signalops.persistence.repositories.evaluation import EvaluationRepository

PROVIDER_SLUG: Final = {"fake": "fake", "anthropic": "anthropic"}


def run_summary(run: EvaluationRun, counts: dict[str, int]) -> EvaluationRunSummary:
    return EvaluationRunSummary(
        id=run.id,
        label=run.label,
        split=run.split,  # type: ignore[arg-type]
        status=run.status,  # type: ignore[arg-type]
        variants=tuple(run.variants),  # type: ignore[arg-type]
        trials=run.trials,
        dataset_hash=run.dataset_hash,
        created_at=run.created_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
        error=run.error,
        case_counts=counts,
    )


def case_row(
    case: EvaluationCase, scenario: EvaluationScenario, replay: EvaluationReplay
) -> dict[str, Any]:
    return {
        "id": case.id,
        "case_key": case.case_key,
        "scenario_name": scenario.name,
        "scenario_version": scenario.version,
        "family": scenario.family,
        "split": scenario.split,
        "synthetic": scenario.synthetic,
        "variant": case.variant,
        "trial": case.trial,
        "status": case.status,
        "replay_scope": replay.replay_scope,
        "investigation_id": case.investigation_id,
        "expected": ExpectedOutcome.model_validate(case.expected_outcome),
        "observed": (
            None
            if case.observed_outcome is None
            else ObservedOutcome.model_validate(case.observed_outcome)
        ),
        "scores": None if case.scores is None else CaseScores.model_validate(case.scores),
        "stage_scores": (
            None if case.stage_scores is None else StageScores.model_validate(case.stage_scores)
        ),
        "failure_stage": case.failure_stage,
        "failure_origin": case.failure_origin,
        "limit_hit": case.limit_hit,
        "error": case.error,
    }


async def run_detail(
    session: AsyncSession, run: EvaluationRun, *, recompute: bool = False
) -> EvaluationRunDetail:
    """A run's configuration, metrics, and every case; `recompute` rebuilds the metrics."""
    repository = EvaluationRepository(session)
    rows = (
        await session.execute(
            select(EvaluationCase, EvaluationScenario, EvaluationReplay)
            .join(EvaluationScenario, EvaluationScenario.id == EvaluationCase.scenario_id)
            .join(EvaluationReplay, EvaluationReplay.id == EvaluationCase.replay_id)
            .where(EvaluationCase.run_id == run.id)
            .order_by(EvaluationCase.case_key)
        )
    ).all()
    counts = await repository.case_counts(run.id)
    metrics = run.metrics
    if recompute and run.metrics is not None:
        metrics = summarize(
            run,
            [case for case, _, _ in rows],
            await repository.replays(run.id),
            await repository.audits(run.id),
        )
    return EvaluationRunDetail(
        **run_summary(run, counts).model_dump(),
        seed=run.seed,
        max_spend_microusd=run.max_spend_microusd,
        approval_ref=run.approval_ref,
        workflow_versions=run.workflow_versions,
        prompt_versions=run.prompt_versions,
        model_settings=run.model_config,
        pricing_config=run.pricing_config,
        limits=run.limits,
        metrics=metrics,
        cases=tuple(
            EvaluationCaseRow(**case_row(case, scenario, replay)) for case, scenario, replay in rows
        ),
    )


def git_revision() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def export_name(detail: EvaluationRunDetail) -> str:
    split = "heldout" if detail.split == "held_out" else "development"
    provider = str(detail.model_settings.get("provider", "unknown"))
    return f"{str(detail.id)[:8]}-{split}-{PROVIDER_SLUG.get(provider, 'model')}"


def export_documents(detail: EvaluationRunDetail, revision: str) -> tuple[str, str]:
    """The retained JSON report and its Markdown summary."""
    document = detail.model_dump(mode="json")
    if document["metrics"] is not None:
        document["metrics"]["reproducibility"] = {
            **document["metrics"].get("reproducibility", {}),
            "git_revision": revision,
            "workflow_versions": detail.workflow_versions,
            "prompt_versions": detail.prompt_versions,
            "model_settings": detail.model_settings,
            "pricing": detail.pricing_config,
            "limits": {k: v for k, v in detail.limits.items() if k != "scenarios"},
        }
    text = json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n"
    return text, markdown(detail, document.get("metrics"))


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _interval(value: Any) -> str:
    if not isinstance(value, dict) or value.get("rate") is None:
        return "—"
    low, high = value.get("ci95") or (None, None)
    bounds = "" if low is None else f" [{_pct(low)}, {_pct(high)}]"
    return f"{_pct(value['rate'])}{bounds} ({value['numerator']}/{value['denominator']})"


def markdown(detail: EvaluationRunDetail, metrics: dict[str, Any] | None) -> str:
    settings = detail.model_settings
    lines = [
        f"# Evaluation run {str(detail.id)[:8]}: {detail.label}",
        "",
        f"- Split: {detail.split}; catalog v{detail.limits.get('catalog_version', '1')}; "
        f"dataset `{detail.dataset_hash[:12]}`",
        f"- Provider: {settings.get('provider')} / {settings.get('model')}; measures model "
        f"quality: {settings.get('measures_model_quality')}",
        f"- Variants: {', '.join(detail.variants)}; trials per agent variant: {detail.trials}",
        f"- Status: {detail.status}",
    ]
    if settings.get("measures_model_quality") is False:
        lines += [
            "",
            "> Fake provider: these numbers show the harness works, not how well a model "
            "diagnoses.",
        ]
    if not metrics:
        return "\n".join([*lines, "", "No metrics were stored.", ""])
    lines += [
        "",
        "## Outcomes (Wilson 95% intervals)",
        "",
        "| Variant | Outcome accuracy | Root-cause accuracy | Evidence precision | "
        "Cost per correct (microUSD) |",
        "| --- | --- | --- | --- | --- |",
    ]
    variants = metrics.get("variants", {})
    for variant in detail.variants:
        summary = variants.get(variant, {})
        intervals = summary.get("intervals", {})
        lines.append(
            f"| {variant} | {_interval(intervals.get('outcome_accuracy'))} | "
            f"{_interval(intervals.get('root_cause_accuracy'))} | "
            f"{_interval(intervals.get('evidence_precision'))} | "
            f"{summary.get('cost_per_correct_microusd', '—')} |"
        )
    lines += ["", "## Paired comparisons", ""]
    for item in metrics.get("comparisons", []):
        accuracy = item.get("accuracy_difference") or {}
        low, high = accuracy.get("ci95") or (None, None)
        detectable = item.get("min_detectable_difference")
        lines.append(
            f"- {item['left']} minus {item['right']}: {_pct(accuracy.get('estimate'))} "
            f"[{_pct(low)}, {_pct(high)}] over {item['scenarios']} scenarios "
            f"({item['pairs']} pairs), {item['test']}"
            + (f", p = {item['mcnemar']['p_value']}" if item.get("mcnemar") else "")
            + f": **{item['verdict']}**. Smallest detectable difference: {_pct(detectable)}."
        )
    if not metrics.get("comparisons"):
        lines.append(f"- {NO_DIFFERENCE}: fewer than two variants.")
    lines += [
        "",
        "## First failing stage",
        "",
        "| Variant | Incorrect | Stages |",
        "| --- | --- | --- |",
    ]
    for variant in detail.variants:
        failures = variants.get(variant, {}).get("failure_stages", {})
        stages = ", ".join(
            f"{stage} {count}" for stage, count in failures.get("by_stage", {}).items() if count
        )
        lines.append(f"| {variant} | {failures.get('incorrect', '—')} | {stages or '—'} |")
    lines += ["", "## Calibration of root-cause confidence", ""]
    for variant in detail.variants:
        calibration = variants.get(variant, {}).get("calibration", {})
        if not calibration:
            continue
        status = "" if calibration.get("meaningful") else " (not meaningful on this provider)"
        lines.append(
            f"- {variant}: Brier {calibration.get('brier', '—')} over "
            f"{calibration.get('samples', 0)} asserted causes{status}"
        )
    reproducibility = metrics.get("reproducibility", {})
    lines += [
        "",
        "## Reproducibility",
        "",
        f"- Git revision at export: `{reproducibility.get('git_revision', 'unknown')}`",
        f"- Bootstrap: seed {reproducibility.get('bootstrap', {}).get('seed')}, "
        f"{reproducibility.get('bootstrap', {}).get('resamples')} resamples",
        "",
    ]
    return "\n".join(lines)
