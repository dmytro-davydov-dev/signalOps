"""The regression gate on the fake provider (P13-T07).

The development suite runs in process and is compared with the committed baseline.
Mutations of fake roles, a handoff check, and redaction each fail the gate against a
baseline taken from the same subset unmutated, naming the cases that moved.

Regenerate the committed baseline (then review its diff) with
`SIGNALOPS_WRITE_BASELINE=1 SIGNALOPS_INTEGRATION=1 uv run pytest
tests/integration/test_regression_gate.py -k development`.
"""

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from signalops.adapters.ingestion import TextRedactor
from signalops.agents import multi_agent
from signalops.contracts.evaluation import ScenarioSplit, SystemVariant
from signalops.evaluation.catalog import CATALOG_V2, entries
from signalops.evaluation.export import export_documents, run_detail
from signalops.evaluation.gate import GateStatus, baseline_of, baseline_text, compare
from signalops.gateway.fake import FakeProvider
from signalops.gateway.fake_roles import synthesizer_step, telemetry_step
from signalops.persistence import database as db
from signalops.persistence.models import EvaluationRun
from tests.integration.evaluation_support import engine_for, run_with
from tests.integration.multi_agent_support import roles_with
from tests.integration.test_stage_scores import edited

pytestmark = pytest.mark.integration

BASELINE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "evaluation"
    / "v1"
    / "baseline-fake-development.json"
)
SUBSET = ("downstream-timeout/dev", "adversarial/dev-1")


async def report_of(database: db.Database, run: EvaluationRun) -> dict[str, Any]:
    async with database.transaction() as session:
        detail = await run_detail(session, run, recompute=True)
    text, _ = export_documents(detail, "test")
    loaded: dict[str, Any] = json.loads(text)
    return loaded


async def test_the_development_suite_matches_the_committed_baseline(
    database: db.Database,
) -> None:
    names = tuple(entry.name for entry in entries(ScenarioSplit.DEVELOPMENT, CATALOG_V2))
    run, _ = await run_with(
        database,
        engine_for(database, FakeProvider(responder=roles_with())),
        scenarios=names,
        variants=tuple(SystemVariant),
        label="regression gate",
    )
    assert run.status == "completed", run.error
    report = await report_of(database, run)
    if os.environ.get("SIGNALOPS_WRITE_BASELINE") == "1":
        BASELINE.write_text(baseline_text(report))
    result = compare(report, json.loads(BASELINE.read_text()))
    assert result.status is GateStatus.PASSED, result.text()


def drop_top_finding(report: dict[str, Any]) -> None:
    if report.get("findings"):
        report["findings"] = report["findings"][1:]
    if not report.get("findings"):
        report["missing_evidence"] = report.get("missing_evidence") or ["Dropped by a mutation"]


def cite_unread_signal(report: dict[str, Any]) -> None:
    for hypothesis in report.get("hypotheses", [])[:1]:
        hypothesis["evidence_signal_ids"].append(str(uuid.uuid4()))


async def subset_report(database: db.Database, **engine: Any) -> dict[str, Any]:
    run, _ = await run_with(
        database,
        engine_for(database, **engine),
        scenarios=SUBSET,
        variants=(SystemVariant.MULTI_AGENT,),
        label="gate mutation",
    )
    assert run.status == "completed", run.error
    return await report_of(database, run)


async def test_each_mutation_fails_the_gate_and_names_the_moved_cases(
    database: db.Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    clean = FakeProvider(responder=roles_with())
    baseline = baseline_of(await subset_report(database, provider=clean))
    # An unchanged tree passes: the fake provider is deterministic.
    again = await subset_report(database, provider=FakeProvider(responder=roles_with()))
    assert compare(again, baseline).status is GateStatus.PASSED

    # 1. The telemetry role drops its top finding: outcomes and stages move.
    dropped = await subset_report(
        database,
        provider=FakeProvider(
            responder=roles_with({"telemetry-v1": edited(telemetry_step, drop_top_finding)})
        ),
    )
    result = compare(dropped, baseline)
    assert result.status is GateStatus.REGRESSED
    assert set(result.moved_cases) == {f"{name}@2/multi_agent/1" for name in SUBSET}

    # 2. The synthesis citation check is disabled and the synthesizer cites a signal no
    # specialist read: the stored violation fails the invariant.
    monkeypatch.setattr(multi_agent, "check_synthesis", lambda _synthesis, _findings: None)
    unchecked = await subset_report(
        database,
        provider=FakeProvider(
            responder=roles_with({"synthesizer-v1": edited(synthesizer_step, cite_unread_signal)})
        ),
    )
    monkeypatch.undo()
    result = compare(unchecked, baseline)
    assert result.status is GateStatus.REGRESSED
    assert "synthesis invariant violations" in result.text()
    assert set(result.moved_cases) >= {f"{name}@2/multi_agent/1" for name in SUBSET}

    # 3. Password and API key redaction is removed: the planted secret reaches a stored
    # claim, and only the adversarial case moves.
    leaky = await subset_report(
        database,
        provider=FakeProvider(responder=roles_with()),
        redactor=TextRedactor(("token",)),
    )
    result = compare(leaky, baseline)
    assert result.status is GateStatus.REGRESSED
    assert "unredacted secrets" in result.text()
    assert result.moved_cases == ["adversarial/dev-1@2/multi_agent/1"]
