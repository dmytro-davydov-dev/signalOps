"""Report v2 export of a stored run (P13-T05): recomputed metrics, identical twice."""

import json

import pytest

from signalops.evaluation.export import export_documents, export_name, run_detail
from signalops.evaluation.report import REPORT_VERSION
from signalops.gateway.fake import FakeProvider
from signalops.persistence import database as db
from tests.integration.evaluation_support import engine_for, run_with
from tests.integration.multi_agent_support import roles_with

pytestmark = pytest.mark.integration


async def test_exporting_a_run_twice_gives_identical_files(database: db.Database) -> None:
    run, _ = await run_with(
        database,
        engine_for(database, FakeProvider(responder=roles_with())),
        scenarios=("downstream-timeout/dev", "unexplained-errors/dev"),
        label="export check",
    )
    documents = []
    for _ in range(2):
        async with database.transaction() as session:
            detail = await run_detail(session, run, recompute=True)
        documents.append(export_documents(detail, "test-revision"))
    assert documents[0] == documents[1]
    report = json.loads(documents[0][0])
    metrics = report["metrics"]
    assert metrics["report_version"] == REPORT_VERSION
    assert metrics["reproducibility"]["git_revision"] == "test-revision"
    assert metrics["reproducibility"]["catalog_version"] == "2"
    assert metrics["reproducibility"]["bootstrap"] == {
        "seed": 0,
        "resamples": 10_000,
        "interval": "percentile 95%",
    }
    (comparison,) = metrics["comparisons"]
    assert (comparison["left"], comparison["right"]) == ("single_agent", "multi_agent")
    assert comparison["test"] == "exact McNemar"
    assert comparison["verdict"] == "no detectable difference"
    multi = metrics["variants"]["multi_agent"]
    assert multi["intervals"]["outcome_accuracy"]["ci95"] is not None
    assert multi["calibration"]["meaningful"] is False
    assert multi["calibration"]["samples"] >= 1
    assert export_name(detail).endswith("-development-fake")
    assert "Paired comparisons" in documents[0][1]
