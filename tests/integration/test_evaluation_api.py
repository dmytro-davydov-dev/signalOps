"""Evaluation API routes (P10-T06) against Compose PostgreSQL.

A run is queued through the API, executed by the runner with an in-process
replay and worker (fake provider), then read back: aggregates, every case, and
the evidence of a failed case, where audits are recorded.
"""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine

from signalops.api.app import create_app
from signalops.config import Settings
from signalops.dependencies import Resources
from signalops.evaluation.runner import RunExecutor
from signalops.persistence import database as db
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.test_evaluation_runs import _context, worker  # noqa: F401 - fixture

pytestmark = pytest.mark.integration

TOKEN, OTHER_TOKEN = "operator-demo", "operator-other"
OTHER_WORKSPACE = uuid.UUID("00000000-0000-4000-8000-0000000000ee")


def _client(settings: Settings, *, evaluation: bool = True) -> TestClient:
    settings = settings.model_copy(
        update={
            "operator_tokens": {
                DEMO_WORKSPACE_ID: SecretStr(TOKEN),
                OTHER_WORKSPACE: SecretStr(OTHER_TOKEN),
            }
        }
    )

    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = db.create_engine(app_settings)
        database = db.Database.from_engine(engine)
        try:
            # The scratch owner stands in for the evaluator login here.
            yield Resources(
                checks=[],
                engine=engine,
                database=database,
                evaluation=database if evaluation else None,
            )
        finally:
            await engine.dispose()

    return TestClient(create_app(settings, resource_factory=resources))


def _headers(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def test_a_reviewer_reads_results_and_drills_into_a_failed_case(
    seeded: tuple[Settings, Engine],
    database: db.Database,
    worker: None,  # noqa: F811
) -> None:
    settings = seeded[0]
    body: dict[str, Any] = {
        "label": "api check",
        "split": "development",
        "variants": ["deterministic", "multi_agent"],
        "trials": 1,
        "scenarios": ["downstream-timeout/dev"],
    }
    with _client(settings) as client:
        assert client.post("/v1/evaluation-runs", json=body).status_code == 401
        unknown = client.post(
            "/v1/evaluation-runs", json={**body, "scenarios": ["nope"]}, headers=_headers()
        )
        assert unknown.status_code == 422
        queued = client.post("/v1/evaluation-runs", json=body, headers=_headers())
        assert queued.status_code == 202, queued.text
        run_id = queued.json()["id"]
        assert queued.json()["status"] == "queued"

    # The request handler only queued it; a runner executes it.
    assert (
        str(await RunExecutor(_context(database), owner="api-test").claim_and_execute()) == run_id
    )

    with _client(settings) as client:
        listed = client.get("/v1/evaluation-runs", headers=_headers()).json()["items"]
        assert run_id in [item["id"] for item in listed]
        assert (
            client.get("/v1/evaluation-runs", headers=_headers(OTHER_TOKEN)).json()["items"] == []
        )
        assert (
            client.get(f"/v1/evaluation-runs/{run_id}", headers=_headers(OTHER_TOKEN)).status_code
            == 404
        )

        detail = client.get(f"/v1/evaluation-runs/{run_id}", headers=_headers()).json()
        assert detail["status"] == "completed"
        assert detail["case_counts"] == {"scored": 2}
        assert detail["model_settings"]["measures_model_quality"] is False
        assert set(detail["metrics"]["variants"]) == {"deterministic", "multi_agent"}
        assert (
            detail["metrics"]["budget_per_case"]["budget_microusd"]
            == detail["limits"]["budget_microusd"]
        )
        cases = {case["variant"]: case for case in detail["cases"]}
        for case in cases.values():
            assert case["synthetic"] is True
            assert case["family"] == "downstream_timeout"
            assert case["expected"]["root_cause_service"] == "bank-gateway"
            assert case["expected"]["supporting_event_ids"] == []
        assert cases["multi_agent"]["scores"]["outcome_correct"] is True
        failed = cases["deterministic"]
        assert failed["scores"]["outcome_correct"] is False

        drill = client.get(
            f"/v1/evaluation-runs/{run_id}/cases/{failed['id']}", headers=_headers()
        ).json()
        assert drill["observed"]["root_cause_service"] != "bank-gateway"
        assert drill["replay_report"]["status"] == "completed"
        assert drill["replay_report"]["detection"]["true_positives"] == 1
        citations = drill["citations"]
        assert citations
        assert len(citations) == len(drill["observed"]["cited_signal_ids"])
        assert all(c["fault_evidence"] in (True, False) for c in citations)
        assert drill["scores"]["cited_supported"] == sum(c["fault_evidence"] for c in citations)

        signal_id = citations[0]["signal_id"]
        path = f"/v1/evaluation-runs/{run_id}/cases/{failed['id']}/audits"
        audit = {
            "signal_id": signal_id,
            "supported": False,
            "auditor": "reviewer-1",
            "note": "symptom",
        }
        recorded = client.post(path, json=audit, headers=_headers())
        assert recorded.status_code == 201, recorded.text
        # A later judgement by the same auditor replaces the first.
        client.post(path, json={**audit, "supported": True}, headers=_headers())
        stray = client.post(
            path, json={**audit, "signal_id": str(uuid.uuid4())}, headers=_headers()
        )
        assert stray.status_code == 422
        audits = client.get(
            f"/v1/evaluation-runs/{run_id}/cases/{failed['id']}", headers=_headers()
        ).json()["audits"]
        assert [(a["auditor"], a["supported"]) for a in audits] == [("reviewer-1", True)]
        missing = client.get(
            f"/v1/evaluation-runs/{run_id}/cases/{uuid.uuid4()}", headers=_headers()
        )
        assert missing.status_code == 404

    with _client(settings, evaluation=False) as client:
        assert client.get("/v1/evaluation-runs", headers=_headers()).status_code == 503
