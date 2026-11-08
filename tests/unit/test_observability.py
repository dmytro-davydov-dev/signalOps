"""Metrics, correlation fields, request IDs, and the snapshot exporter (P11-T01)."""

import json
import logging
import uuid
from datetime import UTC, datetime

import httpx2 as httpx
import pytest
from fastapi import FastAPI
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest

from signalops.api.request_ids import RequestIdMiddleware
from signalops.logs import JsonFormatter
from signalops.observability.context import bind
from signalops.observability.exporter import Exporter
from signalops.observability.snapshot import DatabaseState, OperationsSnapshot, PartitionLag
from signalops.streaming.consumer import ProcessOutcome
from tests.unit.test_consumer import GROUP, FakeRecord, _envelope_bytes, _harness


def _line(message: str = "hello", **extra: object) -> dict[str, object]:
    record = logging.makeLogRecord({"name": "t", "levelname": "INFO", "msg": message, **extra})
    return json.loads(JsonFormatter().format(record))  # type: ignore[no-any-return]


def test_bound_fields_reach_log_lines_and_end_with_the_block() -> None:
    investigation = uuid.uuid4()
    with bind(investigation_id=investigation, job_id=None):
        with bind(step="plan"):
            inner = _line()
        outer = _line()
    after = _line()
    assert inner["investigation_id"] == str(investigation)
    assert inner["step"] == "plan"
    assert "job_id" not in inner
    assert outer["investigation_id"] == str(investigation)
    assert "step" not in outer
    assert "investigation_id" not in after


def test_explicit_extra_fields_win_over_bound_ones() -> None:
    with bind(job_id="bound"):
        line = _line(job_id="explicit")
    assert line["job_id"] == "explicit"


def _sample(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_consumer_counts_outcomes_and_processing_time() -> None:
    labels = {"consumer_group": GROUP, "outcome": "duplicate"}
    before = _sample("signalops_consumer_records_total", labels)
    observed = _sample("signalops_consumer_processing_seconds_count", {"consumer_group": GROUP})
    h = _harness([FakeRecord(1, _envelope_bytes())], outcome=ProcessOutcome.DUPLICATE)
    assert await h.runner.run_once() is ProcessOutcome.DUPLICATE
    assert _sample("signalops_consumer_records_total", labels) == before + 1
    assert (
        _sample("signalops_consumer_processing_seconds_count", {"consumer_group": GROUP})
        == observed + 1
    )


async def test_scheduled_retries_are_counted_but_not_timed() -> None:
    labels = {"consumer_group": GROUP, "outcome": "retry_scheduled"}
    before = _sample("signalops_consumer_records_total", labels)
    observed = _sample("signalops_consumer_processing_seconds_count", {"consumer_group": GROUP})
    h = _harness([FakeRecord(1, _envelope_bytes())], errors=[RuntimeError("db down")])
    assert await h.runner.run_once() is ProcessOutcome.RETRY_SCHEDULED
    assert _sample("signalops_consumer_records_total", labels) == before + 1
    assert (
        _sample("signalops_consumer_processing_seconds_count", {"consumer_group": GROUP})
        == observed
    )


def _app(seen: list[dict[str, object]]) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)

    @app.get("/x")
    async def handler() -> dict[str, str]:
        seen.append(_line("inside"))
        return {"ok": "yes"}

    return app


@pytest.mark.parametrize(
    ("presented", "kept"),
    [("client-42.a:b", True), (None, False), ("bad id with spaces", False), ("x" * 65, False)],
)
async def test_request_ids_are_kept_when_well_formed_and_logged(
    presented: str | None, kept: bool
) -> None:
    seen: list[dict[str, object]] = []
    transport = httpx.ASGITransport(app=_app(seen))
    headers = {"X-Request-ID": presented} if presented is not None else {}
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/x", headers=headers)
    returned = response.headers["x-request-id"]
    assert (returned == presented) is kept
    assert seen[0]["request_id"] == returned


def _snapshot(lag: list[PartitionLag] | None) -> OperationsSnapshot:
    return OperationsSnapshot(
        taken_at=datetime.now(UTC),
        database=DatabaseState(
            outbox_pending=3,
            outbox_quarantined=1,
            outbox_oldest_age_seconds=12.5,
            jobs={"queued": 2, "running": 1, "succeeded": 7, "failed": 0, "cancelled": 0},
            jobs_ready=2,
            jobs_ready_oldest_age_seconds=4.0,
            jobs_expired_leases=1,
            investigations={"running": 1, "completed": 6},
            model_calls_in_flight=1,
            model_calls_unknown=2,
            model_unknown_reserved_microusd=311_140,
        ),
        kafka_lag=lag,
    )


async def test_exporter_serves_the_latest_snapshot_and_keeps_it_after_a_failure() -> None:
    registry = CollectorRegistry()
    snapshots: list[OperationsSnapshot | Exception] = [
        _snapshot(
            [
                PartitionLag(
                    consumer_group="detector-v1",
                    topic="t",
                    partition=0,
                    lag=40,
                    committed=10,
                    end=50,
                )
            ]
        ),
        RuntimeError("database down"),
    ]

    async def read() -> OperationsSnapshot:
        item = snapshots.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    exporter = Exporter(read, registry, interval_seconds=60)
    assert generate_latest(registry).count(b"signalops_jobs{") == 0
    assert await exporter.refresh()
    text = generate_latest(registry).decode()
    assert 'signalops_jobs{status="queued"} 2.0' in text
    assert "signalops_jobs_expired_leases 1.0" in text
    assert "signalops_outbox_oldest_age_seconds 12.5" in text
    assert "signalops_model_unknown_reserved_microusd 311140.0" in text
    assert (
        'signalops_kafka_consumer_lag{consumer_group="detector-v1",partition="0",topic="t"} 40.0'
        in text
    )
    assert not await exporter.refresh()
    text = generate_latest(registry).decode()
    assert "signalops_exporter_refresh_failures_total 1.0" in text
    assert 'signalops_jobs{status="queued"} 2.0' in text


async def test_unavailable_lag_is_omitted_rather_than_reported_as_zero() -> None:
    registry = CollectorRegistry()

    async def read() -> OperationsSnapshot:
        return _snapshot(None)

    exporter = Exporter(read, registry, interval_seconds=60)
    assert await exporter.refresh()
    assert b"signalops_kafka_consumer_lag" not in generate_latest(registry)
