"""The real instrumented shop (P10-T03): real HTTP calls, real timeouts, honest telemetry."""

import json
from datetime import timedelta

from signalops.contracts.adapters import ADAPTER_INPUTS
from signalops.contracts.evidence import SourceKind
from signalops.demo.generator import DemoRecord
from signalops.demo.shop import FaultSchedule, run_shop
from signalops.persistence.seed import DEMO_SOURCES

KINDS = {source.id: source.kind for source in DEMO_SOURCES}
# Words that would tell an investigator a fault was injected rather than observed.
INJECTION_WORDS = ("fault", "inject", "schedule", "scenario", "label", "synthetic", "expected")


async def test_a_real_slowdown_produces_real_timeouts_and_nothing_else() -> None:
    exported: list[DemoRecord] = []

    async def export(records: list[DemoRecord]) -> None:
        exported.extend(records)

    await run_shop(
        rate_per_second=6,
        run_for=timedelta(seconds=4),
        schedule=FaultSchedule(
            after=timedelta(seconds=1.5), duration=timedelta(seconds=5), share=1.0
        ),
        export=export,
        export_interval=timedelta(seconds=1),
        seed=3,
    )

    for record in exported:
        ADAPTER_INPUTS[KINDS[record.source_id]].model_validate(record.payload)
    assert len({record.source_event_id for record in exported}) == len(exported)
    by_kind = {
        kind: [r.payload for r in exported if KINDS[r.source_id] is kind] for kind in SourceKind
    }
    assert {p["service"] for p in by_kind[SourceKind.DEPLOYMENT]} == {
        "checkout",
        "payment",
        "bank-gateway",
    }
    spans = by_kind[SourceKind.TRACE_SUMMARY]
    payment = [p for p in spans if p["service"] == "payment"]
    assert any(p["status"] == "ok" for p in payment)
    failed = [p for p in payment if p["status"] == "error"]
    assert failed
    # The client really waited for its 2-second timeout.
    assert all(int(str(p["duration_ms"])) >= 1_900 for p in failed)
    logs = by_kind[SourceKind.STRUCTURED_LOG]
    assert any(
        p["service"] == "payment" and "timed out after 2000 ms" in str(p["message"]) for p in logs
    )
    assert any(p["service"] == "checkout" and p["level"] == "error" for p in logs)
    # Logs carry the trace they belong to, and bank-gateway's own spans look healthy:
    # it is only slow, as a real dependency would be.
    traced = {p["trace_id"] for p in spans}
    assert all(p["trace_id"] in traced for p in logs if "trace_id" in p)
    assert all(p["status"] == "ok" for p in spans if p["service"] == "bank-gateway")
    text = json.dumps([record.to_json() for record in exported]).lower()
    for word in INJECTION_WORDS:
        assert word not in text, word
