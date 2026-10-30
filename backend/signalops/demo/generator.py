"""Deterministic synthetic telemetry for the demo workspace.

`generate` turns a scenario, seed, start time, and duration into source records shaped
like ingestion batch items (`source_event_id`, `occurred_at`, `payload`) for the seeded
demo sources, plus the scenario's labels. Records never identify the scenario or the
fault: labels are evaluator truth and are returned and written separately.

Randomness comes only from `random.Random.random` and `getrandbits`, whose output for a
seed is stable across CPython versions. Every payload is built through its adapter
contract, so generated records are valid adapter input.
"""

import hashlib
import json
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pydantic import JsonValue

from signalops.contracts.adapters import (
    AdapterInput,
    DeploymentInput,
    StructuredLogInput,
    TraceSummaryInput,
)
from signalops.contracts.common import ContractModel, UtcDatetime
from signalops.contracts.evidence import Severity, SourceKind
from signalops.persistence.seed import DEMO_ENVIRONMENT, DEMO_SOURCES

GENERATOR_VERSION = 1
DEFAULT_START = datetime(2026, 10, 16, 10, 30, tzinfo=UTC)
DEFAULT_DURATION = timedelta(minutes=10)
MIN_DURATION = timedelta(minutes=1)
MAX_DURATION = timedelta(hours=6)

# Fraction of the run before the fault starts and before it clears.
FAULT_START_FRACTION = 0.4
FAULT_END_FRACTION = 0.8
PAYMENT_TIMEOUT_MS = 2000
PAYMENT_POOL_SIZE = 50

SOURCE_IDS = {source.kind: source.id for source in DEMO_SOURCES}
ID_PREFIX = {
    SourceKind.STRUCTURED_LOG: "log",
    SourceKind.TRACE_SUMMARY: "span",
    SourceKind.DEPLOYMENT: "deploy",
}


class Scenario(StrEnum):
    HEALTHY = "healthy"
    DOWNSTREAM_TIMEOUT = "downstream-timeout"


@dataclass(frozen=True)
class DemoRecord:
    source_id: uuid.UUID
    source_event_id: str
    occurred_at: datetime
    payload: dict[str, JsonValue]

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "source_id": str(self.source_id),
            "source_event_id": self.source_event_id,
            "occurred_at": format_timestamp(self.occurred_at),
            "payload": self.payload,
        }


class ScenarioLabels(ContractModel):
    """Evaluator truth for one generated dataset. Never sent to the runtime."""

    scenario: Scenario
    generator_version: int
    seed: int
    start: UtcDatetime
    end: UtcDatetime
    synthetic: bool = True
    expected_incident: bool
    fault_start: UtcDatetime | None
    fault_end: UtcDatetime | None
    root_cause_service: str | None
    affected_services: tuple[str, ...]
    record_count: int
    # SHA-256 of `serialize_records(records)`, the JSON Lines file the CLI writes.
    dataset_hash: str


@dataclass(frozen=True)
class DemoDataset:
    records: tuple[DemoRecord, ...]
    labels: ScenarioLabels


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def serialize_records(records: tuple[DemoRecord, ...]) -> bytes:
    lines = (
        json.dumps(record.to_json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        for record in records
    )
    return "".join(f"{line}\n" for line in lines).encode()


def generate(
    scenario: Scenario,
    *,
    seed: int,
    start: datetime = DEFAULT_START,
    duration: timedelta = DEFAULT_DURATION,
) -> DemoDataset:
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")
    if not MIN_DURATION <= duration <= MAX_DURATION:
        raise ValueError(f"duration must be between {MIN_DURATION} and {MAX_DURATION}")
    start = start.astimezone(UTC)
    end = start + duration
    fault: tuple[datetime, datetime] | None = None
    if scenario is Scenario.DOWNSTREAM_TIMEOUT:
        fault = (start + duration * FAULT_START_FRACTION, start + duration * FAULT_END_FRACTION)

    builder = _Builder(scenario, seed, fault)
    builder.deployment(
        start + duration * 0.15,
        "inventory",
        revision="inventory-2026.10.16-2",
        previous_revision="inventory-2026.10.15-4",
        change_summary="Raise stock cache TTL from 30 s to 60 s",
    )
    at = start + builder.seconds(0.5, 2.0)
    while at < end:
        builder.request(at)
        at += builder.seconds(1.5, 4.5)
    at = start + builder.seconds(5.0, 20.0)
    while at < end:
        builder.background_noise(at)
        at += builder.seconds(15.0, 40.0)

    records = tuple(
        sorted(
            (record for record in builder.records if record.occurred_at < end),
            key=lambda record: (record.occurred_at, record.source_event_id),
        )
    )
    labels = ScenarioLabels(
        scenario=scenario,
        generator_version=GENERATOR_VERSION,
        seed=seed,
        start=start,
        end=end,
        expected_incident=fault is not None,
        fault_start=fault[0] if fault else None,
        fault_end=fault[1] if fault else None,
        root_cause_service="bank-gateway" if fault else None,
        affected_services=("bank-gateway", "checkout", "payment") if fault else (),
        record_count=len(records),
        dataset_hash=hashlib.sha256(serialize_records(records)).hexdigest(),
    )
    return DemoDataset(records, labels)


class _Builder:
    def __init__(
        self, scenario: Scenario, seed: int, fault: tuple[datetime, datetime] | None
    ) -> None:
        self._rng = random.Random(seed)  # noqa: S311 - reproducible synthetic data, not a secret
        self._id_prefix = f"{scenario}:{seed}"
        self._fault = fault
        self._emitted = 0
        self.records: list[DemoRecord] = []

    def seconds(self, low: float, high: float) -> timedelta:
        return timedelta(milliseconds=round(self.between(low * 1000, high * 1000)))

    def between(self, low: float, high: float) -> float:
        return low + (high - low) * self._rng.random()

    def chance(self, probability: float) -> bool:
        return self._rng.random() < probability

    def pick[T](self, options: tuple[T, ...]) -> T:
        return options[int(self._rng.random() * len(options))]

    def in_fault(self, at: datetime) -> bool:
        return self._fault is not None and self._fault[0] <= at < self._fault[1]

    def emit(self, kind: SourceKind, service: str, at: datetime, payload: AdapterInput) -> None:
        # Hashing the scenario into the ID keeps IDs unique across datasets without
        # revealing the scenario in the record.
        digest = hashlib.sha256(f"{self._id_prefix}:{self._emitted}".encode()).hexdigest()[:20]
        self._emitted += 1
        self.records.append(
            DemoRecord(
                source_id=SOURCE_IDS[kind],
                source_event_id=f"{service}-{ID_PREFIX[kind]}-{digest}",
                occurred_at=at,
                payload=payload.model_dump(mode="json", exclude_none=True),
            )
        )

    def log(
        self,
        at: datetime,
        service: str,
        level: Severity,
        message: str,
        attributes: dict[str, JsonValue],
        *,
        trace_id: str | None = None,
    ) -> None:
        payload = StructuredLogInput(
            service=service,
            environment=DEMO_ENVIRONMENT,
            level=level,
            message=message,
            trace_id=trace_id,
            span_id=self._hex(64) if trace_id else None,
            attributes=attributes,
        )
        self.emit(SourceKind.STRUCTURED_LOG, service, at, payload)

    def span(
        self,
        end: datetime,
        service: str,
        trace_id: str,
        operation: str,
        duration_ms: int,
        *,
        error: bool = False,
        span_count: tuple[int, int],
        downstream: tuple[str, ...] = (),
    ) -> None:
        spans = int(self.between(span_count[0], span_count[1] + 1))
        payload = TraceSummaryInput(
            trace_id=trace_id,
            service=service,
            environment=DEMO_ENVIRONMENT,
            operation=operation,
            duration_ms=duration_ms,
            status="error" if error else "ok",
            span_count=spans,
            error_span_count=min(spans, 2) if error else 0,
            downstream_services=downstream,
        )
        self.emit(SourceKind.TRACE_SUMMARY, service, end, payload)

    def deployment(
        self,
        at: datetime,
        service: str,
        *,
        revision: str,
        previous_revision: str,
        change_summary: str,
    ) -> None:
        payload = DeploymentInput(
            service=service,
            environment=DEMO_ENVIRONMENT,
            revision=revision,
            previous_revision=previous_revision,
            change_summary=change_summary,
            attributes={"pipeline": "demo-cd", "commit": self._hex(28)},
        )
        self.emit(SourceKind.DEPLOYMENT, service, at, payload)

    def request(self, at: datetime) -> None:
        """One checkout: inventory, then the orders database, then payment via bank-gateway."""
        trace_id = self._hex(128)
        faulty = self.in_fault(at)

        inventory_ms = round(self.between(15, 60))
        sku_missing = self.chance(0.01)
        inventory_end = at + timedelta(milliseconds=inventory_ms)
        self.span(
            inventory_end,
            "inventory",
            trace_id,
            "GET /stock/{sku}",
            inventory_ms,
            error=sku_missing,
            span_count=(3, 6),
        )
        if sku_missing:
            self.log(
                inventory_end,
                "inventory",
                Severity.ERROR,
                "sku lookup failed: not found",
                {"http.route": "/stock/{sku}", "http.status_code": 404},
                trace_id=trace_id,
            )

        slow_query = self.chance(0.03)
        query_ms = round(self.between(180, 420) if slow_query else self.between(4, 25))
        query_end = inventory_end + timedelta(milliseconds=query_ms)
        if slow_query:
            self.log(
                query_end,
                "orders-db",
                Severity.WARNING,
                f"slow query: duration {query_ms} ms exceeds 150 ms threshold",
                {"db.system": "postgresql", "db.table": "orders", "db.operation": "SELECT"},
                trace_id=trace_id,
            )

        # During the fault most bank-gateway calls outlast payment's timeout and the
        # rest are slow; outside it a rare isolated timeout is ordinary noise.
        if faulty:
            gateway_ms = round(
                self.between(2300, 4000) if self.chance(0.75) else self.between(600, 1800)
            )
        else:
            gateway_ms = round(
                self.between(2100, 2600) if self.chance(0.004) else self.between(60, 160)
            )
        timed_out = gateway_ms > PAYMENT_TIMEOUT_MS
        payment_start = query_end + timedelta(milliseconds=round(self.between(1, 5)))
        self.span(
            payment_start + timedelta(milliseconds=gateway_ms),
            "bank-gateway",
            trace_id,
            "POST /authorize",
            gateway_ms,
            span_count=(2, 3),
        )
        payment_ms = (PAYMENT_TIMEOUT_MS if timed_out else gateway_ms) + round(self.between(15, 40))
        payment_end = payment_start + timedelta(milliseconds=payment_ms)
        pool_active = round(self.between(35, PAYMENT_POOL_SIZE) if faulty else self.between(2, 12))
        pool: dict[str, JsonValue] = {
            "http.route": "/v1/charges",
            "pool.active": pool_active,
            "pool.max": PAYMENT_POOL_SIZE,
        }
        self.span(
            payment_end,
            "payment",
            trace_id,
            "POST /v1/charges",
            payment_ms,
            error=timed_out,
            span_count=(6, 9),
            downstream=("bank-gateway",),
        )
        if timed_out:
            self.log(
                payment_end,
                "payment",
                Severity.ERROR,
                f"upstream bank-gateway call timed out after {PAYMENT_TIMEOUT_MS} ms",
                pool,
                trace_id=trace_id,
            )
        elif self.chance(0.3):
            self.log(
                payment_end, "payment", Severity.INFO, "charge authorized", pool, trace_id=trace_id
            )

        checkout_end = payment_end + timedelta(milliseconds=round(self.between(10, 30)))
        checkout_ms = round((checkout_end - at) / timedelta(milliseconds=1))
        self.span(
            checkout_end,
            "checkout",
            trace_id,
            "POST /checkout",
            checkout_ms,
            error=timed_out or sku_missing,
            span_count=(12, 16),
            downstream=("inventory", "orders-db", "payment"),
        )
        if timed_out:
            self.log(
                checkout_end,
                "checkout",
                Severity.ERROR,
                "payment authorization failed: upstream timeout",
                {"http.route": "/checkout", "http.status_code": 502},
                trace_id=trace_id,
            )
        elif not sku_missing and self.chance(0.2):
            self.log(
                checkout_end,
                "checkout",
                Severity.INFO,
                "order placed",
                {"http.route": "/checkout", "http.status_code": 201},
                trace_id=trace_id,
            )

    def background_noise(self, at: datetime) -> None:
        noise: tuple[Callable[[], None], ...] = (
            lambda: self.log(
                at,
                "checkout",
                Severity.WARNING,
                "feature flag service slow to respond; using cached flags",
                {"flags.age_s": round(self.between(30, 300))},
            ),
            lambda: self.log(
                at,
                "inventory",
                Severity.WARNING,
                "stock cache hit ratio below 80%",
                {"cache.hit_ratio": round(self.between(0.6, 0.79), 2)},
            ),
            lambda: self.log(
                at,
                "payment",
                Severity.WARNING,
                f"GC pause of {round(self.between(80, 240))} ms",
                {"runtime": "jvm"},
            ),
            lambda: self.log(
                at,
                "orders-db",
                Severity.INFO,
                "autovacuum: processing table orders",
                {"db.system": "postgresql", "db.table": "orders"},
            ),
            lambda: self.log(
                at,
                "checkout",
                Severity.DEBUG,
                "health check ok",
                {"http.route": "/healthz"},
            ),
            lambda: self.log(
                at,
                "checkout",
                Severity.WARNING,
                "retrying idempotent read after connection reset (attempt 1)",
                {"peer.service": "inventory"},
            ),
        )
        self.pick(noise)()

    def _hex(self, bits: int) -> str:
        return f"{self._rng.getrandbits(bits):0{bits // 4}x}"
