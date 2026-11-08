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

from pydantic import Field, JsonValue

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

PAYMENT_TIMEOUT_MS = 2000
PAYMENT_POOL_SIZE = 50
# How long payment waits for a pooled connection before giving up.
POOL_WAIT_MS = 1000

SOURCE_IDS = {source.kind: source.id for source in DEMO_SOURCES}
ID_PREFIX = {
    SourceKind.STRUCTURED_LOG: "log",
    SourceKind.TRACE_SUMMARY: "span",
    SourceKind.DEPLOYMENT: "deploy",
}


class Scenario(StrEnum):
    HEALTHY = "healthy"
    DOWNSTREAM_TIMEOUT = "downstream-timeout"
    # payment's database connection pool runs dry; calls fail fast after a wait.
    POOL_EXHAUSTION = "pool-exhaustion"
    # A deployment to `FaultVariant.target` starts failing a share of its requests.
    DEPLOYMENT_REGRESSION = "deployment-regression"
    # payment's heap fills: long GC pauses, slow calls, then out-of-memory errors.
    MEMORY_PRESSURE = "memory-pressure"
    # The downstream timeout with an unrelated checkout deployment at its start.
    MIXED_TIMEOUT_DEPLOY = "mixed-timeout-deploy"
    # checkout fails requests before calling anything, and nothing says why.
    UNEXPLAINED_ERRORS = "unexplained-errors"


@dataclass(frozen=True)
class FaultVariant:
    """Parameters that vary between development and held-out variants of a scenario."""

    start_fraction: float = 0.4
    end_fraction: float = 0.8
    # Share of requests inside the window the fault breaks.
    intensity: float = 0.75
    # The faulty service, for scenarios where it can vary (deployment regression).
    target: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.start_fraction < self.end_fraction <= 1:
            raise ValueError("fault fractions must satisfy 0 <= start < end <= 1")
        if not 0 < self.intensity <= 1:
            raise ValueError("intensity must be in (0, 1]")


DEFAULT_VARIANT = FaultVariant()
TIMEOUT_SCENARIOS = frozenset({Scenario.DOWNSTREAM_TIMEOUT, Scenario.MIXED_TIMEOUT_DEPLOY})
REGRESSION_TARGETS = ("inventory", "payment")


@dataclass(frozen=True)
class GroundTruth:
    root_cause_service: str | None
    failure_type: str | None
    affected_services: tuple[str, ...]


def ground_truth(scenario: Scenario, variant: FaultVariant) -> GroundTruth | None:
    """What a correct diagnosis says; None for healthy traffic."""
    match scenario:
        case Scenario.HEALTHY:
            return None
        case Scenario.DOWNSTREAM_TIMEOUT | Scenario.MIXED_TIMEOUT_DEPLOY:
            return GroundTruth(
                "bank-gateway", "downstream_timeout", ("bank-gateway", "checkout", "payment")
            )
        case Scenario.POOL_EXHAUSTION:
            return GroundTruth("payment", "connection_pool_exhaustion", ("checkout", "payment"))
        case Scenario.DEPLOYMENT_REGRESSION:
            target = variant.target or "inventory"
            return GroundTruth(target, "deployment_regression", tuple(sorted({target, "checkout"})))
        case Scenario.MEMORY_PRESSURE:
            return GroundTruth("payment", "memory_pressure", ("checkout", "payment"))
        case Scenario.UNEXPLAINED_ERRORS:
            # Symptoms without a cause in the evidence: abstaining is correct.
            return GroundTruth(None, None, ("checkout",))


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
    failure_type: str | None = None
    # False when the evidence cannot support a cause, so abstaining is correct.
    diagnosable: bool = False
    affected_services: tuple[str, ...]
    # The fault's parameters; defaults for both P04-T01 datasets.
    variant: dict[str, JsonValue] = Field(default_factory=dict)
    # `source_event_id`s of the records the fault produced, in record order.
    supporting_event_ids: tuple[str, ...] = ()
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
    variant: FaultVariant = DEFAULT_VARIANT,
) -> DemoDataset:
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")
    if not MIN_DURATION <= duration <= MAX_DURATION:
        raise ValueError(f"duration must be between {MIN_DURATION} and {MAX_DURATION}")
    if variant.target is not None and (
        scenario is not Scenario.DEPLOYMENT_REGRESSION or variant.target not in REGRESSION_TARGETS
    ):
        raise ValueError(f"target applies to deployment regression only: {REGRESSION_TARGETS}")
    start = start.astimezone(UTC)
    end = start + duration
    truth = ground_truth(scenario, variant)
    fault: tuple[datetime, datetime] | None = None
    if truth is not None:
        fault = (start + duration * variant.start_fraction, start + duration * variant.end_fraction)

    builder = _Builder(scenario, seed, fault, variant)
    builder.deployment(
        start + duration * 0.15,
        "inventory",
        revision="inventory-2026.10.16-2",
        previous_revision="inventory-2026.10.15-4",
        change_summary="Raise stock cache TTL from 30 s to 60 s",
    )
    if fault is not None and scenario is Scenario.DEPLOYMENT_REGRESSION:
        target = variant.target or "inventory"
        builder.deployment(
            fault[0],
            target,
            revision=f"{target}-2026.10.16-3",
            previous_revision=f"{target}-2026.10.16-1",
            change_summary=REGRESSION_CHANGES[target],
            supporting=True,
        )
    if fault is not None and scenario is Scenario.MIXED_TIMEOUT_DEPLOY:
        builder.deployment(
            fault[0] + timedelta(seconds=20),
            "checkout",
            revision="checkout-2026.10.16-5",
            previous_revision="checkout-2026.10.16-4",
            change_summary="Update order confirmation email template",
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
    kept = {record.source_event_id for record in records}
    labels = ScenarioLabels(
        scenario=scenario,
        generator_version=GENERATOR_VERSION,
        seed=seed,
        start=start,
        end=end,
        expected_incident=fault is not None,
        fault_start=fault[0] if fault else None,
        fault_end=fault[1] if fault else None,
        root_cause_service=truth.root_cause_service if truth else None,
        failure_type=truth.failure_type if truth else None,
        diagnosable=truth is not None and truth.root_cause_service is not None,
        affected_services=truth.affected_services if truth else (),
        variant=_variant_json(variant),
        supporting_event_ids=tuple(event_id for event_id in builder.supporting if event_id in kept),
        record_count=len(records),
        dataset_hash=hashlib.sha256(serialize_records(records)).hexdigest(),
    )
    return DemoDataset(records, labels)


REGRESSION_CHANGES = {
    "inventory": "Move stock reservation to the new warehouse schema",
    "payment": "Replace the charge request mapper",
}


def _variant_json(variant: FaultVariant) -> dict[str, JsonValue]:
    if variant == DEFAULT_VARIANT:
        return {}
    return {
        "start_fraction": variant.start_fraction,
        "end_fraction": variant.end_fraction,
        "intensity": variant.intensity,
        "target": variant.target,
    }


class _Builder:
    def __init__(
        self,
        scenario: Scenario,
        seed: int,
        fault: tuple[datetime, datetime] | None,
        variant: FaultVariant = DEFAULT_VARIANT,
    ) -> None:
        self._rng = random.Random(seed)  # noqa: S311 - reproducible synthetic data, not a secret
        self._id_prefix = f"{scenario}:{seed}"
        self._scenario = scenario
        self._fault = fault
        self._variant = variant
        # The service a deployment regression breaks.
        self._target = (
            (variant.target or "inventory") if scenario is Scenario.DEPLOYMENT_REGRESSION else None
        )
        self._emitted = 0
        self.records: list[DemoRecord] = []
        # IDs of records the fault produced: evaluator truth, never written into a record.
        self.supporting: list[str] = []

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

    def breaks(self, scenario: Scenario, at: datetime, share: float = 1.0) -> bool:
        """Whether this scenario's fault breaks a request at this moment.

        Draws randomness only inside this scenario's fault window, so the other
        scenarios' datasets stay byte for byte as they were.
        """
        if self._scenario is not scenario or not self.in_fault(at):
            return False
        return self.chance(self._variant.intensity * share)

    def emit(
        self,
        kind: SourceKind,
        service: str,
        at: datetime,
        payload: AdapterInput,
        *,
        supporting: bool = False,
    ) -> None:
        # Hashing the scenario into the ID keeps IDs unique across datasets without
        # revealing the scenario in the record.
        digest = hashlib.sha256(f"{self._id_prefix}:{self._emitted}".encode()).hexdigest()[:20]
        self._emitted += 1
        source_event_id = f"{service}-{ID_PREFIX[kind]}-{digest}"
        self.records.append(
            DemoRecord(
                source_id=SOURCE_IDS[kind],
                source_event_id=source_event_id,
                occurred_at=at,
                payload=payload.model_dump(mode="json", exclude_none=True),
            )
        )
        if supporting:
            self.supporting.append(source_event_id)

    def log(
        self,
        at: datetime,
        service: str,
        level: Severity,
        message: str,
        attributes: dict[str, JsonValue],
        *,
        trace_id: str | None = None,
        supporting: bool = False,
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
        self.emit(SourceKind.STRUCTURED_LOG, service, at, payload, supporting=supporting)

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
        supporting: bool = False,
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
        self.emit(SourceKind.TRACE_SUMMARY, service, end, payload, supporting=supporting)

    def deployment(
        self,
        at: datetime,
        service: str,
        *,
        revision: str,
        previous_revision: str,
        change_summary: str,
        supporting: bool = False,
    ) -> None:
        payload = DeploymentInput(
            service=service,
            environment=DEMO_ENVIRONMENT,
            revision=revision,
            previous_revision=previous_revision,
            change_summary=change_summary,
            attributes={"pipeline": "demo-cd", "commit": self._hex(28)},
        )
        self.emit(SourceKind.DEPLOYMENT, service, at, payload, supporting=supporting)

    def request(self, at: datetime) -> None:
        """One checkout: inventory, then the orders database, then payment via bank-gateway.

        Each scenario's fault hooks in at one point and draws randomness only inside
        its own window, so healthy and downstream-timeout datasets are unchanged.
        """
        trace_id = self._hex(128)
        if self.breaks(Scenario.UNEXPLAINED_ERRORS, at):
            self._unexplained_failure(at, trace_id)
            return
        timeout_fault = self.in_fault(at) and self._scenario in TIMEOUT_SCENARIOS

        inventory_ms = round(self.between(15, 60))
        sku_missing = self.chance(0.01)
        if self._target == "inventory" and self.breaks(Scenario.DEPLOYMENT_REGRESSION, at):
            self._inventory_regression(at, trace_id, inventory_ms)
            return
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

        payment_start_offset = query_end
        if self.breaks(Scenario.POOL_EXHAUSTION, at):
            self._payment_failure(at, trace_id, payment_start_offset, sku_missing, pool=True)
            return
        if self._target == "payment" and self.breaks(Scenario.DEPLOYMENT_REGRESSION, at):
            self._payment_failure(at, trace_id, payment_start_offset, sku_missing, pool=False)
            return

        # During the fault most bank-gateway calls outlast payment's timeout and the
        # rest are slow; outside it a rare isolated timeout is ordinary noise.
        if timeout_fault:
            gateway_ms = round(
                self.between(2300, 4000)
                if self.chance(self._variant.intensity)
                else self.between(600, 1800)
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
            supporting=timeout_fault,
        )
        payment_ms = (PAYMENT_TIMEOUT_MS if timed_out else gateway_ms) + round(self.between(15, 40))
        # Memory pressure: GC stalls slow every call in the window; some run out of heap.
        stall = 0
        out_of_memory = False
        if self._scenario is Scenario.MEMORY_PRESSURE and self.in_fault(at):
            stall = round(self.between(700, 2600))
            out_of_memory = self.breaks(Scenario.MEMORY_PRESSURE, at, share=0.5)
            payment_ms += stall
        payment_end = payment_start + timedelta(milliseconds=payment_ms)
        pool_active = round(
            self.between(35, PAYMENT_POOL_SIZE) if timeout_fault else self.between(2, 12)
        )
        pool: dict[str, JsonValue] = {
            "http.route": "/v1/charges",
            "pool.active": pool_active,
            "pool.max": PAYMENT_POOL_SIZE,
        }
        failed = timed_out or out_of_memory
        self.span(
            payment_end,
            "payment",
            trace_id,
            "POST /v1/charges",
            payment_ms,
            error=failed,
            span_count=(6, 9),
            downstream=("bank-gateway",),
            supporting=(timeout_fault and timed_out) or stall > 0,
        )
        if stall and self.chance(0.4):
            self.log(
                payment_end,
                "payment",
                Severity.WARNING,
                f"GC pause of {stall} ms",
                {"runtime": "jvm"},
                supporting=True,
            )
        if out_of_memory:
            self.log(
                payment_end,
                "payment",
                Severity.ERROR,
                "java.lang.OutOfMemoryError: Java heap space",
                {"runtime": "jvm"},
                trace_id=trace_id,
                supporting=True,
            )
        elif timed_out:
            self.log(
                payment_end,
                "payment",
                Severity.ERROR,
                f"upstream bank-gateway call timed out after {PAYMENT_TIMEOUT_MS} ms",
                pool,
                trace_id=trace_id,
                supporting=timeout_fault,
            )
        elif self.chance(0.3):
            self.log(
                payment_end, "payment", Severity.INFO, "charge authorized", pool, trace_id=trace_id
            )

        checkout_end = payment_end + timedelta(milliseconds=round(self.between(10, 30)))
        checkout_ms = round((checkout_end - at) / timedelta(milliseconds=1))
        caused = (timeout_fault and timed_out) or out_of_memory
        self.span(
            checkout_end,
            "checkout",
            trace_id,
            "POST /checkout",
            checkout_ms,
            error=failed or sku_missing,
            span_count=(12, 16),
            downstream=("inventory", "orders-db", "payment"),
            supporting=caused,
        )
        if out_of_memory:
            self.log(
                checkout_end,
                "checkout",
                Severity.ERROR,
                "payment authorization failed: upstream returned 500",
                {"http.route": "/checkout", "http.status_code": 502},
                trace_id=trace_id,
                supporting=True,
            )
        elif timed_out:
            self.log(
                checkout_end,
                "checkout",
                Severity.ERROR,
                "payment authorization failed: upstream timeout",
                {"http.route": "/checkout", "http.status_code": 502},
                trace_id=trace_id,
                supporting=caused,
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

    def _checkout_failure(
        self, at: datetime, trace_id: str, end: datetime, message: str, called: tuple[str, ...]
    ) -> None:
        """checkout's failed span and log after a dependency broke the request."""
        checkout_end = end + timedelta(milliseconds=round(self.between(10, 30)))
        self.span(
            checkout_end,
            "checkout",
            trace_id,
            "POST /checkout",
            round((checkout_end - at) / timedelta(milliseconds=1)),
            error=True,
            span_count=(12, 16),
            downstream=called,
            supporting=True,
        )
        self.log(
            checkout_end,
            "checkout",
            Severity.ERROR,
            message,
            {"http.route": "/checkout", "http.status_code": 502},
            trace_id=trace_id,
            supporting=True,
        )

    def _inventory_regression(self, at: datetime, trace_id: str, inventory_ms: int) -> None:
        inventory_end = at + timedelta(milliseconds=inventory_ms)
        self.span(
            inventory_end,
            "inventory",
            trace_id,
            "GET /stock/{sku}",
            inventory_ms,
            error=True,
            span_count=(3, 6),
            supporting=True,
        )
        self.log(
            inventory_end,
            "inventory",
            Severity.ERROR,
            "unhandled exception in stock reservation: KeyError 'warehouse_id'",
            {"http.route": "/stock/{sku}", "http.status_code": 500},
            trace_id=trace_id,
            supporting=True,
        )
        self._checkout_failure(
            at,
            trace_id,
            inventory_end,
            "stock lookup failed: upstream returned 500",
            ("inventory",),
        )

    def _payment_failure(
        self, at: datetime, trace_id: str, start: datetime, sku_missing: bool, *, pool: bool
    ) -> None:
        """payment fails before calling bank-gateway: no pooled connection, or a new bug."""
        del sku_missing  # the request fails at payment either way
        if pool:
            payment_ms = POOL_WAIT_MS + round(self.between(5, 20))
            active = PAYMENT_POOL_SIZE
            message = (
                f"connection pool exhausted: no connection available within {POOL_WAIT_MS} ms "
                f"({PAYMENT_POOL_SIZE} of {PAYMENT_POOL_SIZE} in use)"
            )
            reply = "payment authorization failed: upstream returned 503"
        else:
            payment_ms = round(self.between(20, 60))
            active = round(self.between(2, 12))
            message = "unhandled exception in charge request mapper: NullPointerException"
            reply = "payment authorization failed: upstream returned 500"
        payment_end = start + timedelta(milliseconds=payment_ms)
        self.span(
            payment_end,
            "payment",
            trace_id,
            "POST /v1/charges",
            payment_ms,
            error=True,
            span_count=(6, 9),
            supporting=True,
        )
        self.log(
            payment_end,
            "payment",
            Severity.ERROR,
            message,
            {"http.route": "/v1/charges", "pool.active": active, "pool.max": PAYMENT_POOL_SIZE},
            trace_id=trace_id,
            supporting=True,
        )
        self._checkout_failure(
            at, trace_id, payment_end, reply, ("inventory", "orders-db", "payment")
        )

    def _unexplained_failure(self, at: datetime, trace_id: str) -> None:
        """checkout rejects the request before calling anything, with a generic error."""
        checkout_ms = round(self.between(3, 12))
        checkout_end = at + timedelta(milliseconds=checkout_ms)
        self.span(
            checkout_end,
            "checkout",
            trace_id,
            "POST /checkout",
            checkout_ms,
            error=True,
            span_count=(1, 2),
            supporting=True,
        )
        self.log(
            checkout_end,
            "checkout",
            Severity.ERROR,
            "request failed: internal error",
            {"http.route": "/checkout", "http.status_code": 500},
            trace_id=trace_id,
            supporting=True,
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
