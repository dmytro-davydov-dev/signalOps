"""A small real instrumented application for fault demonstrations (P10-T03).

Three HTTP services run in one process on loopback ports: `checkout` calls
`payment`, which calls `bank-gateway`, each over real HTTP with real timeouts. A
load generator places orders at a steady rate. Every service measures its own
handling and records a trace summary per request and structured logs, which an
exporter posts to the SignalOps ingestion API with the sources' tokens, exactly
as an external system would. At start each service reports its deployment.

**The fault** is real: during its window `bank-gateway` really sleeps past
payment's 2-second client timeout for a share of calls, so payment's call really
times out and checkout really fails. The schedule is configuration of this
process. Nothing about it is logged or exported, so investigators see only what
the services observed, never the injection.

This replaces the OpenTelemetry Demo for the `demo` profile (decision Q10): it fits
a laptop and exercises the same path, at the cost of a smaller topology.
"""

import asyncio
import contextlib
import logging
import random
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final

import uvicorn
from fastapi import FastAPI, Request, Response
from pydantic import JsonValue

from signalops.contracts.adapters import (
    AdapterInput,
    DeploymentInput,
    StructuredLogInput,
    TraceSummaryInput,
)
from signalops.contracts.evidence import Severity, SourceKind
from signalops.demo.generator import DemoRecord
from signalops.persistence.seed import DEMO_ENVIRONMENT, DEMO_SOURCES

logger = logging.getLogger(__name__)

SOURCE_IDS: Final = {source.kind: source.id for source in DEMO_SOURCES}
PAYMENT_TIMEOUT_SECONDS: Final = 2.0
CHECKOUT_TIMEOUT_SECONDS: Final = 5.0
TRACE_HEADER: Final = "X-Trace-Id"
VERSION: Final = "shop-1"


@dataclass(frozen=True)
class FaultSchedule:
    """When bank-gateway slows down, and for what share of calls. Never exported."""

    after: timedelta
    duration: timedelta
    share: float = 0.75
    latency_ms: tuple[int, int] = (2300, 4000)

    def active(self, elapsed: timedelta) -> bool:
        return self.after <= elapsed < self.after + self.duration


@dataclass
class Telemetry:
    """Records the services produce, waiting for the exporter."""

    records: list[DemoRecord] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, kind: SourceKind, service: str, at: datetime, payload: AdapterInput) -> None:
        record = DemoRecord(
            source_id=SOURCE_IDS[kind],
            source_event_id=f"{service}-{kind.value}-{uuid.uuid4().hex}",
            occurred_at=at,
            payload=payload.model_dump(mode="json", exclude_none=True),
        )
        with self._lock:
            self.records.append(record)

    def drain(self) -> list[DemoRecord]:
        with self._lock:
            taken, self.records = self.records, []
        return taken

    def requeue(self, records: list[DemoRecord]) -> None:
        with self._lock:
            self.records = records + self.records

    def span(
        self,
        service: str,
        trace_id: str,
        operation: str,
        started: float,
        *,
        error: bool,
        downstream: tuple[str, ...] = (),
    ) -> None:
        duration_ms = max(1, round((time.monotonic() - started) * 1000))
        payload = TraceSummaryInput(
            trace_id=trace_id,
            service=service,
            environment=DEMO_ENVIRONMENT,
            operation=operation,
            duration_ms=duration_ms,
            status="error" if error else "ok",
            span_count=1 + len(downstream),
            error_span_count=1 if error else 0,
            downstream_services=downstream,
        )
        self.add(SourceKind.TRACE_SUMMARY, service, datetime.now(UTC), payload)

    def log(
        self,
        service: str,
        level: Severity,
        message: str,
        attributes: dict[str, JsonValue],
        trace_id: str | None = None,
    ) -> None:
        payload = StructuredLogInput(
            service=service,
            environment=DEMO_ENVIRONMENT,
            level=level,
            message=message,
            trace_id=trace_id,
            attributes=attributes,
        )
        self.add(SourceKind.STRUCTURED_LOG, service, datetime.now(UTC), payload)

    def deployment(self, service: str) -> None:
        payload = DeploymentInput(
            service=service,
            environment=DEMO_ENVIRONMENT,
            revision=f"{service}-{VERSION}",
            previous_revision=f"{service}-shop-0",
            change_summary="Process start",
            attributes={"pipeline": "demo-shop"},
        )
        self.add(SourceKind.DEPLOYMENT, service, datetime.now(UTC), payload)


def _post(url: str, trace_id: str, timeout: float) -> int:
    if not url.startswith("http://127.0.0.1:"):
        raise ValueError("the shop only calls its own loopback services")
    request = urllib.request.Request(  # noqa: S310 - loopback URL checked above
        url, data=b"{}", headers={TRACE_HEADER: trace_id}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return int(response.status)
    except urllib.error.HTTPError as error:
        return int(error.code)


async def call(url: str, trace_id: str, timeout: float) -> int | None:  # noqa: ASYNC109 - the client's own socket timeout
    """POST to a sibling service; None when it did not answer in time."""
    try:
        return await asyncio.to_thread(_post, url, trace_id, timeout)
    except TimeoutError:
        return None
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            return None
        raise


@dataclass
class Shop:
    telemetry: Telemetry
    schedule: FaultSchedule | None
    ports: Mapping[str, int]
    rng: random.Random = field(default_factory=random.Random)
    started: float = field(default_factory=time.monotonic)

    def url(self, service: str, path: str) -> str:
        return f"http://127.0.0.1:{self.ports[service]}{path}"

    def elapsed(self) -> timedelta:
        return timedelta(seconds=time.monotonic() - self.started)

    def bank_gateway(self) -> FastAPI:
        app = FastAPI()

        @app.post("/authorize")
        async def authorize(request: Request) -> Response:
            started = time.monotonic()
            trace_id = request.headers.get(TRACE_HEADER, uuid.uuid4().hex)
            slow = (
                self.schedule is not None
                and self.schedule.active(self.elapsed())
                and self.rng.random() < self.schedule.share
            )
            low, high = self.schedule.latency_ms if slow and self.schedule else (60, 160)
            await asyncio.sleep(self.rng.uniform(low, high) / 1000)
            self.telemetry.span("bank-gateway", trace_id, "POST /authorize", started, error=False)
            return Response(status_code=200)

        return app

    def payment(self) -> FastAPI:
        app = FastAPI()

        @app.post("/v1/charges")
        async def charge(request: Request) -> Response:
            started = time.monotonic()
            trace_id = request.headers.get(TRACE_HEADER, uuid.uuid4().hex)
            status = await call(
                self.url("bank-gateway", "/authorize"), trace_id, PAYMENT_TIMEOUT_SECONDS
            )
            attributes: dict[str, JsonValue] = {"http.route": "/v1/charges"}
            failed = status != 200
            if status is None:
                self.telemetry.log(
                    "payment",
                    Severity.ERROR,
                    f"upstream bank-gateway call timed out after "
                    f"{round(PAYMENT_TIMEOUT_SECONDS * 1000)} ms",
                    attributes,
                    trace_id,
                )
            elif failed:
                self.telemetry.log(
                    "payment",
                    Severity.ERROR,
                    f"bank-gateway returned {status}",
                    attributes,
                    trace_id,
                )
            self.telemetry.span(
                "payment",
                trace_id,
                "POST /v1/charges",
                started,
                error=failed,
                downstream=("bank-gateway",),
            )
            return Response(status_code=504 if status is None else 200 if not failed else 502)

        return app

    def checkout(self) -> FastAPI:
        app = FastAPI()

        @app.post("/checkout")
        async def checkout(request: Request) -> Response:
            started = time.monotonic()
            trace_id = request.headers.get(TRACE_HEADER, uuid.uuid4().hex)
            status = await call(
                self.url("payment", "/v1/charges"), trace_id, CHECKOUT_TIMEOUT_SECONDS
            )
            failed = status != 200
            if failed:
                self.telemetry.log(
                    "checkout",
                    Severity.ERROR,
                    "payment authorization failed: upstream "
                    + ("timeout" if status in (None, 504) else f"returned {status}"),
                    {"http.route": "/checkout", "http.status_code": 502},
                    trace_id,
                )
            self.telemetry.span(
                "checkout",
                trace_id,
                "POST /checkout",
                started,
                error=failed,
                downstream=("payment",),
            )
            return Response(status_code=502 if failed else 201)

        return app

    def apps(self) -> dict[str, FastAPI]:
        return {
            "checkout": self.checkout(),
            "payment": self.payment(),
            "bank-gateway": self.bank_gateway(),
        }


def free_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


Exporter = Callable[[list[DemoRecord]], Awaitable[None]]


async def run_shop(
    *,
    rate_per_second: float,
    run_for: timedelta,
    schedule: FaultSchedule | None,
    export: Exporter,
    export_interval: timedelta = timedelta(seconds=2),
    seed: int | None = None,
) -> None:
    """Serve the three services, place orders, and export telemetry until `run_for` ends."""
    names = ("checkout", "payment", "bank-gateway")
    ports = dict(zip(names, free_ports(len(names)), strict=True))
    telemetry = Telemetry()
    shop = Shop(telemetry, schedule, ports, rng=random.Random(seed))  # noqa: S311 - not a secret
    servers = [
        uvicorn.Server(
            uvicorn.Config(
                app, host="127.0.0.1", port=ports[name], log_config=None, access_log=False
            )
        )
        for name, app in shop.apps().items()
    ]
    serving = [asyncio.create_task(server.serve()) for server in servers]
    # uvicorn exposes readiness only as a flag.
    while not all(server.started for server in servers):  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    for name in names:
        telemetry.deployment(name)
    shop.started = time.monotonic()

    async def exporter() -> None:
        while True:
            await asyncio.sleep(export_interval.total_seconds())
            await _export_once(telemetry, export)

    exporting = asyncio.create_task(exporter())
    orders: set[asyncio.Task[int | None]] = set()
    deadline = time.monotonic() + run_for.total_seconds()
    try:
        while time.monotonic() < deadline:
            order = asyncio.create_task(
                call(
                    shop.url("checkout", "/checkout"),
                    uuid.uuid4().hex,
                    CHECKOUT_TIMEOUT_SECONDS + 1,
                )
            )
            orders.add(order)
            order.add_done_callback(orders.discard)
            await asyncio.sleep(1 / rate_per_second)
        await asyncio.gather(*orders, return_exceptions=True)
    finally:
        exporting.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await exporting
        await _export_once(telemetry, export)
        for server in servers:
            server.should_exit = True
        await asyncio.gather(*serving, return_exceptions=True)


async def _export_once(telemetry: Telemetry, export: Exporter) -> None:
    records = telemetry.drain()
    if not records:
        return
    try:
        await export(records)
    except Exception as error:  # noqa: BLE001 - kept and resent on the next round
        logger.warning("telemetry export failed", extra={"error": type(error).__name__})
        telemetry.requeue(records)


def ingestion_exporter(api_url: str, tokens: Mapping[uuid.UUID, str]) -> Exporter:
    """Post records to the ingestion API, resending failed ones with the same IDs."""
    from signalops.demo.poster import post_records, urllib_transport

    async def export(records: list[DemoRecord]) -> None:
        summary = await asyncio.to_thread(
            post_records,
            records,
            api_url=api_url,
            tokens=tokens,
            transport=urllib_transport(10.0),
        )
        logger.info(
            "telemetry exported",
            extra={"accepted": summary.accepted, "rejected": summary.rejected},
        )

    return export
