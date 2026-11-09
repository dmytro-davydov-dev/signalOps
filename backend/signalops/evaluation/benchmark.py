"""Throughput benchmarks against the running Compose stack (P11-T04).

Two runs, each writing one JSON report:

- `replay`: N synthetic records published into their own replay scope through
  Kafka (the running normalizer, detector, and graph projector process them),
  timed from the first publish until PostgreSQL holds every signal, Kafka lag
  is zero, and the outbox is empty. Records come from healthy datasets overlaid
  `density` times per 6-hour window (distinct seeds, so distinct source event
  IDs), consecutive windows in event time; the first window carries one
  downstream timeout so detection has something to find.
- `sustained`: live records posted through the HTTP ingestion API at a target
  rate for a duration, timed per request; then drained.

Both sample Kafka lag and outbox backlog from the stores, CPU and memory per
container from `docker stats`, and the latency of an operator read
(`GET /v1/incidents`) every half second, and reconcile every accepted record
afterwards (`streaming.reconcile`). Storage growth is PostgreSQL's database
size and Kafka's log directory, before and after.

The report records what happened, including failures and timeouts; nothing in
it is a target.
"""

import asyncio
import json
import logging
import os
import platform
import statistics
import subprocess
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from confluent_kafka.admin import AdminClient
from sqlalchemy import func, select, text

from signalops import __version__
from signalops.adapters.ingestion import IngestPolicy, IngestSource, Publisher, ingest_batch
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, replay_scope_for
from signalops.contracts.ingestion import IngestRecordStatus
from signalops.demo.generator import DemoRecord, Scenario, generate
from signalops.detection.closure import flush_replay
from signalops.detection.detector import DetectionPolicy
from signalops.observability.snapshot import database_state, kafka_lag
from signalops.persistence.database import Database
from signalops.persistence.models import Incident, Signal
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.streaming.reconcile import RecordKey, reconcile

logger = logging.getLogger(__name__)

WINDOW = timedelta(hours=6)
SAMPLE_SECONDS = 5.0
PROBE_SECONDS = 0.5


def percentiles(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)

    def at(q: float) -> float:
        return round(ordered[min(len(ordered) - 1, int(q * len(ordered)))], 4)

    return {
        "count": len(ordered),
        "p50": at(0.50),
        "p95": at(0.95),
        "p99": at(0.99),
        "max": round(ordered[-1], 4),
    }


def replay_records(
    count: int, *, density: int, start: datetime, seed: int
) -> Iterator[list[DemoRecord]]:
    """Consecutive 6-hour windows of overlaid healthy traffic, `count` records in all."""
    produced = 0
    window = 0
    while produced < count:
        window_start = start + WINDOW * window
        merged: list[DemoRecord] = []
        for layer in range(density):
            scenario = (
                Scenario.DOWNSTREAM_TIMEOUT if window == 0 and layer == 0 else Scenario.HEALTHY
            )
            dataset = generate(
                scenario, seed=seed + window * density + layer, start=window_start, duration=WINDOW
            )
            merged.extend(dataset.records)
        merged.sort(key=lambda record: (record.occurred_at, record.source_event_id))
        part = merged[: count - produced]
        produced += len(part)
        window += 1
        yield part


def live_records(
    rate: float, duration: timedelta, *, start: datetime, seed: int
) -> list[DemoRecord]:
    """Healthy traffic overlaid to about `rate` records a second, from `start`."""
    target = int(rate * duration.total_seconds())
    merged: list[DemoRecord] = []
    layer = 0
    # Layers differ in size, so add them until the target is covered.
    while len(merged) < target:
        merged.extend(
            generate(Scenario.HEALTHY, seed=seed + layer, start=start, duration=duration).records
        )
        layer += 1
    merged.sort(key=lambda record: (record.occurred_at, record.source_event_id))
    # Thin evenly rather than truncate, so the rate holds to the end of the duration.
    return [merged[index * len(merged) // target] for index in range(target)]


def _item(record: DemoRecord) -> dict[str, Any]:
    item = record.to_json()
    del item["source_id"]
    return item


def payload_bytes(records: Sequence[DemoRecord]) -> dict[str, float]:
    sizes = [len(json.dumps(_item(r), separators=(",", ":"))) for r in records[:5000]]
    return {"mean": round(statistics.fmean(sizes), 1), "max": max(sizes)} if sizes else {}


# Environment and resource sampling


def _run(*command: str, timeout: float = 30) -> str:
    try:
        result = subprocess.run(  # noqa: S603 - fixed commands
            list(command), capture_output=True, text=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip()


def environment() -> dict[str, Any]:
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
        memory_kb = next(
            int(line.split()[1])
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemTotal")
        )
    except (OSError, StopIteration, ValueError):
        memory_kb = 0
    images = _run("docker", "compose", "images", "--format", "json")
    return {
        "cpu_model": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_gib": round(memory_kb / 1024**2, 1),
        "os": platform.platform(),
        "python": platform.python_version(),
        "signalops": __version__,
        "git_revision": _run("git", "rev-parse", "HEAD"),
        "git_dirty": bool(_run("git", "status", "--porcelain")),
        "docker": _run("docker", "version", "--format", "{{.Server.Version}}"),
        "images": sorted(
            {
                f"{i['Repository']}:{i.get('Tag')}"
                for i in _json_lines(images)
                if i.get("Repository")
            }
        ),
        "replicas": _replicas(),
    }


def _json_lines(output: str) -> list[dict[str, Any]]:
    output = output.strip()
    if not output:
        return []
    if output.startswith("["):
        parsed = json.loads(output)
        return parsed if isinstance(parsed, list) else []
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _replicas() -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _json_lines(_run("docker", "compose", "ps", "--format", "json")):
        service = str(row.get("Service"))
        counts[service] = counts.get(service, 0) + 1
    return counts


def _bytes(value: str) -> float:
    units = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "kB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
    }
    for unit in sorted(units, key=len, reverse=True):
        if value.endswith(unit):
            return float(value[: -len(unit)]) * units[unit]
    return 0.0


def container_stats() -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for row in _json_lines(
        _run("docker", "stats", "--no-stream", "--format", "{{json .}}", timeout=20)
    ):
        name = str(row.get("Name", ""))
        if not name.startswith("signalops-"):
            continue
        used = str(row.get("MemUsage", "0B / 0B")).split("/")[0].strip()
        stats[name.removeprefix("signalops-")] = {
            "cpu_percent": float(str(row.get("CPUPerc", "0%")).rstrip("%") or 0),
            "memory_mib": round(_bytes(used) / 1024**2, 1),
        }
    return stats


async def storage(database: Database) -> dict[str, Any]:
    async with database.transaction() as session:
        size = await session.scalar(text("SELECT pg_database_size(current_database())"))
        tables = await session.execute(
            text(
                "SELECT relname, pg_total_relation_size(relid)"
                " FROM pg_catalog.pg_statio_user_tables ORDER BY 2 DESC LIMIT 8"
            )
        )
        largest = {name: int(bytes_) for name, bytes_ in tables}
    kafka = _run("docker", "compose", "exec", "-T", "kafka", "du", "-sb", "/var/lib/kafka/data")
    return {
        "postgres_bytes": int(size or 0),
        "postgres_largest_tables": largest,
        "kafka_log_bytes": int(kafka.split()[0])
        if kafka.split() and kafka.split()[0].isdigit()
        else None,
    }


@dataclass
class Monitor:
    """Lag, backlog, container resources, and operator-read latency while a run goes on."""

    database: Database
    settings: Settings
    api_url: str
    operator_token: str
    started: float = field(default_factory=time.monotonic)
    samples: list[dict[str, Any]] = field(default_factory=list)
    read_latencies: list[float] = field(default_factory=list)
    read_errors: int = 0

    async def run(self, stop: asyncio.Event) -> None:
        await asyncio.gather(self._sample(stop), self._probe(stop))

    async def _sample(self, stop: asyncio.Event) -> None:
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap_servers})
        while not stop.is_set():
            lag = await asyncio.to_thread(kafka_lag, admin)
            try:
                async with self.database.transaction() as session:
                    outbox = (await database_state(session)).outbox_pending
            except Exception as error:  # noqa: BLE001 - a missed sample is recorded, not fatal
                logger.warning("benchmark sample failed", extra={"error": type(error).__name__})
                outbox = None
            by_group: dict[str, int] = {}
            for item in lag or []:
                by_group[item.consumer_group] = by_group.get(item.consumer_group, 0) + item.lag
            self.samples.append(
                {
                    "t": round(time.monotonic() - self.started, 1),
                    "lag": by_group if lag is not None else None,
                    "outbox_pending": outbox,
                    "containers": await asyncio.to_thread(container_stats),
                }
            )
            await _wait(stop, SAMPLE_SECONDS)

    async def _probe(self, stop: asyncio.Event) -> None:
        url = f"{self.api_url.rstrip('/')}/v1/incidents?limit=20"
        headers = {"Authorization": f"Bearer {self.operator_token}"}
        while not stop.is_set():
            began = time.monotonic()
            ok = await asyncio.to_thread(_get, url, headers)
            if ok:
                self.read_latencies.append(time.monotonic() - began)
            else:
                self.read_errors += 1
            await _wait(stop, PROBE_SECONDS)

    def summary(self) -> dict[str, Any]:
        peaks: dict[str, int] = {}
        for sample in self.samples:
            for group, lag in (sample["lag"] or {}).items():
                peaks[group] = max(peaks.get(group, 0), lag)
        cpu: dict[str, list[float]] = {}
        memory: dict[str, float] = {}
        for sample in self.samples:
            for name, values in sample["containers"].items():
                cpu.setdefault(name, []).append(values["cpu_percent"])
                memory[name] = max(memory.get(name, 0.0), values["memory_mib"])
        return {
            "operator_read_seconds": percentiles(self.read_latencies),
            "operator_read_errors": self.read_errors,
            "peak_lag": peaks,
            "peak_outbox_pending": max((s["outbox_pending"] or 0 for s in self.samples), default=0),
            "cpu_percent": {
                name: {"mean": round(statistics.fmean(v), 1), "max": round(max(v), 1)}
                for name, v in sorted(cpu.items())
            },
            "peak_memory_mib": dict(sorted(memory.items())),
        }


def _get(url: str, headers: Mapping[str, str]) -> bool:
    request = urllib.request.Request(url, headers=dict(headers))  # noqa: S310 - loopback API
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            response.read()
            return bool(response.status == 200)
    except OSError:
        return False


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except TimeoutError:
        return


async def drained(
    database: Database,
    settings: Settings,
    scope: EvidenceScope,
    expected_signals: int,
    *,
    within: timedelta,
    progress: Callable[[int], None] = lambda _: None,
) -> bool:
    """Every expected signal stored in the scope, zero lag, and an empty outbox."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    deadline = time.monotonic() + within.total_seconds()
    while time.monotonic() < deadline:
        async with database.transaction() as session:
            stored = await session.scalar(select(func.count()).where(scope.clause(Signal))) or 0
            outbox = (await database_state(session)).outbox_pending
        lag = await asyncio.to_thread(kafka_lag, admin)
        progress(stored)
        if (
            stored >= expected_signals
            and outbox == 0
            and lag is not None
            and not sum(i.lag for i in lag)
        ):
            return True
        await asyncio.sleep(5)
    return False


# Runs


@dataclass
class BenchmarkContext:
    database: Database
    settings: Settings
    sources: Mapping[uuid.UUID, IngestSource]
    workspace_id: uuid.UUID
    api_url: str
    operator_token: str
    drain_timeout: timedelta


async def run_replay(
    context: BenchmarkContext,
    publisher: Publisher,
    *,
    records: int,
    density: int,
    seed: int,
    tick: timedelta = timedelta(seconds=30),
) -> dict[str, Any]:
    run_id = uuid.uuid4()
    scope = EvidenceScope(context.workspace_id, replay_scope_for(run_id))
    # Each batch is received at its simulated tick, so the ingestion bounds hold.
    policy = IngestPolicy.from_settings(context.settings)
    before = await storage(context.database)
    started = datetime.now(UTC)
    # Event time ends well before now, so ingestion bounds hold for the whole run.
    start = (started - WINDOW * (records // (density * 3600) + 2)).replace(
        minute=0, second=0, microsecond=0
    )
    stop = asyncio.Event()
    monitor = Monitor(context.database, context.settings, context.api_url, context.operator_token)
    watching = asyncio.create_task(monitor.run(stop))
    accepted = rejected = failed = 0
    keys: list[RecordKey] = []
    batch_seconds: list[float] = []
    sample: list[DemoRecord] = []
    clock = time.monotonic()
    try:
        for window in replay_records(records, density=density, start=start, seed=seed):
            if not sample:
                sample = window[:5000]
            ticks: dict[tuple[float, uuid.UUID], list[DemoRecord]] = {}
            for record in window:
                bucket = (
                    record.occurred_at.timestamp() // tick.total_seconds() * tick.total_seconds()
                )
                ticks.setdefault((bucket, record.source_id), []).append(record)
            for (bucket, source_id), batch in sorted(ticks.items(), key=lambda item: item[0]):
                began = time.monotonic()
                response = await ingest_batch(
                    [_item(r) for r in batch],
                    source=context.sources[source_id],
                    publisher=publisher,
                    policy=policy,
                    now=datetime.fromtimestamp(bucket, UTC) + tick + timedelta(seconds=1),
                    batch_id=uuid.uuid5(run_id, f"{source_id}:{bucket}"),
                    replay_run_id=run_id,
                )
                batch_seconds.append(time.monotonic() - began)
                accepted += response.accepted
                rejected += response.rejected
                failed += response.failed
                keys += [
                    RecordKey(source_id, result.source_event_id)
                    for result in response.results
                    if result.status is IngestRecordStatus.ACCEPTED and result.source_event_id
                ]
            logger.info("benchmark window published", extra={"accepted": accepted})
        published_s = time.monotonic() - clock
        complete = await drained(
            context.database,
            context.settings,
            scope,
            accepted,
            within=context.drain_timeout,
            progress=lambda stored: logger.info("benchmark draining", extra={"stored": stored}),
        )
        processed_s = time.monotonic() - clock
    finally:
        stop.set()
        await watching
    flush_began = time.monotonic()
    flushed = await flush_replay(
        context.database, scope, DetectionPolicy.from_settings(context.settings)
    )
    flush_s = time.monotonic() - flush_began
    async with context.database.transaction() as session:
        incidents = await session.scalar(select(func.count()).where(scope.clause(Incident))) or 0
    result = await reconcile(context.database, context.settings, keys, scope.replay_scope)
    after = await storage(context.database)
    return {
        "kind": "replay",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "replay_scope": scope.replay_scope,
        "requested_records": records,
        "density": density,
        "seed": seed,
        "tick_seconds": tick.total_seconds(),
        "payload_bytes": payload_bytes(sample),
        "accepted": accepted,
        "rejected": rejected,
        "failed": failed,
        "publish_seconds": round(published_s, 1),
        "publish_records_per_second": round(accepted / published_s, 1) if published_s else None,
        "ingest_batch_seconds": percentiles(batch_seconds),
        "drained": complete,
        "end_to_end_seconds": round(processed_s, 1),
        "end_to_end_records_per_second": round(accepted / processed_s, 1) if processed_s else None,
        "windows_flushed": flushed,
        "flush_seconds": round(flush_s, 1),
        "incidents": incidents,
        "reconciliation": _reconciliation(result),
        "monitor": monitor.summary(),
        "storage_before": before,
        "storage_after": after,
        "storage_growth_bytes": {
            "postgres": after["postgres_bytes"] - before["postgres_bytes"],
            "kafka": (after["kafka_log_bytes"] or 0) - (before["kafka_log_bytes"] or 0),
        },
        "samples": monitor.samples,
    }


def _reconciliation(result: Any) -> dict[str, Any]:
    return {
        "accepted": result.accepted,
        "processed": result.processed,
        "quarantined": len(result.quarantined),
        "missing": len(result.missing),
        "missing_examples": [str(i) for i in result.missing[:20]],
        "signals": result.signals,
        "complete": result.complete,
    }


@dataclass
class Sender:
    """Posts one batch through the HTTP API and times it."""

    api_url: str
    tokens: Mapping[uuid.UUID, str]
    latencies: list[float] = field(default_factory=list)
    statuses: dict[int, int] = field(default_factory=dict)

    def send(self, source_id: uuid.UUID, records: Sequence[DemoRecord]) -> list[RecordKey]:
        body = json.dumps({"records": [_item(r) for r in records]}).encode()
        request = urllib.request.Request(  # noqa: S310 - loopback API
            f"{self.api_url.rstrip('/')}/v1/ingest/{source_id}",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.tokens[source_id]}",
                "Content-Type": "application/json",
            },
        )
        began = time.monotonic()
        status = 0
        accepted: list[RecordKey] = []
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                status = response.status
                results = json.loads(response.read())["results"]
        except urllib.error.HTTPError as error:
            status = error.code
            results = json.loads(error.read() or b"{}").get("results", []) if status == 503 else []
        except OSError:
            results = []
        self.latencies.append(time.monotonic() - began)
        self.statuses[status] = self.statuses.get(status, 0) + 1
        for item in results:
            if item.get("status") == "accepted":
                accepted.append(RecordKey(source_id, records[item["index"]].source_event_id))
        return accepted


async def run_sustained(
    context: BenchmarkContext,
    tokens: Mapping[uuid.UUID, str],
    *,
    rate: float,
    duration: timedelta,
    seed: int,
    tick: timedelta = timedelta(seconds=1),
) -> dict[str, Any]:
    started = datetime.now(UTC)
    start = (started + timedelta(seconds=10)).replace(microsecond=0)
    records = live_records(rate, duration, start=start, seed=seed)
    before = await storage(context.database)
    stop = asyncio.Event()
    monitor = Monitor(context.database, context.settings, context.api_url, context.operator_token)
    watching = asyncio.create_task(monitor.run(stop))
    sender = Sender(context.api_url, tokens)
    keys: list[RecordKey] = []
    per_second: dict[int, int] = {}
    behind: list[float] = []
    sent = 0
    try:
        while sent < len(records):
            now = datetime.now(UTC)
            wait = (records[sent].occurred_at - now).total_seconds()
            if wait > 0:
                await asyncio.sleep(min(wait, tick.total_seconds()))
                continue
            due = sent
            while due < len(records) and records[due].occurred_at <= now:
                due += 1
            behind.append((now - records[sent].occurred_at).total_seconds())
            by_source: dict[uuid.UUID, list[DemoRecord]] = {}
            for record in records[sent:due]:
                by_source.setdefault(record.source_id, []).append(record)
            results = await asyncio.gather(
                *(
                    asyncio.to_thread(sender.send, source, batch)
                    for source, batch in by_source.items()
                )
            )
            second = int((now - start).total_seconds())
            for accepted in results:
                keys += accepted
                per_second[second] = per_second.get(second, 0) + len(accepted)
            sent = due
        posted_s = (datetime.now(UTC) - start).total_seconds()
        complete = await drained(
            context.database,
            context.settings,
            EvidenceScope(context.workspace_id, LIVE_REPLAY_SCOPE),
            0,
            within=context.drain_timeout,
        )
        drain_s = (datetime.now(UTC) - start).total_seconds() - posted_s
    finally:
        stop.set()
        await watching
    result = await reconcile(context.database, context.settings, keys, LIVE_REPLAY_SCOPE)
    after = await storage(context.database)
    minutes: dict[int, int] = {}
    for second, count in per_second.items():
        minutes[second // 60] = minutes.get(second // 60, 0) + count
    return {
        "kind": "sustained",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "target_records_per_second": rate,
        "duration_seconds": duration.total_seconds(),
        "seed": seed,
        "payload_bytes": payload_bytes(records),
        "generated": len(records),
        "accepted": len(keys),
        "achieved_records_per_second": round(len(keys) / posted_s, 2) if posted_s else None,
        "accepted_per_minute": [minutes.get(m, 0) for m in range(max(minutes, default=-1) + 1)],
        "post_seconds": round(posted_s, 1),
        "sender_behind_seconds": percentiles(behind),
        "ingest_request_seconds": percentiles(sender.latencies),
        "ingest_statuses": {str(k): v for k, v in sorted(sender.statuses.items())},
        "drained": complete,
        "drain_after_posting_seconds": round(drain_s, 1),
        "reconciliation": _reconciliation(result),
        "monitor": monitor.summary(),
        "storage_before": before,
        "storage_after": after,
        "storage_growth_bytes": {
            "postgres": after["postgres_bytes"] - before["postgres_bytes"],
            "kafka": (after["kafka_log_bytes"] or 0) - (before["kafka_log_bytes"] or 0),
        },
        "samples": monitor.samples,
    }
