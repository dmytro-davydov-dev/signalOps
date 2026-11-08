"""`metrics-exporter`: durable operational state as Prometheus gauges.

Every `metrics_refresh_seconds` it reads `OperationsSnapshot` in one short
read-only transaction plus a few broker admin calls, and scrapes return the
latest snapshot. A failed refresh keeps the previous values and advances
`signalops_exporter_refresh_failures_total`; `signalops_exporter_last_success_timestamp_seconds`
shows how old the gauges are.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterator

from prometheus_client import CollectorRegistry, Counter, Gauge
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from signalops.observability.snapshot import OperationsSnapshot, PartitionLag

logger = logging.getLogger(__name__)


class SnapshotCollector(Collector):
    def __init__(self) -> None:
        self.snapshot: OperationsSnapshot | None = None

    def collect(self) -> Iterator[Metric]:
        snapshot = self.snapshot
        if snapshot is None:
            return
        db = snapshot.database
        yield _gauge("signalops_outbox_pending", "Unpublished outbox rows.", db.outbox_pending)
        yield _gauge(
            "signalops_outbox_quarantined", "Quarantined outbox rows.", db.outbox_quarantined
        )
        yield _gauge(
            "signalops_outbox_oldest_age_seconds",
            "Age of the oldest unpublished outbox row; 0 when none.",
            db.outbox_oldest_age_seconds or 0.0,
        )
        jobs = GaugeMetricFamily(
            "signalops_jobs", "Investigation jobs by status.", labels=["status"]
        )
        for status, count in sorted(db.jobs.items()):
            jobs.add_metric([status], count)
        yield jobs
        yield _gauge("signalops_jobs_ready", "Queued jobs available to claim now.", db.jobs_ready)
        yield _gauge(
            "signalops_jobs_ready_oldest_age_seconds",
            "How long the oldest claimable job has waited; 0 when none.",
            db.jobs_ready_oldest_age_seconds or 0.0,
        )
        yield _gauge(
            "signalops_jobs_expired_leases",
            "Running jobs whose lease ran out and no worker has reclaimed yet.",
            db.jobs_expired_leases,
        )
        investigations = GaugeMetricFamily(
            "signalops_investigations", "Investigations by status.", labels=["status"]
        )
        for status, count in sorted(db.investigations.items()):
            investigations.add_metric([status], count)
        yield investigations
        yield _gauge(
            "signalops_model_calls_in_flight",
            "Model calls reserved and not yet settled.",
            db.model_calls_in_flight,
        )
        yield _gauge(
            "signalops_model_calls_unknown",
            "Model calls with an ambiguous outcome; the provider may have charged.",
            db.model_calls_unknown,
        )
        yield _gauge(
            "signalops_model_unknown_reserved_microusd",
            "Reservations kept for model calls with an unknown outcome.",
            db.model_unknown_reserved_microusd,
        )
        if snapshot.kafka_lag is not None:
            yield _lag_family(snapshot.kafka_lag)


def _gauge(name: str, documentation: str, value: float) -> GaugeMetricFamily:
    return GaugeMetricFamily(name, documentation, value=value)


def _lag_family(lag: list[PartitionLag]) -> GaugeMetricFamily:
    family = GaugeMetricFamily(
        "signalops_kafka_consumer_lag",
        "Log end offset minus the group's committed offset, as the broker reports it.",
        labels=["consumer_group", "topic", "partition"],
    )
    for item in lag:
        family.add_metric([item.consumer_group, item.topic, str(item.partition)], item.lag)
    return family


class Exporter:
    def __init__(
        self,
        read: Callable[[], Awaitable[OperationsSnapshot]],
        registry: CollectorRegistry,
        *,
        interval_seconds: float,
    ) -> None:
        self._read = read
        self._interval = interval_seconds
        self.collector = SnapshotCollector()
        registry.register(self.collector)
        self._failures = Counter(
            "signalops_exporter_refresh_failures_total",
            "Snapshot refreshes that failed; the previous values stay.",
            registry=registry,
        )
        self._last_success = Gauge(
            "signalops_exporter_last_success_timestamp_seconds",
            "Unix time of the last successful snapshot.",
            registry=registry,
        )

    async def refresh(self) -> bool:
        try:
            snapshot = await self._read()
        except Exception as error:  # noqa: BLE001 - keep serving the last snapshot.
            self._failures.inc()
            logger.warning("operations snapshot failed", extra={"error": type(error).__name__})
            return False
        self.collector.snapshot = snapshot
        self._last_success.set(time.time())
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.refresh()
            try:
                await asyncio.wait_for(stop.wait(), self._interval)
            except TimeoutError:
                continue
