"""Prometheus metrics recorded inside each process.

Each process registers these in the default registry and serves them on
`SIGNALOPS_METRICS_PORT` when it is set (`start_metrics_server`). Counters that
follow a database effect are incremented after the commit (`on_commit`), so a
rolled-back transaction counts nothing; a redelivery that hits its receipt
counts nothing either. Durable state (job queues, outbox age, unknown costs,
Kafka lag) comes from `metrics-exporter` instead, which reads PostgreSQL and
the broker, so it stays correct when the process that owns the work is stuck
or down.

Label values are bounded: source kinds, consumer groups, tool names, providers,
and fixed outcome enums. IDs never become labels; they go to logs.
"""

import logging

from prometheus_client import Counter, Histogram, start_http_server

from signalops.config import Settings

logger = logging.getLogger(__name__)

# Upper bounds in seconds: fast database work through slow model calls.
_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)
# Ingestion to incident spans the detection window plus allowed lateness.
_DETECTION_BUCKETS = (1, 5, 10, 30, 60, 90, 120, 180, 300, 600, 1800)

INGEST_RECORDS = Counter(
    "signalops_ingest_records_total",
    "Records per ingestion outcome; accepted means the broker acknowledged it.",
    ["source_kind", "status"],
)
CONSUMER_RECORDS = Counter(
    "signalops_consumer_records_total",
    "Kafka records per consumer outcome. Redeliveries are counted again.",
    ["consumer_group", "outcome"],
)
CONSUMER_PROCESSING = Histogram(
    "signalops_consumer_processing_seconds",
    "Poll return to offset commit, for records that reached a committed outcome.",
    ["consumer_group"],
    buckets=_LATENCY_BUCKETS,
)
OUTBOX_EVENTS = Counter(
    "signalops_outbox_events_total",
    "Outbox rows per publication outcome.",
    ["outcome"],
)
NORMALIZED_SIGNALS = Counter(
    "signalops_normalized_signals_total",
    "Signals stored by the normalizer, by source kind and quality status.",
    ["source_kind", "quality_status"],
)
QUALITY_FLAGS = Counter(
    "signalops_quality_flags_total",
    "Quality flags on stored signals, by source kind and flag.",
    ["source_kind", "flag"],
)
INCIDENTS_OPENED = Counter(
    "signalops_incidents_opened_total",
    "Incidents created by the detector, by replay kind.",
    ["scope"],
)
DETECTION_LATENCY = Histogram(
    "signalops_detection_latency_seconds",
    "Live incidents: wall time from ingestion of the earliest evidence to the incident commit.",
    buckets=_DETECTION_BUCKETS,
)
JOB_EVENTS = Counter(
    "signalops_job_events_total",
    "Investigation job transitions seen by workers.",
    ["event"],
)
TOOL_CALL_SECONDS = Histogram(
    "signalops_tool_call_seconds",
    "Evidence tool reads, by tool and outcome.",
    ["tool", "outcome"],
    buckets=_LATENCY_BUCKETS,
)
MODEL_CALL_SECONDS = Histogram(
    "signalops_model_call_seconds",
    "Model calls through the gateway, by provider and outcome (succeeded, failed, unknown).",
    ["provider", "outcome"],
    buckets=_LATENCY_BUCKETS,
)
MODEL_TOKENS = Counter(
    "signalops_model_tokens_total",
    "Tokens the provider reported, by kind.",
    ["provider", "kind"],
)
MODEL_COST = Counter(
    "signalops_model_cost_microusd_total",
    "Settled model cost in microUSD, by basis (actual or estimated).",
    ["provider", "basis"],
)


def start_metrics_server(settings: Settings) -> None:
    """Serve the default registry when `metrics_port` is set; otherwise do nothing."""
    if settings.metrics_port is None:
        return
    start_http_server(settings.metrics_port, addr=settings.metrics_host)
    logger.info("metrics listener started", extra={"metrics_port": settings.metrics_port})
