"""Kafka topic catalogue, message routing, and idempotent provisioning.

Partition counts and retention follow SignalOps_Implementation_Plan.md section 4
and section 5 (initial local retention targets). Topics are never auto-created.
Message keys are documented in docs/events.md; changing one changes which
partition, and so which ordering, an aggregate's events share.
"""

import logging
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    EventType,
    IncidentChangedPayload,
    InvestigationCompletedPayload,
    InvestigationRequestedPayload,
    NormalizedSignalPayload,
    ProcessingFailedPayload,
    RawSignalPayload,
)

logger = logging.getLogger(__name__)

DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int

    def config(self) -> dict[str, str]:
        return {"retention.ms": str(self.retention_ms), "cleanup.policy": "delete"}


TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec("signals.raw.v1", partitions=3, retention_ms=3 * DAY_MS),
    TopicSpec("signals.normalized.v1", partitions=3, retention_ms=7 * DAY_MS),
    TopicSpec("incidents.changed.v1", partitions=1, retention_ms=7 * DAY_MS),
    TopicSpec("investigations.requested.v1", partitions=1, retention_ms=7 * DAY_MS),
    TopicSpec("investigations.completed.v1", partitions=1, retention_ms=7 * DAY_MS),
    TopicSpec("processing.failed.v1", partitions=1, retention_ms=7 * DAY_MS),
)


TOPIC_BY_EVENT_TYPE: dict[EventType, str] = {
    EventType.SIGNAL_RAW: "signals.raw.v1",
    EventType.SIGNAL_NORMALIZED: "signals.normalized.v1",
    EventType.INCIDENT_CHANGED: "incidents.changed.v1",
    EventType.INVESTIGATION_REQUESTED: "investigations.requested.v1",
    EventType.INVESTIGATION_COMPLETED: "investigations.completed.v1",
    EventType.PROCESSING_FAILED: "processing.failed.v1",
}


def message_key(envelope: EventEnvelope[EventPayload]) -> str:
    """The partition key for an envelope, derived only from stable identifiers.

    Optional parts are omitted rather than replaced with a placeholder, so a
    signal without a service hint or resolved service shares a partition with
    the rest of its source or workspace.
    """
    payload = envelope.payload
    match payload:
        case RawSignalPayload():
            parts = [str(envelope.source_id), payload.service_hint]
        case NormalizedSignalPayload():
            parts = [str(envelope.workspace_id), _optional(payload.service_id)]
        case IncidentChangedPayload():
            parts = [str(payload.incident_id)]
        case InvestigationRequestedPayload() | InvestigationCompletedPayload():
            parts = [str(payload.investigation_id)]
        case ProcessingFailedPayload():
            if payload.original_event_id is not None:
                parts = [str(payload.original_event_id)]
            else:
                # Unparseable original: its position identifies it across redeliveries.
                position = (payload.original_topic, payload.original_partition)
                parts = [*map(str, position), str(payload.original_offset)]
        case _:
            raise TypeError(f"no message key rule for {type(payload).__name__}")
    return ":".join(part for part in parts if part)


def _optional(value: object | None) -> str | None:
    return None if value is None else str(value)


def topic_for(envelope: EventEnvelope[EventPayload]) -> str:
    return TOPIC_BY_EVENT_TYPE[envelope.event_type]


class TopicMismatchError(RuntimeError):
    """An existing topic has a different partition count than its spec."""


def ensure_topics(
    admin: AdminClient,
    specs: tuple[TopicSpec, ...] = TOPICS,
    *,
    replication_factor: int = 1,
    timeout_seconds: float = 30.0,
) -> list[str]:
    """Create missing topics and verify existing ones. Returns the names created.

    An existing topic with the wrong partition count is an error rather than
    being altered, because changing partitions changes key ordering.
    """
    existing = admin.list_topics(timeout=timeout_seconds).topics
    for spec in specs:
        metadata = existing.get(spec.name)
        if metadata is not None and len(metadata.partitions) != spec.partitions:
            raise TopicMismatchError(
                f"{spec.name} has {len(metadata.partitions)} partitions, expected {spec.partitions}"
            )

    missing = [spec for spec in specs if spec.name not in existing]
    if not missing:
        return []

    futures = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=replication_factor,
                config=spec.config(),
            )
            for spec in missing
        ],
        request_timeout=timeout_seconds,
    )
    created: list[str] = []
    for name, future in futures.items():
        try:
            future.result()
        except KafkaException as exc:
            # A concurrent init-topics run may have created it between list and create.
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        else:
            created.append(name)
    logger.info("kafka topics created", extra={"topics": created})
    return created
