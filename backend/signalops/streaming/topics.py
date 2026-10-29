"""Kafka topic catalogue and idempotent provisioning.

Partition counts and retention follow SignalOps_Implementation_Plan.md section 4
and section 5 (initial local retention targets). Topics are never auto-created.
"""

import logging
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

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
