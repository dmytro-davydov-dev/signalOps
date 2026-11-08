"""Durable operational state read from PostgreSQL and the Kafka broker.

These numbers describe work rather than a process: queued and running jobs,
leases that ran out without renewal, outbox age, model calls whose outcome is
unknown, and consumer-group lag as the broker sees it. Because they come from
the stores that own the work, they stay accurate when a worker is stuck or
down, which is when they matter. `metrics-exporter` turns them into gauges and
`signalops operations-snapshot` prints them.
"""

import logging
from collections.abc import Mapping
from datetime import datetime

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._listoffsets import OffsetSpec
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP
from signalops.agents.investigations import DISPATCHER_CONSUMER_GROUP
from signalops.contracts.events import EventType
from signalops.contracts.workflow import InvestigationStatus, JobStatus, LlmCallStatus
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP
from signalops.detection.graph import GRAPH_CONSUMER_GROUP
from signalops.persistence.models import Investigation, Job, LlmCall
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE

logger = logging.getLogger(__name__)

# Each consumer group and the topic it reads.
CONSUMER_TOPICS: Mapping[str, str] = {
    NORMALIZER_CONSUMER_GROUP: TOPIC_BY_EVENT_TYPE[EventType.SIGNAL_RAW],
    DETECTOR_CONSUMER_GROUP: TOPIC_BY_EVENT_TYPE[EventType.SIGNAL_NORMALIZED],
    GRAPH_CONSUMER_GROUP: TOPIC_BY_EVENT_TYPE[EventType.SIGNAL_NORMALIZED],
    DISPATCHER_CONSUMER_GROUP: TOPIC_BY_EVENT_TYPE[EventType.INVESTIGATION_REQUESTED],
}


class PartitionLag(BaseModel):
    consumer_group: str
    topic: str
    partition: int
    # The log end offset minus the group's committed offset. Without a commit,
    # the group starts from the earliest retained record, so that stands in.
    lag: int
    committed: int | None
    end: int


class DatabaseState(BaseModel):
    outbox_pending: int
    outbox_quarantined: int
    outbox_oldest_age_seconds: float | None
    jobs: dict[str, int]
    # Queued jobs whose `available_at` has passed: waiting for a worker.
    jobs_ready: int
    jobs_ready_oldest_age_seconds: float | None
    # Running jobs whose lease ran out: the owner stopped renewing and no
    # worker has reclaimed them yet.
    jobs_expired_leases: int
    investigations: dict[str, int]
    model_calls_in_flight: int
    model_calls_unknown: int
    model_unknown_reserved_microusd: int


class OperationsSnapshot(BaseModel):
    taken_at: datetime
    database: DatabaseState
    # None when the broker did not answer in time.
    kafka_lag: list[PartitionLag] | None


def _counts(rows: object, names: list[str]) -> dict[str, int]:
    counts = dict.fromkeys(names, 0)
    for name, count in rows:  # type: ignore[attr-defined]
        counts[name] = count
    return counts


async def database_state(session: AsyncSession) -> DatabaseState:
    backlog = await OutboxRepository(session).backlog()
    now = func.clock_timestamp()
    jobs = _counts(
        await session.execute(select(Job.status, func.count()).group_by(Job.status)),
        [status.value for status in JobStatus],
    )
    ready, ready_age = (
        await session.execute(
            select(func.count(), func.extract("epoch", now - func.min(Job.available_at))).where(
                Job.status == JobStatus.QUEUED, Job.available_at <= now
            )
        )
    ).one()
    expired = await session.scalar(
        select(func.count()).where(Job.status == JobStatus.RUNNING, Job.lease_until < now)
    )
    investigations = _counts(
        await session.execute(
            select(Investigation.status, func.count()).group_by(Investigation.status)
        ),
        [status.value for status in InvestigationStatus],
    )
    in_flight, unknown, unknown_reserved = (
        await session.execute(
            select(
                func.count().filter(LlmCall.status == LlmCallStatus.PENDING),
                func.count().filter(LlmCall.status == LlmCallStatus.UNKNOWN),
                func.coalesce(
                    func.sum(LlmCall.reserved_microusd).filter(
                        LlmCall.status == LlmCallStatus.UNKNOWN
                    ),
                    0,
                ),
            ).where(LlmCall.status.in_([LlmCallStatus.PENDING, LlmCallStatus.UNKNOWN]))
        )
    ).one()
    return DatabaseState(
        outbox_pending=backlog.pending,
        outbox_quarantined=backlog.quarantined,
        outbox_oldest_age_seconds=backlog.oldest_age_seconds,
        jobs=jobs,
        jobs_ready=ready,
        jobs_ready_oldest_age_seconds=float(ready_age) if ready_age is not None else None,
        jobs_expired_leases=expired or 0,
        investigations=investigations,
        model_calls_in_flight=in_flight,
        model_calls_unknown=unknown,
        model_unknown_reserved_microusd=int(unknown_reserved),
    )


def kafka_lag(
    admin: AdminClient,
    groups: Mapping[str, str] = CONSUMER_TOPICS,
    *,
    timeout_seconds: float = 5.0,
) -> list[PartitionLag] | None:
    """Lag per group and partition from committed offsets and log end offsets."""
    try:
        topics = set(groups.values())
        metadata = admin.list_topics(timeout=timeout_seconds)
        partitions = [
            TopicPartition(topic, partition)
            for topic in sorted(topics)
            if topic in metadata.topics
            for partition in sorted(metadata.topics[topic].partitions)
        ]
        if not partitions:
            return []
        latest = OffsetSpec.latest()  # type: ignore[no-untyped-call]
        earliest = OffsetSpec.earliest()  # type: ignore[no-untyped-call]
        ends = _offsets(admin, partitions, latest, timeout_seconds)
        starts = _offsets(admin, partitions, earliest, timeout_seconds)
        lag: list[PartitionLag] = []
        for group, topic in groups.items():
            mine = [tp for tp in partitions if tp.topic == topic]
            request = [ConsumerGroupTopicPartitions(group, mine)]
            [future] = admin.list_consumer_group_offsets(
                request, request_timeout=timeout_seconds
            ).values()
            committed = {
                (tp.topic, tp.partition): tp.offset
                for tp in future.result(timeout=timeout_seconds).topic_partitions
            }
            for tp in mine:
                key = (tp.topic, tp.partition)
                offset = committed.get(key, -1)
                start = offset if offset >= 0 else starts[key]
                lag.append(
                    PartitionLag(
                        consumer_group=group,
                        topic=tp.topic,
                        partition=tp.partition,
                        lag=max(0, ends[key] - start),
                        committed=offset if offset >= 0 else None,
                        end=ends[key],
                    )
                )
    except (KafkaException, TimeoutError) as error:
        logger.warning("kafka lag unavailable", extra={"error": type(error).__name__})
        return None
    return lag


def _offsets(
    admin: AdminClient, partitions: list[TopicPartition], spec: OffsetSpec, timeout: float
) -> dict[tuple[str, int], int]:
    futures = admin.list_offsets({tp: spec for tp in partitions}, request_timeout=timeout)
    return {
        (tp.topic, tp.partition): future.result(timeout=timeout).offset
        for tp, future in futures.items()
    }
