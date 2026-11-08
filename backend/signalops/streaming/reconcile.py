"""Account for every accepted raw record after the pipeline drains.

An accepted record is processed when the normalizer's receipt for its raw event
exists (the receipt commits with the stored signal), and explicitly quarantined
when `processing.failed.v1` holds a failure event naming it. Anything else is
missing: accepted, then lost or still in flight. Raw event IDs derive from the
source, replay scope, and `source_event_id` (`raw_event_id`), so the caller only
needs the records it sent.
"""

import uuid
from collections.abc import Collection, Iterable
from dataclasses import dataclass

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import func, select

from signalops.adapters.ingestion import raw_event_id
from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP
from signalops.config import Settings
from signalops.contracts.common import ContractError
from signalops.contracts.events import EventType, ProcessingFailedPayload, decode_event
from signalops.persistence.database import Database
from signalops.persistence.models import ConsumerReceipt, Signal
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE

# Receipt lookups per query; keeps parameter lists bounded for large runs.
_CHUNK = 10_000


@dataclass(frozen=True)
class RecordKey:
    source_id: uuid.UUID
    source_event_id: str


@dataclass(frozen=True)
class Reconciliation:
    accepted: int
    processed: int
    quarantined: tuple[uuid.UUID, ...]
    missing: tuple[uuid.UUID, ...]
    # Stored signals for these records; equal to `processed` unless a record was stored twice.
    signals: int

    @property
    def complete(self) -> bool:
        return not self.missing and self.signals == self.processed


def raw_ids(records: Iterable[RecordKey], replay_scope: str) -> dict[uuid.UUID, RecordKey]:
    return {raw_event_id(r.source_id, replay_scope, r.source_event_id): r for r in records}


async def reconcile(
    database: Database,
    settings: Settings,
    records: Collection[RecordKey],
    replay_scope: str,
    *,
    scan_timeout_seconds: float = 30.0,
) -> Reconciliation:
    ids = raw_ids(records, replay_scope)
    processed: set[uuid.UUID] = set()
    signals = 0
    keys = list(ids)
    async with database.transaction() as session:
        for start in range(0, len(keys), _CHUNK):
            chunk = keys[start : start + _CHUNK]
            processed.update(
                await session.scalars(
                    select(ConsumerReceipt.event_id).where(
                        ConsumerReceipt.consumer_group == NORMALIZER_CONSUMER_GROUP,
                        ConsumerReceipt.event_id.in_(chunk),
                    )
                )
            )
        by_source: dict[uuid.UUID, list[str]] = {}
        for record in ids.values():
            by_source.setdefault(record.source_id, []).append(record.source_event_id)
        for source_id, event_ids in by_source.items():
            for start in range(0, len(event_ids), _CHUNK):
                signals += (
                    await session.scalar(
                        select(func.count()).where(
                            Signal.source_id == source_id,
                            Signal.replay_scope == replay_scope,
                            Signal.source_event_id.in_(event_ids[start : start + _CHUNK]),
                        )
                    )
                    or 0
                )
    unprocessed = set(ids) - processed
    quarantined = (
        quarantined_ids(settings, unprocessed, timeout_seconds=scan_timeout_seconds)
        if unprocessed
        else set()
    )
    return Reconciliation(
        accepted=len(ids),
        processed=len(processed),
        quarantined=tuple(sorted(quarantined)),
        missing=tuple(sorted(unprocessed - quarantined)),
        signals=signals,
    )


def quarantined_ids(
    settings: Settings, candidates: Collection[uuid.UUID], *, timeout_seconds: float
) -> set[uuid.UUID]:
    """Candidates named by a normalizer failure event, read from the start of the topic."""
    topic = TOPIC_BY_EVENT_TYPE[EventType.PROCESSING_FAILED]
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # Never commits, so the reader leaves no group state behind.
            "group.id": f"signalops-reconcile-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    found: set[uuid.UUID] = set()
    wanted = set(candidates)
    try:
        metadata = consumer.list_topics(topic, timeout=timeout_seconds)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        ends = {
            tp.partition: consumer.get_watermark_offsets(tp, timeout=timeout_seconds)[1]
            for tp in partitions
        }
        consumer.assign([TopicPartition(topic, p, 0) for p in ends if ends[p] > 0])
        remaining = {p for p, end in ends.items() if end > 0}
        while remaining:
            message = consumer.poll(timeout_seconds)
            if message is None:
                raise TimeoutError("processing.failed.v1 was not read to its end")
            if message.error() is not None:
                continue
            partition, offset = message.partition(), message.offset()
            if partition is not None and offset is not None and offset + 1 >= ends[partition]:
                remaining.discard(partition)
            try:
                payload = decode_event(message.value() or b"").payload
            except ContractError:
                continue
            if (
                isinstance(payload, ProcessingFailedPayload)
                and payload.consumer_group == NORMALIZER_CONSUMER_GROUP
                and payload.original_event_id in wanted
            ):
                found.add(payload.original_event_id)
    finally:
        consumer.close()
    return found
