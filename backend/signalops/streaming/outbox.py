"""Outbox publisher: send committed outbox rows to Kafka, then mark them published.

Each poll reads a bounded batch of pending rows in one short transaction,
publishes outside any transaction, and marks acknowledged rows in another short
transaction. A crash after the broker acknowledges but before the mark commits
republishes the row on a later poll with the same event_id and key; consumers
deduplicate on their receipts. A failed delivery leaves the row pending with
the attempt counted.

Order assumes a single publisher process (the MVP). Within a batch, each
aggregate's rows go out in aggregate_version order, one version per round, and
an aggregate stops for the rest of the batch at its first failure, so a later
version is never acknowledged before an earlier one. A row the broker rejects
permanently keeps blocking its aggregate until P03-T04 adds quarantine.
"""

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from signalops.persistence.database import Database
from signalops.persistence.models import OutboxEvent
from signalops.persistence.repositories.reliability import OutboxRepository
from signalops.streaming.producer import DeliveryResult, OutgoingMessage

logger = logging.getLogger(__name__)


class MessagePublisher(Protocol):
    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]: ...


@dataclass(frozen=True)
class PublishSummary:
    published: int = 0
    failed: int = 0
    # Rows behind a failed earlier version of their aggregate, left pending.
    deferred: int = 0


def outgoing_message(row: OutboxEvent) -> OutgoingMessage:
    """The stored topic, key, and envelope; nothing is re-derived from current code."""
    value = json.dumps(row.envelope, separators=(",", ":"), ensure_ascii=False).encode()
    return OutgoingMessage(row.topic, row.message_key, value, row.id)


def publish_rounds(rows: Sequence[OutboxEvent]) -> list[list[OutboxEvent]]:
    """Round k holds each aggregate's k-th lowest pending version (all its topics)."""
    by_aggregate: dict[uuid.UUID, dict[int, list[OutboxEvent]]] = {}
    for row in rows:
        versions = by_aggregate.setdefault(row.aggregate_id, {})
        versions.setdefault(row.aggregate_version, []).append(row)
    queues = [[versions[v] for v in sorted(versions)] for versions in by_aggregate.values()]
    depth = max((len(queue) for queue in queues), default=0)
    return [[row for queue in queues if k < len(queue) for row in queue[k]] for k in range(depth)]


class OutboxPublisher:
    def __init__(
        self,
        database: Database,
        producer: MessagePublisher,
        *,
        batch_size: int = 100,
        idle_interval_seconds: float = 0.5,
    ) -> None:
        self._database = database
        self._producer = producer
        self._batch_size = batch_size
        self._idle_interval = idle_interval_seconds

    async def run(self, stop: asyncio.Event) -> None:
        """Publish until `stop` is set; waits only after a poll that published nothing."""
        while not stop.is_set():
            summary = await self.publish_once()
            if summary.published == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._idle_interval)

    async def publish_once(self) -> PublishSummary:
        async with self._database.transaction() as session:
            rows = await OutboxRepository(session).pending(self._batch_size)

        published = failed = deferred = 0
        blocked: set[uuid.UUID] = set()
        for round_rows in publish_rounds(rows):
            ready = [row for row in round_rows if row.aggregate_id not in blocked]
            deferred += len(round_rows) - len(ready)
            if not ready:
                continue
            # Raises ProducerUnusableError after a fatal error; rows stay pending.
            results = await asyncio.to_thread(
                self._producer.publish, [outgoing_message(row) for row in ready]
            )
            delivered: list[uuid.UUID] = []
            undelivered: list[uuid.UUID] = []
            for row, result in zip(ready, results, strict=True):
                if result.delivered:
                    delivered.append(row.id)
                else:
                    undelivered.append(row.id)
                    blocked.add(row.aggregate_id)
            async with self._database.transaction() as session:
                outbox = OutboxRepository(session)
                await outbox.mark_published(delivered)
                await outbox.record_failed_attempts(undelivered)
            published += len(delivered)
            failed += len(undelivered)

        summary = PublishSummary(published, failed, deferred)
        if rows:
            logger.info(
                "outbox batch published",
                extra={
                    "outbox_published": summary.published,
                    "outbox_failed": summary.failed,
                    "outbox_deferred": summary.deferred,
                },
            )
        return summary
