"""Kafka consumption with database-first effects and manual offset commits.

Each record is handled to completion before the next poll: decode, then one
transaction that inserts the consumer receipt, applies the handler's effect,
and stages outbox rows, then a synchronous commit of the next offset. Because
nothing runs ahead of the record being processed, a committed offset never
passes an unfinished earlier record in its partition.

A crash between the database commit and the offset commit redelivers the
record; the receipt turns the second run into a no-op that only commits the
offset.

Failures (see `streaming.failures`):

- Permanent (`ContractError` from decoding or from the handler): the record is
  published to processing.failed.v1 and its offset committed only after the
  broker acknowledges the failure event. No receipt is written, so a redrive
  of the record is processed normally.
- Transient (any other exception, or a failure event that was not
  acknowledged): the partition is paused and rewound to the record, and the
  runner keeps polling, so other partitions continue and the poll deadline is
  never exceeded. When the backoff elapses the partition resumes and the
  record is redelivered. After `RetryPolicy.max_attempts` failures the runner
  stops with `RecordProcessingError`, still without committing.

Lifecycle: `stop` is checked between records, so a stopped runner finishes the
record in hand through its commit, then closes the consumer, which leaves the
group. Rebalance callbacks run inside `poll`, when no record is in progress;
a revoked partition's retry state is dropped and its pause lifted, because its
next owner (possibly this process again) starts from the committed offset.

Every `stats_interval_seconds` the runner logs `consumer stats`: counts since
the previous report, processing latency, and lag per assigned partition
(high watermark minus committed offset), so a growing backlog is visible.
"""

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.persistence.database import Database
from signalops.persistence.repositories.reliability import ReceiptRepository
from signalops.streaming.failures import RecordPosition, failure_envelope, redrive_attempt_id
from signalops.streaming.producer import DeliveryResult, OutgoingMessage
from signalops.streaming.retry import RetryPolicy

logger = logging.getLogger(__name__)


class ProcessOutcome(StrEnum):
    APPLIED = "applied"
    # The receipt already existed; the effect was not repeated.
    DUPLICATE = "duplicate"
    # Runner outcomes. The failure event was acknowledged and the offset committed.
    QUARANTINED = "quarantined"
    # The partition is paused until the backoff elapses; nothing was committed.
    RETRY_SCHEDULED = "retry_scheduled"


class EventHandler(Protocol):
    """Applies one event's domain effect inside the caller's transaction.

    It must not commit, publish to Kafka, or call a model. Events it emits go
    through `OutboxRepository` on the same session.
    """

    async def __call__(
        self, session: AsyncSession, envelope: EventEnvelope[EventPayload]
    ) -> None: ...


class RecordProcessor(Protocol):
    async def process(self, envelope: EventEnvelope[EventPayload]) -> ProcessOutcome: ...


class TransactionalProcessor:
    def __init__(self, database: Database, consumer_group: str, handler: EventHandler) -> None:
        self._database = database
        self._consumer_group = consumer_group
        self._handler = handler

    async def process(self, envelope: EventEnvelope[EventPayload]) -> ProcessOutcome:
        async with self._database.transaction() as session:
            if not await ReceiptRepository(session).record(self._consumer_group, envelope.event_id):
                return ProcessOutcome.DUPLICATE
            await self._handler(session, envelope)
        return ProcessOutcome.APPLIED


class FailureStage(StrEnum):
    DECODE = "decode"
    PROCESS = "process"
    QUARANTINE = "quarantine"


class RecordProcessingError(Exception):
    """A record was not processed after its retries; its offset was not committed."""

    def __init__(
        self, position: RecordPosition, stage: FailureStage, detail: str, *, attempts: int = 1
    ) -> None:
        super().__init__(
            f"{stage} failed at {position.topic}[{position.partition}]@{position.offset}"
            f" after {attempts} attempt(s): {detail}"
        )
        self.position = position
        self.stage = stage
        self.attempts = attempts


class MessagePublisher(Protocol):
    def publish(self, messages: list[OutgoingMessage]) -> list[DeliveryResult]: ...


class KafkaConsumerLike(Protocol):
    def poll(self, timeout: float) -> Message | None: ...

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition] | None: ...

    def pause(self, partitions: list[TopicPartition]) -> None: ...

    def resume(self, partitions: list[TopicPartition]) -> None: ...

    def seek(self, partition: TopicPartition) -> None: ...

    def subscribe(
        self,
        topics: list[str],
        *,
        on_assign: Callable[[Any, list[TopicPartition]], None],
        on_revoke: Callable[[Any, list[TopicPartition]], None],
        on_lost: Callable[[Any, list[TopicPartition]], None],
    ) -> None: ...

    def assignment(self) -> list[TopicPartition]: ...

    def committed(
        self, partitions: list[TopicPartition], timeout: float
    ) -> list[TopicPartition]: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float, cached: bool
    ) -> tuple[int, int] | None: ...

    def close(self) -> None: ...


def consumer_config(settings: Settings, *, group_id: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        # Offsets are committed explicitly after the database transaction commits.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        # A new group starts from retained history rather than skipping it.
        "auto.offset.reset": "earliest",
    }


def create_consumer(settings: Settings, *, group_id: str, client_id: str) -> Consumer:
    """An unsubscribed consumer; `ConsumerRunner.subscribe` adds the rebalance callbacks."""
    config = consumer_config(settings, group_id=group_id, client_id=client_id)
    config["logger"] = logging.getLogger("signalops.kafka")
    return Consumer(config)


@dataclass
class _PendingRetry:
    position: RecordPosition
    failures: int
    due_at: float
    resumed: bool = False


@dataclass
class ConsumerStats:
    """Counts since the last report. Redeliveries are counted again."""

    consumed: int = 0
    applied: int = 0
    duplicates: int = 0
    quarantined: int = 0
    retries_scheduled: int = 0
    processing_seconds_total: float = 0.0
    processing_seconds_max: float = 0.0
    # Records that reached a committed outcome, the latency denominator.
    completed: int = 0

    def observe(self, outcome: ProcessOutcome, seconds: float) -> None:
        if outcome is ProcessOutcome.RETRY_SCHEDULED:
            self.retries_scheduled += 1
            return
        if outcome is ProcessOutcome.APPLIED:
            self.applied += 1
        elif outcome is ProcessOutcome.DUPLICATE:
            self.duplicates += 1
        else:
            self.quarantined += 1
        self.completed += 1
        self.processing_seconds_total += seconds
        self.processing_seconds_max = max(self.processing_seconds_max, seconds)

    def log_fields(self) -> dict[str, Any]:
        average = self.processing_seconds_total / self.completed if self.completed else None
        return {
            "consumed": self.consumed,
            "applied": self.applied,
            "duplicates": self.duplicates,
            "quarantined": self.quarantined,
            "retries_scheduled": self.retries_scheduled,
            "processing_ms_avg": round(average * 1000, 3) if average is not None else None,
            "processing_ms_max": round(self.processing_seconds_max * 1000, 3),
        }


class ConsumerRunner:
    def __init__(
        self,
        consumer: KafkaConsumerLike,
        processor: RecordProcessor,
        *,
        consumer_group: str,
        failure_publisher: MessagePublisher,
        retry_policy: RetryPolicy | None = None,
        poll_timeout_seconds: float = 1.0,
        stats_interval_seconds: float = 30.0,
        broker_timeout_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._consumer_group = consumer_group
        self._failure_publisher = failure_publisher
        self._policy = retry_policy or RetryPolicy()
        self._poll_timeout = poll_timeout_seconds
        self._stats_interval = stats_interval_seconds
        self._broker_timeout = broker_timeout_seconds
        self._clock = clock
        self._rng = rng
        self._retries: dict[tuple[str, int], _PendingRetry] = {}
        self._stats = ConsumerStats()
        self._stats_since = clock()

    def subscribe(self, topics: list[str]) -> None:
        self._consumer.subscribe(
            topics, on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_lost
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Process records until `stop` is set or a record fails; always closes the consumer.

        A stop takes effect between records: the record in hand is committed
        (or left for redelivery) before the consumer closes.
        """
        try:
            while not stop.is_set():
                await self.run_once()
                if self._clock() - self._stats_since >= self._stats_interval:
                    await self.report_stats()
            await self.report_stats()
        finally:
            # Leaves the group; the revoke callback runs inside close.
            await asyncio.to_thread(self._consumer.close)
            logger.info("consumer stopped", extra={"consumer_group": self._consumer_group})

    async def report_stats(self) -> dict[str, Any]:
        """Log and return counts since the last report plus current lag, then reset counts."""
        now = self._clock()
        lag = await asyncio.to_thread(self._lag)
        report: dict[str, Any] = {
            "consumer_group": self._consumer_group,
            "interval_seconds": round(now - self._stats_since, 3),
            **self._stats.log_fields(),
            "paused_partitions": sum(1 for p in self._retries.values() if not p.resumed),
            "lag_total": sum(lag.values()) if lag is not None else None,
            "lag": lag,
        }
        self._stats = ConsumerStats()
        self._stats_since = now
        logger.info("consumer stats", extra=report)
        return report

    async def run_once(self) -> ProcessOutcome | None:
        """Poll once and handle the record, if any, through its offset commit or retry."""
        self._resume_due()
        # Rebalance callbacks run inside poll, when no record is in progress.
        record = await asyncio.to_thread(self._consumer.poll, self._next_poll_timeout())
        if record is None:
            return None
        error = record.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            logger.warning(
                "kafka consumer error", extra={"error_detail": f"{error.name()}: {error.str()}"}
            )
            return None

        topic, partition, offset = record.topic(), record.partition(), record.offset()
        if topic is None or partition is None or offset is None:
            raise RuntimeError("kafka returned a record without a position")
        position = RecordPosition(topic, partition, offset)
        pending = self._retries.get((topic, partition))
        if pending is not None and pending.position != position:
            # Fetched before the pause took effect (or after a rebalance): rewind
            # to the record awaiting retry and keep waiting for its backoff.
            self._rewind(pending)
            return None

        self._stats.consumed += 1
        started = self._clock()
        outcome = await self._handle(record, position)
        self._stats.observe(outcome, self._clock() - started)
        return outcome

    async def _handle(self, record: Message, position: RecordPosition) -> ProcessOutcome:
        value = record.value() or b""
        redrive = redrive_attempt_id(record.headers())
        try:
            # A tombstone (no value) fails decoding like any other malformed record.
            envelope = decode_event(value)
        except ContractError as exc:
            return await self._quarantine(position, value, exc, None, redrive)
        try:
            outcome = await self._processor.process(envelope)
        except ContractError as exc:
            return await self._quarantine(position, value, exc, envelope, redrive)
        except Exception as exc:  # noqa: BLE001 - never acknowledged; retried, then raised
            return self._retry_or_stop(position, FailureStage.PROCESS, type(exc).__name__, exc)
        self._retries.pop((position.topic, position.partition), None)
        await asyncio.to_thread(self._commit, position)
        return outcome

    async def _quarantine(
        self,
        position: RecordPosition,
        value: bytes,
        error: ContractError,
        original: EventEnvelope[EventPayload] | None,
        redrive: uuid.UUID | None,
    ) -> ProcessOutcome:
        failure = failure_envelope(
            consumer_group=self._consumer_group,
            position=position,
            value=value,
            error=error,
            original=original,
            redrive_attempt=redrive,
            failed_at=datetime.now(UTC),
        )
        # Raises ProducerUnusableError after a fatal producer error; nothing is committed.
        [result] = await asyncio.to_thread(
            self._failure_publisher.publish, [OutgoingMessage.from_envelope(failure)]
        )
        if result.error is not None:
            detail = f"{result.error.category}: {result.error.detail}"
            return self._retry_or_stop(position, FailureStage.QUARANTINE, detail, None)
        logger.warning(
            "record quarantined",
            extra={
                "consumer_group": self._consumer_group,
                **_position_fields(position),
                "event_id": str(original.event_id) if original is not None else None,
                "failure_event_id": str(failure.event_id),
                "error_category": error.category.value,
                "error_detail": error.detail,
                "redrive_attempt_id": str(redrive) if redrive is not None else None,
            },
        )
        self._retries.pop((position.topic, position.partition), None)
        await asyncio.to_thread(self._commit, position)
        return ProcessOutcome.QUARANTINED

    def _retry_or_stop(
        self,
        position: RecordPosition,
        stage: FailureStage,
        detail: str,
        cause: BaseException | None,
    ) -> ProcessOutcome:
        key = (position.topic, position.partition)
        previous = self._retries.get(key)
        failures = previous.failures + 1 if previous and previous.position == position else 1
        fields = {
            "consumer_group": self._consumer_group,
            **_position_fields(position),
            "failure_stage": stage.value,
            "attempt": failures,
            "max_attempts": self._policy.max_attempts,
            "error_detail": detail,
        }
        if self._policy.exhausted(failures):
            self._retries.pop(key, None)
            logger.error("record retries exhausted", extra=fields)
            raise RecordProcessingError(position, stage, detail, attempts=failures) from cause
        delay = self._policy.delay(failures, self._rng)
        pending = _PendingRetry(position, failures, self._clock() + delay)
        self._retries[key] = pending
        self._rewind(pending)
        logger.warning("record retry scheduled", extra={**fields, "retry_delay_seconds": delay})
        return ProcessOutcome.RETRY_SCHEDULED

    def _rewind(self, pending: _PendingRetry) -> None:
        """Pause the partition (unless its backoff elapsed) and seek back to the record."""
        position = pending.position
        partition = TopicPartition(position.topic, position.partition, position.offset)
        if pending.due_at > self._clock():
            self._consumer.pause([partition])
            pending.resumed = False
        self._consumer.seek(partition)

    def _resume_due(self) -> None:
        now = self._clock()
        due = [p for p in self._retries.values() if not p.resumed and p.due_at <= now]
        if not due:
            return
        self._consumer.resume([TopicPartition(p.position.topic, p.position.partition) for p in due])
        for pending in due:
            pending.resumed = True

    def _next_poll_timeout(self) -> float:
        waiting = [p.due_at for p in self._retries.values() if not p.resumed]
        if not waiting:
            return self._poll_timeout
        return max(0.0, min(self._poll_timeout, min(waiting) - self._clock()))

    def _commit(self, position: RecordPosition) -> None:
        # The committed offset is the next record to read.
        next_offset = TopicPartition(position.topic, position.partition, position.offset + 1)
        try:
            self._consumer.commit(offsets=[next_offset], asynchronous=False)
        except KafkaException as exc:
            # The effect is durable; a later commit covers this offset, and if the
            # partition moved, its new owner redelivers and hits the receipt.
            logger.warning(
                "kafka offset commit failed",
                extra={**_position_fields(position), "error_detail": str(exc.args[0])},
            )

    def _lag(self) -> dict[str, int] | None:
        """High watermark minus committed offset per assigned partition; None if unavailable.

        Without a committed offset the group starts from the earliest retained
        record, so the low watermark stands in for it.
        """
        try:
            assigned = self._consumer.assignment()
            if not assigned:
                return {}
            committed = self._consumer.committed(assigned, timeout=self._broker_timeout)
            lag: dict[str, int] = {}
            for tp in committed:
                watermarks = self._consumer.get_watermark_offsets(
                    tp, timeout=self._broker_timeout, cached=False
                )
                if watermarks is None:
                    return None
                low, high = watermarks
                start = tp.offset if tp.offset >= 0 else low
                lag[f"{tp.topic}[{tp.partition}]"] = max(0, high - start)
        except KafkaException as exc:
            logger.warning("consumer lag unavailable", extra={"error_detail": str(exc.args[0])})
            return None
        return lag

    def _on_assign(self, consumer: object, partitions: list[TopicPartition]) -> None:
        logger.info(
            "partitions assigned",
            extra={"consumer_group": self._consumer_group, "partitions": _names(partitions)},
        )

    def _on_revoke(self, consumer: object, partitions: list[TopicPartition]) -> None:
        # Every record handed out before this poll was committed or left for
        # redelivery, so there is nothing to finish; only local state goes.
        self._forget(partitions)
        logger.info(
            "partitions revoked",
            extra={"consumer_group": self._consumer_group, "partitions": _names(partitions)},
        )

    def _on_lost(self, consumer: object, partitions: list[TopicPartition]) -> None:
        # Another member may already own these; commits for them would fail.
        self._forget(partitions)
        logger.warning(
            "partitions lost",
            extra={"consumer_group": self._consumer_group, "partitions": _names(partitions)},
        )

    def _forget(self, partitions: list[TopicPartition]) -> None:
        paused = []
        for tp in partitions:
            pending = self._retries.pop((tp.topic, tp.partition), None)
            if pending is not None and not pending.resumed:
                paused.append(TopicPartition(tp.topic, tp.partition))
        if paused:
            # librdkafka keeps the pause across a revoke; without this, a
            # partition reassigned to this member would never be fetched again.
            self._consumer.resume(paused)


def _names(partitions: list[TopicPartition]) -> list[str]:
    return [f"{tp.topic}[{tp.partition}]" for tp in partitions]


def _position_fields(position: RecordPosition) -> dict[str, Any]:
    return {"topic": position.topic, "partition": position.partition, "offset": position.offset}
