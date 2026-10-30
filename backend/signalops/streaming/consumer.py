"""Kafka consumption with database-first effects and manual offset commits.

Each record is handled to completion before the next poll: decode, then one
transaction that inserts the consumer receipt, applies the handler's effect,
and stages outbox rows, then a synchronous commit of the next offset. Because
nothing runs ahead of the record being processed, a committed offset never
passes an unfinished earlier record in its partition.

A crash between the database commit and the offset commit redelivers the
record; the receipt turns the second run into a no-op that only commits the
offset. A record that fails to decode or to apply stops the runner without
committing, so it is redelivered after restart. Quarantine of permanent
failures to processing.failed.v1 arrives with P03-T04; until then a poison
record halts its consumer.
"""

import asyncio
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Protocol

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.config import Settings
from signalops.contracts.common import ContractError
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.persistence.database import Database
from signalops.persistence.repositories.reliability import ReceiptRepository

logger = logging.getLogger(__name__)


class ProcessOutcome(StrEnum):
    APPLIED = "applied"
    # The receipt already existed; the effect was not repeated.
    DUPLICATE = "duplicate"


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


@dataclass(frozen=True)
class RecordPosition:
    topic: str
    partition: int
    offset: int


class FailureStage(StrEnum):
    DECODE = "decode"
    PROCESS = "process"


class RecordProcessingError(Exception):
    """A record was not processed; its offset was not committed."""

    def __init__(self, position: RecordPosition, stage: FailureStage, detail: str) -> None:
        super().__init__(
            f"{stage} failed at {position.topic}[{position.partition}]@{position.offset}: {detail}"
        )
        self.position = position
        self.stage = stage


class KafkaConsumerLike(Protocol):
    def poll(self, timeout: float) -> Message | None: ...

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition] | None: ...

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


def create_consumer(
    settings: Settings, *, group_id: str, client_id: str, topics: list[str]
) -> Consumer:
    config = consumer_config(settings, group_id=group_id, client_id=client_id)
    config["logger"] = logging.getLogger("signalops.kafka")
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer


class ConsumerRunner:
    def __init__(
        self,
        consumer: KafkaConsumerLike,
        processor: RecordProcessor,
        *,
        poll_timeout_seconds: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._poll_timeout = poll_timeout_seconds

    async def run(self, stop: asyncio.Event) -> None:
        """Process records until `stop` is set or a record fails; always closes the consumer."""
        try:
            while not stop.is_set():
                await self.run_once()
        finally:
            await asyncio.to_thread(self._consumer.close)

    async def run_once(self) -> ProcessOutcome | None:
        """Poll once and process the record, if any, through its offset commit."""
        # Rebalance callbacks run inside poll, when no record is in progress.
        record = await asyncio.to_thread(self._consumer.poll, self._poll_timeout)
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
        try:
            # A tombstone (no value) fails decoding like any other malformed record.
            envelope = decode_event(record.value() or b"")
        except ContractError as exc:
            raise RecordProcessingError(position, FailureStage.DECODE, str(exc)) from exc
        try:
            outcome = await self._processor.process(envelope)
        except Exception as exc:
            raise RecordProcessingError(position, FailureStage.PROCESS, type(exc).__name__) from exc
        await asyncio.to_thread(self._commit, position)
        return outcome

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
                extra={
                    "topic": position.topic,
                    "partition": position.partition,
                    "offset": position.offset,
                    "error_detail": str(exc.args[0]),
                },
            )
