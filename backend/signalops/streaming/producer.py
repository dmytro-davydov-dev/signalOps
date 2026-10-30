"""Kafka producer with idempotence, full acknowledgement, and per-message outcomes.

`EventProducer.publish` blocks until every message is acknowledged or has
failed, and returns one `DeliveryResult` per message in input order. Callers
decide what a failure means: ingestion reports it in the batch outcome, the
outbox leaves the row pending. Nothing here treats an unacknowledged message
as sent. Async callers run `publish` in a worker thread.
"""

import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, Self

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from signalops.config import Settings
from signalops.contracts.events import EventEnvelope, EventPayload
from signalops.streaming.topics import message_key, topic_for

logger = logging.getLogger(__name__)

# Added to the delivery timeout when waiting in flush, so librdkafka reports
# its own timeout per message before the wait gives up.
FLUSH_MARGIN_SECONDS = 5.0


class DeliveryErrorCategory(StrEnum):
    # Not acknowledged within the delivery timeout. The broker may still have
    # written it, so a retry can duplicate; consumers deduplicate on event_id.
    TIMEOUT = "timeout"
    # Retriable broker or network error that outlasted librdkafka's own retries.
    TRANSIENT = "transient"
    # Refused for this message permanently (size, unknown topic, authorization).
    REJECTED = "rejected"
    # Local queue full; the message was never handed to librdkafka.
    QUEUE_FULL = "queue_full"
    # The idempotent producer lost its guarantees; this instance must be replaced.
    FATAL = "fatal"


@dataclass(frozen=True)
class DeliveryError:
    category: DeliveryErrorCategory
    # librdkafka error name and reason; never message contents.
    detail: str

    @classmethod
    def from_kafka(cls, error: KafkaError) -> Self:
        if error.fatal():
            category = DeliveryErrorCategory.FATAL
        elif error.code() == KafkaError._MSG_TIMED_OUT:
            category = DeliveryErrorCategory.TIMEOUT
        elif error.retriable():
            category = DeliveryErrorCategory.TRANSIENT
        else:
            category = DeliveryErrorCategory.REJECTED
        return cls(category, f"{error.name()}: {error.str()}")


@dataclass(frozen=True)
class OutgoingMessage:
    topic: str
    key: str
    value: bytes
    event_id: uuid.UUID

    @classmethod
    def from_envelope(cls, envelope: EventEnvelope[EventPayload]) -> Self:
        return cls(topic_for(envelope), message_key(envelope), envelope.encode(), envelope.event_id)


@dataclass(frozen=True)
class DeliveryResult:
    message: OutgoingMessage
    partition: int | None = None
    offset: int | None = None
    error: DeliveryError | None = None

    @property
    def delivered(self) -> bool:
        return self.error is None


class ProducerUnusableError(RuntimeError):
    """A fatal error was reported earlier; create a new producer."""


class KafkaProducerLike(Protocol):
    def produce(
        self,
        topic: str,
        value: bytes,
        key: str,
        *,
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def producer_config(settings: Settings, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": client_id,
        # Idempotence implies acks=all and bounded in-flight requests; both are
        # stated so a later edit cannot weaken one silently.
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "message.timeout.ms": settings.kafka_delivery_timeout_ms,
        "linger.ms": 5,
        # Same key-to-partition mapping as the Java client's default partitioner.
        "partitioner": "murmur2_random",
    }


class EventProducer:
    def __init__(
        self,
        settings: Settings,
        *,
        client_id: str,
        producer_factory: Callable[[dict[str, Any]], KafkaProducerLike] | None = None,
    ) -> None:
        config = producer_config(settings, client_id)
        if producer_factory is None:
            config["logger"] = logging.getLogger("signalops.kafka")
            self._producer: KafkaProducerLike = Producer(config)
        else:
            self._producer = producer_factory(config)
        self._flush_timeout = settings.kafka_delivery_timeout_ms / 1000 + FLUSH_MARGIN_SECONDS
        self._fatal: DeliveryError | None = None

    def publish(self, messages: Sequence[OutgoingMessage]) -> list[DeliveryResult]:
        if self._fatal is not None:
            raise ProducerUnusableError(self._fatal.detail)
        results: list[DeliveryResult | None] = [None] * len(messages)

        def callback_for(index: int) -> Callable[[KafkaError | None, Message], None]:
            def on_delivery(error: KafkaError | None, record: Message) -> None:
                message = messages[index]
                if error is None:
                    results[index] = DeliveryResult(message, record.partition(), record.offset())
                else:
                    results[index] = DeliveryResult(message, error=DeliveryError.from_kafka(error))

            return on_delivery

        for index, message in enumerate(messages):
            try:
                self._producer.produce(
                    message.topic, message.value, message.key, on_delivery=callback_for(index)
                )
            except BufferError:
                error = DeliveryError(DeliveryErrorCategory.QUEUE_FULL, "local producer queue full")
                results[index] = DeliveryResult(message, error=error)
            except KafkaException as exc:
                results[index] = DeliveryResult(
                    message, error=DeliveryError.from_kafka(exc.args[0])
                )
            # Serve callbacks for earlier messages so the local queue drains.
            self._producer.poll(0)
        self._producer.flush(self._flush_timeout)

        timed_out = DeliveryError(DeliveryErrorCategory.TIMEOUT, "no acknowledgement before flush")
        final = [
            result if result is not None else DeliveryResult(message, error=timed_out)
            for message, result in zip(messages, results, strict=True)
        ]
        for result in final:
            if result.error is None:
                continue
            if result.error.category is DeliveryErrorCategory.FATAL:
                self._fatal = result.error
            logger.warning(
                "kafka delivery failed",
                extra={
                    "topic": result.message.topic,
                    "event_id": str(result.message.event_id),
                    "error_category": result.error.category.value,
                    "error_detail": result.error.detail,
                },
            )
        return final

    def close(self, timeout_seconds: float = 10.0) -> int:
        """Wait for outstanding deliveries; returns the number still undelivered."""
        return self._producer.flush(timeout_seconds)
