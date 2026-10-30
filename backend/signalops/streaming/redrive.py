"""Controlled redrive of one quarantined record.

The operator names the original record by the topic, partition, and offset in
its processing.failed.v1 event. The record is read back from Kafka (it must
still be retained) and republished to the same topic with its original key,
value, and headers, plus a fresh redrive attempt ID in `REDRIVE_HEADER`. The
envelope is unchanged, so its event_id stays the original's: consumer groups
that already applied the event skip it by receipt, and the group that
quarantined it, which wrote no receipt, processes it again.
"""

import logging
import time
import uuid
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from signalops.config import Settings
from signalops.contracts.events import EventType
from signalops.streaming.consumer import consumer_config
from signalops.streaming.failures import REDRIVE_HEADER, RecordPosition, envelope_event_id
from signalops.streaming.producer import EventProducer, OutgoingMessage
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE, TOPICS

logger = logging.getLogger(__name__)

# Failure events are triage records, not inputs to redrive.
REDRIVABLE_TOPICS = frozenset(spec.name for spec in TOPICS) - {
    TOPIC_BY_EVENT_TYPE[EventType.PROCESSING_FAILED]
}
REDRIVE_CLIENT_ID = "signalops-redrive"


class RedriveError(RuntimeError):
    """The record could not be redriven; nothing was published."""


@dataclass(frozen=True)
class RedriveResult:
    attempt_id: uuid.UUID
    event_id: uuid.UUID | None
    topic: str
    partition: int
    offset: int


def fetch_record(consumer: Consumer, position: RecordPosition, timeout_seconds: float) -> Message:
    """Read exactly the record at `position` with a consumer that never commits."""
    consumer.assign([TopicPartition(position.topic, position.partition, position.offset)])
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        record = consumer.poll(1.0)
        if record is None:
            continue
        error = record.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            continue
        offset = record.offset()
        if offset == position.offset:
            return record
        # An out-of-range offset makes the consumer reset to the earliest one.
        reason = (
            "is no longer retained"
            if offset is not None and offset > position.offset
            else "is beyond the end of the partition"
        )
        raise RedriveError(
            f"{position.topic}[{position.partition}]@{position.offset} {reason};"
            f" earliest available offset is {offset}"
        )
    raise RedriveError(
        f"{position.topic}[{position.partition}]@{position.offset} was not read within"
        f" {timeout_seconds:g}s"
    )


def redrive_message(record: Message, attempt_id: uuid.UUID) -> OutgoingMessage:
    topic, value, key = record.topic(), record.value(), record.key()
    if topic is None or value is None:
        raise RedriveError("the record has no value; there is nothing to redrive")
    if key is None:
        raise RedriveError("the record has no key; its partition cannot be preserved")
    raw_headers = record.headers() or []
    items = raw_headers.items() if isinstance(raw_headers, dict) else raw_headers
    # Earlier redrive attempt IDs are replaced, not accumulated.
    kept = tuple(
        (name, h if isinstance(h, bytes) else (h or "").encode())
        for name, h in items
        if name != REDRIVE_HEADER
    )
    body = value if isinstance(value, bytes) else value.encode()
    return OutgoingMessage(
        topic=topic,
        key=key.decode() if isinstance(key, bytes) else key,
        value=body,
        event_id=envelope_event_id(body),
        headers=(*kept, (REDRIVE_HEADER, str(attempt_id).encode())),
    )


def redrive_record(
    settings: Settings,
    position: RecordPosition,
    *,
    allowed_topics: frozenset[str] = REDRIVABLE_TOPICS,
    timeout_seconds: float = 30.0,
) -> RedriveResult:
    if position.topic not in allowed_topics:
        raise RedriveError(f"{position.topic} is not a redrivable topic")
    config = consumer_config(settings, group_id=REDRIVE_CLIENT_ID, client_id=REDRIVE_CLIENT_ID)
    config["logger"] = logging.getLogger("signalops.kafka")
    consumer = Consumer(config)
    try:
        record = fetch_record(consumer, position, timeout_seconds)
    finally:
        consumer.close()

    attempt_id = uuid.uuid4()
    message = redrive_message(record, attempt_id)
    producer = EventProducer(settings, client_id=REDRIVE_CLIENT_ID)
    try:
        [result] = producer.publish([message])
    finally:
        producer.close()
    if result.error is not None or result.partition is None or result.offset is None:
        detail = f"{result.error.category}: {result.error.detail}" if result.error else "no ack"
        raise RedriveError(f"redrive was not acknowledged ({detail}); it may be retried")
    outcome = RedriveResult(
        attempt_id, message.event_id, message.topic, result.partition, result.offset
    )
    logger.info(
        "record redriven",
        extra={
            "redrive_attempt_id": str(attempt_id),
            "event_id": str(outcome.event_id) if outcome.event_id else None,
            "original_topic": position.topic,
            "original_partition": position.partition,
            "original_offset": position.offset,
            "partition": outcome.partition,
            "offset": outcome.offset,
        },
    )
    return outcome
