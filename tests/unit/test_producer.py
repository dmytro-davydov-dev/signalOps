import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from signalops.config import Settings
from signalops.contracts.events import decode_event
from signalops.streaming.producer import (
    DeliveryErrorCategory,
    EventProducer,
    OutgoingMessage,
    ProducerUnusableError,
    producer_config,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"

# "silent" means no callback before flush returns.
Outcome = KafkaError | BaseException | str | None


@dataclass
class FakeRecord:
    _partition: int
    _offset: int

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


@dataclass
class FakeProducer:
    """Applies one scripted outcome per produce call; callbacks fire on flush."""

    outcomes: list[Outcome]
    config: dict[str, Any] = field(default_factory=dict)
    pending: list[tuple[Callable[..., None], Outcome]] = field(default_factory=list)
    flushed: int = 0

    def produce(
        self, topic: str, value: bytes, key: str, *, on_delivery: Callable[..., None]
    ) -> None:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        self.pending.append((on_delivery, outcome))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self.flushed += 1
        for offset, (callback, outcome) in enumerate(self.pending):
            if isinstance(outcome, str):
                continue
            callback(outcome, FakeRecord(0, offset))
        self.pending.clear()
        return 0


def _settings() -> Settings:
    return Settings(database_url="postgresql://unused/db")  # type: ignore[arg-type]


def _producer(outcomes: list[Outcome]) -> tuple[EventProducer, FakeProducer]:
    fake = FakeProducer(outcomes)

    def factory(config: dict[str, Any]) -> FakeProducer:
        fake.config = config
        return fake

    return EventProducer(_settings(), client_id="test", producer_factory=factory), fake


def _message(n: int = 0) -> OutgoingMessage:
    return OutgoingMessage("signals.raw.v1", f"key-{n}", b"{}", uuid.uuid4())


def test_config_requires_idempotence_and_full_acknowledgement() -> None:
    config = producer_config(_settings(), "api")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["max.in.flight.requests.per.connection"] <= 5
    assert config["message.timeout.ms"] == 30_000
    assert config["partitioner"] == "murmur2_random"


def test_message_from_envelope_uses_topic_and_key_rules() -> None:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    message = OutgoingMessage.from_envelope(envelope)
    assert message.topic == "signals.raw.v1"
    assert message.key == "b6c72d79-7ec9-4df7-88d0-1d4332c88808:payment"
    assert decode_event(message.value) == envelope
    assert message.event_id == envelope.event_id


def test_results_follow_input_order_with_categories() -> None:
    producer, fake = _producer(
        [
            None,
            KafkaError(KafkaError._MSG_TIMED_OUT, "timed out"),
            KafkaError(KafkaError.NOT_ENOUGH_REPLICAS, "isr", retriable=True),
            KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "too large"),
            BufferError("full"),
            KafkaException(KafkaError(KafkaError._UNKNOWN_TOPIC, "unknown topic")),
            "silent",
        ]
    )
    messages = [_message(n) for n in range(7)]
    results = producer.publish(messages)

    assert [result.message for result in results] == messages
    assert results[0].delivered
    assert (results[0].partition, results[0].offset) == (0, 0)
    categories = [result.error.category if result.error else None for result in results]
    assert categories == [
        None,
        DeliveryErrorCategory.TIMEOUT,
        DeliveryErrorCategory.TRANSIENT,
        DeliveryErrorCategory.REJECTED,
        DeliveryErrorCategory.QUEUE_FULL,
        DeliveryErrorCategory.REJECTED,
        # No callback before flush returned: never reported as delivered.
        DeliveryErrorCategory.TIMEOUT,
    ]
    assert fake.flushed == 1


def test_fatal_error_makes_the_producer_unusable() -> None:
    producer, _ = _producer(
        [KafkaError(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, "sequence", fatal=True)]
    )
    [result] = producer.publish([_message()])
    assert result.error is not None
    assert result.error.category is DeliveryErrorCategory.FATAL
    with pytest.raises(ProducerUnusableError):
        producer.publish([_message()])


def test_empty_batch_is_a_no_op() -> None:
    producer, _ = _producer([])
    assert producer.publish([]) == []
