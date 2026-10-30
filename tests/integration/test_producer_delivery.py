"""Producer acknowledgement and partitioning against the Compose Kafka broker.

Each test uses its own topics so nothing reaches the topics workers consume.
"""

import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from signalops.config import Settings
from signalops.streaming.producer import DeliveryErrorCategory, EventProducer, OutgoingMessage
from signalops.streaming.topics import TopicSpec, ensure_topics

pytestmark = pytest.mark.integration


@pytest.fixture
def scratch_topic(infra_settings: Settings) -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    name = f"test.producer.{uuid.uuid4().hex[:8]}"
    ensure_topics(admin, (TopicSpec(name, partitions=3, retention_ms=60_000),))
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            future.result()


def _settings(settings: Settings, **changes: object) -> Settings:
    return settings.model_copy(update={"kafka_delivery_timeout_ms": 5_000, **changes})


def _message(topic: str, key: str, value: bytes = b"{}") -> OutgoingMessage:
    return OutgoingMessage(topic, key, value, uuid.uuid4())


def test_acknowledged_messages_report_their_position(
    infra_settings: Settings, scratch_topic: str
) -> None:
    producer = EventProducer(_settings(infra_settings), client_id="test-producer")
    messages = [_message(scratch_topic, f"key-{n}", f'{{"n":{n}}}'.encode()) for n in range(6)]
    results = producer.publish(messages)
    assert all(result.delivered for result in results)

    consumer = Consumer(
        {
            "bootstrap.servers": infra_settings.kafka_bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        for result in results:
            assert result.partition is not None
            assert result.offset is not None
            consumer.assign([TopicPartition(scratch_topic, result.partition, result.offset)])
            record = consumer.poll(10)
            assert record is not None
            assert record.error() is None
            assert record.key() == result.message.key.encode()
            assert record.value() == result.message.value
    finally:
        consumer.close()


def test_same_key_always_lands_on_the_same_partition(
    infra_settings: Settings, scratch_topic: str
) -> None:
    keys = [str(uuid.uuid4()) for _ in range(20)]
    first = EventProducer(_settings(infra_settings), client_id="test-producer-a")
    # A separate producer instance must agree: the mapping depends only on the key.
    second = EventProducer(_settings(infra_settings), client_id="test-producer-b")
    batch = [_message(scratch_topic, key) for key in keys]
    a = first.publish(batch)
    b = second.publish(batch)
    again = first.publish(batch)
    assert [r.partition for r in a] == [r.partition for r in b] == [r.partition for r in again]
    assert len({r.partition for r in a}) > 1, "20 random keys should spread over partitions"


def test_unknown_topic_failure_reaches_the_caller(infra_settings: Settings) -> None:
    producer = EventProducer(_settings(infra_settings), client_id="test-producer")
    [result] = producer.publish([_message(f"test.missing.{uuid.uuid4().hex[:8]}", "k")])
    assert not result.delivered
    assert result.error is not None
    assert result.error.category in {DeliveryErrorCategory.REJECTED, DeliveryErrorCategory.TIMEOUT}


def test_oversized_message_is_rejected_without_affecting_the_batch(
    infra_settings: Settings, scratch_topic: str
) -> None:
    producer = EventProducer(_settings(infra_settings), client_id="test-producer")
    too_large = b"x" * (2 * 1024 * 1024)
    results = producer.publish(
        [
            _message(scratch_topic, "a"),
            _message(scratch_topic, "b", too_large),
            _message(scratch_topic, "c"),
        ]
    )
    assert [result.delivered for result in results] == [True, False, True]
    assert results[1].error is not None
    assert results[1].error.category is DeliveryErrorCategory.REJECTED


def test_unreachable_broker_times_out_to_the_caller(infra_settings: Settings) -> None:
    # Port 9 (discard) on loopback has no Kafka listener.
    settings = _settings(
        infra_settings, kafka_bootstrap_servers="127.0.0.1:9", kafka_delivery_timeout_ms=1_000
    )
    producer = EventProducer(settings, client_id="test-producer")
    [result] = producer.publish([_message("signals.raw.v1", "k")])
    assert not result.delivered
    assert result.error is not None
    assert result.error.category is DeliveryErrorCategory.TIMEOUT
