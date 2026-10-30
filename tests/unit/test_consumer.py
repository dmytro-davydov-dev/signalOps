import asyncio
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from signalops.config import Settings
from signalops.contracts.events import EventEnvelope, EventPayload, decode_event
from signalops.streaming.consumer import (
    ConsumerRunner,
    FailureStage,
    ProcessOutcome,
    RecordPosition,
    RecordProcessingError,
    consumer_config,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
TOPIC = "signals.raw.v1"


def _envelope_bytes() -> bytes:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    return envelope.model_copy(update={"event_id": uuid.uuid4()}).encode()


@dataclass
class FakeRecord:
    _offset: int
    _value: bytes | None
    _error: KafkaError | None = None
    _partition: int = 1

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> KafkaError | None:
        return self._error


@dataclass
class FakeConsumer:
    records: list[FakeRecord | None]
    calls: list[str] = field(default_factory=list)
    commit_error: KafkaException | None = None
    committed: list[tuple[str, int, int]] = field(default_factory=list)
    closed: bool = False

    def poll(self, timeout: float) -> FakeRecord | None:
        self.calls.append("poll")
        return self.records.pop(0) if self.records else None

    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[False]) -> None:
        assert asynchronous is False
        self.calls.append("commit")
        if self.commit_error is not None:
            raise self.commit_error
        self.committed.extend((tp.topic, tp.partition, tp.offset) for tp in offsets)

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeProcessor:
    calls: list[str]
    outcome: ProcessOutcome = ProcessOutcome.APPLIED
    error: Exception | None = None
    seen: list[uuid.UUID] = field(default_factory=list)

    async def process(self, envelope: EventEnvelope[EventPayload]) -> ProcessOutcome:
        self.calls.append("process")
        self.seen.append(envelope.event_id)
        if self.error is not None:
            raise self.error
        return self.outcome


def _runner(consumer: FakeConsumer, processor: FakeProcessor) -> ConsumerRunner:
    return ConsumerRunner(consumer, processor, poll_timeout_seconds=0)  # type: ignore[arg-type]


def test_auto_commit_is_disabled() -> None:
    settings = Settings(database_url="postgresql://u:p@localhost/db")  # type: ignore[arg-type]
    config = consumer_config(settings, group_id="normalizer", client_id="normalizer-1")
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["group.id"] == "normalizer"
    assert config["auto.offset.reset"] == "earliest"


@pytest.mark.parametrize("outcome", list(ProcessOutcome))
async def test_next_offset_is_committed_after_processing(outcome: ProcessOutcome) -> None:
    consumer = FakeConsumer([FakeRecord(41, _envelope_bytes())])
    processor = FakeProcessor(consumer.calls, outcome=outcome)
    assert await _runner(consumer, processor).run_once() is outcome
    assert consumer.calls == ["poll", "process", "commit"]
    assert consumer.committed == [(TOPIC, 1, 42)]


async def test_empty_poll_commits_nothing() -> None:
    consumer = FakeConsumer([None])
    processor = FakeProcessor(consumer.calls)
    assert await _runner(consumer, processor).run_once() is None
    assert consumer.calls == ["poll"]


async def test_processing_failure_does_not_commit() -> None:
    consumer = FakeConsumer([FakeRecord(7, _envelope_bytes())])
    processor = FakeProcessor(consumer.calls, error=RuntimeError("db down"))
    with pytest.raises(RecordProcessingError) as raised:
        await _runner(consumer, processor).run_once()
    assert raised.value.stage is FailureStage.PROCESS
    assert raised.value.position == RecordPosition(TOPIC, 1, 7)
    assert consumer.committed == []


@pytest.mark.parametrize("value", [b"not json", None])
async def test_undecodable_record_stops_without_commit(value: bytes | None) -> None:
    consumer = FakeConsumer([FakeRecord(3, value)])
    processor = FakeProcessor(consumer.calls)
    with pytest.raises(RecordProcessingError) as raised:
        await _runner(consumer, processor).run_once()
    assert raised.value.stage is FailureStage.DECODE
    assert consumer.calls == ["poll"]


async def test_failed_record_is_not_skipped_by_later_records() -> None:
    first, second = _envelope_bytes(), _envelope_bytes()
    consumer = FakeConsumer([FakeRecord(0, first), FakeRecord(1, second), FakeRecord(2, first)])
    processor = FakeProcessor(consumer.calls)
    runner = _runner(consumer, processor)
    await runner.run_once()
    processor.error = RuntimeError("boom")
    with pytest.raises(RecordProcessingError):
        await runner.run_once()
    # The runner stops at the failure; offset 2 was never polled or committed.
    assert consumer.committed == [(TOPIC, 1, 1)]
    assert len(consumer.records) == 1


async def test_commit_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    consumer = FakeConsumer(
        [FakeRecord(5, _envelope_bytes())],
        commit_error=KafkaException(KafkaError(KafkaError.REBALANCE_IN_PROGRESS)),
    )
    processor = FakeProcessor(consumer.calls)
    assert await _runner(consumer, processor).run_once() is ProcessOutcome.APPLIED
    assert "kafka offset commit failed" in caplog.text


async def test_non_fatal_record_error_is_skipped_without_commit() -> None:
    error = KafkaError(KafkaError._TRANSPORT)
    consumer = FakeConsumer([FakeRecord(0, None, error)])
    processor = FakeProcessor(consumer.calls)
    assert await _runner(consumer, processor).run_once() is None
    assert consumer.calls == ["poll"]


async def test_fatal_record_error_raises() -> None:
    error = KafkaError(KafkaError._FATAL, fatal=True)
    consumer = FakeConsumer([FakeRecord(0, None, error)])
    with pytest.raises(KafkaException):
        await _runner(consumer, FakeProcessor(consumer.calls)).run_once()


async def test_run_closes_the_consumer_on_failure() -> None:
    consumer = FakeConsumer([FakeRecord(0, b"{}")])
    with pytest.raises(RecordProcessingError):
        await _runner(consumer, FakeProcessor(consumer.calls)).run(asyncio.Event())
    assert consumer.closed
    assert consumer.committed == []


async def test_run_stops_when_requested() -> None:
    consumer = FakeConsumer([])
    stop = asyncio.Event()
    stop.set()
    await _runner(consumer, FakeProcessor(consumer.calls)).run(stop)
    assert consumer.closed
    assert consumer.calls == []
