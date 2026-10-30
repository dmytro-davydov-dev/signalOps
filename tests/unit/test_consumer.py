import asyncio
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from signalops.config import Settings
from signalops.contracts.common import ContractError, ContractErrorCategory
from signalops.contracts.events import (
    EventEnvelope,
    EventPayload,
    ProcessingFailedPayload,
    decode_event,
)
from signalops.streaming.consumer import (
    ConsumerRunner,
    FailureStage,
    ProcessOutcome,
    RecordProcessingError,
    consumer_config,
)
from signalops.streaming.failures import (
    REDRIVE_HEADER,
    UNKNOWN_WORKSPACE_ID,
    RecordPosition,
    failure_event_id,
)
from signalops.streaming.producer import (
    DeliveryError,
    DeliveryErrorCategory,
    DeliveryResult,
    OutgoingMessage,
)
from signalops.streaming.retry import RetryPolicy

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"
TOPIC = "signals.raw.v1"
GROUP = "normalizer-v1"


def _envelope() -> EventEnvelope[EventPayload]:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    return envelope.model_copy(update={"event_id": uuid.uuid4()})


def _envelope_bytes() -> bytes:
    return _envelope().encode()


@dataclass
class FakeRecord:
    _offset: int
    _value: bytes | None
    _error: KafkaError | None = None
    _partition: int = 1
    _headers: list[tuple[str, str | bytes | None]] | None = None

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, str | bytes | None]] | None:
        return self._headers

    def error(self) -> KafkaError | None:
        return self._error


@dataclass
class FakeConsumer:
    """One partition's queued records; seek replays fetched records from an offset."""

    records: list[FakeRecord | None]
    calls: list[str] = field(default_factory=list)
    commit_error: KafkaException | None = None
    committed: list[tuple[str, int, int]] = field(default_factory=list)
    timeouts: list[float] = field(default_factory=list)
    fetched: list[FakeRecord] = field(default_factory=list)
    paused: bool = False
    closed: bool = False

    def poll(self, timeout: float) -> FakeRecord | None:
        self.calls.append("poll")
        self.timeouts.append(timeout)
        if self.paused or not self.records:
            return None
        record = self.records.pop(0)
        if record is not None and record.error() is None:
            self.fetched.append(record)
        return record

    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[False]) -> None:
        assert asynchronous is False
        self.calls.append("commit")
        if self.commit_error is not None:
            raise self.commit_error
        self.committed.extend((tp.topic, tp.partition, tp.offset) for tp in offsets)

    def pause(self, partitions: list[TopicPartition]) -> None:
        self.calls.append("pause")
        self.paused = True

    def resume(self, partitions: list[TopicPartition]) -> None:
        self.calls.append("resume")
        self.paused = False

    def seek(self, partition: TopicPartition) -> None:
        self.calls.append(f"seek:{partition.offset}")
        replay = {r.offset(): r for r in self.fetched if r.offset() >= partition.offset}
        queued = [r for r in self.records if r is None or r.offset() not in replay]
        replayed: list[FakeRecord | None] = [replay[offset] for offset in sorted(replay)]
        self.records = replayed + queued
        self.fetched = [r for r in self.fetched if r.offset() < partition.offset]

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeProcessor:
    calls: list[str]
    outcome: ProcessOutcome = ProcessOutcome.APPLIED
    errors: list[Exception] = field(default_factory=list)
    seen: list[uuid.UUID] = field(default_factory=list)

    async def process(self, envelope: EventEnvelope[EventPayload]) -> ProcessOutcome:
        self.calls.append("process")
        self.seen.append(envelope.event_id)
        if self.errors:
            raise self.errors.pop(0)
        return self.outcome


@dataclass
class FakeFailurePublisher:
    calls: list[str]
    # One entry per publish call; True acknowledges. Defaults to acknowledging.
    acks: list[bool] = field(default_factory=list)
    sent: list[OutgoingMessage] = field(default_factory=list)

    def publish(self, messages: list[OutgoingMessage]) -> list[DeliveryResult]:
        self.calls.append("publish_failure")
        self.sent.extend(messages)
        if self.acks and not self.acks.pop(0):
            error = DeliveryError(DeliveryErrorCategory.TIMEOUT, "_MSG_TIMED_OUT")
            return [DeliveryResult(message, error=error) for message in messages]
        return [DeliveryResult(message, partition=0, offset=0) for message in messages]

    def payloads(self) -> list[ProcessingFailedPayload]:
        envelopes = [decode_event(message.value) for message in self.sent]
        return [e.payload for e in envelopes if isinstance(e.payload, ProcessingFailedPayload)]


@dataclass
class FakeClock:
    now: float = 100.0

    def __call__(self) -> float:
        return self.now


@dataclass
class Harness:
    consumer: FakeConsumer
    processor: FakeProcessor
    publisher: FakeFailurePublisher
    clock: FakeClock
    runner: ConsumerRunner


def _harness(
    records: list[FakeRecord | None],
    *,
    errors: list[Exception] | None = None,
    acks: list[bool] | None = None,
    max_attempts: int = 5,
    outcome: ProcessOutcome = ProcessOutcome.APPLIED,
    commit_error: KafkaException | None = None,
) -> Harness:
    consumer = FakeConsumer(records, commit_error=commit_error)
    processor = FakeProcessor(consumer.calls, outcome=outcome, errors=errors or [])
    publisher = FakeFailurePublisher(consumer.calls, acks=acks or [])
    clock = FakeClock()
    runner = ConsumerRunner(
        consumer,  # type: ignore[arg-type]
        processor,
        consumer_group=GROUP,
        failure_publisher=publisher,
        retry_policy=RetryPolicy(max_attempts, base_delay_seconds=1, max_delay_seconds=30),
        poll_timeout_seconds=0.5,
        clock=clock,
    )
    return Harness(consumer, processor, publisher, clock, runner)


def test_auto_commit_is_disabled() -> None:
    settings = Settings(database_url="postgresql://u:p@localhost/db")  # type: ignore[arg-type]
    config = consumer_config(settings, group_id="normalizer", client_id="normalizer-1")
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["group.id"] == "normalizer"
    assert config["auto.offset.reset"] == "earliest"


@pytest.mark.parametrize("outcome", [ProcessOutcome.APPLIED, ProcessOutcome.DUPLICATE])
async def test_next_offset_is_committed_after_processing(outcome: ProcessOutcome) -> None:
    h = _harness([FakeRecord(41, _envelope_bytes())], outcome=outcome)
    assert await h.runner.run_once() is outcome
    assert h.consumer.calls == ["poll", "process", "commit"]
    assert h.consumer.committed == [(TOPIC, 1, 42)]


async def test_empty_poll_commits_nothing() -> None:
    h = _harness([None])
    assert await h.runner.run_once() is None
    assert h.consumer.calls == ["poll"]


async def test_transient_failure_pauses_rewinds_and_retries_after_backoff() -> None:
    h = _harness([FakeRecord(7, _envelope_bytes())], errors=[RuntimeError("db down")])

    assert await h.runner.run_once() is ProcessOutcome.RETRY_SCHEDULED
    assert h.consumer.calls == ["poll", "process", "pause", "seek:7"]
    assert h.consumer.committed == []

    # Still paused: the runner keeps polling (poll deadline) but gets nothing.
    assert await h.runner.run_once() is None
    assert h.processor.calls.count("process") == 1

    h.clock.now += 1.0  # the first retry waits at most base_delay
    assert await h.runner.run_once() is ProcessOutcome.APPLIED
    assert h.consumer.calls[-4:] == ["resume", "poll", "process", "commit"]
    assert h.consumer.committed == [(TOPIC, 1, 8)]


async def test_poll_waits_no_longer_than_the_pending_backoff() -> None:
    h = _harness([FakeRecord(7, _envelope_bytes())], errors=[RuntimeError("db down")])
    await h.runner.run_once()
    await h.runner.run_once()
    # The first poll uses the configured timeout; later ones end by the retry time.
    assert h.consumer.timeouts[0] == 0.5
    assert 0 <= h.consumer.timeouts[1] <= 0.5


async def test_exhausted_retries_stop_without_commit() -> None:
    h = _harness(
        [FakeRecord(7, _envelope_bytes())],
        errors=[RuntimeError("db down"), RuntimeError("db down")],
        max_attempts=2,
    )
    assert await h.runner.run_once() is ProcessOutcome.RETRY_SCHEDULED
    h.clock.now += 1.0
    with pytest.raises(RecordProcessingError) as raised:
        await h.runner.run_once()
    assert raised.value.stage is FailureStage.PROCESS
    assert raised.value.position == RecordPosition(TOPIC, 1, 7)
    assert raised.value.attempts == 2
    assert h.consumer.committed == []
    assert h.publisher.sent == []


async def test_later_records_wait_for_the_retried_record() -> None:
    first, failing, third = _envelope(), _envelope(), _envelope()
    records: list[FakeRecord | None] = [
        FakeRecord(0, first.encode()),
        FakeRecord(1, failing.encode()),
        FakeRecord(2, third.encode()),
    ]
    h = _harness(records)
    assert await h.runner.run_once() is ProcessOutcome.APPLIED
    h.processor.errors.append(RuntimeError("boom"))
    assert await h.runner.run_once() is ProcessOutcome.RETRY_SCHEDULED
    h.clock.now += 1.0
    assert await h.runner.run_once() is ProcessOutcome.APPLIED
    assert await h.runner.run_once() is ProcessOutcome.APPLIED
    assert h.processor.seen == [first.event_id, failing.event_id, failing.event_id, third.event_id]
    assert h.consumer.committed == [(TOPIC, 1, 1), (TOPIC, 1, 2), (TOPIC, 1, 3)]


async def test_record_fetched_past_a_pending_retry_is_rewound_not_processed() -> None:
    h = _harness([FakeRecord(7, _envelope_bytes())], errors=[RuntimeError("db down")])
    await h.runner.run_once()
    # Simulate offset 8 arriving after the pause, as a prefetched record can.
    h.consumer.paused = False
    h.consumer.records = [FakeRecord(8, _envelope_bytes())]

    assert await h.runner.run_once() is None
    assert h.processor.calls.count("process") == 1
    assert h.consumer.calls[-3:] == ["poll", "pause", "seek:7"]
    assert h.consumer.committed == []


@pytest.mark.parametrize("value", [b"not json", None])
async def test_undecodable_record_is_quarantined_then_committed(value: bytes | None) -> None:
    h = _harness([FakeRecord(3, value)])

    assert await h.runner.run_once() is ProcessOutcome.QUARANTINED

    assert h.consumer.calls == ["poll", "publish_failure", "commit"]
    assert h.consumer.committed == [(TOPIC, 1, 4)]
    [message] = h.publisher.sent
    failure = decode_event(message.value)
    assert message.topic == "processing.failed.v1"
    assert message.key == f"{TOPIC}:1:3"
    assert failure.event_id == failure_event_id(GROUP, RecordPosition(TOPIC, 1, 3))
    assert failure.workspace_id == UNKNOWN_WORKSPACE_ID
    [payload] = h.publisher.payloads()
    assert payload.consumer_group == GROUP
    assert (payload.original_topic, payload.original_partition, payload.original_offset) == (
        TOPIC,
        1,
        3,
    )
    assert payload.original_event_id is None
    assert payload.error_category is ContractErrorCategory.MALFORMED_JSON


async def test_unsupported_version_keeps_original_identity_and_scope() -> None:
    envelope = _envelope()
    replay_run = uuid.uuid4()
    raw = envelope.model_dump_json().replace('"schema_version":1', '"schema_version":9')
    raw = raw.replace('"replay_run_id":null', f'"replay_run_id":"{replay_run}"')
    h = _harness([FakeRecord(3, raw.encode())])

    assert await h.runner.run_once() is ProcessOutcome.QUARANTINED

    [message] = h.publisher.sent
    failure = decode_event(message.value)
    assert message.key == str(envelope.event_id)
    assert failure.workspace_id == envelope.workspace_id
    assert failure.replay_run_id == replay_run
    assert failure.causation_id == envelope.event_id
    [payload] = h.publisher.payloads()
    assert payload.original_event_id == envelope.event_id
    assert payload.error_category is ContractErrorCategory.UNSUPPORTED_VERSION


async def test_handler_contract_error_is_quarantined() -> None:
    envelope = _envelope()
    rejection = ContractError(ContractErrorCategory.INVALID_PAYLOAD, "record.level: bad value")
    h = _harness([FakeRecord(5, envelope.encode())], errors=[rejection])

    assert await h.runner.run_once() is ProcessOutcome.QUARANTINED

    assert h.consumer.calls == ["poll", "process", "publish_failure", "commit"]
    [payload] = h.publisher.payloads()
    assert payload.original_event_id == envelope.event_id
    assert payload.error_detail == "record.level: bad value"
    assert payload.redrive_attempt_id is None


async def test_unacknowledged_failure_event_does_not_commit_the_original() -> None:
    h = _harness([FakeRecord(3, b"not json")], acks=[False, True])

    assert await h.runner.run_once() is ProcessOutcome.RETRY_SCHEDULED
    assert h.consumer.committed == []
    assert h.consumer.calls == ["poll", "publish_failure", "pause", "seek:3"]

    h.clock.now += 1.0
    assert await h.runner.run_once() is ProcessOutcome.QUARANTINED
    assert h.consumer.committed == [(TOPIC, 1, 4)]
    # Both attempts carry the same failure event, so consumers deduplicate it.
    assert len({message.event_id for message in h.publisher.sent}) == 1


async def test_failure_event_that_never_publishes_stops_the_runner() -> None:
    h = _harness([FakeRecord(3, b"not json")], acks=[False, False], max_attempts=2)
    await h.runner.run_once()
    h.clock.now += 1.0
    with pytest.raises(RecordProcessingError) as raised:
        await h.runner.run_once()
    assert raised.value.stage is FailureStage.QUARANTINE
    assert h.consumer.committed == []


async def test_failure_of_a_redriven_record_carries_the_redrive_attempt() -> None:
    attempt = uuid.uuid4()
    headers: list[tuple[str, str | bytes | None]] = [(REDRIVE_HEADER, str(attempt).encode())]
    h = _harness([FakeRecord(9, b"not json", _headers=headers)])
    await h.runner.run_once()
    [payload] = h.publisher.payloads()
    assert payload.redrive_attempt_id == attempt


async def test_commit_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    h = _harness(
        [FakeRecord(5, _envelope_bytes())],
        commit_error=KafkaException(KafkaError(KafkaError.REBALANCE_IN_PROGRESS)),
    )
    assert await h.runner.run_once() is ProcessOutcome.APPLIED
    assert "kafka offset commit failed" in caplog.text


async def test_non_fatal_record_error_is_skipped_without_commit() -> None:
    h = _harness([FakeRecord(0, None, KafkaError(KafkaError._TRANSPORT))])
    assert await h.runner.run_once() is None
    assert h.consumer.calls == ["poll"]


async def test_fatal_record_error_raises() -> None:
    h = _harness([FakeRecord(0, None, KafkaError(KafkaError._FATAL, fatal=True))])
    with pytest.raises(KafkaException):
        await h.runner.run_once()


async def test_run_closes_the_consumer_on_failure() -> None:
    h = _harness([FakeRecord(0, _envelope_bytes())], errors=[RuntimeError("x")], max_attempts=1)
    with pytest.raises(RecordProcessingError):
        await h.runner.run(asyncio.Event())
    assert h.consumer.closed
    assert h.consumer.committed == []


async def test_run_stops_when_requested() -> None:
    h = _harness([])
    stop = asyncio.Event()
    stop.set()
    await h.runner.run(stop)
    assert h.consumer.closed
    assert h.consumer.calls == []
