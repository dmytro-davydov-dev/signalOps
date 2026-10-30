import uuid
from dataclasses import dataclass

import pytest

from signalops.streaming.failures import (
    REDRIVE_HEADER,
    RecordPosition,
    envelope_event_id,
    failure_event_id,
    redrive_attempt_id,
)
from signalops.streaming.redrive import REDRIVABLE_TOPICS, RedriveError, redrive_message


def test_failure_event_id_is_stable_per_group_and_position() -> None:
    position = RecordPosition("signals.raw.v1", 2, 1187)
    moved = RecordPosition("signals.raw.v1", 2, 1188)
    original = failure_event_id("normalizer-v1", position)
    assert original == failure_event_id("normalizer-v1", position)
    assert original != failure_event_id("detector-v1", position)
    assert original != failure_event_id("normalizer-v1", moved)


def test_redrive_attempt_id_reads_list_and_dict_headers() -> None:
    attempt = uuid.uuid4()
    assert redrive_attempt_id([("other", b"x"), (REDRIVE_HEADER, str(attempt).encode())]) == attempt
    assert redrive_attempt_id({REDRIVE_HEADER: str(attempt)}) == attempt


@pytest.mark.parametrize("headers", [None, [], [(REDRIVE_HEADER, b"not-a-uuid")], [("x", None)]])
def test_missing_or_malformed_redrive_header_is_none(
    headers: list[tuple[str, str | bytes | None]] | None,
) -> None:
    assert redrive_attempt_id(headers) is None


def test_envelope_event_id_parses_only_what_it_can() -> None:
    event_id = uuid.uuid4()
    assert envelope_event_id(f'{{"event_id": "{event_id}"}}'.encode()) == event_id
    assert envelope_event_id(b'{"event_id": 5}') is None
    assert envelope_event_id(b"\xff") is None
    assert envelope_event_id(b"[]") is None


@dataclass
class FakeRecord:
    _value: bytes | None
    _key: bytes | None
    _headers: list[tuple[str, str | bytes | None]] | None

    def topic(self) -> str:
        return "signals.raw.v1"

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def headers(self) -> list[tuple[str, str | bytes | None]] | None:
        return self._headers


def test_redrive_message_keeps_key_value_and_headers_and_replaces_the_attempt() -> None:
    event_id, old_attempt, new_attempt = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    value = f'{{"event_id": "{event_id}"}}'.encode()
    record = FakeRecord(
        value, b"source:svc", [("trace", b"abc"), (REDRIVE_HEADER, str(old_attempt).encode())]
    )

    message = redrive_message(record, new_attempt)  # type: ignore[arg-type]

    assert (message.topic, message.key, message.value) == ("signals.raw.v1", "source:svc", value)
    assert message.event_id == event_id
    assert message.headers == (("trace", b"abc"), (REDRIVE_HEADER, str(new_attempt).encode()))


@pytest.mark.parametrize(("value", "key"), [(None, b"k"), (b"{}", None)])
def test_redrive_message_refuses_records_it_cannot_reproduce(
    value: bytes | None, key: bytes | None
) -> None:
    with pytest.raises(RedriveError):
        redrive_message(FakeRecord(value, key, None), uuid.uuid4())  # type: ignore[arg-type]


def test_failure_topic_is_not_redrivable() -> None:
    assert "processing.failed.v1" not in REDRIVABLE_TOPICS
    assert "signals.raw.v1" in REDRIVABLE_TOPICS
