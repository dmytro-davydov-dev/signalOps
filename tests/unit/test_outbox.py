import json
import uuid
from pathlib import Path

from signalops.contracts.events import decode_event
from signalops.persistence.models import OutboxEvent
from signalops.streaming.outbox import outgoing_message, publish_rounds

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"


def _row(aggregate_id: uuid.UUID, version: int, topic: str = "t") -> OutboxEvent:
    return OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=aggregate_id,
        aggregate_version=version,
        topic=topic,
        message_key="k",
        envelope={},
    )


def test_rounds_order_each_aggregate_by_version() -> None:
    a, b = uuid.uuid4(), uuid.uuid4()
    # Query order can put a later version first when created_at ties or inverts.
    a2, b1, a1, a3 = _row(a, 2), _row(b, 1), _row(a, 1), _row(a, 3)

    rounds = publish_rounds([a2, b1, a1, a3])

    assert rounds == [[a1, b1], [a2], [a3]]


def test_rows_of_one_version_share_a_round() -> None:
    a = uuid.uuid4()
    first_topic, second_topic, next_version = _row(a, 1, "x"), _row(a, 1, "y"), _row(a, 2)

    assert publish_rounds([first_topic, next_version, second_topic]) == [
        [first_topic, second_topic],
        [next_version],
    ]


def test_no_rows_no_rounds() -> None:
    assert publish_rounds([]) == []


def test_outgoing_message_uses_stored_topic_key_and_envelope() -> None:
    envelope = decode_event((FIXTURES / "signal_raw.json").read_bytes())
    row = OutboxEvent(
        id=envelope.event_id,
        aggregate_id=uuid.uuid4(),
        aggregate_version=1,
        topic="stored.topic",
        message_key="stored-key",
        envelope=json.loads(envelope.encode()),
    )

    message = outgoing_message(row)

    assert (message.topic, message.key, message.event_id) == (
        "stored.topic",
        "stored-key",
        envelope.event_id,
    )
    assert decode_event(message.value) == envelope
