import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from signalops.contracts.events import EventEnvelope, EventPayload, EventType, decode_event
from signalops.streaming.topics import (
    DAY_MS,
    TOPIC_BY_EVENT_TYPE,
    TOPICS,
    message_key,
    topic_for,
)


def test_topic_catalogue_matches_plan() -> None:
    by_name = {spec.name: spec for spec in TOPICS}
    assert len(by_name) == len(TOPICS), "duplicate topic names"
    assert {name: spec.partitions for name, spec in by_name.items()} == {
        "signals.raw.v1": 3,
        "signals.normalized.v1": 3,
        "incidents.changed.v1": 1,
        "investigations.requested.v1": 1,
        "investigations.completed.v1": 1,
        "processing.failed.v1": 1,
    }
    assert by_name["signals.raw.v1"].retention_ms == 3 * DAY_MS
    assert by_name["signals.normalized.v1"].retention_ms == 7 * DAY_MS


def test_topic_config_is_delete_policy_with_retention() -> None:
    config = TOPICS[0].config()
    assert config == {"retention.ms": str(TOPICS[0].retention_ms), "cleanup.policy": "delete"}


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1" / "events"


def _fixture(name: str) -> EventEnvelope[EventPayload]:
    return decode_event((FIXTURES / f"{name}.json").read_bytes())


def _with_payload(
    envelope: EventEnvelope[EventPayload], **changes: object
) -> EventEnvelope[EventPayload]:
    return envelope.model_copy(update={"payload": envelope.payload.model_copy(update=changes)})


def test_every_event_type_routes_to_a_provisioned_topic() -> None:
    assert set(TOPIC_BY_EVENT_TYPE) == set(EventType)
    assert sorted(TOPIC_BY_EVENT_TYPE.values()) == sorted(spec.name for spec in TOPICS)


# Pinned so a change to a key rule, which moves aggregates between partitions, fails here.
@pytest.mark.parametrize(
    ("fixture", "topic", "key"),
    [
        (
            "signal_raw",
            "signals.raw.v1",
            "b6c72d79-7ec9-4df7-88d0-1d4332c88808:payment",
        ),
        (
            "signal_normalized",
            "signals.normalized.v1",
            "82631aa1-86f5-4d37-b39f-fac74ebbdac8:5f3e2d1c-0b9a-4876-9543-210fedcba987",
        ),
        ("incident_changed", "incidents.changed.v1", "6a7b8c9d-0e1f-4a2b-9c3d-4e5f6a7b8c9d"),
        (
            "investigation_requested",
            "investigations.requested.v1",
            "0f1e2d3c-4b5a-4968-8776-5a4b3c2d1e0f",
        ),
        (
            "investigation_completed",
            "investigations.completed.v1",
            "0f1e2d3c-4b5a-4968-8776-5a4b3c2d1e0f",
        ),
        (
            "processing_failed",
            "processing.failed.v1",
            "f5a6b7c8-d9e0-4f12-8345-6789abcdef01",
        ),
    ],
)
def test_fixture_routing(fixture: str, topic: str, key: str) -> None:
    envelope = _fixture(fixture)
    assert topic_for(envelope) == topic
    assert message_key(envelope) == key


def test_keys_ignore_per_delivery_fields() -> None:
    for path in FIXTURES.glob("*.json"):
        envelope = decode_event(path.read_bytes())
        republished = envelope.model_copy(
            update={
                "event_id": uuid.uuid4(),
                "occurred_at": datetime(2030, 1, 1, tzinfo=UTC),
                "ingested_at": datetime(2030, 1, 1, tzinfo=UTC),
                "correlation_id": "other",
                "causation_id": uuid.uuid4(),
            }
        )
        assert message_key(republished) == message_key(envelope), path.name


def test_optional_key_parts_are_omitted() -> None:
    raw = _with_payload(_fixture("signal_raw"), service_hint=None)
    assert message_key(raw) == str(raw.source_id)
    normalized = _with_payload(_fixture("signal_normalized"), service_id=None)
    assert message_key(normalized) == str(normalized.workspace_id)


def test_unparseable_failure_is_keyed_by_original_position() -> None:
    failed = _with_payload(_fixture("processing_failed"), original_event_id=None)
    assert message_key(failed) == "signals.raw.v1:2:1187"
