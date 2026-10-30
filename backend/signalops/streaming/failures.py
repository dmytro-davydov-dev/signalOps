"""Failure events for records a consumer quarantines, and the redrive header.

A record fails permanently when it does not satisfy its contract: decoding
raises `ContractError`, or the handler raises one for content it cannot accept
(for example an adapter record that fails validation). Every other exception
is treated as transient and retried.

The failure event's ID is derived from the consumer group and the record's
position, so republishing it after a crash produces the same event. Its
workspace and replay run come from the original envelope when it decoded, else
from those fields of the raw document when they parse, else the workspace is
`UNKNOWN_WORKSPACE_ID`. The error detail is the `ContractError` detail, which
names field locations and messages but never input values.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from signalops.contracts.common import ContractError
from signalops.contracts.events import (
    MAX_ENVELOPE_BYTES,
    EventEnvelope,
    EventPayload,
    EventType,
    ProcessingFailedPayload,
)

# Kafka header carrying the redrive attempt ID on a redriven record; the
# envelope, and so its event_id, is the original's.
REDRIVE_HEADER = "signalops-redrive-attempt-id"

# Workspace of a failure event whose original did not name a readable workspace.
UNKNOWN_WORKSPACE_ID = uuid.UUID(int=0)

FAILURE_EVENT_NAMESPACE = uuid.UUID("8d0f3c52-5e7a-4b8e-9a51-2f1c6b7d9e04")


@dataclass(frozen=True)
class RecordPosition:
    topic: str
    partition: int
    offset: int


def failure_event_id(consumer_group: str, position: RecordPosition) -> uuid.UUID:
    name = f"{consumer_group}\n{position.topic}\n{position.partition}\n{position.offset}"
    return uuid.uuid5(FAILURE_EVENT_NAMESPACE, name)


KafkaHeaders = dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]]


def redrive_attempt_id(headers: KafkaHeaders | None) -> uuid.UUID | None:
    """The redrive attempt ID header, if present and well formed."""
    items = headers.items() if isinstance(headers, dict) else headers or ()
    for name, value in items:
        if name != REDRIVE_HEADER or value is None:
            continue
        text = value.decode(errors="replace") if isinstance(value, bytes) else value
        try:
            return uuid.UUID(text)
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class _Scope:
    workspace_id: uuid.UUID
    replay_run_id: uuid.UUID | None


def _uuid_field(document: dict[str, object], name: str) -> uuid.UUID | None:
    value = document.get(name)
    if not isinstance(value, str):
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


def _document(value: bytes) -> dict[str, object] | None:
    if len(value) > MAX_ENVELOPE_BYTES:
        return None
    try:
        document = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return document if isinstance(document, dict) else None


def envelope_event_id(value: bytes) -> uuid.UUID | None:
    """The event_id of a raw envelope, when it parses that far."""
    document = _document(value)
    return None if document is None else _uuid_field(document, "event_id")


def _scope_of_raw(value: bytes) -> _Scope:
    document = _document(value)
    if document is None:
        return _Scope(UNKNOWN_WORKSPACE_ID, None)
    return _Scope(
        _uuid_field(document, "workspace_id") or UNKNOWN_WORKSPACE_ID,
        _uuid_field(document, "replay_run_id"),
    )


def failure_envelope(
    *,
    consumer_group: str,
    position: RecordPosition,
    value: bytes,
    error: ContractError,
    original: EventEnvelope[EventPayload] | None,
    redrive_attempt: uuid.UUID | None,
    failed_at: datetime,
) -> EventEnvelope[EventPayload]:
    if original is not None:
        scope = _Scope(original.workspace_id, original.replay_run_id)
        original_event_id: uuid.UUID | None = original.event_id
    else:
        scope = _scope_of_raw(value)
        original_event_id = error.event_id
    payload = ProcessingFailedPayload(
        consumer_group=consumer_group,
        original_topic=position.topic,
        original_partition=position.partition,
        original_offset=position.offset,
        original_event_id=original_event_id,
        error_category=error.category,
        error_detail=error.detail or error.category.value,
        failed_at=failed_at,
        redrive_attempt_id=redrive_attempt,
    )
    return EventEnvelope[EventPayload](
        event_id=failure_event_id(consumer_group, position),
        event_type=EventType.PROCESSING_FAILED,
        schema_version=1,
        workspace_id=scope.workspace_id,
        occurred_at=failed_at,
        ingested_at=failed_at,
        correlation_id=original.correlation_id if original is not None else None,
        causation_id=original_event_id,
        trace_id=original.trace_id if original is not None else None,
        replay_run_id=scope.replay_run_id,
        payload=payload,
    )
