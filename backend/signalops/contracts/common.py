"""Shared contract types, size limits, and error categories.

Length limits that also appear in persistence CHECK constraints are defined here
so contracts and migrations agree. Migrations hardcode the values, so changing a
shared limit requires a new revision.
"""

import json
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    JsonValue,
    StringConstraints,
    ValidationError,
)

LIVE_REPLAY_SCOPE = "live"

MAX_REPLAY_SCOPE_LENGTH = 128
MAX_SOURCE_EVENT_ID_LENGTH = 512
MAX_KEY_LENGTH = 256
MAX_NAME_LENGTH = 128
MAX_MESSAGE_LENGTH = 8192
MAX_ATTRIBUTES_BYTES = 16 * 1024
MAX_ERROR_DETAIL_LENGTH = 2048


class ContractModel(BaseModel):
    """Base for contracts the application produces and validates exactly.

    Unknown fields are rejected. Event contracts that must tolerate additive
    fields from newer producers override `extra`.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class ContractErrorCategory(StrEnum):
    OVERSIZED = "oversized"
    MALFORMED_JSON = "malformed_json"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    UNSUPPORTED_VERSION = "unsupported_version"
    INVALID_ENVELOPE = "invalid_envelope"
    INVALID_PAYLOAD = "invalid_payload"


class ContractError(Exception):
    """A document failed contract validation.

    The detail lists field locations and messages only, never input values,
    so it is safe to log and to publish on processing.failed.v1.
    """

    def __init__(
        self,
        category: ContractErrorCategory,
        detail: str,
        *,
        event_id: uuid.UUID | None = None,
    ) -> None:
        super().__init__(f"{category}: {detail}")
        self.category = category
        self.detail = detail[:MAX_ERROR_DETAIL_LENGTH]
        self.event_id = event_id


def describe_validation_error(error: ValidationError) -> str:
    parts = []
    for item in error.errors(include_url=False, include_input=False, include_context=False):
        location = ".".join(str(part) for part in item["loc"]) or "<root>"
        parts.append(f"{location}: {item['msg']}")
    return "; ".join(parts)


def _reject_numeric_timestamp(value: Any) -> Any:
    # Pydantic's lax mode reads numbers as Unix timestamps; contracts require RFC 3339.
    if isinstance(value, int | float) and not isinstance(value, bool):
        raise ValueError("timestamp must be an RFC 3339 string with a UTC offset")
    return value


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


UtcDatetime = Annotated[
    AwareDatetime,
    BeforeValidator(_reject_numeric_timestamp),
    AfterValidator(_to_utc),
]
"""Timezone-aware timestamp normalized to UTC. Naive values are rejected."""


def bounded_text(max_length: int) -> Any:
    return StringConstraints(min_length=1, max_length=max_length)


Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")]

ReplayScope = Annotated[
    str,
    StringConstraints(
        min_length=1, max_length=MAX_REPLAY_SCOPE_LENGTH, pattern=r"^[a-z0-9][a-z0-9._:-]*$"
    ),
]
SourceEventId = Annotated[str, bounded_text(MAX_SOURCE_EVENT_ID_LENGTH)]
TraceId = Annotated[Identifier, bounded_text(128)]
CorrelationId = Annotated[Identifier, bounded_text(MAX_KEY_LENGTH)]
Key = Annotated[Identifier, bounded_text(MAX_KEY_LENGTH)]
Name = Annotated[Identifier, bounded_text(MAX_NAME_LENGTH)]
Version = Annotated[Identifier, bounded_text(64)]
ShortText = Annotated[str, bounded_text(500)]
LongText = Annotated[str, bounded_text(2000)]
Message = Annotated[str, bounded_text(MAX_MESSAGE_LENGTH)]


def json_size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())


def bounded_json_object(max_bytes: int) -> AfterValidator:
    def check(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if json_size(value) > max_bytes:
            raise ValueError(f"object exceeds {max_bytes} bytes when serialized")
        return value

    return AfterValidator(check)


Attributes = Annotated[dict[str, JsonValue], bounded_json_object(MAX_ATTRIBUTES_BYTES)]
"""Source-specific JSON object, bounded by its compact serialized size."""


def replay_scope_for(replay_run_id: uuid.UUID | None) -> str:
    """Replay scope stored with evidence for an envelope's replay run."""
    return LIVE_REPLAY_SCOPE if replay_run_id is None else f"replay:{replay_run_id}"


def require_unique(values: tuple[Any, ...], field: str) -> None:
    if len(set(values)) != len(values):
        raise ValueError(f"{field} contains duplicates")
