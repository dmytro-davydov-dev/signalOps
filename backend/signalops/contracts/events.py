"""Kafka event envelope, per-type payloads, and versioned decoding.

Every message on a SignalOps topic is an `EventEnvelope` whose payload model is
selected by `(event_type, schema_version)`. Envelopes and payloads ignore unknown
fields, so a v1 consumer accepts additive optional fields from newer producers.
A breaking change registers a new schema version; consumers that do not know it
reject the message as `unsupported_version` and quarantine it.
"""

import json
import uuid
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import ConfigDict, Field, JsonValue, SerializeAsAny, ValidationError, model_validator

from signalops.contracts.common import (
    MAX_ERROR_DETAIL_LENGTH,
    ContractError,
    ContractErrorCategory,
    ContractModel,
    CorrelationId,
    Key,
    Name,
    ReplayScope,
    SourceEventId,
    TraceId,
    UtcDatetime,
    Version,
    bounded_json_object,
    describe_validation_error,
    replay_scope_for,
)
from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evidence import QualityStatus, SignalKind, SourceKind
from signalops.contracts.investigations import check_diagnosis_status
from signalops.contracts.workflow import (
    TERMINAL_INVESTIGATION_STATUSES,
    IncidentSeverity,
    IncidentStatus,
    InvestigationStatus,
)

MAX_ENVELOPE_BYTES = 256 * 1024
MAX_RAW_RECORD_BYTES = 64 * 1024


class EventType(StrEnum):
    SIGNAL_RAW = "signal.raw"
    SIGNAL_NORMALIZED = "signal.normalized"
    INCIDENT_CHANGED = "incident.changed"
    INVESTIGATION_REQUESTED = "investigation.requested"
    INVESTIGATION_COMPLETED = "investigation.completed"
    PROCESSING_FAILED = "processing.failed"


# Event types produced from a source record; their envelopes carry source_id.
SOURCE_EVENT_TYPES = frozenset({EventType.SIGNAL_RAW, EventType.SIGNAL_NORMALIZED})


class EventPayload(ContractModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class RawSignalPayload(EventPayload):
    source_kind: SourceKind
    source_event_id: SourceEventId
    # True when ingestion derived source_event_id because the source sent none.
    source_event_id_derived: bool = False
    # Optional routing hint from the source; the normalizer resolves identity.
    service_hint: Name | None = None
    # Adapter-specific record, validated by the normalizer (see contracts.adapters).
    record: Annotated[dict[str, JsonValue], bounded_json_object(MAX_RAW_RECORD_BYTES)]


class NormalizedSignalPayload(EventPayload):
    signal_id: uuid.UUID
    service_id: uuid.UUID | None
    kind: SignalKind
    quality_status: QualityStatus


class IncidentChange(StrEnum):
    CREATED = "created"
    UPDATED = "updated"
    RESOLVED = "resolved"
    REOPENED = "reopened"


class IncidentChangedPayload(EventPayload):
    incident_id: uuid.UUID
    # incidents.version after the change; consumers ignore older versions.
    incident_version: int = Field(ge=1)
    change: IncidentChange
    status: IncidentStatus
    severity: IncidentSeverity
    replay_scope: ReplayScope


class InvestigationTrigger(StrEnum):
    OPERATOR = "operator"
    DETECTOR = "detector"


class InvestigationRequestedPayload(EventPayload):
    investigation_id: uuid.UUID
    incident_id: uuid.UUID
    request_key: Key
    trigger: InvestigationTrigger
    workflow_version: Version


class InvestigationCompletedPayload(EventPayload):
    investigation_id: uuid.UUID
    incident_id: uuid.UUID
    status: InvestigationStatus
    # Follows DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.
    diagnosis_status: DiagnosisStatus | None = None
    workflow_version: Version

    @model_validator(mode="after")
    def _check_terminal(self) -> Self:
        if self.status not in TERMINAL_INVESTIGATION_STATUSES:
            raise ValueError("status must be a terminal investigation status")
        check_diagnosis_status(self.status, self.diagnosis_status)
        return self


class ProcessingFailedPayload(EventPayload):
    consumer_group: Name
    original_topic: Name
    original_partition: int = Field(ge=0)
    original_offset: int = Field(ge=0)
    # None when the original message could not be parsed far enough to read it.
    original_event_id: uuid.UUID | None = None
    error_category: ContractErrorCategory
    # Field locations and messages only; never original values.
    error_detail: str = Field(min_length=1, max_length=MAX_ERROR_DETAIL_LENGTH)
    failed_at: UtcDatetime
    # Set on failures of a redriven message; the event ID stays the original's.
    redrive_attempt_id: uuid.UUID | None = None


EVENT_PAYLOADS: dict[tuple[EventType, int], type[EventPayload]] = {
    (EventType.SIGNAL_RAW, 1): RawSignalPayload,
    (EventType.SIGNAL_NORMALIZED, 1): NormalizedSignalPayload,
    (EventType.INCIDENT_CHANGED, 1): IncidentChangedPayload,
    (EventType.INVESTIGATION_REQUESTED, 1): InvestigationRequestedPayload,
    (EventType.INVESTIGATION_COMPLETED, 1): InvestigationCompletedPayload,
    (EventType.PROCESSING_FAILED, 1): ProcessingFailedPayload,
}


class EventHeader(ContractModel):
    """Envelope fields other than the payload."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: uuid.UUID
    event_type: EventType
    schema_version: int = Field(ge=1)
    workspace_id: uuid.UUID
    # Required for source events; optional for other domain events.
    source_id: uuid.UUID | None = None
    occurred_at: UtcDatetime
    ingested_at: UtcDatetime
    correlation_id: CorrelationId | None = None
    # event_id of the envelope that caused this one.
    causation_id: uuid.UUID | None = None
    trace_id: TraceId | None = None
    # None for live traffic; otherwise the isolated evaluation run.
    replay_run_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _check_source(self) -> Self:
        if self.event_type in SOURCE_EVENT_TYPES and self.source_id is None:
            raise ValueError(f"source_id is required for {self.event_type}")
        return self

    @property
    def replay_scope(self) -> str:
        return replay_scope_for(self.replay_run_id)


class EventEnvelope[P: EventPayload](EventHeader):
    # SerializeAsAny keeps subclass fields when P is the EventPayload base.
    payload: SerializeAsAny[P]

    @model_validator(mode="after")
    def _check_payload_type(self) -> Self:
        expected = EVENT_PAYLOADS.get((self.event_type, self.schema_version))
        if expected is None:
            raise ValueError(f"no payload contract for {self.event_type} v{self.schema_version}")
        if type(self.payload) is not expected:
            raise ValueError(f"payload must be {expected.__name__}")
        return self

    def encode(self) -> bytes:
        data = self.model_dump_json().encode()
        if len(data) > MAX_ENVELOPE_BYTES:
            raise ContractError(
                ContractErrorCategory.OVERSIZED,
                f"envelope is {len(data)} bytes; limit {MAX_ENVELOPE_BYTES}",
                event_id=self.event_id,
            )
        return data


def _event_id_of(document: dict[str, Any]) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(document.get("event_id")))
    except ValueError:
        return None


def decode_event(data: bytes) -> EventEnvelope[EventPayload]:
    """Parse and validate a message value, raising `ContractError` by category."""
    if len(data) > MAX_ENVELOPE_BYTES:
        raise ContractError(
            ContractErrorCategory.OVERSIZED,
            f"envelope is {len(data)} bytes; limit {MAX_ENVELOPE_BYTES}",
        )
    try:
        document = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(ContractErrorCategory.MALFORMED_JSON, type(error).__name__) from None
    if not isinstance(document, dict):
        raise ContractError(ContractErrorCategory.INVALID_ENVELOPE, "envelope must be an object")

    event_id = _event_id_of(document)
    raw_type, raw_version = document.get("event_type"), document.get("schema_version")
    if raw_type not in set(EventType):
        raise ContractError(
            ContractErrorCategory.UNKNOWN_EVENT_TYPE,
            "event_type is missing or unknown",
            event_id=event_id,
        )
    event_type = EventType(raw_type)
    if type(raw_version) is not int:
        raise ContractError(
            ContractErrorCategory.INVALID_ENVELOPE,
            "schema_version must be an integer",
            event_id=event_id,
        )
    payload_model = EVENT_PAYLOADS.get((event_type, raw_version))
    if payload_model is None:
        raise ContractError(
            ContractErrorCategory.UNSUPPORTED_VERSION,
            f"{event_type} v{raw_version} is not supported",
            event_id=event_id,
        )

    try:
        header = EventHeader.model_validate(document)
    except ValidationError as error:
        detail = describe_validation_error(error)
        raise ContractError(
            ContractErrorCategory.INVALID_ENVELOPE, detail, event_id=event_id
        ) from None
    try:
        payload = payload_model.model_validate(document.get("payload"))
    except ValidationError as error:
        detail = describe_validation_error(error)
        raise ContractError(
            ContractErrorCategory.INVALID_PAYLOAD, detail, event_id=header.event_id
        ) from None
    return EventEnvelope[EventPayload](**dict(header), payload=payload)
