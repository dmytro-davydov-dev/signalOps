"""Evidence enumerations and the normalized signal contract.

The enumerations drive persistence CHECK constraints; adding a member requires a
migration that widens the matching constraint. Quality flags are stored as a JSON
array without a database check, so new flags are additive.
"""

import uuid
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from signalops.contracts.common import (
    Attributes,
    ContractModel,
    Message,
    ReplayScope,
    SourceEventId,
    TraceId,
    UtcDatetime,
    require_unique,
)


class SourceKind(StrEnum):
    STRUCTURED_LOG = "structured_log"
    TRACE_SUMMARY = "trace_summary"
    DEPLOYMENT = "deployment"


class SignalKind(StrEnum):
    LOG = "log"
    TRACE = "trace"
    DEPLOYMENT = "deployment"


class Severity(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class QualityStatus(StrEnum):
    ACCEPTED = "accepted"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"


class QualityFlag(StrEnum):
    MALFORMED_PAYLOAD = "malformed_payload"
    MISSING_SERVICE = "missing_service"
    UNKNOWN_SERVICE = "unknown_service"
    CONTRADICTORY_IDENTITY = "contradictory_identity"
    CLOCK_SKEW = "clock_skew"
    LATE_ARRIVAL = "late_arrival"
    TRUNCATED_MESSAGE = "truncated_message"
    # The source sent no source_event_id; ingestion derived one (adapters.quality).
    DERIVED_EVENT_ID = "derived_event_id"


class NormalizedSignal(ContractModel):
    """A signal row as written by the normalizer."""

    id: uuid.UUID
    workspace_id: uuid.UUID
    source_id: uuid.UUID
    # None when service identity could not be resolved; never guessed.
    service_id: uuid.UUID | None
    source_event_id: SourceEventId
    replay_scope: ReplayScope
    kind: SignalKind
    occurred_at: UtcDatetime
    ingested_at: UtcDatetime
    severity: Severity
    message: Message
    trace_id: TraceId | None = None
    attributes: Attributes = Field(default_factory=dict)
    quality_status: QualityStatus
    quality_flags: tuple[QualityFlag, ...] = Field(default=(), max_length=len(QualityFlag))

    @model_validator(mode="after")
    def _check_quality(self) -> Self:
        require_unique(self.quality_flags, "quality_flags")
        if self.quality_status is not QualityStatus.ACCEPTED and not self.quality_flags:
            raise ValueError(f"{self.quality_status} signals must carry at least one quality flag")
        return self
