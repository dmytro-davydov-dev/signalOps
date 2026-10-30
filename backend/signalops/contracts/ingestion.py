"""Request and response bodies for `POST /v1/ingest/{source_id}`.

The API validates each record on its own, so one bad record is rejected with a
reason while the rest of the batch proceeds. Payloads are not checked against
the adapter contracts here: the normalizer does that and records malformed
payloads as quarantined evidence, so the outcome stays auditable.
"""

import uuid
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, JsonValue, model_validator

from signalops.contracts.common import (
    MAX_ERROR_DETAIL_LENGTH,
    ContractModel,
    SourceEventId,
    UtcDatetime,
    bounded_json_object,
)
from signalops.contracts.events import MAX_RAW_RECORD_BYTES


class IngestRecord(ContractModel):
    source_event_id: SourceEventId
    occurred_at: UtcDatetime
    # Adapter-specific record for the source's kind (see contracts.adapters).
    payload: Annotated[dict[str, JsonValue], bounded_json_object(MAX_RAW_RECORD_BYTES)]


class IngestRequest(ContractModel):
    """Documents the request body. The route validates `records` item by item."""

    records: list[IngestRecord] = Field(min_length=1)


class IngestRecordStatus(StrEnum):
    # Kafka acknowledged the record on signals.raw.v1.
    ACCEPTED = "accepted"
    # The record itself is invalid; resending it unchanged fails again.
    REJECTED = "rejected"
    # Not acknowledged. Retry with the same source_event_id.
    FAILED = "failed"


class IngestRejectReason(StrEnum):
    INVALID_RECORD = "invalid_record"
    OVERSIZED = "oversized"
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    DUPLICATE_IN_BATCH = "duplicate_in_batch"


class IngestFailureReason(StrEnum):
    # The broker may still have written the record; a retry can duplicate it,
    # and the pipeline deduplicates on the event ID derived from source_event_id.
    AMBIGUOUS_TIMEOUT = "ambiguous_timeout"
    # Not written.
    BROKER_UNAVAILABLE = "broker_unavailable"


class IngestRecordResult(ContractModel):
    # Position of the record in the request.
    index: int = Field(ge=0)
    # None when the record was too malformed to read one.
    source_event_id: SourceEventId | None = None
    status: IngestRecordStatus
    # Envelope event ID on signals.raw.v1; the same for every retry of a record.
    event_id: uuid.UUID | None = None
    reason: IngestRejectReason | IngestFailureReason | None = None
    # Field locations and messages only; never record values.
    detail: str | None = Field(default=None, max_length=MAX_ERROR_DETAIL_LENGTH)

    @model_validator(mode="after")
    def _check_status(self) -> Self:
        match self.status:
            case IngestRecordStatus.ACCEPTED:
                consistent = self.event_id is not None and self.reason is None
            case IngestRecordStatus.REJECTED:
                consistent = isinstance(self.reason, IngestRejectReason)
            case IngestRecordStatus.FAILED:
                consistent = self.event_id is not None and isinstance(
                    self.reason, IngestFailureReason
                )
        if not consistent:
            raise ValueError(f"event_id and reason do not match status {self.status}")
        return self


class IngestResponse(ContractModel):
    # Correlation ID on every envelope from this request.
    batch_id: uuid.UUID
    accepted: int = Field(ge=0)
    rejected: int = Field(ge=0)
    failed: int = Field(ge=0)
    results: list[IngestRecordResult]

    @model_validator(mode="after")
    def _check_counts(self) -> Self:
        counts = {status: 0 for status in IngestRecordStatus}
        for result in self.results:
            counts[result.status] += 1
        expected = (self.accepted, self.rejected, self.failed)
        actual = tuple(counts[status] for status in IngestRecordStatus)
        if expected != actual:
            raise ValueError("counts do not match results")
        return self
