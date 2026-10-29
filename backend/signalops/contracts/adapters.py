"""Raw record contracts for the three initial source adapters.

The ingestion API forwards each record's payload unchanged on signals.raw.v1;
the normalizer validates it against the model for the source kind. Fields a
model does not know are moved into `attributes` rather than dropped, so newer
producers can add fields without breaking older normalizers.
"""

from typing import Annotated, Any, Literal, Self

from pydantic import BeforeValidator, Field, NonNegativeInt, model_validator

from signalops.contracts.common import (
    Attributes,
    ContractModel,
    Message,
    Name,
    ShortText,
    TraceId,
    Version,
)
from signalops.contracts.evidence import Severity, SourceKind

MAX_DURATION_MS = 24 * 60 * 60 * 1000
MAX_DOWNSTREAM_SERVICES = 64

# Common spellings from logging libraries; anything else must be a Severity value.
LEVEL_ALIASES = {
    "trace": Severity.DEBUG,
    "warn": Severity.WARNING,
    "err": Severity.ERROR,
    "fatal": Severity.CRITICAL,
}


def _normalize_level(value: Any) -> Any:
    if isinstance(value, str):
        lowered = value.strip().lower()
        return LEVEL_ALIASES.get(lowered, lowered)
    return value


class AdapterInput(ContractModel):
    attributes: Attributes = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _preserve_unknown_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        unknown = {key: value for key, value in data.items() if key not in cls.model_fields}
        if not unknown:
            return data
        attributes = data.get("attributes", {})
        if not isinstance(attributes, dict):
            # Let field validation report the wrong type.
            return data
        conflicts = sorted(unknown.keys() & attributes.keys())
        if conflicts:
            raise ValueError(f"fields also present in attributes: {', '.join(conflicts)}")
        known = {key: value for key, value in data.items() if key in cls.model_fields}
        return {**known, "attributes": {**attributes, **unknown}}


class StructuredLogInput(AdapterInput):
    # Service identity may be missing; the normalizer flags it rather than guessing.
    service: Name | None = None
    environment: Name | None = None
    level: Annotated[Severity, BeforeValidator(_normalize_level)]
    message: Message
    trace_id: TraceId | None = None
    span_id: TraceId | None = None


class TraceSummaryInput(AdapterInput):
    trace_id: TraceId
    service: Name | None = None
    environment: Name | None = None
    operation: ShortText
    duration_ms: Annotated[int, Field(ge=0, le=MAX_DURATION_MS)]
    status: Literal["ok", "error"]
    span_count: Annotated[int, Field(ge=1)]
    error_span_count: NonNegativeInt = 0
    downstream_services: tuple[Name, ...] = Field(default=(), max_length=MAX_DOWNSTREAM_SERVICES)

    @model_validator(mode="after")
    def _check_counts(self) -> Self:
        if self.error_span_count > self.span_count:
            raise ValueError("error_span_count exceeds span_count")
        return self


class DeploymentInput(AdapterInput):
    service: Name
    environment: Name
    revision: Version
    previous_revision: Version | None = None
    change_summary: ShortText | None = None


ADAPTER_INPUTS: dict[SourceKind, type[AdapterInput]] = {
    SourceKind.STRUCTURED_LOG: StructuredLogInput,
    SourceKind.TRACE_SUMMARY: TraceSummaryInput,
    SourceKind.DEPLOYMENT: DeploymentInput,
}
