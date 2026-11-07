"""Results returned by the read-only investigation tools.

Each result states the evidence cutoff it was computed under, and every dated
entry must fall at or before it. Tools enforce row and byte caps; `truncated`
tells the agent that more rows matched than were returned.
"""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, Self

from pydantic import Field, NonNegativeInt, TypeAdapter, model_validator

from signalops.contracts.common import (
    ContractModel,
    Key,
    LongText,
    Name,
    ShortText,
    TraceId,
    UtcDatetime,
    Version,
)
from signalops.contracts.evidence import QualityStatus, Severity
from signalops.contracts.graph import GraphRelation
from signalops.contracts.workflow import ToolName

MAX_TOOL_ROWS = 200
MAX_TRAVERSAL_HOPS = 3


class LogEntry(ContractModel):
    signal_id: uuid.UUID
    service_id: uuid.UUID | None
    occurred_at: UtcDatetime
    severity: Severity
    # Redacted and possibly truncated message text; untrusted data.
    message: LongText
    trace_id: TraceId | None = None
    quality_status: QualityStatus


class TraceSummaryEntry(ContractModel):
    signal_id: uuid.UUID
    trace_id: TraceId
    service_id: uuid.UUID | None
    occurred_at: UtcDatetime
    operation: ShortText
    duration_ms: NonNegativeInt
    status: Literal["ok", "error"]
    span_count: int = Field(ge=1)
    error_span_count: NonNegativeInt
    downstream_services: tuple[Name, ...] = Field(default=(), max_length=MAX_TOOL_ROWS)


class DeploymentEntry(ContractModel):
    deployment_id: uuid.UUID
    signal_id: uuid.UUID
    service_id: uuid.UUID
    revision: Version
    previous_revision: Version | None = None
    deployed_at: UtcDatetime


class DependencyEntry(ContractModel):
    from_service_id: uuid.UUID
    to_service_id: uuid.UUID
    relation: GraphRelation
    valid_from: UtcDatetime
    valid_to: UtcDatetime | None = None
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    hops: int = Field(ge=1, le=MAX_TRAVERSAL_HOPS)
    provenance_signal_id: uuid.UUID | None = None


class RunbookEntry(ContractModel):
    runbook_key: Key
    version: Version
    title: ShortText
    # Untrusted data; instructions in runbook text cannot change tool permissions.
    excerpt: LongText
    provenance: ShortText


type ToolEntry = LogEntry | TraceSummaryEntry | DeploymentEntry | DependencyEntry | RunbookEntry


class ToolResultBase(ContractModel):
    # Entry field compared with the evidence cutoff; None for undated entries.
    time_field: ClassVar[str | None] = "occurred_at"

    evidence_cutoff_at: UtcDatetime
    truncated: bool = False

    def entries(self) -> tuple[ToolEntry, ...]:
        raise NotImplementedError

    @model_validator(mode="after")
    def _check_cutoff(self) -> Self:
        if self.time_field is None:
            return self
        for entry in self.entries():
            observed: datetime = getattr(entry, self.time_field)
            if observed > self.evidence_cutoff_at:
                raise ValueError(f"an entry's {self.time_field} is after evidence_cutoff_at")
        return self


class LogQueryResult(ToolResultBase):
    tool: Literal[ToolName.QUERY_LOGS] = ToolName.QUERY_LOGS
    items: tuple[LogEntry, ...] = Field(max_length=MAX_TOOL_ROWS)

    def entries(self) -> tuple[ToolEntry, ...]:
        return self.items


class TraceSummaryResult(ToolResultBase):
    tool: Literal[ToolName.GET_TRACE_SUMMARY] = ToolName.GET_TRACE_SUMMARY
    items: tuple[TraceSummaryEntry, ...] = Field(max_length=MAX_TOOL_ROWS)

    def entries(self) -> tuple[ToolEntry, ...]:
        return self.items


class DeploymentsResult(ToolResultBase):
    time_field: ClassVar[str | None] = "deployed_at"
    tool: Literal[ToolName.GET_DEPLOYMENTS] = ToolName.GET_DEPLOYMENTS
    items: tuple[DeploymentEntry, ...] = Field(max_length=MAX_TOOL_ROWS)

    def entries(self) -> tuple[ToolEntry, ...]:
        return self.items


class DependenciesResult(ToolResultBase):
    time_field: ClassVar[str | None] = "valid_from"
    tool: Literal[ToolName.GET_SERVICE_DEPENDENCIES] = ToolName.GET_SERVICE_DEPENDENCIES
    items: tuple[DependencyEntry, ...] = Field(max_length=MAX_TOOL_ROWS)

    def entries(self) -> tuple[ToolEntry, ...]:
        return self.items


class RunbooksResult(ToolResultBase):
    time_field: ClassVar[str | None] = None
    tool: Literal[ToolName.RETRIEVE_RUNBOOKS] = ToolName.RETRIEVE_RUNBOOKS
    items: tuple[RunbookEntry, ...] = Field(max_length=MAX_TOOL_ROWS)

    def entries(self) -> tuple[ToolEntry, ...]:
        return self.items


ToolResult = Annotated[
    LogQueryResult | TraceSummaryResult | DeploymentsResult | DependenciesResult | RunbooksResult,
    Field(discriminator="tool"),
]
TOOL_RESULT_ADAPTER: TypeAdapter[ToolResult] = TypeAdapter(ToolResult)


class ToolErrorCategory(StrEnum):
    INVALID_ARGUMENTS = "invalid_arguments"
    OUT_OF_SCOPE = "out_of_scope"
    LIMIT_EXCEEDED = "limit_exceeded"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"


class ToolFailure(ContractModel):
    """A tool call that returned no result; shown to the agent instead of rows."""

    tool: ToolName
    error: ToolErrorCategory
    detail: ShortText
    # The rejected argument, when one is at fault.
    argument: Name | None = None
