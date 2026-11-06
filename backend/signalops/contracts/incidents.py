"""Incident read models returned by the API."""

import uuid
from typing import Self

from pydantic import Field, NonNegativeInt, model_validator

from signalops.contracts.common import (
    ContractModel,
    Key,
    Name,
    ReplayScope,
    ShortText,
    UtcDatetime,
)
from signalops.contracts.evidence import NormalizedSignal, QualityStatus, Severity, SignalKind
from signalops.contracts.queries import MAX_QUERY_ITEMS
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus

MAX_PAGE_SIZE = 100
MAX_INCIDENT_EVIDENCE = 200
MAX_INCIDENT_TRIGGERS = 200


class IncidentSummary(ContractModel):
    id: uuid.UUID
    replay_scope: ReplayScope
    title: ShortText
    severity: IncidentSeverity
    status: IncidentStatus
    detected_at: UtcDatetime
    first_seen_at: UtcDatetime
    last_seen_at: UtcDatetime
    resolved_at: UtcDatetime | None = None
    version: int = Field(ge=1)

    @model_validator(mode="after")
    def _check_times(self) -> Self:
        if self.first_seen_at > self.last_seen_at:
            raise ValueError("first_seen_at is after last_seen_at")
        if (self.status is IncidentStatus.RESOLVED) != (self.resolved_at is not None):
            raise ValueError("resolved_at is set exactly when the incident is resolved")
        return self


class IncidentServiceRef(ContractModel):
    service_id: uuid.UUID
    name: Name
    environment: Name
    role: IncidentServiceRole


class EvidenceRef(ContractModel):
    signal_id: uuid.UUID
    kind: SignalKind
    occurred_at: UtcDatetime
    severity: Severity
    quality_status: QualityStatus
    relevance: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)


class IncidentTrigger(ContractModel):
    """A rule firing cited by the incident, in event time."""

    trigger_id: uuid.UUID
    service_id: uuid.UUID
    rule_id: Key
    metric_name: Key
    window_start: UtcDatetime
    window_end: UtcDatetime
    observed: float = Field(allow_inf_nan=False)
    threshold: float = Field(allow_inf_nan=False)
    sample_count: NonNegativeInt
    error_count: NonNegativeInt


class IncidentDetail(IncidentSummary):
    fingerprint: Key
    correlation_window_start: UtcDatetime
    services: tuple[IncidentServiceRef, ...] = Field(max_length=MAX_PAGE_SIZE)
    evidence: tuple[EvidenceRef, ...] = Field(max_length=MAX_INCIDENT_EVIDENCE)
    # Oldest first.
    triggers: tuple[IncidentTrigger, ...] = Field(default=(), max_length=MAX_INCIDENT_TRIGGERS)
    # True when the incident has more triggers than `triggers` holds.
    triggers_truncated: bool = False


class IncidentPage(ContractModel):
    items: tuple[IncidentSummary, ...] = Field(max_length=MAX_PAGE_SIZE)
    # Opaque; None on the last page.
    next_cursor: Key | None = None


class IncidentStatusUpdate(ContractModel):
    """An operator's status change, based on the version they last read."""

    status: IncidentStatus
    version: int = Field(ge=1)


class IncidentEvidence(ContractModel):
    signal: NormalizedSignal
    relevance: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)


class IncidentEvidencePage(ContractModel):
    """An incident's cited signals by `occurred_at` ascending, then ID ascending."""

    incident_id: uuid.UUID
    replay_scope: ReplayScope
    items: tuple[IncidentEvidence, ...] = Field(max_length=MAX_QUERY_ITEMS)
    # Opaque; None on the last page.
    next_cursor: Key | None = None
