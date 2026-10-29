"""Incident read models returned by the API."""

import uuid
from typing import Self

from pydantic import Field, model_validator

from signalops.contracts.common import (
    ContractModel,
    Key,
    Name,
    ReplayScope,
    ShortText,
    UtcDatetime,
)
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus

MAX_PAGE_SIZE = 100
MAX_INCIDENT_EVIDENCE = 200


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


class IncidentDetail(IncidentSummary):
    fingerprint: Key
    correlation_window_start: UtcDatetime
    services: tuple[IncidentServiceRef, ...] = Field(max_length=MAX_PAGE_SIZE)
    evidence: tuple[EvidenceRef, ...] = Field(max_length=MAX_INCIDENT_EVIDENCE)


class IncidentPage(ContractModel):
    items: tuple[IncidentSummary, ...] = Field(max_length=MAX_PAGE_SIZE)
    # Opaque; None on the last page.
    next_cursor: Key | None = None
