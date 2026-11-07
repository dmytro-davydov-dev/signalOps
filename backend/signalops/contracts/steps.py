"""Output of each workflow node, stored in `agent_steps.output`.

A resumed worker validates stored output against these models before reusing it.
Output that fails is never run again, because a completed step cannot change, so
a change here that old rows would fail needs a new workflow version.
"""

import uuid
from typing import Self

from pydantic import Field, model_validator

from signalops.contracts.common import (
    ContractModel,
    LongText,
    Name,
    ShortText,
    UtcDatetime,
    require_unique,
)
from signalops.contracts.diagnosis import (
    MAX_EVIDENCE_IDS,
    MAX_LIST_ITEMS,
    Alternative,
    DiagnosisStatus,
    FailureType,
    SuggestedAction,
)

SUBMIT_TOOL = "submit_findings"
MAX_BRIEF_SERVICES = 100
MAX_BRIEF_EVIDENCE = 20


class ServiceBrief(ContractModel):
    service_id: uuid.UUID
    name: Name


class IncidentBrief(ContractModel):
    """Only fields fixed at detection: severity may have moved past the evidence cutoff."""

    incident_id: uuid.UUID
    title: ShortText
    detected_at: UtcDatetime
    evidence_cutoff_at: UtcDatetime
    # The start of the window that opened the incident.
    window_start: UtcDatetime | None = None
    # The workspace's services, so an agent can name those the incident does not.
    services: tuple[ServiceBrief, ...] = Field(default=(), max_length=MAX_BRIEF_SERVICES)
    # Signals the detector cited, known by the cutoff.
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_BRIEF_EVIDENCE)


class InvestigatorFindings(ContractModel):
    missing_evidence: tuple[ShortText, ...] = Field(min_length=1, max_length=MAX_LIST_ITEMS)


class ProposedCause(ContractModel):
    service_id: uuid.UUID
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(min_length=1, max_length=MAX_EVIDENCE_IDS)

    @model_validator(mode="after")
    def _check_evidence(self) -> Self:
        require_unique(self.evidence_signal_ids, "evidence_signal_ids")
        return self


class ProposedDiagnosis(ContractModel):
    """What the agent submits. Code verifies it before it becomes a `Diagnosis`."""

    status: DiagnosisStatus
    summary: LongText
    root_cause: ProposedCause | None = None
    alternatives: tuple[Alternative, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    suggested_actions: tuple[SuggestedAction, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    limitations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    missing_evidence: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)

    @model_validator(mode="after")
    def _check_status(self) -> Self:
        if self.status is DiagnosisStatus.DIAGNOSED and self.root_cause is None:
            raise ValueError("a diagnosed result needs a root_cause")
        if self.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE:
            if self.root_cause is not None:
                raise ValueError("an insufficient_evidence result must have a null root_cause")
            if not self.missing_evidence:
                raise ValueError("an insufficient_evidence result must list missing_evidence")
        return self
