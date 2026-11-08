"""Investigation read model and the investigation-status to diagnosis-status mapping.

The mapping is the single source for the API contract, the
`investigation.completed` payload, the `ck_investigations_diagnosis_status`
database constraint, and the generated frontend module:

- `completed` always stores a `diagnosed` diagnosis;
- `insufficient_evidence` always stores an `insufficient_evidence` diagnosis
  (an abstention is a successful outcome, not a failure);
- every other status stores no diagnosis.
"""

import uuid
from typing import Self

from pydantic import Field, NonNegativeInt, PositiveInt, model_validator

from signalops.contracts.common import ContractModel, ReplayScope, UtcDatetime, Version
from signalops.contracts.diagnosis import MAX_EVIDENCE_IDS, Diagnosis, DiagnosisStatus
from signalops.contracts.evidence import NormalizedSignal
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES, InvestigationStatus

MAX_INCIDENT_INVESTIGATIONS = 20

DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS: dict[InvestigationStatus, DiagnosisStatus | None] = {
    InvestigationStatus.QUEUED: None,
    InvestigationStatus.RUNNING: None,
    InvestigationStatus.COMPLETED: DiagnosisStatus.DIAGNOSED,
    InvestigationStatus.INSUFFICIENT_EVIDENCE: DiagnosisStatus.INSUFFICIENT_EVIDENCE,
    InvestigationStatus.FAILED: None,
    InvestigationStatus.CANCELLED: None,
    InvestigationStatus.BUDGET_EXCEEDED: None,
}

INVESTIGATION_STATUS_BY_DIAGNOSIS_STATUS: dict[DiagnosisStatus, InvestigationStatus] = {
    diagnosis: investigation
    for investigation, diagnosis in DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.items()
    if diagnosis is not None
}


def terminal_status_for(diagnosis: Diagnosis) -> InvestigationStatus:
    """Terminal investigation status that stores this diagnosis."""
    return INVESTIGATION_STATUS_BY_DIAGNOSIS_STATUS[diagnosis.status]


def check_diagnosis_status(
    status: InvestigationStatus, diagnosis_status: DiagnosisStatus | None
) -> None:
    expected = DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS[status]
    if diagnosis_status != expected:
        wanted = f"a {expected} diagnosis" if expected else "no diagnosis"
        raise ValueError(f"a {status} investigation has {wanted}")


class InvestigationRequest(ContractModel):
    """Body of POST /v1/incidents/{id}/investigations; limits only lower the configured ones."""

    budget_microusd: PositiveInt | None = None
    max_steps: PositiveInt | None = None
    deadline_seconds: int | None = Field(default=None, ge=60)


class InvestigationAccepted(ContractModel):
    """Body of the 202 response."""

    id: uuid.UUID
    incident_id: uuid.UUID
    status: InvestigationStatus
    created_at: UtcDatetime


class InvestigationUsage(ContractModel):
    budget_microusd: NonNegativeInt
    reserved_microusd: NonNegativeInt
    spent_microusd: NonNegativeInt
    # Model calls so far, whatever their outcome.
    model_calls: NonNegativeInt = 0
    input_tokens: NonNegativeInt = 0
    output_tokens: NonNegativeInt = 0
    cache_read_tokens: NonNegativeInt = 0
    cache_write_tokens: NonNegativeInt = 0
    # `spent_microusd` split by how it is known: provider-reported tokens at published
    # prices, or an estimate.
    actual_microusd: NonNegativeInt = 0
    estimated_microusd: NonNegativeInt = 0
    # Calls whose cost cannot be known, and what they keep reserved. They may have billed.
    unknown_calls: NonNegativeInt = 0
    unknown_reserved_microusd: NonNegativeInt = 0


class InvestigationView(ContractModel):
    """Body of GET /v1/investigations/{id}."""

    id: uuid.UUID
    incident_id: uuid.UUID
    replay_scope: ReplayScope
    status: InvestigationStatus
    workflow_version: Version
    # Tools never read evidence after this instant.
    evidence_cutoff_at: UtcDatetime
    created_at: UtcDatetime
    deadline_at: UtcDatetime
    completed_at: UtcDatetime | None = None
    # A running investigation stays running until its worker acts on this.
    cancel_requested_at: UtcDatetime | None = None
    max_steps: int = Field(ge=1)
    usage: InvestigationUsage
    diagnosis: Diagnosis | None = None

    @model_validator(mode="after")
    def _check_state(self) -> Self:
        if (self.status in TERMINAL_INVESTIGATION_STATUSES) != (self.completed_at is not None):
            raise ValueError("completed_at is set exactly when the investigation is terminal")
        check_diagnosis_status(self.status, self.diagnosis.status if self.diagnosis else None)
        return self


class InvestigationSummary(ContractModel):
    """One investigation of an incident, without its usage or diagnosis body."""

    id: uuid.UUID
    status: InvestigationStatus
    created_at: UtcDatetime
    completed_at: UtcDatetime | None = None
    cancel_requested_at: UtcDatetime | None = None
    diagnosis_status: DiagnosisStatus | None = None

    @model_validator(mode="after")
    def _check_state(self) -> Self:
        if (self.status in TERMINAL_INVESTIGATION_STATUSES) != (self.completed_at is not None):
            raise ValueError("completed_at is set exactly when the investigation is terminal")
        check_diagnosis_status(self.status, self.diagnosis_status)
        return self


class IncidentInvestigations(ContractModel):
    """Body of GET /v1/incidents/{id}/investigations, newest first."""

    incident_id: uuid.UUID
    replay_scope: ReplayScope
    items: tuple[InvestigationSummary, ...] = Field(max_length=MAX_INCIDENT_INVESTIGATIONS)
    # True when the incident has more investigations than `items` holds.
    truncated: bool = False


class InvestigationEvidence(ContractModel):
    """Body of GET /v1/investigations/{id}/evidence.

    `items` are the diagnosis's cited signals that exist in the investigation's scope
    and were known by its cutoff, in citation order and redacted as `/v1/signals` is.
    `unverified_signal_ids` are citations that fail that check; a stored diagnosis
    should have none, because the workflow checked them before saving it.
    """

    investigation_id: uuid.UUID
    replay_scope: ReplayScope
    evidence_cutoff_at: UtcDatetime
    items: tuple[NormalizedSignal, ...] = Field(max_length=MAX_EVIDENCE_IDS)
    unverified_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_EVIDENCE_IDS)


MAX_TRACE_BATCHES = 20


class TraceEvidence(ContractModel):
    """The incident's cited signals as they arrived."""

    signals: NonNegativeInt
    first_occurred_at: UtcDatetime | None
    first_ingested_at: UtcDatetime | None
    last_ingested_at: UtcDatetime | None
    # Ingestion batch IDs (the envelopes' `correlation_id`), at most MAX_TRACE_BATCHES.
    ingestion_batches: tuple[str, ...] = Field(max_length=MAX_TRACE_BATCHES)


class TraceJob(ContractModel):
    job_id: uuid.UUID
    status: str
    attempts: NonNegativeInt
    lease_generation: NonNegativeInt
    # Category only, for example `lease_expired`; error details stay in the job row.
    last_error_category: str | None


class TraceStep(ContractModel):
    step_key: str
    agent_role: str
    status: str
    attempt: NonNegativeInt
    lease_generation: NonNegativeInt
    started_at: UtcDatetime | None
    completed_at: UtcDatetime | None
    model_calls: NonNegativeInt
    model_ms: NonNegativeInt
    tool_calls: NonNegativeInt
    tool_ms: NonNegativeInt


class TraceStages(ContractModel):
    """Wall-clock seconds between stages; null when a stage has not happened.

    Replay incidents carry event-time detection, so their ingestion-to-detection
    and detection-to-request stages are null. `model_s` and `tool_s` sum every
    call's duration, so parallel specialist branches can make them exceed
    `running_s`.
    """

    ingestion_to_detection_s: float | None
    detection_to_request_s: float | None
    request_to_first_step_s: float | None
    running_s: float | None
    model_s: float
    tool_s: float


class InvestigationTrace(ContractModel):
    """One investigation followed back to the ingestion batches that opened its incident.

    `log_fields` are the structured log fields that select this path's lines
    across processes.
    """

    investigation_id: uuid.UUID
    incident_id: uuid.UUID
    replay_scope: ReplayScope
    status: InvestigationStatus
    workflow_version: Version
    incident_detected_at: UtcDatetime
    created_at: UtcDatetime
    completed_at: UtcDatetime | None
    evidence: TraceEvidence
    job: TraceJob | None
    steps: tuple[TraceStep, ...]
    stages: TraceStages
    log_fields: dict[str, tuple[str, ...]]
