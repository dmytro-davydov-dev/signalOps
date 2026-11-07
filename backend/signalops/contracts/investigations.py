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
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.workflow import TERMINAL_INVESTIGATION_STATUSES, InvestigationStatus

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
