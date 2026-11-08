"""Structured diagnosis produced by an investigation.

This is the validated shape of model output. Whether cited signals exist within
the investigation's scope and evidence cutoff is checked against the database by
the verifier, not here.
"""

import uuid
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from signalops.contracts.common import (
    ContractModel,
    LongText,
    Name,
    ShortText,
    Version,
    require_unique,
)
from signalops.contracts.workflow import AgentRole

MAX_EVIDENCE_IDS = 50
MAX_LIST_ITEMS = 10
MAX_CONTRADICTION_ROLES = 3
MAX_CONTRADICTION_EVIDENCE = 20

FailureType = Annotated[str, StringConstraints(max_length=64, pattern=r"^[a-z][a-z0-9_]*$")]


class DiagnosisStatus(StrEnum):
    DIAGNOSED = "diagnosed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class RootCause(ContractModel):
    service_id: uuid.UUID
    service_name: Name
    failure_type: FailureType
    # A model assessment until evaluation calibrates it.
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(min_length=1, max_length=MAX_EVIDENCE_IDS)

    @model_validator(mode="after")
    def _check_evidence(self) -> Self:
        require_unique(self.evidence_signal_ids, "evidence_signal_ids")
        return self


class Alternative(ContractModel):
    hypothesis: ShortText
    assessment: LongText


class SuggestedAction(ContractModel):
    action: LongText
    # SignalOps never remediates on its own.
    requires_approval: Literal[True] = True


class Contradiction(ContractModel):
    """Findings that point different ways, kept visible rather than merged away."""

    description: LongText
    # The roles whose findings disagree.
    roles: tuple[AgentRole, ...] = Field(default=(), max_length=MAX_CONTRADICTION_ROLES)
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(
        default=(), max_length=MAX_CONTRADICTION_EVIDENCE
    )

    @model_validator(mode="after")
    def _check(self) -> Self:
        require_unique(self.evidence_signal_ids, "evidence_signal_ids")
        require_unique(self.roles, "roles")
        return self


class Diagnosis(ContractModel):
    status: DiagnosisStatus
    summary: LongText
    root_cause: RootCause | None
    alternatives: tuple[Alternative, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    suggested_actions: tuple[SuggestedAction, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    limitations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    # Evidence that would have been needed; required when abstaining.
    missing_evidence: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    # Disagreements between specialists (workflow v3), whatever the outcome.
    contradictions: tuple[Contradiction, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    workflow_version: Version

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
