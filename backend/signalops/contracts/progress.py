"""Investigation progress, stored in `progress_events` and read through the API.

An event records a durable change: a step opening or ending, or the investigation
ending. It commits with that change, so the history cannot disagree with the
steps. It carries identifiers and categories only. Findings stay in
`agent_steps.output` and the outcome in `investigations.diagnosis`.
"""

import uuid
from enum import StrEnum
from typing import Final, Self

from pydantic import Field, PositiveInt, model_validator

from signalops.contracts.common import ContractModel, Key, Name, ReplayScope, UtcDatetime
from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.investigations import check_diagnosis_status
from signalops.contracts.queries import MAX_QUERY_ITEMS
from signalops.contracts.workflow import (
    TERMINAL_INVESTIGATION_STATUSES,
    AgentRole,
    InvestigationStatus,
)

# Error category of a step left running by an investigation that ended.
STEP_ABANDONED: Final = "abandoned"


class ProgressEventType(StrEnum):
    STEP_STARTED = "step.started"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"
    INVESTIGATION_ENDED = "investigation.ended"


class StepProgress(ContractModel):
    step_key: Key
    agent_role: AgentRole
    attempt: int = Field(ge=1)


class StepFailed(StepProgress):
    error_category: Name


class InvestigationEnded(ContractModel):
    status: InvestigationStatus
    # Follows DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.
    diagnosis_status: DiagnosisStatus | None = None

    @model_validator(mode="after")
    def _check_terminal(self) -> Self:
        if self.status not in TERMINAL_INVESTIGATION_STATUSES:
            raise ValueError("status must be a terminal investigation status")
        check_diagnosis_status(self.status, self.diagnosis_status)
        return self


ProgressPayload = StepFailed | StepProgress | InvestigationEnded

PROGRESS_PAYLOADS: Final[dict[ProgressEventType, type[ProgressPayload]]] = {
    ProgressEventType.STEP_STARTED: StepProgress,
    ProgressEventType.STEP_COMPLETED: StepProgress,
    ProgressEventType.STEP_FAILED: StepFailed,
    ProgressEventType.INVESTIGATION_ENDED: InvestigationEnded,
}


class ProgressEventView(ContractModel):
    # The durable cursor: ascending in commit order within an investigation.
    id: PositiveInt
    event_type: ProgressEventType
    payload: ProgressPayload
    created_at: UtcDatetime

    @model_validator(mode="after")
    def _check_payload(self) -> Self:
        expected = PROGRESS_PAYLOADS[self.event_type]
        if type(self.payload) is not expected:
            raise ValueError(f"payload must be {expected.__name__}")
        return self


class ProgressPage(ContractModel):
    """Body of GET /v1/investigations/{id}/progress, oldest event first."""

    investigation_id: uuid.UUID
    replay_scope: ReplayScope
    items: tuple[ProgressEventView, ...] = Field(max_length=MAX_QUERY_ITEMS)
    # Pass as `after` for the next page; None when no more events exist yet.
    next_after: PositiveInt | None = None
