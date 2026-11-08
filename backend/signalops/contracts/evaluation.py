"""Evaluation contracts: scenarios, ground truth, observed outcomes, and scores.

Ground truth (`ExpectedOutcome`, a scenario's injection spec) belongs to the
evaluator. It is stored in the `evaluation` schema, which the runtime database
role cannot read, and no investigation tool or prompt ever receives it.
"""

import uuid
from enum import StrEnum
from typing import Self

from pydantic import Field, JsonValue, model_validator

from signalops.contracts.common import ContractModel, Name, SourceEventId, UtcDatetime, Version
from signalops.contracts.diagnosis import FailureType

MAX_SUPPORTING_EVENTS = 5_000


class ScenarioFamily(StrEnum):
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    CONNECTION_POOL_EXHAUSTION = "connection_pool_exhaustion"
    DEPLOYMENT_REGRESSION = "deployment_regression"
    MEMORY_PRESSURE = "memory_pressure"
    HEALTHY_NOISE = "healthy_noise"
    MIXED = "mixed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class ScenarioSplit(StrEnum):
    # Variants a developer may look at while building prompts and rules.
    DEVELOPMENT = "development"
    # Variants used only for reporting; never used to tune anything.
    HELD_OUT = "held_out"


class SystemVariant(StrEnum):
    DETERMINISTIC = "deterministic"
    SINGLE_AGENT = "single_agent"
    MULTI_AGENT = "multi_agent"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ReplayStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    # The pipeline did not drain before the replay's timeout.
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class CaseStatus(StrEnum):
    PENDING = "pending"
    INVESTIGATING = "investigating"
    SCORED = "scored"
    # The harness could not produce an outcome; scored as a failure, never skipped.
    FAILED = "failed"


TERMINAL_RUN_STATUSES = frozenset({RunStatus.COMPLETED, RunStatus.FAILED})
TERMINAL_CASE_STATUSES = frozenset({CaseStatus.SCORED, CaseStatus.FAILED})


class FaultWindow(ContractModel):
    start: UtcDatetime
    end: UtcDatetime

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.end <= self.start:
            raise ValueError("a fault window must end after it starts")
        return self


class ExpectedOutcome(ContractModel):
    """What a correct system concludes about one scenario. Evaluator truth only."""

    incident_expected: bool
    # False: the evidence underdetermines the cause, so abstaining is correct.
    diagnosable: bool
    root_cause_service: Name | None = None
    failure_type: FailureType | None = None
    affected_services: tuple[Name, ...] = ()
    fault_window: FaultWindow | None = None
    # Source event IDs the fault produced; a citation outside them is unsupported.
    supporting_event_ids: tuple[SourceEventId, ...] = Field(
        default=(), max_length=MAX_SUPPORTING_EVENTS
    )

    @model_validator(mode="after")
    def _check(self) -> Self:
        named = self.root_cause_service is not None and self.failure_type is not None
        if self.diagnosable != named:
            raise ValueError("a diagnosable outcome names a service and failure type, and only it")
        if self.diagnosable and not self.incident_expected:
            raise ValueError("a diagnosable outcome needs an expected incident")
        if self.incident_expected and self.fault_window is None:
            raise ValueError("an expected incident needs a fault window")
        return self

    def summary(self) -> "ExpectedOutcome":
        """The outcome without its evidence labels, for display next to a case."""
        return self.model_copy(update={"supporting_event_ids": ()})


class ScenarioDefinition(ContractModel):
    """One versioned scenario variant, as registered in `evaluation.scenarios`."""

    name: Name
    version: Version
    family: ScenarioFamily
    split: ScenarioSplit
    synthetic: bool = True
    # How to rebuild the dataset, for example `generator:v2`.
    dataset_ref: Name
    dataset_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_count: int = Field(ge=0)
    # Generator parameters and the injected fault; evaluator truth like the outcome.
    injection_spec: dict[str, JsonValue]
    expected_outcome: ExpectedOutcome


class ObservedOutcome(ContractModel):
    """What the system produced for one case, read back from runtime tables."""

    incident_ids: tuple[uuid.UUID, ...] = ()
    investigation_id: uuid.UUID | None = None
    investigation_status: str | None = None
    diagnosis_status: str | None = None
    root_cause_service: Name | None = None
    failure_type: FailureType | None = None
    cited_signal_ids: tuple[uuid.UUID, ...] = ()
    # Source event IDs of the cited signals in the case's replay scope.
    cited_event_ids: tuple[SourceEventId, ...] = ()
    # Event time in a replay, so not comparable with the wall-clock times below.
    detected_at: UtcDatetime | None = None
    # When the investigation was requested and ended, by wall clock.
    started_at: UtcDatetime | None = None
    completed_at: UtcDatetime | None = None
    spent_microusd: int = 0
    unknown_reserved_microusd: int = 0
    model_calls: int = 0


class CaseScores(ContractModel):
    """Deterministic scores of one case against its labels."""

    # Diagnosed the labelled service and failure type, or abstained when that is correct.
    outcome_correct: bool
    # Service and failure type both match; None when the scenario is not diagnosable.
    root_cause_correct: bool | None
    abstained: bool
    # Asserted a cause that is wrong, or any cause where none is supported.
    false_diagnosis: bool
