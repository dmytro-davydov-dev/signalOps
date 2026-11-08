"""Evaluation API views (P10-T06).

A case shows its expected outcome without the evidence labels, but each cited
signal carries whether the labels count it as fault evidence, so a reviewer can
see why a citation scored as unsupported. Metrics and replay reports are passed
through as stored; `docs/evaluation.md` defines their fields.
"""

import uuid
from typing import Any

from pydantic import Field

from signalops.contracts.common import ContractModel, UtcDatetime
from signalops.contracts.evaluation import (
    CaseScores,
    CaseStatus,
    ExpectedOutcome,
    ObservedOutcome,
    RunStatus,
    ScenarioFamily,
    ScenarioSplit,
    SystemVariant,
)


class EvaluationRunSummary(ContractModel):
    id: uuid.UUID
    label: str
    split: ScenarioSplit
    status: RunStatus
    variants: tuple[SystemVariant, ...]
    trials: int
    dataset_hash: str
    created_at: UtcDatetime
    started_at: UtcDatetime | None
    completed_at: UtcDatetime | None
    error: str | None
    case_counts: dict[str, int] = Field(default_factory=dict)


class EvaluationRunList(ContractModel):
    items: tuple[EvaluationRunSummary, ...]


class EvaluationCaseRow(ContractModel):
    id: uuid.UUID
    case_key: str
    scenario_name: str
    scenario_version: str
    family: ScenarioFamily
    split: ScenarioSplit
    synthetic: bool
    variant: SystemVariant
    trial: int
    status: CaseStatus
    replay_scope: str
    investigation_id: uuid.UUID | None
    expected: ExpectedOutcome
    observed: ObservedOutcome | None
    scores: CaseScores | None
    error: str | None


class EvaluationRunDetail(EvaluationRunSummary):
    seed: int
    workflow_versions: dict[str, Any]
    prompt_versions: dict[str, Any]
    model_settings: dict[str, Any]
    pricing_config: dict[str, Any]
    limits: dict[str, Any]
    metrics: dict[str, Any] | None
    cases: tuple[EvaluationCaseRow, ...]


class CitedSignal(ContractModel):
    signal_id: uuid.UUID
    source_event_id: str
    service_name: str | None
    kind: str
    severity: str
    occurred_at: UtcDatetime
    message: str
    # Whether the deterministic labels count this record as produced by the fault.
    fault_evidence: bool


class AuditView(ContractModel):
    signal_id: uuid.UUID
    supported: bool
    auditor: str
    note: str | None
    created_at: UtcDatetime


class EvaluationCaseDetail(EvaluationCaseRow):
    replay_report: dict[str, Any] | None
    citations: tuple[CitedSignal, ...]
    audits: tuple[AuditView, ...]
