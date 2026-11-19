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
from signalops.contracts.evaluation_stages import FailureStage, StageScores
from signalops.contracts.roles import AgentEnding, Verdict
from signalops.contracts.workflow import AgentRole


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
    # Per-stage scores from the stored steps (P13-T02); None for `d1` and older cases.
    stage_scores: StageScores | None = None
    # The first failing stage of an incorrect case (P13-T03); None when correct.
    failure_stage: FailureStage | None = None
    failure_origin: str | None = None
    limit_hit: bool | None = None
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


class StageClaim(ContractModel):
    """A finding, hypothesis, or proposal as a stage stored it, with services named."""

    claim: str
    service_name: str | None
    failure_type: str | None
    confidence: float | None
    evidence_signal_ids: tuple[uuid.UUID, ...] = ()
    supporting_roles: tuple[AgentRole, ...] = ()


class StageCitationCheck(ContractModel):
    signal_id: uuid.UUID
    supports: bool


class StageOutputView(ContractModel):
    """One stored workflow step of a case, for reading next to its stage scores."""

    step_key: str
    role: AgentRole
    ended_by: AgentEnding | None = None
    status: str | None = None
    findings: tuple[StageClaim, ...] = ()
    # The synthesis's leading hypothesis, or the single agent's proposal.
    leading: StageClaim | None = None
    verdict: Verdict | None = None
    citation_checks: tuple[StageCitationCheck, ...] = ()
    missing_evidence: tuple[str, ...] = ()
    note: str | None = None


class EvaluationCaseDetail(EvaluationCaseRow):
    replay_report: dict[str, Any] | None
    citations: tuple[CitedSignal, ...]
    audits: tuple[AuditView, ...]
    # Stored stage outputs (P13-T03) and every signal they cite, labelled.
    stage_outputs: tuple[StageOutputView, ...] = ()
    stage_signals: tuple[CitedSignal, ...] = ()
