"""Handoffs between the roles of the multi-agent workflow (`v3`, ADR-031).

Each role gets a brief built by code holding only what it needs, and returns a
typed report. Code checks a report before it is stored, then stores it with its
provenance: which role and prompt version wrote it, what the role read, and how
its loop ended. Stored outputs are reused after a restart, so a change that old
rows would fail needs a new workflow version.

Confidence everywhere is a model's own assessment, never a measurement.
"""

import uuid
from enum import StrEnum
from typing import Final, Self

from pydantic import Field, NonNegativeInt, model_validator

from signalops.contracts.common import (
    ContractModel,
    Key,
    LongText,
    Name,
    ShortText,
    UtcDatetime,
    Version,
    require_unique,
)
from signalops.contracts.diagnosis import (
    MAX_EVIDENCE_IDS,
    MAX_LIST_ITEMS,
    DiagnosisStatus,
    FailureType,
    SuggestedAction,
)
from signalops.contracts.steps import MAX_BRIEF_EVIDENCE, MAX_BRIEF_SERVICES, ServiceBrief
from signalops.contracts.workflow import AgentRole, ToolName

PLAN_VERSION: Final = "plan-v1"
MAX_FINDINGS: Final = 5
MAX_FINDING_EVIDENCE: Final = 20
MAX_HYPOTHESES: Final = 5
MAX_CONTRADICTIONS: Final = 10
MAX_BRANCHES: Final = 3
MAX_PACKET_SIGNALS: Final = MAX_EVIDENCE_IDS
MAX_PROVENANCE_TOOL_CALLS: Final = 64

SPECIALIST_ROLES: Final = (
    AgentRole.TELEMETRY_INVESTIGATOR,
    AgentRole.DEPLOYMENT_INVESTIGATOR,
    AgentRole.DEPENDENCY_INVESTIGATOR,
)


def _unique_ids(values: tuple[uuid.UUID, ...], field: str) -> None:
    require_unique(values, field)


def _specialist(role: AgentRole) -> AgentRole:
    if role not in SPECIALIST_ROLES:
        raise ValueError(f"{role} is not a specialist role")
    return role


class IncidentFacts(ContractModel):
    """What every role may know about the incident: fields fixed at detection."""

    incident_id: uuid.UUID
    title: ShortText
    detected_at: UtcDatetime
    evidence_cutoff_at: UtcDatetime
    window_start: UtcDatetime | None = None


# --- Specialists ---


class SpecialistBrief(ContractModel):
    """A specialist's first message. Code fills only the fields its role needs."""

    role: AgentRole
    incident: IncidentFacts
    services: tuple[ServiceBrief, ...] = Field(default=(), max_length=MAX_BRIEF_SERVICES)
    # Telemetry: signals the detector cited.
    detector_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_BRIEF_EVIDENCE)
    # Deployment and dependency: the services linked to the incident.
    incident_service_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_BRIEF_SERVICES)
    # Set only on the verifier's follow-up round.
    question: ShortText | None = None

    @model_validator(mode="after")
    def _check_role(self) -> Self:
        _specialist(self.role)
        return self


class BranchPlan(ContractModel):
    role: AgentRole
    step_key: Key
    # Model turns this branch may take; 0 means it reports without a call.
    max_turns: NonNegativeInt
    # The most this branch's calls may hold and spend together (P09-T02).
    budget_share_microusd: NonNegativeInt
    # Everything the branch is told: code chose it.
    brief: SpecialistBrief

    @model_validator(mode="after")
    def _check_role(self) -> Self:
        _specialist(self.role)
        if self.brief.role is not self.role:
            raise ValueError("brief.role must match role")
        return self


class InvestigationPlan(ContractModel):
    """The coordinator's output, built by code from the investigation's limits."""

    plan_version: Version = PLAN_VERSION
    branches: tuple[BranchPlan, ...] = Field(min_length=1, max_length=MAX_BRANCHES)
    synthesis_turns: NonNegativeInt
    verification_turns: NonNegativeInt
    follow_up_turns: NonNegativeInt
    # Held back from the branches so synthesis and verification can still run.
    downstream_reserve_microusd: NonNegativeInt

    @model_validator(mode="after")
    def _check_branches(self) -> Self:
        require_unique(tuple(branch.role for branch in self.branches), "branches.role")
        require_unique(tuple(branch.step_key for branch in self.branches), "branches.step_key")
        return self

    def branch(self, role: AgentRole) -> BranchPlan:
        return next(branch for branch in self.branches if branch.role is role)


class Finding(ContractModel):
    claim: LongText
    service_id: uuid.UUID | None = None
    failure_type: FailureType | None = None
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    # What would make this finding wrong, or what was not checked.
    uncertainty: ShortText
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_FINDING_EVIDENCE)
    # Runbooks read, as `key@version`.
    references: tuple[Key, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)

    @model_validator(mode="after")
    def _check(self) -> Self:
        _unique_ids(self.evidence_signal_ids, "evidence_signal_ids")
        require_unique(self.references, "references")
        if self.failure_type is not None and self.service_id is None:
            raise ValueError("a failure_type needs a service_id")
        return self


class SpecialistReport(ContractModel):
    """What a specialist submits."""

    findings: tuple[Finding, ...] = Field(default=(), max_length=MAX_FINDINGS)
    missing_evidence: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    limitations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.findings and not self.missing_evidence:
            raise ValueError("a report with no findings must list missing_evidence")
        return self

    def cited(self) -> frozenset[uuid.UUID]:
        return frozenset(i for finding in self.findings for i in finding.evidence_signal_ids)


class AgentEnding(StrEnum):
    SUBMITTED = "submitted"
    NO_TURNS = "no_turns"
    TURN_LIMIT = "turn_limit"
    REPAIR_LIMIT = "repair_limit"
    REFUSED = "refused"
    BUDGET_SHARE = "budget_share"
    # Code found nothing for the role to assess, so no call was made.
    SKIPPED = "skipped"


class ToolCallRef(ContractModel):
    tool: ToolName
    call_key: Key


class Provenance(ContractModel):
    """How a role's output came about. Never the model's reasoning."""

    role: AgentRole
    prompt_version: Version
    # None when the role made no model call.
    model: Name | None = None
    model_turns: NonNegativeInt = 0
    tool_calls: tuple[ToolCallRef, ...] = Field(default=(), max_length=MAX_PROVENANCE_TOOL_CALLS)
    signals_read: NonNegativeInt = 0
    ended_by: AgentEnding


class SpecialistFindings(ContractModel):
    """A specialist step's stored output."""

    role: AgentRole
    report: SpecialistReport
    provenance: Provenance

    @model_validator(mode="after")
    def _check_role(self) -> Self:
        _specialist(self.role)
        if self.provenance.role is not self.role:
            raise ValueError("provenance.role must match role")
        return self


# --- Synthesizer ---


class RoleReport(ContractModel):
    role: AgentRole
    ended_by: AgentEnding
    report: SpecialistReport


class SynthesisBrief(ContractModel):
    """The synthesizer reads the specialists' reports, not their evidence or tool calls."""

    incident: IncidentFacts
    services: tuple[ServiceBrief, ...] = Field(default=(), max_length=MAX_BRIEF_SERVICES)
    reports: tuple[RoleReport, ...] = Field(min_length=1, max_length=MAX_BRANCHES + 1)


class Hypothesis(ContractModel):
    service_id: uuid.UUID
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(min_length=1, max_length=MAX_EVIDENCE_IDS)
    supporting_roles: tuple[AgentRole, ...] = Field(min_length=1, max_length=MAX_BRANCHES)
    assessment: LongText

    @model_validator(mode="after")
    def _check(self) -> Self:
        _unique_ids(self.evidence_signal_ids, "evidence_signal_ids")
        require_unique(self.supporting_roles, "supporting_roles")
        for role in self.supporting_roles:
            _specialist(role)
        return self


class Contradiction(ContractModel):
    description: LongText
    roles: tuple[AgentRole, ...] = Field(default=(), max_length=MAX_BRANCHES)
    evidence_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_FINDING_EVIDENCE)

    @model_validator(mode="after")
    def _check(self) -> Self:
        _unique_ids(self.evidence_signal_ids, "evidence_signal_ids")
        require_unique(self.roles, "roles")
        return self


class SynthesisReport(ContractModel):
    """What the synthesizer submits: ranked hypotheses, the first one leading."""

    status: DiagnosisStatus
    summary: LongText
    hypotheses: tuple[Hypothesis, ...] = Field(default=(), max_length=MAX_HYPOTHESES)
    contradictions: tuple[Contradiction, ...] = Field(default=(), max_length=MAX_CONTRADICTIONS)
    suggested_actions: tuple[SuggestedAction, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    limitations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    missing_evidence: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.status is DiagnosisStatus.DIAGNOSED and not self.hypotheses:
            raise ValueError("a diagnosed synthesis needs a hypothesis")
        if self.status is DiagnosisStatus.INSUFFICIENT_EVIDENCE and not self.missing_evidence:
            raise ValueError("an insufficient_evidence synthesis must list missing_evidence")
        return self

    @property
    def leading(self) -> Hypothesis | None:
        if self.status is not DiagnosisStatus.DIAGNOSED:
            return None
        return self.hypotheses[0]


class SynthesisOutput(ContractModel):
    report: SynthesisReport
    provenance: Provenance


# --- Verifier ---


class CitedSignal(ContractModel):
    """One cited signal as code read it from PostgreSQL, redacted."""

    signal_id: uuid.UUID
    kind: Name
    service_id: uuid.UUID | None = None
    occurred_at: UtcDatetime
    text: LongText


class VerificationBrief(ContractModel):
    incident: IncidentFacts
    services: tuple[ServiceBrief, ...] = Field(default=(), max_length=MAX_BRIEF_SERVICES)
    synthesis: SynthesisReport
    # The leading hypothesis's citations, read again by code.
    cited: tuple[CitedSignal, ...] = Field(default=(), max_length=MAX_PACKET_SIGNALS)
    # Cited IDs that code could not read within scope and cutoff.
    unreadable_signal_ids: tuple[uuid.UUID, ...] = Field(default=(), max_length=MAX_EVIDENCE_IDS)


class Verdict(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"


class CitationCheck(ContractModel):
    signal_id: uuid.UUID
    supports: bool
    note: ShortText


class VerificationReport(ContractModel):
    """What the verifier submits about the leading hypothesis."""

    verdict: Verdict
    citation_checks: tuple[CitationCheck, ...] = Field(default=(), max_length=MAX_EVIDENCE_IDS)
    missing_evidence: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    limitations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)

    @model_validator(mode="after")
    def _check(self) -> Self:
        require_unique(tuple(check.signal_id for check in self.citation_checks), "citation_checks")
        if self.verdict is Verdict.UNSUPPORTED and not self.missing_evidence:
            raise ValueError("an unsupported verdict must list missing_evidence")
        return self

    def supporting(self) -> tuple[uuid.UUID, ...]:
        return tuple(check.signal_id for check in self.citation_checks if check.supports)


class VerificationOutput(ContractModel):
    report: VerificationReport
    provenance: Provenance
