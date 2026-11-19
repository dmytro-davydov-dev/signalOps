"""Stage scores of one evaluation case, computed from its stored workflow steps (P13-T02).

Each field compares what a stage stored (`agent_steps.output`) with the scenario's
deterministic labels. Nothing calls a model. A field is None when the stage did not
apply to the case: a single-agent case has no specialists, and a scenario without a
true cause has no lead to find. Evaluator data, stored in the `evaluation` schema.
"""

from enum import StrEnum
from typing import Final

from pydantic import Field, NonNegativeInt

from signalops.contracts.common import ContractModel, Name, ShortText, Version
from signalops.contracts.diagnosis import DiagnosisStatus, FailureType
from signalops.contracts.roles import MAX_BRANCHES, AgentEnding, Verdict
from signalops.contracts.workflow import AgentRole

STAGE_SCORES_VERSION: Final = "stage-scores/v1"
# Role endings that mean a bound, not the model's judgement, ended the role.
LIMIT_ENDINGS: Final = frozenset(
    {
        AgentEnding.BUDGET_SHARE,
        AgentEnding.TURN_LIMIT,
        AgentEnding.REPAIR_LIMIT,
        AgentEnding.REFUSED,
        AgentEnding.NO_TURNS,
    }
)
MAX_STAGE_NOTES: Final = 20


class VerdictCell(StrEnum):
    """The first verification against whether the leading hypothesis is the true cause."""

    CORRECT_ACCEPTED = "correct_accepted"
    CORRECT_REJECTED = "correct_rejected"
    CORRECT_FOLLOW_UP = "correct_follow_up"
    WRONG_ACCEPTED = "wrong_accepted"
    WRONG_REJECTED = "wrong_rejected"
    WRONG_FOLLOW_UP = "wrong_follow_up"


class FollowUpEffect(StrEnum):
    # Whether the verifier's decision was right, before and after the follow-up round.
    WRONG_TO_RIGHT = "wrong_to_right"
    RIGHT_TO_WRONG = "right_to_wrong"
    UNCHANGED = "unchanged"


class Conclusion(StrEnum):
    """What the single agent's `conclude` did with its proposal."""

    KEPT = "kept"
    CORRECTED = "corrected"
    DROPPED = "dropped"
    ABSTAINED = "abstained"


class FailureStage(StrEnum):
    """The first stage that failed in an incorrect case (P13-T03), checked in this order."""

    HARNESS_FAILURE = "harness_failure"
    DETECTION_MISS = "detection_miss"
    NO_LEAD = "no_lead"
    LEAD_LOST_IN_SYNTHESIS = "lead_lost_in_synthesis"
    VERIFIER_FALSE_REJECT = "verifier_false_reject"
    VERIFIER_FALSE_ACCEPT = "verifier_false_accept"
    CONCLUDE_DROPPED = "conclude_dropped"
    WRONG_FAILURE_TYPE = "wrong_failure_type"
    FALSE_DIAGNOSIS_ON_UNDERDETERMINED = "false_diagnosis_on_underdetermined"
    RULE_ERROR = "rule_error"
    # An agent case whose stage scores are missing: no stage can be named honestly.
    UNATTRIBUTED = "unattributed"


class Attribution(ContractModel):
    stage: FailureStage
    # The step that first asserted a cause, for a false diagnosis on underdetermined evidence.
    origin: Name | None = None
    # Some role ended on a limit; orthogonal to the stage.
    limit_hit: bool = False


class RoleEnding(ContractModel):
    step_key: Name
    role: AgentRole
    ended_by: AgentEnding


class SpecialistStage(ContractModel):
    role: AgentRole
    ended_by: AgentEnding
    findings: NonNegativeInt
    # Some finding names the true service; strictly, with the true failure type too.
    named_service: bool
    named_cause: bool
    cited: NonNegativeInt
    # Cited signals the scenario's fault produced.
    cited_supported: NonNegativeInt


class StageScores(ContractModel):
    version: Version = STAGE_SCORES_VERSION
    workflow_version: Version
    diagnosable: bool

    # --- Multi-agent (v3) ---
    specialists: tuple[SpecialistStage, ...] = Field(default=(), max_length=MAX_BRANCHES)
    # Diagnosable cases only: some specialist named the true service (and cause).
    lead_found: bool | None = None
    lead_found_strict: bool | None = None
    # Some role in the scenario's `lead_roles` named the true service.
    lead_role_found: bool | None = None
    synthesis_status: DiagnosisStatus | None = None
    leading_service: Name | None = None
    leading_failure_type: FailureType | None = None
    # The leading hypothesis is the true cause (service and failure type).
    leading_correct: bool | None = None
    # Some specialist named the true cause and the leading hypothesis kept it.
    lead_retained: bool | None = None
    # Handoff invariants code enforces (ADR-031); any entry is a harness defect.
    synthesis_violations: tuple[ShortText, ...] = Field(default=(), max_length=MAX_STAGE_NOTES)
    expected_contradiction: Name | None = None
    contradiction_listed: bool | None = None
    first_verdict: Verdict | None = None
    verdict_cell: VerdictCell | None = None
    follow_up_ran: bool = False
    final_verdict: Verdict | None = None
    follow_up_effect: FollowUpEffect | None = None

    # --- Single agent (v2) ---
    proposal_status: DiagnosisStatus | None = None
    proposal_service: Name | None = None
    proposal_failure_type: FailureType | None = None
    # The proposal names the true cause; None when the scenario has none.
    proposal_correct: bool | None = None
    proposal_service_correct: bool | None = None
    conclusion: Conclusion | None = None

    # --- Both ---
    endings: tuple[RoleEnding, ...] = Field(default=(), max_length=MAX_STAGE_NOTES)

    @property
    def limit_endings(self) -> tuple[RoleEnding, ...]:
        return tuple(ending for ending in self.endings if ending.ended_by in LIMIT_ENDINGS)

    @property
    def limit_hit(self) -> bool:
        return bool(self.limit_endings)
