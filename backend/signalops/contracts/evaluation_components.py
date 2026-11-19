"""Component evaluation of one role on frozen briefs with planted defects (P13-T04).

A corpus case pairs a brief, which the role sees, with its expected handling, which
only the evaluator sees. The brief is a workflow contract (`SynthesisBrief`,
`VerificationBrief`) and holds no label; `expected` is evaluator truth, stored in the
`evaluation` schema and in the corpus files beside the brief, never inside it.
"""

import uuid
from enum import StrEnum
from typing import Final, Self

from pydantic import Field, JsonValue, model_validator

from signalops.contracts.common import ContractModel, Key, Name, Version
from signalops.contracts.diagnosis import FailureType
from signalops.contracts.roles import SynthesisBrief, Verdict, VerificationBrief

STAGE_CORPUS_VERSION: Final = "1"


class ComponentRole(StrEnum):
    SYNTHESIZER = "synthesizer"
    VERIFIER = "verifier"


class Defect(StrEnum):
    # Verifier briefs: the leading hypothesis is the true cause with fault citations.
    NONE = "none"
    # The leading hypothesis cites records of its service that the fault did not produce.
    NON_FAULT_CITATIONS = "non_fault_citations"
    # The leading hypothesis cites records of services the fault never touched.
    OTHER_SERVICE_CITATIONS = "other_service_citations"
    # Synthesizer report sets.
    LEAD_IN_ONE_ROLE = "lead_in_one_role"
    ROLES_IN_CONFLICT = "roles_in_conflict"
    WEAK_FINDINGS = "weak_findings"


class Expectation(StrEnum):
    VERDICT = "verdict"
    LEADING = "leading"
    CONTRADICTION = "contradiction"
    ABSTENTION = "abstention"


class ExpectedHandling(ContractModel):
    """What a correct role does with the brief. Evaluator truth only."""

    kind: Expectation
    verdict: Verdict | None = None
    # Per cited signal: whether it should be judged supporting.
    citation_supports: dict[uuid.UUID, bool] = Field(default_factory=dict)
    leading_service: Name | None = None
    leading_failure_type: FailureType | None = None
    # The competing service a contradiction should name or cite.
    contradiction_service: Name | None = None
    contradiction_signal_ids: tuple[uuid.UUID, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> Self:
        required = {
            Expectation.VERDICT: self.verdict is not None,
            Expectation.LEADING: self.leading_service is not None
            and self.leading_failure_type is not None,
            Expectation.CONTRADICTION: self.contradiction_service is not None,
            Expectation.ABSTENTION: True,
        }
        if not required[self.kind]:
            raise ValueError(f"a {self.kind} expectation is missing its fields")
        return self


class CorpusCase(ContractModel):
    """One file of `fixtures/evaluation/v<version>/stage-corpus/<role>/`."""

    version: Version = STAGE_CORPUS_VERSION
    key: Key
    role: ComponentRole
    defect: Defect
    # The catalog scenario the brief was built from, and its catalog version.
    source: Name
    source_version: Version
    brief: dict[str, JsonValue]
    expected: ExpectedHandling

    def synthesis_brief(self) -> SynthesisBrief:
        return SynthesisBrief.model_validate(self.brief)

    def verification_brief(self) -> VerificationBrief:
        return VerificationBrief.model_validate(self.brief)

    @model_validator(mode="after")
    def _check(self) -> Self:
        # The brief must validate as the role's input contract, which forbids extra fields.
        if self.role is ComponentRole.SYNTHESIZER:
            self.synthesis_brief()
        else:
            self.verification_brief()
        return self


class ComponentCaseScores(ContractModel):
    """Deterministic scores of one role output against the case's expected handling."""

    # The output matched the expectation (verdict, leading cause, contradiction, abstention).
    correct: bool
    accepted: bool | None = None
    citations_expected: int = 0
    citations_agreeing: int = 0
    leading_service: Name | None = None
    contradiction_listed: bool | None = None
    abstained: bool | None = None
