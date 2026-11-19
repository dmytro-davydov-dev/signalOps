"""The committed stage corpus (P13-T04): briefs hold no labels; every planted defect is covered."""

from pathlib import Path

import pytest

from signalops.contracts.evaluation_components import ComponentRole, Defect, Expectation
from signalops.contracts.roles import SynthesisBrief, VerificationBrief
from signalops.evaluation.components import corpus_dir, load_corpus

ROOT = Path(__file__).resolve().parents[2]
CORPUS = corpus_dir(ROOT)
# Words a brief would need to carry evaluator truth; none may appear as a key.
LABEL_KEYS = {
    "expected",
    "defect",
    "citation_supports",
    "supporting_event_ids",
    "root_cause_service",
    "diagnosable",
    "lead_roles",
    "clean_twin",
    "fault_window",
    "contradiction_service",
}
DEFECTS = {
    ComponentRole.VERIFIER: {
        Defect.NONE,
        Defect.NON_FAULT_CITATIONS,
        Defect.OTHER_SERVICE_CITATIONS,
    },
    ComponentRole.SYNTHESIZER: {
        Defect.LEAD_IN_ONE_ROLE,
        Defect.ROLES_IN_CONFLICT,
        Defect.WEAK_FINDINGS,
    },
}


def keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {k for item in value.values() for k in keys(item)}
    if isinstance(value, list):
        return {k for item in value for k in keys(item)}
    return set()


@pytest.mark.parametrize("role", list(ComponentRole))
def test_briefs_validate_as_workflow_contracts_and_hold_no_labels(role: ComponentRole) -> None:
    cases, digest = load_corpus(CORPUS, role)
    assert len(digest) == 64
    model = SynthesisBrief if role is ComponentRole.SYNTHESIZER else VerificationBrief
    for case in cases:
        # The role's own input contract forbids any extra field.
        model.model_validate(case.brief)
        assert not keys(case.brief) & LABEL_KEYS, case.key
        assert case.role is role
    assert {case.defect for case in cases} == DEFECTS[role]
    assert len({case.source for case in cases}) == 3


def test_expected_handling_matches_each_defect() -> None:
    verifier, _ = load_corpus(CORPUS, ComponentRole.VERIFIER)
    for case in verifier:
        brief = case.verification_brief()
        assert case.expected.kind is Expectation.VERDICT
        leading = brief.synthesis.leading
        assert leading is not None
        assert set(case.expected.citation_supports) == set(leading.evidence_signal_ids)
        clean = case.defect is Defect.NONE
        assert set(case.expected.citation_supports.values()) == {clean}
        assert brief.follow_up_allowed is False
    synthesizer, _ = load_corpus(CORPUS, ComponentRole.SYNTHESIZER)
    kinds = {
        Defect.LEAD_IN_ONE_ROLE: Expectation.LEADING,
        Defect.ROLES_IN_CONFLICT: Expectation.CONTRADICTION,
        Defect.WEAK_FINDINGS: Expectation.ABSTENTION,
    }
    for case in synthesizer:
        assert case.expected.kind is kinds[case.defect]
