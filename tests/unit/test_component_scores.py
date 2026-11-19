"""Scores of one role output against a corpus case's expected handling (P13-T04)."""

import uuid
from pathlib import Path

from signalops.contracts.diagnosis import Contradiction, DiagnosisStatus
from signalops.contracts.evaluation_components import ComponentRole, Defect
from signalops.contracts.roles import (
    AgentEnding,
    CitationCheck,
    Provenance,
    SynthesisOutput,
    SynthesisReport,
    Verdict,
    VerificationOutput,
    VerificationReport,
)
from signalops.contracts.workflow import AgentRole
from signalops.evaluation.components import corpus_dir, load_corpus, score_output

CORPUS = corpus_dir(Path(__file__).resolve().parents[2])


def _verification(verdict: Verdict, supports: dict[uuid.UUID, bool]) -> dict[str, object]:
    report = VerificationReport(
        verdict=verdict,
        citation_checks=tuple(
            CitationCheck(signal_id=i, supports=s, note="n") for i, s in supports.items()
        ),
        missing_evidence=("m",) if verdict is Verdict.UNSUPPORTED else (),
    )
    provenance = Provenance(
        role=AgentRole.VERIFIER, prompt_version="v", ended_by=AgentEnding.SUBMITTED
    )
    return VerificationOutput(report=report, provenance=provenance).model_dump(mode="json")


def test_verifier_scores_verdict_and_citation_agreement() -> None:
    cases, _ = load_corpus(CORPUS, ComponentRole.VERIFIER)
    defect = next(case for case in cases if case.defect is Defect.NON_FAULT_CITATIONS)
    expected = defect.expected.citation_supports
    accepted = score_output(defect, _verification(Verdict.SUPPORTED, dict.fromkeys(expected, True)))
    assert (accepted.correct, accepted.accepted, accepted.citations_agreeing) == (False, True, 0)
    rejected = score_output(defect, _verification(Verdict.UNSUPPORTED, dict(expected)))
    assert (rejected.correct, rejected.citations_agreeing) == (True, len(expected))
    assert rejected.citations_expected == len(expected)


def test_synthesizer_scores_contradictions_and_abstentions() -> None:
    cases, _ = load_corpus(CORPUS, ComponentRole.SYNTHESIZER)
    conflict = next(case for case in cases if case.defect is Defect.ROLES_IN_CONFLICT)
    provenance = Provenance(
        role=AgentRole.SYNTHESIZER, prompt_version="v", ended_by=AgentEnding.SUBMITTED
    )
    silent = SynthesisOutput(
        report=SynthesisReport(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE, summary="s", missing_evidence=("m",)
        ),
        provenance=provenance,
    ).model_dump(mode="json")
    assert score_output(conflict, silent).correct is False
    listed = SynthesisOutput(
        report=SynthesisReport(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
            summary="s",
            missing_evidence=("m",),
            contradictions=(
                Contradiction(
                    description="Two roles disagree.",
                    roles=(AgentRole.TELEMETRY_INVESTIGATOR, AgentRole.DEPLOYMENT_INVESTIGATOR),
                    evidence_signal_ids=conflict.expected.contradiction_signal_ids,
                ),
            ),
        ),
        provenance=provenance,
    ).model_dump(mode="json")
    assert score_output(conflict, listed).contradiction_listed is True
    weak = next(case for case in cases if case.defect is Defect.WEAK_FINDINGS)
    assert score_output(weak, silent).correct is True
