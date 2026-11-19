"""Score each workflow stage of a case from its stored step outputs (P13-T02, ADR-040).

After a case is scored, the runner reads the investigation's completed steps through
the evaluator connection and compares each stage's output with the scenario's labels:

- **Specialists** (`v3`): whether a finding names the true service (and, strictly, the
  failure type), per role and for the scenario's `lead_roles`, and how many of each
  role's citations are fault evidence.
- **Synthesis**: the leading hypothesis, whether it kept a cause a specialist found,
  and the handoff invariants code enforces (ADR-031). An invariant violation means the
  harness or a check is broken, not that a model erred.
- **Contradictions**: whether the diagnosis lists the labelled competing service.
- **Verification**: the first verdict against whether the lead is the true cause, and
  what a follow-up round changed.
- **Single agent** (`v2`): whether the proposal named the true cause, and whether
  `conclude` kept, corrected, or dropped it.
- **Endings**: how every role's loop ended; a limit ending is a bound, not a judgement.

Nothing here calls a model: the inputs are rows already stored. Findings name services
by ID, mapped to names through the workspace's catalog.
"""

import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from pydantic import BaseModel, ValidationError
from sqlalchemy import select

from signalops.agents.investigator import ABSTENTION_REASONS
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.evaluation import ExpectedOutcome
from signalops.contracts.evaluation_stages import (
    MAX_STAGE_NOTES,
    Conclusion,
    FollowUpEffect,
    RoleEnding,
    SpecialistStage,
    StageScores,
    VerdictCell,
)
from signalops.contracts.roles import (
    AgentEnding,
    FollowUpOutput,
    SpecialistFindings,
    SynthesisOutput,
    Verdict,
    VerificationOutput,
)
from signalops.contracts.steps import ProposedDiagnosis
from signalops.contracts.workflow import AgentRole, StepStatus
from signalops.evaluation.invariants import forbidden_calls, secrets_in
from signalops.persistence.database import Database
from signalops.persistence.models import AgentStep, Investigation, Service, Signal, ToolCall
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.scope import EvidenceScope

SPECIALIST_STEPS: Final = ("telemetry", "deployment", "dependency")
ENDING_BY_REASON: Final = {reason: ending for ending, reason in ABSTENTION_REASONS.items()}


class StageOutputError(ValueError):
    """A stored step output does not validate; the case's stages cannot be scored."""


@dataclass(frozen=True)
class StageInputs:
    """Everything stage scoring reads, gathered from stored rows."""

    workflow_version: str
    # Completed steps' outputs by step key.
    steps: Mapping[str, dict[str, Any]]
    diagnosis: Diagnosis | None
    service_names: Mapping[uuid.UUID, str]
    # For cited signals in the case's scope: source event ID and service name.
    event_ids: Mapping[uuid.UUID, str] = field(default_factory=dict)
    signal_services: Mapping[uuid.UUID, str] = field(default_factory=dict)
    # Invariant inputs (P13-T07): stored texts, tool calls by step, and citations that
    # are not in scope or not known by the cutoff.
    texts: tuple[str, ...] = ()
    tool_calls: tuple[tuple[str, str], ...] = ()
    out_of_scope: int = 0


def _parse[T: BaseModel](model: type[T], steps: Mapping[str, dict[str, Any]], key: str) -> T | None:
    output = steps.get(key)
    if output is None:
        return None
    try:
        return model.model_validate(output)
    except ValidationError as error:
        raise StageOutputError(f"step {key} does not validate") from error


def _cause_matches(
    expected: ExpectedOutcome, service: str | None, failure_type: str | None
) -> tuple[bool, bool]:
    """Whether a service, and a service with a failure type, are the true cause."""
    if not expected.diagnosable or service is None:
        return False, False
    named = service == expected.root_cause_service
    return named, named and failure_type == expected.failure_type


def _supported(expected: ExpectedOutcome, inputs: StageInputs, ids: Iterable[uuid.UUID]) -> int:
    supporting = set(expected.supporting_event_ids)
    return sum(inputs.event_ids.get(signal_id) in supporting for signal_id in ids)


def score_stages(expected: ExpectedOutcome, inputs: StageInputs) -> StageScores | None:
    """Stage scores of an agent case; None for a workflow without stages (`d1`)."""
    if inputs.workflow_version == "v3":
        scores = _score_v3(expected, inputs)
    elif inputs.workflow_version == "v2":
        scores = _score_v2(expected, inputs)
    else:
        return None
    round_ = _parse(FollowUpOutput, inputs.steps, "follow_up")
    follow_up_role = round_.findings.role if round_ and round_.findings else None
    return scores.model_copy(
        update={
            "secrets_exposed": secrets_in(inputs.texts),
            "forbidden_tool_calls": forbidden_calls(inputs.tool_calls, follow_up_role),
            "out_of_scope_citations": inputs.out_of_scope,
        }
    )


def _score_v3(expected: ExpectedOutcome, inputs: StageInputs) -> StageScores:
    names = inputs.service_names
    findings = [
        found
        for key in SPECIALIST_STEPS
        if (found := _parse(SpecialistFindings, inputs.steps, key)) is not None
    ]
    specialists = []
    for stored in findings:
        named = [
            _cause_matches(
                expected,
                names.get(finding.service_id) if finding.service_id else None,
                finding.failure_type,
            )
            for finding in stored.report.findings
        ]
        cited = stored.report.cited()
        specialists.append(
            SpecialistStage(
                role=stored.role,
                ended_by=stored.provenance.ended_by,
                findings=len(stored.report.findings),
                named_service=any(service for service, _ in named),
                named_cause=any(cause for _, cause in named),
                cited=len(cited),
                cited_supported=_supported(expected, inputs, cited),
            )
        )
    endings = [
        RoleEnding(step_key=key, role=stored.role, ended_by=stored.provenance.ended_by)
        for key, stored in zip(SPECIALIST_STEPS, findings, strict=False)
    ]
    scores: dict[str, Any] = {
        "specialists": tuple(specialists),
        "expected_contradiction": expected.expected_contradiction,
    }
    if expected.diagnosable:
        lead_roles = set(expected.lead_roles)
        scores |= {
            "lead_found": any(s.named_service for s in specialists),
            "lead_found_strict": any(s.named_cause for s in specialists),
            "lead_role_found": any(s.named_service and s.role in lead_roles for s in specialists),
        }

    synthesis = _parse(SynthesisOutput, inputs.steps, "synthesize")
    if synthesis is not None:
        endings.append(
            RoleEnding(
                step_key="synthesize",
                role=AgentRole.SYNTHESIZER,
                ended_by=synthesis.provenance.ended_by,
            )
        )
        leading = synthesis.report.leading
        scores |= {
            "synthesis_status": synthesis.report.status,
            "synthesis_violations": _synthesis_violations(synthesis, findings),
        }
        if leading is not None:
            service = names.get(leading.service_id)
            _, correct = _cause_matches(expected, service, leading.failure_type)
            scores |= {
                "leading_service": service,
                "leading_failure_type": leading.failure_type,
                "leading_correct": correct,
            }
        if scores.get("lead_found_strict"):
            scores["lead_retained"] = bool(scores.get("leading_correct"))

    if expected.expected_contradiction is not None and inputs.diagnosis is not None:
        target = expected.expected_contradiction
        scores["contradiction_listed"] = any(
            target in item.description
            or any(inputs.signal_services.get(i) == target for i in item.evidence_signal_ids)
            for item in inputs.diagnosis.contradictions
        )

    verification = _parse(VerificationOutput, inputs.steps, "verify")
    round_ = _parse(FollowUpOutput, inputs.steps, "follow_up")
    final = _parse(VerificationOutput, inputs.steps, "reverify")
    for key, output in (("verify", verification), ("reverify", final)):
        if output is not None:
            endings.append(
                RoleEnding(
                    step_key=key, role=AgentRole.VERIFIER, ended_by=output.provenance.ended_by
                )
            )
    if round_ is not None and round_.findings is not None:
        endings.append(
            RoleEnding(
                step_key="follow_up",
                role=round_.findings.role,
                ended_by=round_.findings.provenance.ended_by,
            )
        )
    lead_correct = scores.get("leading_correct")
    if (
        verification is not None
        and lead_correct is not None
        and verification.provenance.ended_by is not AgentEnding.SKIPPED
    ):
        verdict = verification.report.verdict
        scores |= {"first_verdict": verdict, "verdict_cell": _cell(lead_correct, verdict)}
        ran = round_ is not None and round_.ran
        if ran and final is not None and final.provenance.ended_by is not AgentEnding.SKIPPED:
            before = (verdict is Verdict.SUPPORTED) == lead_correct
            after = (final.report.verdict is Verdict.SUPPORTED) == lead_correct
            scores |= {
                "follow_up_ran": True,
                "final_verdict": final.report.verdict,
                "follow_up_effect": (
                    FollowUpEffect.UNCHANGED
                    if before == after
                    else FollowUpEffect.WRONG_TO_RIGHT
                    if after
                    else FollowUpEffect.RIGHT_TO_WRONG
                ),
            }
        else:
            scores["final_verdict"] = verdict
    return StageScores(
        workflow_version=inputs.workflow_version,
        diagnosable=expected.diagnosable,
        endings=tuple(endings[:MAX_STAGE_NOTES]),
        **scores,
    )


def _cell(correct: bool, verdict: Verdict) -> VerdictCell:
    if verdict is Verdict.SUPPORTED:
        return VerdictCell.CORRECT_ACCEPTED if correct else VerdictCell.WRONG_ACCEPTED
    if verdict is Verdict.NEEDS_FOLLOW_UP:
        return VerdictCell.CORRECT_FOLLOW_UP if correct else VerdictCell.WRONG_FOLLOW_UP
    return VerdictCell.CORRECT_REJECTED if correct else VerdictCell.WRONG_REJECTED


def _synthesis_violations(
    synthesis: SynthesisOutput, findings: list[SpecialistFindings]
) -> tuple[str, ...]:
    """The handoff checks of ADR-031, asserted again on what was stored."""
    cited = frozenset().union(*(f.report.cited() for f in findings))
    reporting = {f.role for f in findings if f.report.findings}
    problems = []
    for position, hypothesis in enumerate(synthesis.report.hypotheses):
        if set(hypothesis.evidence_signal_ids) - cited:
            problems.append(f"hypotheses[{position}] cites a signal no specialist cited")
        if set(hypothesis.supporting_roles) - reporting:
            problems.append(f"hypotheses[{position}] credits a role with no findings")
    return tuple(problems[:MAX_STAGE_NOTES])


def _score_v2(expected: ExpectedOutcome, inputs: StageInputs) -> StageScores:
    proposal = _parse(ProposedDiagnosis, inputs.steps, "investigate")
    scores: dict[str, Any] = {}
    endings = []
    if proposal is not None:
        cause = proposal.root_cause
        service = inputs.service_names.get(cause.service_id) if cause else None
        named, correct = _cause_matches(expected, service, cause.failure_type if cause else None)
        diagnosed = proposal.status is DiagnosisStatus.DIAGNOSED
        ended = AgentEnding.SUBMITTED
        if not diagnosed:
            reasons = [ENDING_BY_REASON.get(item) for item in proposal.missing_evidence]
            ended = next((reason for reason in reasons if reason), AgentEnding.SUBMITTED)
        endings.append(
            RoleEnding(step_key="investigate", role=AgentRole.INVESTIGATOR, ended_by=ended)
        )
        scores |= {
            "proposal_status": proposal.status,
            "proposal_service": service,
            "proposal_failure_type": cause.failure_type if cause else None,
            # With no true cause, asserting any is wrong and abstaining is right.
            "proposal_correct": correct if expected.diagnosable else not diagnosed,
            "proposal_service_correct": named if expected.diagnosable else None,
            "conclusion": _conclusion(proposal, inputs.diagnosis),
        }
    return StageScores(
        workflow_version=inputs.workflow_version,
        diagnosable=expected.diagnosable,
        endings=tuple(endings),
        **scores,
    )


def _conclusion(proposal: ProposedDiagnosis, diagnosis: Diagnosis | None) -> Conclusion | None:
    if proposal.root_cause is None:
        return Conclusion.ABSTAINED
    if diagnosis is None:
        return None
    if diagnosis.root_cause is None:
        return Conclusion.DROPPED
    same = (diagnosis.root_cause.service_id, diagnosis.root_cause.failure_type) == (
        proposal.root_cause.service_id,
        proposal.root_cause.failure_type,
    )
    return Conclusion.KEPT if same else Conclusion.CORRECTED


def _cited_ids(steps: Mapping[str, dict[str, Any]], diagnosis: Diagnosis | None) -> set[uuid.UUID]:
    ids: set[uuid.UUID] = set()
    for key in (*SPECIALIST_STEPS, "follow_up"):
        output = steps.get(key)
        if output is None:
            continue
        stored = output.get("findings") if key == "follow_up" else output
        if not isinstance(stored, dict):
            continue
        try:
            ids |= SpecialistFindings.model_validate(stored).report.cited()
        except ValidationError as error:
            raise StageOutputError(f"step {key} does not validate") from error
    if diagnosis is not None:
        for item in diagnosis.contradictions:
            ids |= set(item.evidence_signal_ids)
    return ids


async def load_stage_inputs(
    database: Database, scope: EvidenceScope, investigation_id: uuid.UUID
) -> StageInputs:
    """The case's stored steps, outcome, and the catalog and signals they refer to."""
    async with database.transaction() as session:
        investigation = await session.get_one(Investigation, investigation_id)
        rows = await session.scalars(
            select(AgentStep).where(
                AgentStep.investigation_id == investigation_id,
                AgentStep.status == StepStatus.COMPLETED,
            )
        )
        stored = list(rows)
        steps = {row.step_key: row.output for row in stored if row.output is not None}
        calls = (
            await session.execute(
                select(AgentStep.step_key, ToolCall.tool_name, ToolCall.arguments)
                .join(ToolCall, ToolCall.agent_step_id == AgentStep.id)
                .where(AgentStep.investigation_id == investigation_id)
            )
        ).all()
        diagnosis = (
            None
            if investigation.diagnosis is None
            else Diagnosis.model_validate(investigation.diagnosis)
        )
        services = await session.execute(
            select(Service.id, Service.name).where(Service.workspace_id == scope.workspace_id)
        )
        names = {service_id: name for service_id, name in services}
        cited = _cited_ids(steps, diagnosis)
        if diagnosis is not None and diagnosis.root_cause is not None:
            cited |= set(diagnosis.root_cause.evidence_signal_ids)
        known = await EvidenceRepository(session).known_signals(
            scope, cited, known_by=investigation.evidence_cutoff_at
        )
        event_ids: dict[uuid.UUID, str] = {}
        signal_services: dict[uuid.UUID, str] = {}
        if cited:
            signals = await session.execute(
                select(Signal.id, Signal.source_event_id, Signal.service_id).where(
                    scope.clause(Signal), Signal.id.in_(cited)
                )
            )
            for signal_id, event_id, service_id in signals:
                event_ids[signal_id] = event_id
                if service_id in names:
                    signal_services[signal_id] = names[service_id]
    return StageInputs(
        workflow_version=investigation.workflow_version,
        steps=steps,
        diagnosis=diagnosis,
        service_names=names,
        event_ids=event_ids,
        signal_services=signal_services,
        texts=(
            *(json.dumps(output, sort_keys=True) for output in steps.values()),
            json.dumps(investigation.diagnosis, sort_keys=True),
            *(json.dumps(arguments, sort_keys=True) for _, _, arguments in calls),
        ),
        tool_calls=tuple((step, tool) for step, tool, _ in calls),
        out_of_scope=len(cited - set(known)),
    )
