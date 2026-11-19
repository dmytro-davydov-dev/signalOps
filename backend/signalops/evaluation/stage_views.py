"""Stored step outputs of a case, shaped for the case page (P13-T03).

Shows what each stage concluded, with services named and signal IDs kept so the page
can link them to the cited evidence. Nothing here scores anything: `stages.py` does.
A step that does not validate is shown with a note rather than hidden.
"""

import uuid
from collections.abc import Mapping
from typing import Any, Final

from pydantic import BaseModel, ValidationError

from signalops.contracts.evaluation_api import StageCitationCheck, StageClaim, StageOutputView
from signalops.contracts.roles import (
    Finding,
    FollowUpOutput,
    Hypothesis,
    SpecialistFindings,
    SynthesisOutput,
    VerificationOutput,
)
from signalops.contracts.steps import ProposedDiagnosis
from signalops.contracts.workflow import AgentRole

ORDER: Final = (
    "investigate",
    "telemetry",
    "deployment",
    "dependency",
    "synthesize",
    "verify",
    "follow_up",
    "reverify",
)
ROLE_BY_STEP: Final = {
    "investigate": AgentRole.INVESTIGATOR,
    "telemetry": AgentRole.TELEMETRY_INVESTIGATOR,
    "deployment": AgentRole.DEPLOYMENT_INVESTIGATOR,
    "dependency": AgentRole.DEPENDENCY_INVESTIGATOR,
    "synthesize": AgentRole.SYNTHESIZER,
    "verify": AgentRole.VERIFIER,
    "follow_up": AgentRole.COORDINATOR,
    "reverify": AgentRole.VERIFIER,
}
MODELS: Final[Mapping[str, type[BaseModel]]] = {
    "investigate": ProposedDiagnosis,
    "telemetry": SpecialistFindings,
    "deployment": SpecialistFindings,
    "dependency": SpecialistFindings,
    "synthesize": SynthesisOutput,
    "verify": VerificationOutput,
    "follow_up": FollowUpOutput,
    "reverify": VerificationOutput,
}


def _finding(item: Finding, names: Mapping[uuid.UUID, str]) -> StageClaim:
    return StageClaim(
        claim=item.claim,
        service_name=names.get(item.service_id) if item.service_id else None,
        failure_type=item.failure_type,
        confidence=item.confidence,
        evidence_signal_ids=item.evidence_signal_ids,
    )


def _hypothesis(item: Hypothesis, names: Mapping[uuid.UUID, str]) -> StageClaim:
    return StageClaim(
        claim=item.assessment,
        service_name=names.get(item.service_id),
        failure_type=item.failure_type,
        confidence=item.confidence,
        evidence_signal_ids=item.evidence_signal_ids,
        supporting_roles=item.supporting_roles,
    )


def _specialist(
    key: str, stored: SpecialistFindings, names: Mapping[uuid.UUID, str]
) -> StageOutputView:
    return StageOutputView(
        step_key=key,
        role=stored.role,
        ended_by=stored.provenance.ended_by,
        findings=tuple(_finding(item, names) for item in stored.report.findings),
        missing_evidence=stored.report.missing_evidence,
    )


def _view(key: str, output: BaseModel, names: Mapping[uuid.UUID, str]) -> StageOutputView:
    if isinstance(output, SpecialistFindings):
        return _specialist(key, output, names)
    if isinstance(output, SynthesisOutput):
        leading = output.report.leading
        return StageOutputView(
            step_key=key,
            role=AgentRole.SYNTHESIZER,
            ended_by=output.provenance.ended_by,
            status=output.report.status.value,
            findings=tuple(_hypothesis(item, names) for item in output.report.hypotheses),
            leading=None if leading is None else _hypothesis(leading, names),
            missing_evidence=output.report.missing_evidence,
        )
    if isinstance(output, VerificationOutput):
        return StageOutputView(
            step_key=key,
            role=AgentRole.VERIFIER,
            ended_by=output.provenance.ended_by,
            verdict=output.report.verdict,
            citation_checks=tuple(
                StageCitationCheck(signal_id=check.signal_id, supports=check.supports)
                for check in output.report.citation_checks
            ),
            missing_evidence=output.report.missing_evidence,
        )
    if isinstance(output, FollowUpOutput):
        if output.findings is not None:
            return _specialist(key, output.findings, names)
        return StageOutputView(step_key=key, role=AgentRole.COORDINATOR, note=output.reason)
    assert isinstance(output, ProposedDiagnosis)  # noqa: S101 - MODELS lists five types
    cause = output.root_cause
    return StageOutputView(
        step_key=key,
        role=AgentRole.INVESTIGATOR,
        status=output.status.value,
        leading=None
        if cause is None
        else StageClaim(
            claim=output.summary,
            service_name=names.get(cause.service_id),
            failure_type=cause.failure_type,
            confidence=cause.confidence,
            evidence_signal_ids=cause.evidence_signal_ids,
        ),
        missing_evidence=output.missing_evidence,
    )


def stage_outputs(
    steps: Mapping[str, dict[str, Any]], names: Mapping[uuid.UUID, str]
) -> tuple[StageOutputView, ...]:
    views = []
    for key in ORDER:
        output = steps.get(key)
        if output is None:
            continue
        try:
            parsed = MODELS[key].model_validate(output)
        except ValidationError:
            views.append(
                StageOutputView(
                    step_key=key, role=ROLE_BY_STEP[key], note="Stored output does not validate"
                )
            )
            continue
        views.append(_view(key, parsed, names))
    return tuple(views)


def cited_by(views: tuple[StageOutputView, ...]) -> list[uuid.UUID]:
    """Every signal the views refer to, in first-mention order."""
    ids: dict[uuid.UUID, None] = {}
    for view in views:
        for claim in (*view.findings, *([view.leading] if view.leading else [])):
            ids.update(dict.fromkeys(claim.evidence_signal_ids))
        ids.update(dict.fromkeys(check.signal_id for check in view.citation_checks))
    return list(ids)
