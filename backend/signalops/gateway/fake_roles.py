"""Deterministic stand-ins for every role, chosen by the request's prompt version.

Each plays its role by a fixed rule over what the workflow gave it: the specialists
read real evidence through their tools, the synthesizer merges their reports, and the
verifier judges citations by service. Like `fake_investigator`, they prove the
workflow, the handoffs, and the checks run end to end, and say nothing about how well
a model would investigate.
"""

import math
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Final

from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.roles import (
    MAX_FINDING_EVIDENCE,
    CitationCheck,
    Contradiction,
    Finding,
    Hypothesis,
    SpecialistBrief,
    SpecialistReport,
    SynthesisBrief,
    SynthesisReport,
    Verdict,
    VerificationBrief,
    VerificationReport,
)
from signalops.contracts.workflow import ToolName
from signalops.gateway.fake import FakeAnswer, FakeStep
from signalops.gateway.fake_investigator import (
    MAXIMUM_CITATIONS,
    calls,
    entries,
    investigator_step,
    results_of,
    submit,
    telemetry_turn,
    turn_of,
    window_of,
)
from signalops.gateway.types import ModelRequest, TextBlock

DEPLOYMENT_LOOK_BACK: Final = timedelta(minutes=30)
DEPLOYMENT_CONFIDENCE: Final = 0.3
TELEMETRY_CONFIDENCE: Final = 0.7
DEPENDENCY_CONFIDENCE: Final = 0.5
# Below this, the leading hypothesis is not worth asserting.
DIAGNOSIS_CONFIDENCE: Final = 0.5
MAX_DEPENDENCY_READS: Final = 3
LIMITATION: Final = "Produced by a deterministic fake role."


def _first_text(request: ModelRequest) -> str:
    first = request.messages[0].content[0]
    assert isinstance(first, TextBlock)  # noqa: S101 - the workflow writes this message
    return first.text


def _specialist_brief(request: ModelRequest) -> SpecialistBrief:
    return SpecialistBrief.model_validate_json(_first_text(request))


def _ids(items: list[dict[str, Any]], limit: int = MAX_FINDING_EVIDENCE) -> tuple[uuid.UUID, ...]:
    return tuple(dict.fromkeys(uuid.UUID(item["signal_id"]) for item in items))[:limit]


def telemetry_step(request: ModelRequest) -> FakeAnswer:
    brief = _specialist_brief(request)
    incident = brief.incident
    outcome = telemetry_turn(
        request, incident.window_start, incident.evidence_cutoff_at, brief.services
    )
    if isinstance(outcome, FakeAnswer):
        return outcome
    turn = turn_of(request)
    if outcome.service_id is None:
        return submit(turn, SpecialistReport(missing_evidence=(outcome.reason,)))
    finding = Finding(
        claim="Failing calls end at this service, and its traces and its callers' logs show it.",
        service_id=outcome.service_id,
        failure_type="downstream_timeout" if outcome.timed_out else "elevated_errors",
        confidence=TELEMETRY_CONFIDENCE,
        uncertainty="A fixed rule on error counts and durations; correlation in time only.",
        evidence_signal_ids=_ids(list(outcome.cited), MAXIMUM_CITATIONS),
    )
    return submit(turn, SpecialistReport(findings=(finding,), limitations=(LIMITATION,)))


def deployment_step(request: ModelRequest) -> FakeAnswer:
    brief = _specialist_brief(request)
    incident = brief.incident
    turn = turn_of(request)
    start = incident.window_start or incident.evidence_cutoff_at
    if turn == 0:
        window = window_of(start - DEPLOYMENT_LOOK_BACK, incident.evidence_cutoff_at)
        return calls(turn, (ToolName.GET_DEPLOYMENTS, window))
    recent = [
        item
        for item in entries(results_of(request), ToolName.GET_DEPLOYMENTS)
        if datetime.fromisoformat(item["deployed_at"]) >= start - DEPLOYMENT_LOOK_BACK
    ]
    if not recent:
        return submit(
            turn,
            SpecialistReport(
                missing_evidence=("No deployment in the half hour before the window",)
            ),
        )
    findings = tuple(
        Finding(
            claim=f"Revision {item['revision']} of this service was deployed shortly before the "
            "incident window.",
            service_id=uuid.UUID(item["service_id"]),
            failure_type="deployment_regression",
            confidence=DEPLOYMENT_CONFIDENCE,
            uncertainty="Timing alone: no error was traced to this revision.",
            evidence_signal_ids=(uuid.UUID(item["signal_id"]),),
        )
        for item in recent[:5]
    )
    return submit(turn, SpecialistReport(findings=findings, limitations=(LIMITATION,)))


def dependency_step(request: ModelRequest) -> FakeAnswer:
    brief = _specialist_brief(request)
    turn = turn_of(request)
    services = brief.incident_service_ids[:MAX_DEPENDENCY_READS]
    if turn == 0:
        reads: list[tuple[ToolName, dict[str, Any]]] = [
            (ToolName.GET_SERVICE_DEPENDENCIES, {"service_id": str(service)})
            for service in services
        ]
        reads.append((ToolName.RETRIEVE_RUNBOOKS, {}))
        return calls(turn, *reads)
    results = results_of(request)
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    for result in results:
        if result.get("tool") == ToolName.GET_SERVICE_DEPENDENCIES:
            for item in result["items"]:
                edges.setdefault((item["from_service_id"], item["to_service_id"]), item)
    runbooks = entries_by_key(results)
    if not edges:
        return submit(
            turn, SpecialistReport(missing_evidence=("No observed dependencies as of the cutoff",))
        )
    provenance = [
        {"signal_id": item["provenance_signal_id"]}
        for item in edges.values()
        if item.get("provenance_signal_id")
    ]
    finding = Finding(
        claim=f"The incident's services have {len(edges)} observed dependencies as of the "
        "cutoff, so a failure could spread along them.",
        confidence=DEPENDENCY_CONFIDENCE,
        uncertainty="Structure only: it says how a failure could spread, not where it began.",
        evidence_signal_ids=_ids(provenance),
        references=tuple(runbooks)[:10],
    )
    return submit(turn, SpecialistReport(findings=(finding,), limitations=(LIMITATION,)))


def entries_by_key(results: list[dict[str, Any]]) -> list[str]:
    return list(
        dict.fromkeys(
            f"{item['runbook_key']}@{item['version']}"
            for result in results
            if result.get("tool") == ToolName.RETRIEVE_RUNBOOKS
            for item in result["items"]
        )
    )


def synthesizer_step(request: ModelRequest) -> FakeAnswer:
    brief = SynthesisBrief.model_validate_json(_first_text(request))
    turn = turn_of(request)
    grouped: dict[tuple[uuid.UUID, str], dict[str, Any]] = {}
    for role_report in brief.reports:
        for finding in role_report.report.findings:
            if (
                finding.service_id is None
                or finding.failure_type is None
                or not finding.evidence_signal_ids
            ):
                continue
            group = grouped.setdefault(
                (finding.service_id, finding.failure_type),
                {"roles": [], "ids": [], "confidence": 0.0, "claims": []},
            )
            if role_report.role not in group["roles"]:
                group["roles"].append(role_report.role)
            group["ids"].extend(i for i in finding.evidence_signal_ids if i not in group["ids"])
            group["confidence"] = max(group["confidence"], finding.confidence)
            group["claims"].append(finding.claim)
    ranked = sorted(
        grouped.items(),
        key=lambda pair: (len(pair[1]["roles"]), pair[1]["confidence"], len(pair[1]["ids"])),
        reverse=True,
    )
    hypotheses = tuple(
        Hypothesis(
            service_id=service_id,
            failure_type=failure_type,
            confidence=group["confidence"],
            evidence_signal_ids=tuple(group["ids"])[:50],
            supporting_roles=tuple(group["roles"]),
            assessment=" ".join(group["claims"])[:2000],
        )
        for (service_id, failure_type), group in ranked[:5]
    )
    contradictions = tuple(
        Contradiction(
            description=f"{_roles(a)} point to one service and {_roles(b)} to another.",
            roles=tuple(dict.fromkeys((*a.supporting_roles, *b.supporting_roles)))[:3],
            evidence_signal_ids=tuple(
                dict.fromkeys((*a.evidence_signal_ids[:3], *b.evidence_signal_ids[:3]))
            ),
        )
        for index, a in enumerate(hypotheses)
        for b in hypotheses[index + 1 :]
        if a.service_id != b.service_id and set(a.supporting_roles).isdisjoint(b.supporting_roles)
    )[:10]
    missing = tuple(
        dict.fromkeys(item for r in brief.reports for item in r.report.missing_evidence)
    )[:10]
    if not hypotheses or hypotheses[0].confidence < DIAGNOSIS_CONFIDENCE:
        report = SynthesisReport(
            status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
            summary="No finding is strong enough to name a cause.",
            hypotheses=hypotheses,
            contradictions=contradictions,
            missing_evidence=missing or ("No finding is strong enough to name a cause",),
            limitations=(LIMITATION,),
        )
    else:
        report = SynthesisReport(
            status=DiagnosisStatus.DIAGNOSED,
            summary="Failing calls end at this service, and its evidence shows the fault.",
            hypotheses=hypotheses,
            contradictions=contradictions,
            limitations=(LIMITATION,),
        )
    return submit(turn, report, "submit_synthesis")


def _roles(hypothesis: Hypothesis) -> str:
    return " and ".join(role.value.replace("_", " ") for role in hypothesis.supporting_roles)


def verifier_step(request: ModelRequest) -> FakeAnswer:
    brief = VerificationBrief.model_validate_json(_first_text(request))
    turn = turn_of(request)
    leading = brief.synthesis.hypotheses[0]
    names = {service.service_id: service.name for service in brief.services}
    name = names.get(leading.service_id, "")
    by_id = {cited.signal_id: cited for cited in brief.cited}
    checks = []
    for signal_id in leading.evidence_signal_ids:
        cited = by_id.get(signal_id)
        supports = cited is not None and (
            cited.service_id == leading.service_id or (bool(name) and name in cited.text)
        )
        checks.append(
            CitationCheck(
                signal_id=signal_id,
                supports=supports,
                note="From the named service, or names it." if supports else "Unrelated or unread.",
            )
        )
    supporting = sum(check.supports for check in checks)
    if supporting and supporting >= math.ceil(len(checks) / 2):
        report = VerificationReport(
            verdict=Verdict.SUPPORTED, citation_checks=tuple(checks), limitations=(LIMITATION,)
        )
    else:
        report = VerificationReport(
            verdict=Verdict.UNSUPPORTED,
            citation_checks=tuple(checks),
            missing_evidence=("Most cited signals do not come from or name the service",),
        )
    return submit(turn, report, "submit_verification")


ROLE_STEPS: Final[Mapping[str, Callable[[ModelRequest], FakeAnswer]]] = {
    "investigator-v1": investigator_step,
    "telemetry-v1": telemetry_step,
    "deployment-v1": deployment_step,
    "dependency-v1": dependency_step,
    "synthesizer-v1": synthesizer_step,
    "verifier-v1": verifier_step,
}


def role_step(request: ModelRequest) -> FakeStep:
    """The fake answer for whichever role sent this request."""
    return ROLE_STEPS[request.prompt_version](request)
