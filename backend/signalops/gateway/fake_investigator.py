"""A deterministic stand-in for a model, for development and CI.

It plays the investigator's tool loop against real evidence, by a fixed rule: read
error traces and logs, follow the failing calls downstream, check the last service's
durations, and cite what it read. With too few errors it abstains. It proves the
workflow, the tools, and the verification run end to end. It says nothing about how
well a model diagnoses, and no result from it is a measure of quality.
"""

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from signalops.contracts.common import ContractModel
from signalops.contracts.diagnosis import DiagnosisStatus
from signalops.contracts.evidence import Severity
from signalops.contracts.steps import (
    SUBMIT_TOOL,
    IncidentBrief,
    ProposedCause,
    ProposedDiagnosis,
    ServiceBrief,
)
from signalops.contracts.workflow import ToolName
from signalops.gateway.fake import FakeAnswer
from signalops.gateway.types import (
    ModelRequest,
    ModelRole,
    StopReason,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

ROW_LIMIT: Final = 25
# Below this, errors in one service are ordinary noise.
MINIMUM_ERRORS: Final = 5
SLOW_TRACE_MS: Final = 1_000
MAXIMUM_CITATIONS: Final = 10
LOOK_BACK: Final = timedelta(minutes=5)
LONGEST_WINDOW: Final = timedelta(hours=6)


def _brief(request: ModelRequest) -> IncidentBrief:
    first = request.messages[0].content[0]
    assert isinstance(first, TextBlock)  # noqa: S101 - the investigator writes this message
    return IncidentBrief.model_validate_json(first.text)


def results_of(request: ModelRequest) -> list[dict[str, Any]]:
    return [
        json.loads(block.content)
        for message in request.messages
        if message.role is ModelRole.USER
        for block in message.content
        if isinstance(block, ToolResultBlock) and not block.is_error
    ]


def entries(results: list[dict[str, Any]], tool: ToolName) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for result in results:
        if result.get("tool") == tool:
            seen |= {item["signal_id"]: item for item in result["items"]}
    return list(seen.values())


def window_of(window_start: datetime | None, cutoff: datetime) -> dict[str, str]:
    start: datetime = window_start or cutoff - LOOK_BACK
    since = max(start - LOOK_BACK, cutoff - LONGEST_WINDOW)
    return {"since": since.isoformat(), "until": cutoff.isoformat()}


def calls(turn: int, *calls: tuple[ToolName, dict[str, Any]]) -> FakeAnswer:
    return FakeAnswer(
        content=tuple(
            ToolUseBlock(id=f"toolu_{turn}_{index}", name=tool.value, input=arguments)
            for index, (tool, arguments) in enumerate(calls)
        ),
        stop_reason=StopReason.TOOL_USE,
    )


def submit(turn: int, output: ContractModel, name: str = SUBMIT_TOOL) -> FakeAnswer:
    block = ToolUseBlock(
        id=f"toolu_{turn}_submit",
        name=name,
        input=output.model_dump(mode="json", exclude_none=True),
    )
    return FakeAnswer(content=(block,), stop_reason=StopReason.TOOL_USE)


def _last_failing_service(
    errors: list[dict[str, Any]], names: dict[str, uuid.UUID]
) -> tuple[uuid.UUID, uuid.UUID | None] | None:
    """The failing service furthest downstream, and the one service it alone calls, if any."""
    counts = Counter(item["service_id"] for item in errors if item["service_id"])
    failing = {service for service, count in counts.items() if count >= MINIMUM_ERRORS}
    if not failing:
        return None
    current = counts.most_common(1)[0][0]
    visited = {current}
    while True:
        downstream = {
            name
            for item in errors
            if item["service_id"] == current
            for name in item["downstream_services"]
        }
        following = sorted(
            str(names[name])
            for name in downstream
            if name in names and str(names[name]) in failing - visited
        )
        if not following:
            callee = next(iter(downstream)) if len(downstream) == 1 else None
            return uuid.UUID(current), names.get(callee) if callee else None
        current = following[0]
        visited.add(current)


def _abstention(reason: str) -> ProposedDiagnosis:
    return ProposedDiagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary="The evidence does not support a root cause.",
        missing_evidence=(reason,),
    )


def _diagnosis(
    service_id: uuid.UUID, *, cited: list[dict[str, Any]], logs: list[dict[str, Any]]
) -> ProposedDiagnosis:
    timed_out = any("timed out" in item["message"] or "timeout" in item["message"] for item in logs)
    return ProposedDiagnosis(
        status=DiagnosisStatus.DIAGNOSED,
        summary="Failing calls end at this service, and its evidence shows the fault.",
        root_cause=ProposedCause(
            service_id=service_id,
            failure_type="downstream_timeout" if timed_out else "elevated_errors",
            confidence=0.7,
            evidence_signal_ids=tuple(
                uuid.UUID(item["signal_id"]) for item in cited[:MAXIMUM_CITATIONS]
            ),
        ),
        limitations=("Produced by the deterministic fake investigator.",),
    )


@dataclass(frozen=True)
class TelemetryVerdict:
    """What the fixed rule concludes: a service with what it read, or why there is none."""

    service_id: uuid.UUID | None
    reason: str = ""
    cited: tuple[dict[str, Any], ...] = ()
    logs: tuple[dict[str, Any], ...] = ()

    @property
    def timed_out(self) -> bool:
        return any(
            "timed out" in item["message"] or "timeout" in item["message"] for item in self.logs
        )


def turn_of(request: ModelRequest) -> int:
    return sum(message.role is ModelRole.ASSISTANT for message in request.messages)


def telemetry_turn(
    request: ModelRequest,
    window_start: datetime | None,
    cutoff: datetime,
    services: tuple[ServiceBrief, ...],
) -> FakeAnswer | TelemetryVerdict:
    """The next tool calls by the fixed rule, or its verdict once it has read enough."""
    turn = turn_of(request)
    window = window_of(window_start, cutoff)
    if turn == 0:
        return calls(
            turn,
            (ToolName.GET_TRACE_SUMMARY, {**window, "status": "error", "limit": ROW_LIMIT}),
            (
                ToolName.QUERY_LOGS,
                {**window, "minimum_severity": Severity.ERROR.value, "limit": ROW_LIMIT},
            ),
        )
    results = results_of(request)
    errors = entries(results, ToolName.GET_TRACE_SUMMARY)
    errors = [item for item in errors if item["status"] == "error"]
    logs = entries(results, ToolName.QUERY_LOGS)
    names = {service.name: service.service_id for service in services}
    found = _last_failing_service(errors, names)
    if found is None:
        return TelemetryVerdict(None, "No service shows sustained errors in this window")
    failing, callee = found
    if turn == 1 and callee is not None:
        moments = sorted(item["occurred_at"] for item in errors)
        return calls(
            turn,
            (
                ToolName.GET_TRACE_SUMMARY,
                {
                    "since": moments[0],
                    "until": moments[-1],
                    "service_id": str(callee),
                    "limit": ROW_LIMIT,
                },
            ),
        )
    caller_traces = [item for item in errors if item["service_id"] == str(failing)]
    caller_logs = tuple(item for item in logs if item["service_id"] == str(failing))
    slow = [
        item
        for item in entries(results, ToolName.GET_TRACE_SUMMARY)
        if callee is not None
        and item["service_id"] == str(callee)
        and item["duration_ms"] >= SLOW_TRACE_MS
    ]
    if callee is not None and slow:
        return TelemetryVerdict(callee, cited=(*slow, *caller_logs), logs=caller_logs)
    return TelemetryVerdict(failing, cited=(*caller_traces, *caller_logs), logs=caller_logs)


def investigator_step(request: ModelRequest) -> FakeAnswer:
    brief = _brief(request)
    outcome = telemetry_turn(request, brief.window_start, brief.evidence_cutoff_at, brief.services)
    if isinstance(outcome, FakeAnswer):
        return outcome
    turn = turn_of(request)
    if outcome.service_id is None:
        return submit(turn, _abstention(outcome.reason))
    return submit(
        turn, _diagnosis(outcome.service_id, cited=list(outcome.cited), logs=list(outcome.logs))
    )
