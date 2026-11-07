"""The read-only evidence tools an agent can call (ADR-027).

**Authority.** Scope and cutoff come from the investigation row through `ToolScope`.
The model supplies only the typed arguments below, which forbid unknown fields, so
no input can name a workspace, a replay run, or a later cutoff. Log and runbook text
are data: nothing here reads them for instructions, and no tool writes, runs SQL
from input, or reaches a shell.

**Cutoff.** A signal is visible when both `occurred_at` and `ingested_at` are at or
before the cutoff, so late-arriving evidence stays future evidence. A window reaching
past the cutoff is rejected rather than clamped, so the agent learns what it cannot
see.

**Bounds.** Every call has a time window, a row limit, a byte cap, a timeout, and
redaction. `truncated` tells the agent that more matched.

**Failures.** A rejected or failed call returns a `ToolFailure` for the agent to read.
It records a failed `tool_calls` row and never ends the job.
"""

import asyncio
import hashlib
import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal

from pydantic import Field, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.ingestion import REDACTED
from signalops.config import Settings
from signalops.contracts.common import ContractModel, TraceId, UtcDatetime, json_size
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind
from signalops.contracts.tools import (
    MAX_TRAVERSAL_HOPS,
    DependenciesResult,
    DeploymentEntry,
    DeploymentsResult,
    LogEntry,
    LogQueryResult,
    RunbookEntry,
    RunbooksResult,
    ToolErrorCategory,
    ToolFailure,
    TraceSummaryEntry,
    TraceSummaryResult,
)
from signalops.contracts.tools import ToolResult as ToolResultModel
from signalops.contracts.workflow import ToolCallStatus, ToolName
from signalops.gateway.types import ToolDefinition
from signalops.persistence.database import Database
from signalops.persistence.models import Signal
from signalops.persistence.repositories.evidence import EvidenceRepository, SignalFilter
from signalops.persistence.repositories.graph import GraphDirection, GraphRepository
from signalops.persistence.repositories.jobs import JobLease, JobRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.repositories.tool_calls import ToolCallRepository

logger = logging.getLogger(__name__)

MAX_TOOL_LIMIT: Final = 50
MAX_TOOL_WINDOW: Final = timedelta(hours=6)
DEFAULT_TOOL_WINDOW: Final = timedelta(hours=1)
MAX_TOOL_RESULT_BYTES: Final = 16 * 1024
MAX_EXCERPT_LENGTH: Final = 2000
_SEVERITY_ORDER: Final = tuple(Severity)


class WindowArguments(ContractModel):
    since: UtcDatetime | None = Field(
        default=None, description="Start of the window. Defaults to an hour before `until`."
    )
    until: UtcDatetime | None = Field(
        default=None,
        description="End of the window. Defaults to, and may not pass, the evidence cutoff.",
    )
    limit: int = Field(default=MAX_TOOL_LIMIT, ge=1, le=MAX_TOOL_LIMIT)


class QueryLogsArguments(WindowArguments):
    service_id: uuid.UUID | None = None
    minimum_severity: Severity | None = None


class TraceSummaryArguments(WindowArguments):
    service_id: uuid.UUID | None = None
    trace_id: TraceId | None = None
    status: Literal["ok", "error"] | None = None


class DeploymentsArguments(WindowArguments):
    service_id: uuid.UUID | None = None


class DependenciesArguments(ContractModel):
    service_id: uuid.UUID
    direction: GraphDirection = GraphDirection.DOWNSTREAM
    max_hops: int = Field(default=MAX_TRAVERSAL_HOPS, ge=1, le=MAX_TRAVERSAL_HOPS)
    limit: int = Field(default=MAX_TOOL_LIMIT, ge=1, le=MAX_TOOL_LIMIT)


class RunbooksArguments(ContractModel):
    service_id: uuid.UUID | None = None
    limit: int = Field(default=5, ge=1, le=MAX_TOOL_LIMIT)


@dataclass(frozen=True)
class ToolScope:
    """What a call may see, set from the investigation and never from the model."""

    evidence: EvidenceScope
    cutoff: datetime


@dataclass(frozen=True)
class ToolLimits:
    timeout_seconds: float = 5.0
    max_result_bytes: int = MAX_TOOL_RESULT_BYTES

    @classmethod
    def from_settings(cls, settings: Settings) -> "ToolLimits":
        return cls(timeout_seconds=settings.tool_timeout_seconds)


@dataclass(frozen=True)
class ToolOutcome:
    """A call's result as the agent reads it."""

    tool: ToolName
    # Canonical JSON of the result or failure.
    content: str
    is_error: bool


class ToolRejectedError(Exception):
    """The call is refused for a reason the agent can act on."""

    def __init__(
        self, category: ToolErrorCategory, detail: str, argument: str | None = None
    ) -> None:
        super().__init__(detail)
        self.category = category
        self.detail = detail
        self.argument = argument


class TextRedactor:
    """Masks `key=value` pairs of sensitive keys in free text, which ingestion keeps as is."""

    def __init__(self, fields: tuple[str, ...]) -> None:
        names = "|".join(sorted((re.escape(field) for field in fields), key=len, reverse=True))
        self._pairs = re.compile(rf"(?i)\b({names})\b(\s*[:=]\s*)(?:bearer\s+)?[^\s,;\"']+")
        self._bearer = re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+")

    def redact(self, text: str) -> str:
        masked = self._pairs.sub(rf"\1\2{REDACTED}", text)
        return self._bearer.sub(f"Bearer {REDACTED}", masked)


def _window(arguments: WindowArguments, cutoff: datetime) -> tuple[datetime, datetime]:
    """The half-open read bounds; `until` includes the cutoff itself."""
    for name, value in (("since", arguments.since), ("until", arguments.until)):
        if value is not None and value > cutoff:
            raise ToolRejectedError(
                ToolErrorCategory.OUT_OF_SCOPE, "the time is after the evidence cutoff", name
            )
    until = arguments.until or cutoff
    since = arguments.since or until - DEFAULT_TOOL_WINDOW
    if since >= until:
        raise ToolRejectedError(
            ToolErrorCategory.INVALID_ARGUMENTS, "since must be before until", "since"
        )
    if until - since > MAX_TOOL_WINDOW:
        raise ToolRejectedError(
            ToolErrorCategory.LIMIT_EXCEEDED, "the window exceeds six hours", "since"
        )
    return since, until + timedelta(microseconds=1)


def _severities_from(minimum: Severity | None) -> tuple[Severity, ...] | None:
    if minimum is None:
        return None
    return _SEVERITY_ORDER[_SEVERITY_ORDER.index(minimum) :]


class _Reads:
    """The five reads, in one short transaction each."""

    def __init__(self, session: AsyncSession, scope: ToolScope, redactor: TextRedactor) -> None:
        self._session = session
        self._scope = scope
        self._redactor = redactor

    async def _check_service(self, service_id: uuid.UUID | None) -> None:
        if service_id is None:
            return
        evidence = EvidenceRepository(self._session)
        if not await evidence.has_service(self._scope.evidence.workspace_id, service_id):
            raise ToolRejectedError(
                ToolErrorCategory.OUT_OF_SCOPE, "no such service in scope", "service_id"
            )

    def _text(self, text: str) -> str:
        return self._redactor.redact(text)[:MAX_EXCERPT_LENGTH]

    async def _signals(self, criteria: SignalFilter, limit: int) -> tuple[list[Signal], bool]:
        rows = await EvidenceRepository(self._session).list_signals(
            self._scope.evidence, criteria, after=None, limit=limit + 1
        )
        return rows[:limit], len(rows) > limit

    async def query_logs(self, arguments: QueryLogsArguments) -> LogQueryResult:
        await self._check_service(arguments.service_id)
        since, until = _window(arguments, self._scope.cutoff)
        rows, truncated = await self._signals(
            SignalFilter(
                since=since,
                until=until,
                service_id=arguments.service_id,
                kind=SignalKind.LOG,
                severities=_severities_from(arguments.minimum_severity),
                known_by=self._scope.cutoff,
            ),
            arguments.limit,
        )
        return LogQueryResult(
            evidence_cutoff_at=self._scope.cutoff,
            truncated=truncated,
            items=tuple(self._log(row) for row in rows),
        )

    def _log(self, row: Signal) -> LogEntry:
        return LogEntry(
            signal_id=row.id,
            service_id=row.service_id,
            occurred_at=row.occurred_at,
            severity=Severity(row.severity),
            message=self._text(row.message),
            trace_id=row.trace_id,
            quality_status=QualityStatus(row.quality_status),
        )

    async def get_trace_summary(self, arguments: TraceSummaryArguments) -> TraceSummaryResult:
        await self._check_service(arguments.service_id)
        since, until = _window(arguments, self._scope.cutoff)
        severities = None
        if arguments.status is not None:
            severities = (Severity.ERROR,) if arguments.status == "error" else (Severity.INFO,)
        rows, truncated = await self._signals(
            SignalFilter(
                since=since,
                until=until,
                service_id=arguments.service_id,
                kind=SignalKind.TRACE,
                severities=severities,
                trace_id=arguments.trace_id,
                known_by=self._scope.cutoff,
            ),
            arguments.limit,
        )
        entries = [entry for entry in map(self._trace, rows) if entry is not None]
        return TraceSummaryResult(
            evidence_cutoff_at=self._scope.cutoff, truncated=truncated, items=tuple(entries)
        )

    def _trace(self, row: Signal) -> TraceSummaryEntry | None:
        record = row.attributes.get("record", {})
        if row.trace_id is None or "duration_ms" not in record:
            # A quarantined record: its adapter could not read it.
            return None
        return TraceSummaryEntry(
            signal_id=row.id,
            trace_id=row.trace_id,
            service_id=row.service_id,
            occurred_at=row.occurred_at,
            operation=self._text(record["operation"])[:500],
            duration_ms=record["duration_ms"],
            status=record["status"],
            span_count=record["span_count"],
            error_span_count=record["error_span_count"],
            downstream_services=tuple(record.get("downstream_services", ())),
        )

    async def get_deployments(self, arguments: DeploymentsArguments) -> DeploymentsResult:
        await self._check_service(arguments.service_id)
        since, until = _window(arguments, self._scope.cutoff)
        rows = await EvidenceRepository(self._session).list_deployments(
            self._scope.evidence,
            service_id=arguments.service_id,
            since=since,
            until=until,
            known_by=self._scope.cutoff,
            limit=arguments.limit + 1,
        )
        return DeploymentsResult(
            evidence_cutoff_at=self._scope.cutoff,
            truncated=len(rows) > arguments.limit,
            items=tuple(
                DeploymentEntry(
                    deployment_id=row.id,
                    signal_id=row.signal_id,
                    service_id=row.service_id,
                    revision=row.revision,
                    previous_revision=row.attributes.get("previous_revision"),
                    deployed_at=row.deployed_at,
                )
                for row in rows[: arguments.limit]
            ),
        )

    async def get_service_dependencies(
        self, arguments: DependenciesArguments
    ) -> DependenciesResult:
        await self._check_service(arguments.service_id)
        walk = await GraphRepository(self._session).dependencies(
            self._scope.evidence,
            arguments.service_id,
            as_of=self._scope.cutoff,
            direction=arguments.direction,
            max_hops=arguments.max_hops,
            limit=arguments.limit,
        )
        return DependenciesResult(
            evidence_cutoff_at=self._scope.cutoff, truncated=walk.truncated, items=walk.items
        )

    async def retrieve_runbooks(self, arguments: RunbooksArguments) -> RunbooksResult:
        await self._check_service(arguments.service_id)
        rows = await GraphRepository(self._session).runbooks(
            self._scope.evidence,
            service_id=arguments.service_id,
            as_of=self._scope.cutoff,
            limit=arguments.limit + 1,
        )
        return RunbooksResult(
            evidence_cutoff_at=self._scope.cutoff,
            truncated=len(rows) > arguments.limit,
            items=tuple(
                RunbookEntry(
                    runbook_key=row.key,
                    version=row.version,
                    title=row.title,
                    excerpt=self._text(row.content),
                    provenance=row.provenance,
                )
                for row in rows[: arguments.limit]
            ),
        )


@dataclass(frozen=True)
class ToolSpec:
    name: ToolName
    description: str
    arguments: type[ContractModel]
    read: Callable[[_Reads, Any], Awaitable[ToolResultModel]]


TOOLS: Final[Mapping[ToolName, ToolSpec]] = {
    spec.name: spec
    for spec in (
        ToolSpec(
            ToolName.QUERY_LOGS,
            "Read log signals in a time window, newest first. Filter by service and severity.",
            QueryLogsArguments,
            _Reads.query_logs,
        ),
        ToolSpec(
            ToolName.GET_TRACE_SUMMARY,
            "Read trace summaries in a time window, newest first. Filter by service, trace, "
            "or status.",
            TraceSummaryArguments,
            _Reads.get_trace_summary,
        ),
        ToolSpec(
            ToolName.GET_DEPLOYMENTS,
            "Read deployments in a time window, newest first.",
            DeploymentsArguments,
            _Reads.get_deployments,
        ),
        ToolSpec(
            ToolName.GET_SERVICE_DEPENDENCIES,
            "Walk a service's observed dependencies as of the evidence cutoff, up to three hops.",
            DependenciesArguments,
            _Reads.get_service_dependencies,
        ),
        ToolSpec(
            ToolName.RETRIEVE_RUNBOOKS,
            "Read runbooks in force at the evidence cutoff, for a service or for all services.",
            RunbooksArguments,
            _Reads.retrieve_runbooks,
        ),
    )
}


def tool_definitions() -> tuple[ToolDefinition, ...]:
    return tuple(
        ToolDefinition(
            name=spec.name.value,
            description=spec.description,
            input_schema=spec.arguments.model_json_schema(),
        )
        for spec in TOOLS.values()
    )


def _bounded(result: ToolResultModel, max_bytes: int) -> ToolResultModel:
    """The result without the rows past the byte cap, marked truncated."""
    items = result.items
    kept = len(items)
    while kept and json_size(result.model_dump(mode="json")) > max_bytes:
        kept -= 1
        result = result.model_copy(update={"items": items[:kept], "truncated": True})
    return result


def _canonical(document: ContractModel) -> str:
    return json.dumps(document.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


class ToolExecutor:
    def __init__(
        self, database: Database, redactor: TextRedactor, limits: ToolLimits | None = None
    ) -> None:
        self._database = database
        self._redactor = redactor
        self._limits = limits or ToolLimits()

    async def run(
        self,
        lease: JobLease,
        step_key: str,
        scope: ToolScope,
        tool: ToolName,
        raw_arguments: Mapping[str, Any],
    ) -> ToolOutcome:
        spec = TOOLS[tool]
        call_key = self._call_key(tool, raw_arguments)
        async with self._database.transaction() as session:
            await JobRepository(session).hold(lease)
            call_id = await ToolCallRepository(session).start(
                lease.investigation_id, step_key, call_key, tool, dict(raw_arguments)
            )
        started = asyncio.get_running_loop().time()
        document: ContractModel
        try:
            document = await self._read(spec, scope, raw_arguments)
        except ToolRejectedError as rejected:
            document = ToolFailure(
                tool=tool,
                error=rejected.category,
                detail=rejected.detail,
                argument=rejected.argument,
            )
        failed = isinstance(document, ToolFailure)
        content = _canonical(document)
        elapsed = round((asyncio.get_running_loop().time() - started) * 1000)
        async with self._database.transaction() as session:
            await JobRepository(session).hold(lease)
            await ToolCallRepository(session).finish(
                call_id,
                ToolCallStatus.FAILED if failed else ToolCallStatus.SUCCEEDED,
                None if failed else f"sha256:{hashlib.sha256(content.encode()).hexdigest()}",
                elapsed,
            )
        logger.info(
            "tool call finished",
            extra={"tool": str(tool), "failed": failed, "duration_ms": elapsed},
        )
        return ToolOutcome(tool, content, failed)

    @staticmethod
    def _call_key(tool: ToolName, raw_arguments: Mapping[str, Any]) -> str:
        encoded = json.dumps(raw_arguments, sort_keys=True, separators=(",", ":"), default=str)
        return f"{tool}:{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"

    async def _read(
        self, spec: ToolSpec, scope: ToolScope, raw_arguments: Mapping[str, Any]
    ) -> ToolResultModel:
        try:
            arguments = spec.arguments.model_validate(raw_arguments)
        except ValidationError as error:
            first = error.errors(include_url=False, include_input=False)[0]
            raise ToolRejectedError(
                ToolErrorCategory.INVALID_ARGUMENTS,
                f"{first['msg']}"[:500],
                str(first["loc"][0]) if first["loc"] else None,
            ) from None
        try:
            async with (
                asyncio.timeout(self._limits.timeout_seconds),
                self._database.transaction() as session,
            ):
                result = await spec.read(_Reads(session, scope, self._redactor), arguments)
        except TimeoutError:
            raise ToolRejectedError(ToolErrorCategory.TIMEOUT, "the read took too long") from None
        except SQLAlchemyError as error:
            logger.warning("tool read failed", extra={"error": type(error).__name__})
            raise ToolRejectedError(
                ToolErrorCategory.UNAVAILABLE, "evidence is unavailable"
            ) from None
        return _bounded(result, self._limits.max_result_bytes)
