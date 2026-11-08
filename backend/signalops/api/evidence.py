"""Evidence queries: signal pages, trace lookup, and per-source quality counts.

Every route requires an operator token; its workspace and the `replay_scope`
parameter (default `live`) bound each query through `EvidenceScope`. Results
are limited by `SIGNALOPS_QUERY_*` settings: a limit or window above them is
422, never silently clamped. Attributes are redacted again on the way out with
the current process and source field lists, so a field added to either list
after ingestion is hidden too. Message text is returned as stored.
"""

import base64
import binascii
import hashlib
import json
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.ingestion import (
    Redactor,
    SourceConfigError,
    TextRedactor,
    source_redact_fields,
)
from signalops.api.auth import OperatorWorkspace
from signalops.config import Settings
from signalops.contracts.common import LIVE_REPLAY_SCOPE, ReplayScope, TraceId, UtcDatetime
from signalops.contracts.evidence import (
    NormalizedSignal,
    QualityFlag,
    QualityStatus,
    Severity,
    SignalKind,
    SourceKind,
)
from signalops.contracts.queries import (
    QualitySummary,
    SignalPage,
    SourceQuality,
    TimeWindow,
    TraceEvidence,
)
from signalops.dependencies import Resources
from signalops.persistence.database import Database
from signalops.persistence.models import Signal, Source
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.evidence import (
    EvidenceRepository,
    SignalFilter,
    SignalPosition,
)
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["evidence"])

RETRY_AFTER_SECONDS = "5"
CURSOR_VERSION = 1

ReplayScopeQuery = Annotated[ReplayScope, Query(description="Replay dataset; `live` by default")]
SinceQuery = Annotated[UtcDatetime | None, Query(description="Inclusive lower bound (RFC 3339)")]
UntilQuery = Annotated[UtcDatetime | None, Query(description="Exclusive upper bound (RFC 3339)")]
LimitQuery = Annotated[int | None, Query(ge=1, description="At most SIGNALOPS_QUERY_MAX_LIMIT")]


class InvalidQueryError(Exception):
    """The request asks for more than the configured bounds (422)."""

    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT


class InvalidCursorError(InvalidQueryError):
    """The cursor is malformed or belongs to another query (400)."""

    status_code = status.HTTP_400_BAD_REQUEST


def resolve_window(
    since: datetime | None,
    until: datetime | None,
    *,
    now: datetime,
    default: timedelta,
    maximum: timedelta,
) -> TimeWindow:
    """`until` defaults to now and `since` to `until - default`; the span may not exceed maximum."""
    until = until or now
    since = since or until - default
    if since >= until:
        raise InvalidQueryError("since must be earlier than until")
    if until - since > maximum:
        raise InvalidQueryError(f"window exceeds {int(maximum.total_seconds())} seconds")
    return TimeWindow(since=since, until=until)


def check_limit(limit: int | None, settings: Settings, *, ceiling: int | None = None) -> int:
    """The page size; `ceiling` is a route's contract limit when below the configured one."""
    maximum = (
        settings.query_max_limit if ceiling is None else min(settings.query_max_limit, ceiling)
    )
    if limit is None:
        return min(settings.query_default_limit, maximum)
    if limit > maximum:
        raise InvalidQueryError(f"limit exceeds {maximum}")
    return limit


@dataclass(frozen=True)
class SignalQuery:
    scope: EvidenceScope
    criteria: SignalFilter

    def digest(self) -> str:
        """Identifies the query a cursor belongs to, window included."""
        fields = {**asdict(self.scope), **asdict(self.criteria)}
        canonical = json.dumps(fields, default=str, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:32]


@dataclass(frozen=True)
class SignalCursor:
    window: TimeWindow
    position: SignalPosition
    query_digest: str

    def encode(self) -> str:
        # Integer microseconds and hex IDs keep the cursor within the 256-character `Key`.
        document = {
            "v": CURSOR_VERSION,
            "w": [to_micros(self.window.since), to_micros(self.window.until)],
            "p": [to_micros(self.position.occurred_at), self.position.id.hex],
            "q": self.query_digest,
        }
        raw = json.dumps(document, separators=(",", ":")).encode()
        # base64url without padding fits the `Key` pattern; JSON objects encode to "eyJ…".
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @classmethod
    def decode(cls, value: str) -> "SignalCursor":
        try:
            raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            document = json.loads(raw)
            if not isinstance(document, dict) or document.get("v") != CURSOR_VERSION:
                raise InvalidCursorError("invalid cursor")
            since, until = document["w"]
            occurred_at, signal_id = document["p"]
            query_digest = document["q"]
            if not isinstance(query_digest, str) or not isinstance(signal_id, str):
                raise InvalidCursorError("invalid cursor")
            return cls(
                window=TimeWindow(since=from_micros(since), until=from_micros(until)),
                position=SignalPosition(from_micros(occurred_at), uuid.UUID(hex=signal_id)),
                query_digest=query_digest,
            )
        except (
            binascii.Error,
            UnicodeDecodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            OverflowError,
            ValidationError,
        ):
            raise InvalidCursorError("invalid cursor") from None


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_micros(value: datetime) -> int:
    return (value - EPOCH) // timedelta(microseconds=1)


def from_micros(value: object) -> datetime:
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidCursorError("invalid cursor")
    return EPOCH + timedelta(microseconds=value)


class OutputRedaction:
    """Redacts signals with the process list plus each signal's source list.

    Attributes are masked by field name; the free-text message gets the same
    `key=value` and bearer-token masking the agents' tools apply, so an operator
    never sees more of a secret in a log line than a model does.
    """

    def __init__(self, global_fields: Sequence[str], sources: Sequence[Source]) -> None:
        self._default = (Redactor(global_fields), TextRedactor(tuple(global_fields)))
        # Raises SourceConfigError for an invalid config: fail closed, as ingestion does.
        self._by_source: dict[uuid.UUID, tuple[Redactor, TextRedactor]] = {}
        for source in sources:
            fields = (*global_fields, *source_redact_fields(source.config))
            self._by_source[source.id] = (Redactor(fields), TextRedactor(fields))

    def signal(self, row: Signal) -> NormalizedSignal:
        redactor, text = self._by_source.get(row.source_id, self._default)
        attributes, _ = redactor.redact(row.attributes)
        assert isinstance(attributes, dict)  # noqa: S101 - redaction preserves the top-level type.
        return NormalizedSignal(
            id=row.id,
            workspace_id=row.workspace_id,
            source_id=row.source_id,
            service_id=row.service_id,
            source_event_id=row.source_event_id,
            replay_scope=row.replay_scope,
            kind=SignalKind(row.kind),
            occurred_at=row.occurred_at,
            ingested_at=row.ingested_at,
            severity=Severity(row.severity),
            message=text.redact(row.message),
            trace_id=row.trace_id,
            attributes=attributes,
            quality_status=QualityStatus(row.quality_status),
            quality_flags=tuple(QualityFlag(flag) for flag in row.quality_flags),
        )


def invalid_query(error: InvalidQueryError) -> HTTPException:
    return HTTPException(error.status_code, str(error))


def read_database(request: Request) -> Database:
    resources: Resources = request.app.state.resources
    if resources.database is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "evidence queries are not configured",
            headers={"Retry-After": RETRY_AFTER_SECONDS},
        )
    return resources.database


class ReadFailed(HTTPException):
    def __init__(self, route: str, error: Exception) -> None:
        if isinstance(error, SourceConfigError):
            logger.error("evidence query source misconfigured", extra={"error": str(error)})
            super().__init__(
                status.HTTP_500_INTERNAL_SERVER_ERROR, "source configuration is invalid"
            )
            return
        logger.warning(
            "evidence query failed", extra={"route": route, "error": type(error).__name__}
        )
        super().__init__(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "evidence store unavailable",
            headers={"Retry-After": RETRY_AFTER_SECONDS},
        )


async def output_redaction(
    session: AsyncSession, settings: Settings, workspace_id: uuid.UUID
) -> OutputRedaction:
    sources = await CatalogRepository(session).list_sources(workspace_id)
    return OutputRedaction(settings.ingest_redact_fields, sources)


@router.get("/signals", responses={400: {"description": "Invalid or mismatched cursor"}})
async def list_signals(
    request: Request,
    workspace_id: OperatorWorkspace,
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    since: SinceQuery = None,
    until: UntilQuery = None,
    source_id: uuid.UUID | None = None,
    service_id: uuid.UUID | None = None,
    kind: SignalKind | None = None,
    quality_status: QualityStatus | None = None,
    limit: LimitQuery = None,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> SignalPage:
    """Signals by `occurred_at` (newest first) in the window, with keyset pagination.

    A cursor carries its query's window; pass it with the same filters and
    leave `since`/`until` out (or equal). A cursor from another query is 400.
    """
    settings: Settings = request.app.state.settings
    try:
        page_size = check_limit(limit, settings)
        decoded = None if cursor is None else SignalCursor.decode(cursor)
        if decoded is not None:
            since = since or decoded.window.since
            until = until or decoded.window.until
        window = resolve_window(
            since,
            until,
            now=datetime.now(UTC),
            default=timedelta(seconds=settings.query_default_window_seconds),
            maximum=timedelta(seconds=settings.query_max_window_seconds),
        )
        query = SignalQuery(
            scope=EvidenceScope(workspace_id, replay_scope),
            criteria=SignalFilter(
                since=window.since,
                until=window.until,
                source_id=source_id,
                service_id=service_id,
                kind=kind,
                quality_status=quality_status,
            ),
        )
        if decoded is not None and decoded.query_digest != query.digest():
            raise InvalidCursorError("cursor belongs to a different query")
    except InvalidQueryError as error:
        raise invalid_query(error) from None

    try:
        async with read_database(request).transaction() as session:
            redaction = await output_redaction(session, settings, workspace_id)
            rows = await EvidenceRepository(session).list_signals(
                query.scope,
                query.criteria,
                after=None if decoded is None else decoded.position,
                limit=page_size + 1,
            )
    except (SQLAlchemyError, SourceConfigError) as error:
        raise ReadFailed("signals", error) from None

    items = [redaction.signal(row) for row in rows[:page_size]]
    next_cursor = None
    if len(rows) > page_size:
        last = items[-1]
        next_cursor = SignalCursor(
            window=window,
            position=SignalPosition(last.occurred_at, last.id),
            query_digest=query.digest(),
        ).encode()
    return SignalPage(
        replay_scope=replay_scope, window=window, items=tuple(items), next_cursor=next_cursor
    )


@router.get("/traces/{trace_id}", responses={404: {"description": "No signals in this scope"}})
async def get_trace(
    request: Request,
    workspace_id: OperatorWorkspace,
    trace_id: Annotated[TraceId, Path()],
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    limit: LimitQuery = None,
) -> TraceEvidence:
    """Signals carrying the trace ID, oldest first; `truncated` when more exist."""
    settings: Settings = request.app.state.settings
    try:
        page_size = check_limit(limit, settings)
    except InvalidQueryError as error:
        raise invalid_query(error) from None
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            redaction = await output_redaction(session, settings, workspace_id)
            rows = await EvidenceRepository(session).trace_signals(
                scope, trace_id, limit=page_size + 1
            )
    except (SQLAlchemyError, SourceConfigError) as error:
        raise ReadFailed("traces", error) from None
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "trace not found")
    return TraceEvidence(
        trace_id=trace_id,
        replay_scope=replay_scope,
        signals=tuple(redaction.signal(row) for row in rows[:page_size]),
        truncated=len(rows) > page_size,
    )


def _source_quality(
    source: Source,
    by_status: Mapping[tuple[uuid.UUID, str], int],
    by_flag: Mapping[tuple[uuid.UUID, str], int],
) -> SourceQuality:
    statuses = {state: by_status.get((source.id, state), 0) for state in QualityStatus}
    flags = {
        flag: count for flag in QualityFlag if (count := by_flag.get((source.id, flag), 0)) > 0
    }
    return SourceQuality(
        source_id=source.id,
        name=source.name,
        kind=SourceKind(source.kind),
        enabled=source.enabled,
        total=sum(statuses.values()),
        by_status=statuses,
        by_flag=flags,
    )


@router.get("/sources/quality")
async def source_quality(
    request: Request,
    workspace_id: OperatorWorkspace,
    replay_scope: ReplayScopeQuery = LIVE_REPLAY_SCOPE,
    since: SinceQuery = None,
    until: UntilQuery = None,
) -> QualitySummary:
    """Per-source signal counts by quality status and flag, for signals ingested in the window."""
    settings: Settings = request.app.state.settings
    try:
        window = resolve_window(
            since,
            until,
            now=datetime.now(UTC),
            default=timedelta(seconds=settings.query_default_window_seconds),
            maximum=timedelta(seconds=settings.query_max_window_seconds),
        )
    except InvalidQueryError as error:
        raise invalid_query(error) from None
    scope = EvidenceScope(workspace_id, replay_scope)
    try:
        async with read_database(request).transaction() as session:
            sources = await CatalogRepository(session).list_sources(workspace_id)
            counts = await EvidenceRepository(session).quality_counts(
                scope, [source.id for source in sources], since=window.since, until=window.until
            )
    except SQLAlchemyError as error:
        raise ReadFailed("sources/quality", error) from None
    return QualitySummary(
        replay_scope=replay_scope,
        window=window,
        sources=tuple(_source_quality(s, counts.by_status, counts.by_flag) for s in sources),
    )
