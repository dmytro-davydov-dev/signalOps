"""Retention cleanup that cannot remove cited evidence or needed deduplication history.

`signalops retention` reports what each category would delete (the default) and,
with `--apply`, deletes it in short batches. Categories and their age bounds:

- signals: stored (`stored_at`, wall clock) longer ago than
  `SIGNALOPS_RETENTION_SIGNALS_DAYS`, except any
  signal an incident, a deployment, a graph edge, or a stored diagnosis (root
  cause or contradiction) cites. Those stay in full, so every citation keeps
  resolving; their detection samples go with the rest (cascade). `ingested_at`
  is simulated for replays, so it cannot tell how long a row has been stored.
- aggregates: closed detection windows whose `window_end` and `closed_at` (wall
  clock) are both older than `SIGNALOPS_RETENTION_AGGREGATES_DAYS`. The bound must
  exceed how old an accepted record may be, or a late record could reopen a
  deleted window; `closed_at` keeps windows of a replay that closed recently.
- receipts: per consumer group, older than the longest time a duplicate can
  still arrive: its topic's retention, and for the normalizer also the oldest
  `occurred_at` ingestion accepts (re-sent records get the same raw event ID),
  plus a margin. These bounds are computed, not configured, so cleanup cannot
  shorten deduplication.
- outbox: published rows older than `SIGNALOPS_RETENTION_OUTBOX_DAYS`. Unpublished
  and quarantined rows stay.
- progress: progress history of investigations that ended more than
  `SIGNALOPS_RETENTION_PROGRESS_DAYS` ago. Their state, steps, and diagnosis stay.
- cache: Redis keys under `cache:` and `limit:` without an expiry (every writer sets
  one, so normally none). Keys with an expiry are left to Redis.

`scope` limits signals and aggregates to one replay scope (for example a
benchmark's), leaving other data untouched.
"""

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, Uuid, and_, column, delete, exists, func, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP
from signalops.agents.investigations import DISPATCHER_CONSUMER_GROUP
from signalops.config import Settings
from signalops.contracts.events import EventType
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP
from signalops.detection.graph import GRAPH_CONSUMER_GROUP
from signalops.persistence.database import Database
from signalops.persistence.models import (
    ConsumerReceipt,
    Deployment,
    GraphEdge,
    IncidentSignal,
    Investigation,
    OutboxEvent,
    ProgressEvent,
    Signal,
    SignalAggregate,
)
from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE, TOPICS

logger = logging.getLogger(__name__)

RECEIPT_MARGIN = timedelta(days=1)
CACHE_PATTERNS = ("cache:*", "limit:*")


class RetentionPolicyError(ValueError):
    """A configured bound would remove history that correctness still needs."""


def _topic_retention(event_type: EventType) -> timedelta:
    name = TOPIC_BY_EVENT_TYPE[event_type]
    [spec] = [topic for topic in TOPICS if topic.name == name]
    return timedelta(milliseconds=spec.retention_ms)


@dataclass(frozen=True)
class RetentionPolicy:
    signals: timedelta
    aggregates: timedelta
    outbox: timedelta
    progress: timedelta
    # The oldest `occurred_at` ingestion accepts, plus how far ahead it accepts.
    ingest_horizon: timedelta
    # The newest evidence a running investigation can read reaches this far back.
    investigation_reach: timedelta

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetentionPolicy":
        policy = cls(
            signals=timedelta(days=settings.retention_signals_days),
            aggregates=timedelta(days=settings.retention_aggregates_days),
            outbox=timedelta(days=settings.retention_outbox_days),
            progress=timedelta(days=settings.retention_progress_days),
            ingest_horizon=timedelta(
                seconds=settings.ingest_max_age_seconds + settings.ingest_max_future_seconds
            ),
            # A deadline plus the six-hour tool window before the cutoff.
            investigation_reach=timedelta(
                seconds=settings.investigation_deadline_seconds + 6 * 60 * 60
            ),
        )
        policy.check()
        return policy

    def check(self) -> None:
        if self.aggregates <= self.ingest_horizon:
            raise RetentionPolicyError(
                "aggregate retention must exceed the ingestion age bound plus the future bound,"
                " or a late record could reopen a deleted window"
            )
        if self.signals <= self.investigation_reach:
            raise RetentionPolicyError(
                "signal retention must exceed an investigation's deadline plus its tool window"
            )

    def receipt_bounds(self) -> dict[str, timedelta]:
        """Per consumer group: how long a duplicate of a processed event can still arrive."""
        raw = max(_topic_retention(EventType.SIGNAL_RAW), self.ingest_horizon)
        normalized = _topic_retention(EventType.SIGNAL_NORMALIZED)
        requested = _topic_retention(EventType.INVESTIGATION_REQUESTED)
        return {
            NORMALIZER_CONSUMER_GROUP: raw + RECEIPT_MARGIN,
            DETECTOR_CONSUMER_GROUP: normalized + RECEIPT_MARGIN,
            GRAPH_CONSUMER_GROUP: normalized + RECEIPT_MARGIN,
            DISPATCHER_CONSUMER_GROUP: requested + RECEIPT_MARGIN,
        }

    def longest_receipt_bound(self) -> timedelta:
        return max(self.receipt_bounds().values())


@dataclass
class CategoryReport:
    name: str
    cutoff: datetime | None
    eligible: int = 0
    # Old enough, but kept because something cites or references it.
    kept: int = 0
    deleted: int = 0
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class RetentionReport:
    applied: bool
    now: datetime
    scope: str | None
    categories: list[CategoryReport]

    def as_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "now": self.now.isoformat(),
            "scope": self.scope,
            "categories": [
                {
                    "name": c.name,
                    "cutoff": c.cutoff.isoformat() if c.cutoff else None,
                    "eligible": c.eligible,
                    "kept": c.kept,
                    "deleted": c.deleted,
                    **c.detail,
                }
                for c in self.categories
            ],
        }


_CITED_SQL = """
SELECT (jsonb_path_query(diagnosis, '$.root_cause.evidence_signal_ids[*]') #>> '{}')::uuid
    AS signal_id
FROM investigations WHERE diagnosis IS NOT NULL
UNION
SELECT (jsonb_path_query(diagnosis, '$.contradictions[*].evidence_signal_ids[*]') #>> '{}')::uuid
FROM investigations WHERE diagnosis IS NOT NULL
"""


def cited_signal_ids() -> Any:
    """Signal IDs a stored diagnosis cites (root cause or contradiction), as a subquery."""
    return text(_CITED_SQL).columns(column("signal_id", Uuid())).subquery("cited")


def _referenced(cited: Any) -> ColumnElement[bool]:
    return (
        exists().where(IncidentSignal.signal_id == Signal.id)
        | exists().where(Deployment.signal_id == Signal.id)
        | exists().where(GraphEdge.provenance_signal_id == Signal.id)
        | Signal.id.in_(select(cited.c.signal_id))
    )


async def _count(session: AsyncSession, model: Any, condition: ColumnElement[bool]) -> int:
    return int(await session.scalar(select(func.count()).select_from(model).where(condition)) or 0)


class Retention:
    def __init__(
        self,
        database: Database,
        policy: RetentionPolicy,
        *,
        batch_size: int = 5_000,
        cache_keys: Callable[[], Awaitable[Mapping[str, int]]] | None = None,
        delete_cache_keys: Callable[[], Awaitable[int]] | None = None,
    ) -> None:
        self._database = database
        self._policy = policy
        self._batch = batch_size
        self._cache_keys = cache_keys
        self._delete_cache_keys = delete_cache_keys

    async def run(self, now: datetime, *, apply: bool, scope: str | None = None) -> RetentionReport:
        categories = [
            await self._signals(now, apply, scope),
            await self._aggregates(now, apply, scope),
            await self._receipts(now, apply),
            await self._outbox(now, apply),
            await self._progress(now, apply),
        ]
        if self._cache_keys is not None:
            categories.append(await self._cache(apply))
        report = RetentionReport(apply, now, scope, categories)
        logger.info("retention finished", extra=report.as_dict())
        return report

    async def _delete_batches(self, model: Any, key: Any, condition: ColumnElement[bool]) -> int:
        deleted = 0
        while True:
            async with self._database.transaction() as session:
                ids = select(key).where(condition).limit(self._batch).scalar_subquery()
                result = await session.execute(delete(model).where(key.in_(ids)))
            count = int(result.rowcount or 0)  # type: ignore[attr-defined]
            deleted += count
            if count < self._batch:
                return deleted

    async def _signals(self, now: datetime, apply: bool, scope: str | None) -> CategoryReport:
        cutoff = now - self._policy.signals
        old = Signal.stored_at < cutoff
        if scope is not None:
            old = and_(old, Signal.replay_scope == scope)
        cited = cited_signal_ids()
        report = CategoryReport("signals", cutoff)
        async with self._database.transaction() as session:
            report.eligible = await _count(session, Signal, old)
            report.kept = await _count(session, Signal, and_(old, _referenced(cited)))
        if apply:
            report.deleted = await self._delete_batches(
                Signal, Signal.id, and_(old, ~_referenced(cited_signal_ids()))
            )
        return report

    async def _aggregates(self, now: datetime, apply: bool, scope: str | None) -> CategoryReport:
        cutoff = now - self._policy.aggregates
        old = and_(
            SignalAggregate.closed_at.is_not(None),
            SignalAggregate.closed_at < cutoff,
            SignalAggregate.window_end < cutoff,
        )
        if scope is not None:
            old = and_(old, SignalAggregate.replay_scope == scope)
        report = CategoryReport("aggregates", cutoff)
        async with self._database.transaction() as session:
            report.eligible = await _count(session, SignalAggregate, old)
        if apply:
            report.deleted = await self._delete_batches(SignalAggregate, SignalAggregate.id, old)
        return report

    async def _receipts(self, now: datetime, apply: bool) -> CategoryReport:
        bounds = self._policy.receipt_bounds()
        report = CategoryReport(
            "receipts",
            None,
            detail={"bounds_days": {g: b.total_seconds() / 86400 for g, b in bounds.items()}},
        )
        async with self._database.transaction() as session:
            groups = list(await session.scalars(select(ConsumerReceipt.consumer_group).distinct()))
        longest = self._policy.longest_receipt_bound()
        for group in groups:
            # Unknown groups (redrive tooling, older versions) get the longest bound.
            old = and_(
                ConsumerReceipt.consumer_group == group,
                ConsumerReceipt.processed_at < now - bounds.get(group, longest),
            )
            async with self._database.transaction() as session:
                report.eligible += await _count(session, ConsumerReceipt, old)
            if apply:
                report.deleted += await self._delete_receipts(old)
        return report

    async def _delete_receipts(self, condition: ColumnElement[bool]) -> int:
        # Composite key: delete by row identity in batches.
        deleted = 0
        while True:
            async with self._database.transaction() as session:
                rows = select(ConsumerReceipt.consumer_group, ConsumerReceipt.event_id).where(
                    condition
                )
                keys = (await session.execute(rows.limit(self._batch))).all()
                if not keys:
                    return deleted
                result = await session.execute(
                    delete(ConsumerReceipt).where(
                        tuple_(ConsumerReceipt.consumer_group, ConsumerReceipt.event_id).in_(
                            [(g, e) for g, e in keys]
                        )
                    )
                )
            deleted += int(result.rowcount or 0)  # type: ignore[attr-defined]
            if len(keys) < self._batch:
                return deleted

    async def _outbox(self, now: datetime, apply: bool) -> CategoryReport:
        cutoff = now - self._policy.outbox
        old = and_(OutboxEvent.published_at.is_not(None), OutboxEvent.published_at < cutoff)
        report = CategoryReport("outbox", cutoff)
        async with self._database.transaction() as session:
            report.eligible = await _count(session, OutboxEvent, old)
        if apply:
            report.deleted = await self._delete_batches(OutboxEvent, OutboxEvent.id, old)
        return report

    async def _progress(self, now: datetime, apply: bool) -> CategoryReport:
        cutoff = now - self._policy.progress
        ended = select(Investigation.id).where(
            Investigation.completed_at.is_not(None), Investigation.completed_at < cutoff
        )
        old = ProgressEvent.investigation_id.in_(ended)
        report = CategoryReport("progress", cutoff)
        async with self._database.transaction() as session:
            report.eligible = await _count(session, ProgressEvent, old)
        if apply:
            report.deleted = await self._delete_batches(ProgressEvent, ProgressEvent.id, old)
        return report

    async def _cache(self, apply: bool) -> CategoryReport:
        assert self._cache_keys is not None  # noqa: S101 - checked by the caller
        counts = await self._cache_keys()
        report = CategoryReport("cache", None, detail={"keys": dict(counts)})
        report.eligible = int(counts.get("without_expiry", 0))
        if apply and report.eligible and self._delete_cache_keys is not None:
            report.deleted = await self._delete_cache_keys()
        return report


async def redis_cache_keys(client: Any) -> dict[str, int]:
    """Keys under the cache and limit prefixes, and how many have no expiry."""
    total = without = 0
    for pattern in CACHE_PATTERNS:
        async for key in client.scan_iter(match=pattern, count=1000):
            total += 1
            if await client.ttl(key) == -1:
                without += 1
    return {"total": total, "without_expiry": without}


async def delete_unexpiring_cache_keys(client: Any) -> int:
    deleted = 0
    for pattern in CACHE_PATTERNS:
        async for key in client.scan_iter(match=pattern, count=1000):
            if await client.ttl(key) == -1:
                deleted += int(await client.delete(key))
    return deleted
