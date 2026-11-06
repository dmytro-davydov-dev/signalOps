"""Detector window state: samples, aggregates, closure, clocks, and rule triggers.

Sample writes run in the detector's consumer transaction; closure runs there
for replay and in the closer's own transaction for live windows. Aggregate
updates are single upserts that add to the stored counts, so concurrent writers
to one window serialize on its row and never lose an increment. Closure locks
the row, so a sample either lands in the counts closure evaluates or sees the
window closed and evaluates it itself.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import BigInteger, ColumnElement, Text, cast, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.common import LIVE_REPLAY_SCOPE
from signalops.detection.rules import Window, WindowClosure, WindowCounts
from signalops.persistence.models import (
    DetectionClock,
    DetectionSample,
    DetectionTrigger,
    SignalAggregate,
)
from signalops.persistence.repositories.scope import EvidenceScope


@dataclass(frozen=True)
class SeriesKey:
    """One service's aggregate series in one replay scope."""

    workspace_id: uuid.UUID
    service_id: uuid.UUID
    replay_scope: str
    metric_name: str


@dataclass(frozen=True)
class Sample:
    signal_id: uuid.UUID
    is_error: bool
    # A duration in milliseconds and its histogram bucket; None for metrics without values.
    value: float | None = None
    bucket: str | None = None


@dataclass(frozen=True)
class WindowUpdate:
    counts: WindowCounts
    closed: bool


@dataclass(frozen=True)
class OpenWindow:
    """An unclosed window, row-locked until the transaction ends."""

    id: uuid.UUID
    series: SeriesKey
    window: Window
    counts: WindowCounts


@dataclass(frozen=True)
class TriggerRecord:
    rule_id: str
    rule_version: int
    observed: float
    threshold: float
    counts: WindowCounts


def _counts(row: Any) -> WindowCounts:
    return WindowCounts(
        sample_count=row.sample_count,
        error_count=row.error_count,
        histogram={str(key): int(count) for key, count in row.histogram.items()},
    )


class DetectionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def advance_clock(self, series: SeriesKey, at: datetime) -> datetime:
        """Move the service's replay clock forward to `at`; returns the clock, never earlier."""
        statement = insert(DetectionClock).values(
            service_id=series.service_id,
            replay_scope=series.replay_scope,
            workspace_id=series.workspace_id,
            clock_at=at,
        )
        clock: datetime | None = await self._session.scalar(
            statement.on_conflict_do_update(
                index_elements=[DetectionClock.service_id, DetectionClock.replay_scope],
                set_={
                    "clock_at": func.greatest(DetectionClock.clock_at, statement.excluded.clock_at)
                },
            ).returning(DetectionClock.clock_at)
        )
        assert clock is not None  # noqa: S101 - the upsert always returns its row
        return clock

    async def record_sample(
        self, series: SeriesKey, window: Window, sample: Sample, *, late: bool
    ) -> bool:
        """Insert the sample; False when the series already counts this signal."""
        inserted = await self._session.scalar(
            insert(DetectionSample)
            .values(
                signal_id=sample.signal_id,
                metric_name=series.metric_name,
                workspace_id=series.workspace_id,
                service_id=series.service_id,
                replay_scope=series.replay_scope,
                window_start=window.start,
                is_error=sample.is_error,
                value=sample.value,
                late=late,
            )
            .on_conflict_do_nothing()
            .returning(DetectionSample.signal_id)
        )
        return inserted is not None

    async def add_to_window(
        self, series: SeriesKey, window: Window, sample: Sample
    ) -> WindowUpdate:
        """Add one sample to its window's aggregate and return the updated window."""
        histogram = {sample.bucket: 1} if sample.bucket is not None else {}
        statement = insert(SignalAggregate).values(
            workspace_id=series.workspace_id,
            service_id=series.service_id,
            replay_scope=series.replay_scope,
            metric_name=series.metric_name,
            window_start=window.start,
            window_end=window.end,
            sample_count=1,
            error_count=int(sample.is_error),
            value_sum=sample.value or 0.0,
            histogram=histogram,
        )
        updates: dict[str, ColumnElement[Any]] = {
            "sample_count": SignalAggregate.sample_count + 1,
            "error_count": SignalAggregate.error_count + statement.excluded.error_count,
            "value_sum": SignalAggregate.value_sum + statement.excluded.value_sum,
        }
        if sample.bucket is not None:
            # `||` replaces the bucket's key with its incremented count.
            stored = SignalAggregate.histogram
            current = func.coalesce(stored[sample.bucket].astext.cast(BigInteger), 0)
            updates["histogram"] = stored.op("||")(
                func.jsonb_build_object(cast(sample.bucket, Text), current + 1)
            )
        row = (
            await self._session.execute(
                statement.on_conflict_do_update(
                    constraint="uq_signal_aggregates_window", set_=updates
                ).returning(
                    SignalAggregate.sample_count,
                    SignalAggregate.error_count,
                    SignalAggregate.histogram,
                    SignalAggregate.closed_at,
                )
            )
        ).one()
        return WindowUpdate(_counts(row), closed=row.closed_at is not None)

    async def add_late_to_window(self, series: SeriesKey, window: Window) -> None:
        """Count a sample past the deadline; the window's evaluated counts do not change."""
        statement = insert(SignalAggregate).values(
            workspace_id=series.workspace_id,
            service_id=series.service_id,
            replay_scope=series.replay_scope,
            metric_name=series.metric_name,
            window_start=window.start,
            window_end=window.end,
            late_count=1,
        )
        await self._session.execute(
            statement.on_conflict_do_update(
                constraint="uq_signal_aggregates_window",
                set_={"late_count": SignalAggregate.late_count + 1},
            )
        )

    async def due_live_windows(self, before: datetime, limit: int) -> Sequence[OpenWindow]:
        """Lock the oldest live windows ending before `before`, skipping locked rows."""
        return await self._lock_open(
            SignalAggregate.replay_scope == LIVE_REPLAY_SCOPE,
            SignalAggregate.window_end < before,
            limit=limit,
        )

    async def due_service_windows(
        self, series: SeriesKey, before: datetime
    ) -> Sequence[OpenWindow]:
        """Lock the service's windows in a replay scope ending before `before`, oldest first."""
        return await self._lock_open(
            SignalAggregate.service_id == series.service_id,
            SignalAggregate.replay_scope == series.replay_scope,
            SignalAggregate.window_end < before,
        )

    async def open_scope_windows(self, scope: EvidenceScope, limit: int) -> Sequence[OpenWindow]:
        """Lock the scope's oldest open windows, skipping locked rows."""
        return await self._lock_open(
            SignalAggregate.workspace_id == scope.workspace_id,
            SignalAggregate.replay_scope == scope.replay_scope,
            limit=limit,
        )

    async def _lock_open(
        self, *criteria: ColumnElement[bool], limit: int | None = None
    ) -> Sequence[OpenWindow]:
        result = await self._session.execute(
            select(
                SignalAggregate.id,
                SignalAggregate.workspace_id,
                SignalAggregate.service_id,
                SignalAggregate.replay_scope,
                SignalAggregate.metric_name,
                SignalAggregate.window_start,
                SignalAggregate.window_end,
                SignalAggregate.sample_count,
                SignalAggregate.error_count,
                SignalAggregate.histogram,
            )
            .where(SignalAggregate.closed_at.is_(None), *criteria)
            .order_by(SignalAggregate.window_start, SignalAggregate.metric_name)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            OpenWindow(
                id=row.id,
                series=SeriesKey(
                    workspace_id=row.workspace_id,
                    service_id=row.service_id,
                    replay_scope=row.replay_scope,
                    metric_name=row.metric_name,
                ),
                window=Window(row.window_start, row.window_end),
                counts=_counts(row),
            )
            for row in result
        ]

    async def mark_closed(self, window_ids: Sequence[uuid.UUID], closed_by: WindowClosure) -> None:
        await self._session.execute(
            update(SignalAggregate)
            .where(SignalAggregate.id.in_(window_ids))
            .values(closed_at=func.now(), closed_by=closed_by)
        )

    async def lock_rule(self, series: SeriesKey, rule_id: str) -> None:
        """Serialize trigger decisions for one rule and service until the transaction ends."""
        key = f"detection:{series.service_id}:{series.replay_scope}:{rule_id}"
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0)))
        )

    async def triggered_near(
        self, series: SeriesKey, rule_id: str, window_start: datetime, cooldown: timedelta
    ) -> bool:
        """Whether the rule fired for a window starting within `cooldown` of `window_start`."""
        found = await self._session.scalar(
            select(DetectionTrigger.id)
            .where(
                DetectionTrigger.service_id == series.service_id,
                DetectionTrigger.replay_scope == series.replay_scope,
                DetectionTrigger.rule_id == rule_id,
                DetectionTrigger.window_start >= window_start - cooldown,
                DetectionTrigger.window_start <= window_start + cooldown,
            )
            .limit(1)
        )
        return found is not None

    async def add_trigger(
        self, series: SeriesKey, window: Window, record: TriggerRecord
    ) -> uuid.UUID | None:
        """Insert the trigger; None when the rule already fired for this window."""
        return await self._session.scalar(
            insert(DetectionTrigger)
            .values(
                workspace_id=series.workspace_id,
                service_id=series.service_id,
                replay_scope=series.replay_scope,
                rule_id=record.rule_id,
                rule_version=record.rule_version,
                metric_name=series.metric_name,
                window_start=window.start,
                window_end=window.end,
                observed=record.observed,
                threshold=record.threshold,
                sample_count=record.counts.sample_count,
                error_count=record.counts.error_count,
            )
            .on_conflict_do_nothing(constraint="uq_detection_triggers_window")
            .returning(DetectionTrigger.id)
        )

    async def link_trigger(self, trigger_id: uuid.UUID, incident_id: uuid.UUID) -> None:
        await self._session.execute(
            update(DetectionTrigger)
            .where(DetectionTrigger.id == trigger_id)
            .values(incident_id=incident_id)
        )
