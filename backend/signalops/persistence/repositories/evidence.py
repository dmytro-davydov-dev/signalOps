"""Normalized signals and the deployments derived from them.

Reads take an `EvidenceScope` and filter by it before anything else, and are
bounded by a time window and a row limit the caller supplies.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select, true, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.evidence import NormalizedSignal, QualityStatus, SignalKind
from signalops.persistence.models import Deployment, Signal
from signalops.persistence.repositories.scope import EvidenceScope


@dataclass(frozen=True)
class SignalFilter:
    """Signals with `since <= occurred_at < until` matching every given field."""

    since: datetime
    until: datetime
    source_id: uuid.UUID | None = None
    service_id: uuid.UUID | None = None
    kind: SignalKind | None = None
    quality_status: QualityStatus | None = None


@dataclass(frozen=True)
class SignalPosition:
    """Keyset position: the last signal of the previous page."""

    occurred_at: datetime
    id: uuid.UUID


@dataclass(frozen=True)
class QualityCounts:
    by_status: dict[tuple[uuid.UUID, str], int]
    by_flag: dict[tuple[uuid.UUID, str], int]


class EvidenceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_signal(self, signal: NormalizedSignal) -> bool:
        """Insert the signal; False when its source event is already stored in this scope.

        `UNIQUE(source_id, replay_scope, source_event_id)` is the business key,
        so a redelivery that slips past the consumer receipt still stores one row.
        """
        inserted = await self._session.scalar(
            insert(Signal)
            .values(
                **signal.model_dump(exclude={"quality_flags"}),
                quality_flags=[str(flag) for flag in signal.quality_flags],
            )
            .on_conflict_do_nothing(
                index_elements=[Signal.source_id, Signal.replay_scope, Signal.source_event_id]
            )
            .returning(Signal.id)
        )
        return inserted is not None

    async def insert_deployment(
        self,
        *,
        deployment_id: uuid.UUID,
        signal: NormalizedSignal,
        revision: str,
        deployed_at: datetime,
        attributes: dict[str, Any],
    ) -> None:
        """Insert the deployment for a signal just stored with a resolved service."""
        if signal.service_id is None:
            raise ValueError("a deployment requires a resolved service")
        self._session.add(
            Deployment(
                id=deployment_id,
                workspace_id=signal.workspace_id,
                service_id=signal.service_id,
                signal_id=signal.id,
                replay_scope=signal.replay_scope,
                revision=revision,
                deployed_at=deployed_at,
                attributes=attributes,
            )
        )
        await self._session.flush()

    async def list_signals(
        self,
        scope: EvidenceScope,
        criteria: SignalFilter,
        *,
        after: SignalPosition | None,
        limit: int,
    ) -> list[Signal]:
        """Up to `limit` signals by `occurred_at` then ID, both descending, past `after`."""
        statement = select(Signal).where(
            scope.clause(Signal),
            Signal.occurred_at >= criteria.since,
            Signal.occurred_at < criteria.until,
        )
        if criteria.source_id is not None:
            statement = statement.where(Signal.source_id == criteria.source_id)
        if criteria.service_id is not None:
            statement = statement.where(Signal.service_id == criteria.service_id)
        if criteria.kind is not None:
            statement = statement.where(Signal.kind == criteria.kind)
        if criteria.quality_status is not None:
            statement = statement.where(Signal.quality_status == criteria.quality_status)
        if after is not None:
            statement = statement.where(
                tuple_(Signal.occurred_at, Signal.id) < tuple_(after.occurred_at, after.id)
            )
        result = await self._session.scalars(
            statement.order_by(Signal.occurred_at.desc(), Signal.id.desc()).limit(limit)
        )
        return list(result)

    async def trace_signals(
        self, scope: EvidenceScope, trace_id: str, *, limit: int
    ) -> list[Signal]:
        """Up to `limit` signals of one trace by `occurred_at` then ID, ascending."""
        result = await self._session.scalars(
            select(Signal)
            .where(scope.clause(Signal), Signal.trace_id == trace_id)
            .order_by(Signal.occurred_at, Signal.id)
            .limit(limit)
        )
        return list(result)

    async def quality_counts(
        self,
        scope: EvidenceScope,
        source_ids: Collection[uuid.UUID],
        *,
        since: datetime,
        until: datetime,
    ) -> QualityCounts:
        """Signal counts per (source, status) and (source, flag), by `ingested_at` in the window."""
        if not source_ids:
            return QualityCounts(by_status={}, by_flag={})
        in_window = (
            scope.clause(Signal),
            # Listing the sources lets each use its (source_id, replay_scope, ingested_at) index.
            Signal.source_id.in_(source_ids),
            Signal.ingested_at >= since,
            Signal.ingested_at < until,
        )
        status_rows = await self._session.execute(
            select(Signal.source_id, Signal.quality_status, func.count())
            .where(*in_window)
            .group_by(Signal.source_id, Signal.quality_status)
        )
        flags = func.jsonb_array_elements_text(Signal.quality_flags).table_valued("value")
        flag = flags.lateral("flag")
        flag_rows = await self._session.execute(
            select(Signal.source_id, flag.c.value, func.count())
            .select_from(Signal)
            .join(flag, true())
            .where(*in_window)
            .group_by(Signal.source_id, flag.c.value)
        )
        return QualityCounts(
            by_status={(source, status): count for source, status, count in status_rows},
            by_flag={(source, name): count for source, name, count in flag_rows},
        )
