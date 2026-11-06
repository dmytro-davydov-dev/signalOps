"""Incident rows the detector writes: correlation lookup, services, evidence, and versions.

Writers hold `lock_scope`, so one correlation decision is made at a time, and
`locked`/`find_correlated` take the row, so an operator's versioned update waits
or fails cleanly. `apply` carries the version predicate data-model.md requires
of Core updates.
"""

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import exists, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError

from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus
from signalops.detection.rules import Window
from signalops.persistence.models import (
    DetectionSample,
    Incident,
    IncidentService,
    IncidentSignal,
    Service,
)
from signalops.persistence.repositories.detection import SeriesKey
from signalops.persistence.repositories.scope import EvidenceScope


@dataclass(frozen=True)
class NewIncident:
    scope: EvidenceScope
    fingerprint: str
    title: str
    severity: IncidentSeverity
    window: Window
    detected_at: datetime


class IncidentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def lock_scope(self, replay_scope: str) -> None:
        """Serialize incident writes in a replay scope until the transaction ends.

        Keyed by scope alone: a closer batch spans workspaces, and one key per
        transaction cannot be taken in conflicting orders.
        """
        key = f"incidents:{replay_scope}"
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0)))
        )

    async def get_service(self, series: SeriesKey) -> Service:
        service = await self._session.scalar(
            select(Service).where(
                Service.workspace_id == series.workspace_id, Service.id == series.service_id
            )
        )
        assert service is not None  # noqa: S101 - the aggregate's foreign key guarantees it
        return service

    async def find_correlated(
        self, scope: EvidenceScope, fingerprint: str, window: Window, gap: timedelta
    ) -> Incident | None:
        """Lock the latest incident of the fingerprint that `window` falls within `gap` of."""
        return await self._session.scalar(
            select(Incident)
            .where(
                scope.clause(Incident),
                Incident.fingerprint == fingerprint,
                Incident.last_seen_at >= window.start - gap,
                Incident.first_seen_at <= window.end + gap,
            )
            .order_by(Incident.last_seen_at.desc())
            .limit(1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def locked(self, scope: EvidenceScope, incident_id: uuid.UUID) -> Incident | None:
        return await self._session.scalar(
            select(Incident)
            .where(scope.clause(Incident), Incident.id == incident_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def create(self, new: NewIncident) -> uuid.UUID:
        incident_id: uuid.UUID | None = await self._session.scalar(
            insert(Incident)
            .values(
                workspace_id=new.scope.workspace_id,
                replay_scope=new.scope.replay_scope,
                fingerprint=new.fingerprint,
                correlation_window_start=new.window.start,
                title=new.title,
                severity=new.severity,
                status=IncidentStatus.OPEN,
                detected_at=new.detected_at,
                first_seen_at=new.window.start,
                last_seen_at=new.window.end,
            )
            .returning(Incident.id)
        )
        assert incident_id is not None  # noqa: S101 - an insert without ON CONFLICT returns
        return incident_id

    async def apply(self, incident: Incident, changes: Mapping[str, Any]) -> int:
        """Write `changes` and bump the version; returns the new version."""
        version: int | None = await self._session.scalar(
            update(Incident)
            .where(Incident.id == incident.id, Incident.version == incident.version)
            .values(**changes, version=Incident.version + 1)
            .returning(Incident.version)
        )
        if version is None:
            raise StaleDataError(f"incident {incident.id} changed under its row lock")
        return version

    async def add_service(
        self, series: SeriesKey, incident_id: uuid.UUID, role: IncidentServiceRole
    ) -> bool:
        """Attach the service; False when it is already attached."""
        inserted = await self._session.scalar(
            insert(IncidentService)
            .values(
                incident_id=incident_id,
                service_id=series.service_id,
                workspace_id=series.workspace_id,
                role=role,
            )
            .on_conflict_do_nothing()
            .returning(IncidentService.service_id)
        )
        return inserted is not None

    async def attach_breaching_samples(
        self,
        series: SeriesKey,
        window: Window,
        incident_id: uuid.UUID,
        criterion: Any,
        *,
        relevance: float,
        limit: int,
        incident_limit: int,
    ) -> int:
        """Cite the window's breaching samples, worst first; returns how many are new."""
        cited = await self._session.scalar(
            select(func.count())
            .select_from(IncidentSignal)
            .where(IncidentSignal.incident_id == incident_id)
        )
        room = min(limit, incident_limit - (cited or 0))
        if room <= 0:
            return 0
        samples = (
            select(
                literal(incident_id).label("incident_id"),
                DetectionSample.signal_id,
                DetectionSample.workspace_id,
                DetectionSample.replay_scope,
                literal(relevance).label("relevance"),
            )
            .where(
                DetectionSample.service_id == series.service_id,
                DetectionSample.replay_scope == series.replay_scope,
                DetectionSample.metric_name == series.metric_name,
                DetectionSample.window_start == window.start,
                DetectionSample.late.is_(False),
                criterion,
            )
            .order_by(DetectionSample.value.desc().nulls_last(), DetectionSample.signal_id)
            .limit(room)
        )
        inserted = await self._session.scalars(
            insert(IncidentSignal)
            .from_select(
                ["incident_id", "signal_id", "workspace_id", "replay_scope", "relevance"], samples
            )
            .on_conflict_do_nothing()
            .returning(IncidentSignal.signal_id)
        )
        return len(inserted.all())

    async def recovering(self, series: SeriesKey, seen_by: datetime) -> Sequence[uuid.UUID]:
        """Open incidents of the service last seen at or before `seen_by`."""
        rows = await self._session.scalars(
            select(Incident.id)
            .join(IncidentService, IncidentService.incident_id == Incident.id)
            .where(
                Incident.workspace_id == series.workspace_id,
                Incident.replay_scope == series.replay_scope,
                Incident.status == IncidentStatus.OPEN,
                Incident.last_seen_at <= seen_by,
                IncidentService.service_id == series.service_id,
            )
            .order_by(Incident.last_seen_at)
        )
        return rows.all()

    async def mark_healthy(
        self, incident_id: uuid.UUID, service_id: uuid.UUID, through: datetime
    ) -> None:
        await self._session.execute(
            update(IncidentService)
            .where(
                IncidentService.incident_id == incident_id,
                IncidentService.service_id == service_id,
            )
            .values(healthy_through=func.greatest(IncidentService.healthy_through, through))
        )

    async def all_healthy_through(self, incident_id: uuid.UUID, at: datetime) -> bool:
        """Whether every service of the incident has shown health at or after `at`."""
        pending = exists().where(
            IncidentService.incident_id == incident_id,
            or_(IncidentService.healthy_through.is_(None), IncidentService.healthy_through < at),
        )
        return not await self._session.scalar(select(pending))
