"""Normalized signals and the deployments derived from them."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.evidence import NormalizedSignal
from signalops.persistence.models import Deployment, Signal


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
