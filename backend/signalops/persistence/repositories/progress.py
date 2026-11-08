"""Durable investigation progress.

Callers write an event in the transaction of the change it records, holding the
job row lock, so a later ID always means a later commit within an investigation.
"""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.progress import ProgressEventType, ProgressPayload
from signalops.persistence.database import mark_progress
from signalops.persistence.models import ProgressEvent


class ProgressRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self, investigation_id: uuid.UUID, event_type: ProgressEventType, payload: ProgressPayload
    ) -> None:
        await self._session.execute(
            insert(ProgressEvent).values(
                investigation_id=investigation_id,
                event_type=event_type,
                payload=payload.model_dump(mode="json"),
            )
        )
        mark_progress(self._session, investigation_id)

    async def exists(self, investigation_id: uuid.UUID, event_id: int) -> bool:
        """Whether this event ID belongs to this investigation's retained history."""
        found = await self._session.scalar(
            select(ProgressEvent.id).where(
                ProgressEvent.investigation_id == investigation_id, ProgressEvent.id == event_id
            )
        )
        return found is not None

    async def after(
        self, investigation_id: uuid.UUID, after: int, *, limit: int
    ) -> Sequence[ProgressEvent]:
        rows = await self._session.scalars(
            select(ProgressEvent)
            .where(ProgressEvent.investigation_id == investigation_id, ProgressEvent.id > after)
            .order_by(ProgressEvent.id)
            .limit(limit)
        )
        return rows.all()
