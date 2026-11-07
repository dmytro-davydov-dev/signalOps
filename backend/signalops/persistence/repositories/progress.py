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
