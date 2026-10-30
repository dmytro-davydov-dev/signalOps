"""Consumer receipts and outbox rows: staged by consumers, read and marked by the publisher."""

import json
import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.events import EventEnvelope, EventPayload
from signalops.persistence.models import ConsumerReceipt, OutboxEvent
from signalops.streaming.topics import message_key, topic_for


class ReceiptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(self, consumer_group: str, event_id: uuid.UUID) -> bool:
        """Insert the receipt; False when this group already processed the event.

        A concurrent insert of the same receipt waits for the other transaction
        and then reports a duplicate, so two members never both apply an effect.
        """
        inserted = await self._session.scalar(
            insert(ConsumerReceipt)
            .values(consumer_group=consumer_group, event_id=event_id)
            .on_conflict_do_nothing()
            .returning(ConsumerReceipt.event_id)
        )
        return inserted is not None


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        envelope: EventEnvelope[EventPayload],
        *,
        aggregate_id: uuid.UUID,
        aggregate_version: int,
    ) -> OutboxEvent:
        """Stage an event for the outbox publisher; the row id is the envelope's event_id.

        Topic and key are fixed here so the publisher sends exactly what was
        committed. Encoding first rejects an oversized envelope while the caller
        can still roll back, instead of leaving an unpublishable row.

        `created_at` is the insert time, not the transaction start: when the
        caller allocates aggregate versions under the aggregate's row lock, a
        later version then sorts after an earlier one in the publisher's query.
        """
        row = OutboxEvent(
            id=envelope.event_id,
            created_at=func.clock_timestamp(),
            aggregate_id=aggregate_id,
            aggregate_version=aggregate_version,
            topic=topic_for(envelope),
            message_key=message_key(envelope),
            envelope=json.loads(envelope.encode()),
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row, ["created_at"])
        return row

    async def pending(self, limit: int) -> list[OutboxEvent]:
        """The oldest unpublished rows, served by the partial index on created_at."""
        query = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.aggregate_version)
            .limit(limit)
        )
        return list((await self._session.scalars(query)).all())

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> int:
        """Record acknowledged rows; a row already marked by an earlier run is left as is."""
        if not ids:
            return 0
        result = await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(ids), OutboxEvent.published_at.is_(None))
            .values(published_at=func.clock_timestamp(), attempts=OutboxEvent.attempts + 1)
        )
        return result.rowcount  # type: ignore[attr-defined, no-any-return]

    async def record_failed_attempts(self, ids: Sequence[uuid.UUID]) -> None:
        """Count a failed publish; the row stays pending for the next poll."""
        if not ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(ids), OutboxEvent.published_at.is_(None))
            .values(attempts=OutboxEvent.attempts + 1)
        )
