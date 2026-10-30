"""Consumer receipts and outbox rows, written inside the consumer's transaction."""

import json
import uuid

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
        """
        row = OutboxEvent(
            id=envelope.event_id,
            aggregate_id=aggregate_id,
            aggregate_version=aggregate_version,
            topic=topic_for(envelope),
            message_key=message_key(envelope),
            envelope=json.loads(envelope.encode()),
        )
        self._session.add(row)
        await self._session.flush()
        return row
