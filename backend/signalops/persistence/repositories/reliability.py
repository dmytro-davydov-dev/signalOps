"""Consumer receipts and outbox rows: staged by consumers, read and marked by the publisher."""

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from signalops.contracts.common import MAX_ERROR_DETAIL_LENGTH
from signalops.contracts.events import EventEnvelope, EventPayload
from signalops.persistence.models import ConsumerReceipt, OutboxEvent
from signalops.streaming.topics import message_key, topic_for


@dataclass(frozen=True)
class OutboxBacklog:
    pending: int
    quarantined: int
    # Age of the oldest unpublished row, quarantined or not; None when there is none.
    oldest_age_seconds: float | None


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
        """The oldest rows that are due for publication.

        A row is skipped while it, or an earlier pending version of its
        aggregate, is quarantined or waiting out a retry delay, so an
        aggregate's versions are never published out of order.
        """
        blocker = aliased(OutboxEvent)
        blocked = (
            select(blocker.id)
            .where(
                blocker.aggregate_id == OutboxEvent.aggregate_id,
                blocker.aggregate_version <= OutboxEvent.aggregate_version,
                blocker.published_at.is_(None),
                or_(
                    blocker.quarantined_at.is_not(None),
                    blocker.next_attempt_at > func.clock_timestamp(),
                ),
            )
            .exists()
        )
        query = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), ~blocked)
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
            .values(
                published_at=func.clock_timestamp(),
                attempts=OutboxEvent.attempts + 1,
                next_attempt_at=None,
            )
        )
        return result.rowcount  # type: ignore[attr-defined, no-any-return]

    async def schedule_retry(self, event_id: uuid.UUID, delay_seconds: float, error: str) -> None:
        """Count a transient failure and defer the row (and its aggregate) by the delay."""
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.published_at.is_(None))
            .values(
                attempts=OutboxEvent.attempts + 1,
                next_attempt_at=func.clock_timestamp() + timedelta(seconds=delay_seconds),
                last_error=_error_text(error),
            )
        )

    async def quarantine(self, event_id: uuid.UUID, error: str) -> None:
        """Count a permanent rejection and stop retrying the row until it is redriven."""
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.published_at.is_(None))
            .values(
                attempts=OutboxEvent.attempts + 1,
                quarantined_at=func.clock_timestamp(),
                next_attempt_at=None,
                last_error=_error_text(error),
            )
        )

    async def redrive(self, event_id: uuid.UUID) -> bool:
        """Make a quarantined row due again; False when no such quarantined row is pending."""
        result = await self._session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.published_at.is_(None),
                OutboxEvent.quarantined_at.is_not(None),
            )
            .values(quarantined_at=None, next_attempt_at=None)
        )
        return result.rowcount == 1  # type: ignore[attr-defined, no-any-return]

    async def backlog(self) -> OutboxBacklog:
        """Unpublished rows, read through the partial index on pending rows."""
        query = select(
            func.count(),
            func.count().filter(OutboxEvent.quarantined_at.is_not(None)),
            func.extract("epoch", func.clock_timestamp() - func.min(OutboxEvent.created_at)),
        ).where(OutboxEvent.published_at.is_(None))
        pending, quarantined, age = (await self._session.execute(query)).one()
        return OutboxBacklog(pending, quarantined, float(age) if age is not None else None)


def _error_text(error: str) -> str:
    return error[:MAX_ERROR_DETAIL_LENGTH] or "unknown"
