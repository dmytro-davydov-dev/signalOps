"""Consumer receipts and the transactional outbox.

A consumer inserts its receipt, the domain effect, and any outbox rows in one
transaction; a receipt conflict means the event was already processed. The outbox
publisher reads pending rows through the partial `created_at` index and sets
`published_at` only after broker acknowledgement.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from signalops.persistence.base import Base
from signalops.persistence.models.common import created_at, json_object, length_between, uuid_pk

MAX_NAME_LENGTH = 256


class ConsumerReceipt(Base):
    __tablename__ = "consumer_receipts"
    __table_args__ = (
        CheckConstraint(
            length_between("consumer_group", 1, MAX_NAME_LENGTH), name="consumer_group_length"
        ),
    )

    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = created_at()


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint("aggregate_id", "aggregate_version", "topic"),
        CheckConstraint("aggregate_version >= 1", name="aggregate_version_positive"),
        CheckConstraint(length_between("topic", 1, MAX_NAME_LENGTH), name="topic_length"),
        CheckConstraint("char_length(message_key) > 0", name="message_key_nonempty"),
        CheckConstraint(json_object("envelope"), name="envelope_object"),
        CheckConstraint("attempts >= 0", name="attempts_nonnegative"),
        CheckConstraint(
            "published_at IS NULL OR published_at >= created_at", name="published_after_created"
        ),
        Index(
            "ix_outbox_events_created_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    aggregate_id: Mapped[uuid.UUID]
    aggregate_version: Mapped[int] = mapped_column(BigInteger)
    topic: Mapped[str] = mapped_column(Text)
    message_key: Mapped[str] = mapped_column(Text)
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = created_at()
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
