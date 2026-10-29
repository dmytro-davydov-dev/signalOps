"""Column and constraint helpers shared by the ORM models."""

import uuid
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import DateTime, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.common import LIVE_REPLAY_SCOPE


def one_of(column: str, values: Iterable[StrEnum]) -> str:
    return f"{column} IN ({', '.join(repr(member.value) for member in values)})"


def length_between(column: str, low: int, high: int) -> str:
    return f"char_length({column}) BETWEEN {low} AND {high}"


def nonempty(column: str) -> str:
    return f"char_length({column}) > 0"


def json_object(column: str) -> str:
    return f"jsonb_typeof({column}) = 'object'"


def uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=func.gen_random_uuid()
    )


def created_at() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())


def json_attributes() -> Mapped[dict[str, Any]]:
    return mapped_column(JSONB, default=dict, server_default=text("'{}'::jsonb"))


def replay_scope() -> Mapped[str]:
    return mapped_column(Text, default=LIVE_REPLAY_SCOPE, server_default=LIVE_REPLAY_SCOPE)
