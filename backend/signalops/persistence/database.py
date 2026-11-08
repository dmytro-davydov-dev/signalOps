"""Bounded engine construction and the transaction boundary used by repositories.

Repositories receive an `AsyncSession` that is already inside a transaction and
never commit or roll back themselves. The caller owns the unit of work through
`Database.transaction()`, which commits when the block finishes and rolls back
every write in it when the block raises. Keep these blocks short: no model
calls, broker publication, or other slow I/O while one is open.
"""

import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from signalops.config import Settings

logger = logging.getLogger(__name__)

# `session.info` key: investigations whose progress this transaction wrote.
PROGRESS_WRITTEN = "signalops.progress_written"

# `session.info` key: callbacks to run once this transaction commits.
AFTER_COMMIT = "signalops.after_commit"

# Told after a commit which investigations gained progress events. Best effort:
# it must not raise, and readers never depend on it for correctness.
ProgressHook = Callable[[frozenset[uuid.UUID]], Awaitable[None]]


def mark_progress(session: AsyncSession, investigation_id: uuid.UUID) -> None:
    """Record that this transaction wrote progress for an investigation."""
    session.info.setdefault(PROGRESS_WRITTEN, set()).add(investigation_id)


def on_commit(session: AsyncSession, callback: Callable[[], None]) -> None:
    """Run `callback` after this transaction commits; a rollback drops it.

    For process-local effects such as metrics. It must not raise or block.
    """
    session.info.setdefault(AFTER_COMMIT, []).append(callback)


def create_engine(settings: Settings) -> AsyncEngine:
    """An async engine whose pool and per-session server limits come from settings."""
    server_limits = " ".join(
        [
            f"-c statement_timeout={settings.db_statement_timeout_ms}",
            f"-c lock_timeout={settings.db_lock_timeout_ms}",
            f"-c idle_in_transaction_session_timeout={settings.db_idle_in_transaction_timeout_ms}",
            "-c timezone=UTC",
        ]
    )
    return create_async_engine(
        settings.validated_database_url(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": settings.db_connect_timeout_seconds,
            "options": server_limits,
            "application_name": "signalops",
        },
    )


@dataclass(frozen=True)
class Database:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    on_progress: ProgressHook | None = None

    @classmethod
    def from_engine(
        cls, engine: AsyncEngine, *, on_progress: ProgressHook | None = None
    ) -> "Database":
        # expire_on_commit=False lets callers read returned rows after the block ends.
        return cls(engine, async_sessionmaker(engine, expire_on_commit=False), on_progress)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """One unit of work: commit on success, roll back everything on any exception.

        After a commit that wrote progress, `on_progress` hears of it, outside the
        transaction. A rollback tells it nothing.
        """
        async with self.sessions() as session:
            async with session.begin():
                yield session
            written: set[uuid.UUID] | None = session.info.pop(PROGRESS_WRITTEN, None)
            callbacks: list[Callable[[], None]] = session.info.pop(AFTER_COMMIT, [])
        for callback in callbacks:
            try:
                callback()
            except Exception as error:  # noqa: BLE001 - local bookkeeping only.
                logger.warning(
                    "after-commit callback failed", extra={"error": type(error).__name__}
                )
        if written and self.on_progress is not None:
            try:
                await self.on_progress(frozenset(written))
            except Exception as error:  # noqa: BLE001 - a lost wake-up only delays a reader.
                logger.warning("progress hook failed", extra={"error": type(error).__name__})
