"""Bounded engine construction and the transaction boundary used by repositories.

Repositories receive an `AsyncSession` that is already inside a transaction and
never commit or roll back themselves. The caller owns the unit of work through
`Database.transaction()`, which commits when the block finishes and rolls back
every write in it when the block raises. Keep these blocks short: no model
calls, broker publication, or other slow I/O while one is open.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from signalops.config import Settings


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

    @classmethod
    def from_engine(cls, engine: AsyncEngine) -> "Database":
        # expire_on_commit=False lets callers read returned rows after the block ends.
        return cls(engine, async_sessionmaker(engine, expire_on_commit=False))

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """One unit of work: commit on success, roll back everything on any exception."""
        async with self.sessions() as session, session.begin():
            yield session
