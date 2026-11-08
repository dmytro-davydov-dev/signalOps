"""Migrations from empty, transaction boundaries, seed data, and pool limits (P02-T06)."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, create_engine, func, insert, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from signalops.config import Settings
from signalops.contracts.evidence import SourceKind
from signalops.persistence import database as db
from signalops.persistence.base import Base
from signalops.persistence.migrations import alembic_config, upgrade_to_head
from signalops.persistence.models import Service, Source, Workspace
from signalops.persistence.repositories.catalog import CatalogConflictError, CatalogRepository
from signalops.persistence.seed import (
    DEMO_SERVICES,
    DEMO_SOURCES,
    DEMO_WORKSPACE_ID,
    seed_demo,
)

pytestmark = pytest.mark.integration

# Columns that scope a reference rather than identify the parent row.
SCOPE_COLUMNS = {"workspace_id", "replay_scope"}


def test_empty_database_upgrades_to_head(scratch_database: Settings) -> None:
    upgrade_to_head(scratch_database)
    head = ScriptDirectory.from_config(alembic_config(scratch_database)).get_current_head()
    engine = create_engine(scratch_database.validated_database_url())
    try:
        with engine.connect() as conn:
            assert MigrationContext.configure(conn).get_current_revision() == head
            inspector = inspect(conn)
            tables = set(inspector.get_table_names()) - {"alembic_version"}
            tables |= {f"evaluation.{name}" for name in inspector.get_table_names("evaluation")}
    finally:
        engine.dispose()
    assert tables == set(Base.metadata.tables)


def test_every_foreign_key_has_a_supporting_index(engine: Engine) -> None:
    """Each reference to a non-workspace parent is the leading column of some index."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT c.conrelid::regclass::text, c.conname,
                       array_agg(a.attname::text ORDER BY k.ord),
                       ARRAY(
                           SELECT a2.attname::text FROM pg_index i
                           JOIN pg_attribute a2
                             ON a2.attrelid = i.indrelid AND a2.attnum = i.indkey[0]
                           WHERE i.indrelid = c.conrelid
                       )
                FROM pg_constraint c
                CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
                WHERE c.contype = 'f'
                  AND c.connamespace IN ('public'::regnamespace, 'evaluation'::regnamespace)
                GROUP BY c.oid, c.conrelid, c.conname
                """
            )
        ).all()
    assert rows
    unsupported = [
        (table, name)
        for table, name, columns, leading in rows
        if (identifying := set(columns) - SCOPE_COLUMNS) and not identifying & set(leading)
    ]
    assert unsupported == []


@pytest.fixture
async def database(migrated: tuple[Settings, Engine]) -> AsyncIterator[db.Database]:
    engine = db.create_engine(migrated[0])
    try:
        yield db.Database.from_engine(engine)
    finally:
        await engine.dispose()


async def count(database: db.Database, model: type[Base], **where: Any) -> int:
    async with database.transaction() as session:
        query = select(func.count()).select_from(model).filter_by(**where)
        return (await session.execute(query)).scalar_one()


async def test_committed_rows_remain_readable_after_the_block(database: db.Database) -> None:
    workspace_id = uuid.uuid4()
    async with database.transaction() as session:
        workspace = await CatalogRepository(session).ensure_workspace(workspace_id, "committed")
    assert workspace.name == "committed"
    assert await count(database, Workspace, id=workspace_id) == 1


async def test_failed_write_rolls_back_the_whole_transaction(database: db.Database) -> None:
    workspace_id = uuid.uuid4()

    async def write() -> None:
        async with database.transaction() as session:
            catalog = CatalogRepository(session)
            await catalog.ensure_workspace(workspace_id, "partial")
            await catalog.ensure_source(
                source_id=uuid.uuid4(),
                workspace_id=workspace_id,
                name="logs",
                kind=SourceKind.STRUCTURED_LOG,
            )
            # Same (workspace_id, name) as the source above: the third write fails.
            await session.execute(
                insert(Source).values(
                    workspace_id=workspace_id, name="logs", kind=SourceKind.DEPLOYMENT
                )
            )

    with pytest.raises(IntegrityError):
        await write()
    assert await count(database, Workspace, id=workspace_id) == 0
    assert await count(database, Source, workspace_id=workspace_id) == 0


async def test_application_error_rolls_back_earlier_writes(database: db.Database) -> None:
    workspace_id = uuid.uuid4()

    async def write() -> None:
        async with database.transaction() as session:
            catalog = CatalogRepository(session)
            await catalog.ensure_workspace(workspace_id, "abandoned")
            await catalog.ensure_service(
                service_id=uuid.uuid4(), workspace_id=workspace_id, name="api", environment="demo"
            )
            raise RuntimeError("after writes")

    with pytest.raises(RuntimeError, match="after writes"):
        await write()
    assert await count(database, Workspace, id=workspace_id) == 0
    assert await count(database, Service, workspace_id=workspace_id) == 0


async def test_conflicting_catalog_row_is_rejected_without_changes(database: db.Database) -> None:
    workspace_id = uuid.uuid4()
    source_id = uuid.uuid4()
    async with database.transaction() as session:
        catalog = CatalogRepository(session)
        await catalog.ensure_workspace(workspace_id, "conflicts")
        await catalog.ensure_source(
            source_id=source_id, workspace_id=workspace_id, name="logs", kind=SourceKind.DEPLOYMENT
        )

    async def write() -> None:
        async with database.transaction() as session:
            catalog = CatalogRepository(session)
            await catalog.ensure_service(
                service_id=uuid.uuid4(), workspace_id=workspace_id, name="api", environment="demo"
            )
            await catalog.ensure_source(
                source_id=source_id,
                workspace_id=workspace_id,
                name="logs",
                kind=SourceKind.TRACE_SUMMARY,
            )

    with pytest.raises(CatalogConflictError):
        await write()
    assert await count(database, Service, workspace_id=workspace_id) == 0
    async with database.transaction() as session:
        stored = await session.get_one(Source, source_id)
    assert stored.kind == SourceKind.DEPLOYMENT


async def catalog_snapshot(database: db.Database) -> list[tuple[Any, ...]]:
    async with database.transaction() as session:
        sources = await session.execute(
            select(Source.id, Source.name, Source.kind, Source.created_at)
            .where(Source.workspace_id == DEMO_WORKSPACE_ID)
            .order_by(Source.name)
        )
        services = await session.execute(
            select(Service.id, Service.name, Service.environment, Service.created_at)
            .where(Service.workspace_id == DEMO_WORKSPACE_ID)
            .order_by(Service.name)
        )
        return [tuple(row) for row in [*sources.all(), *services.all()]]


async def test_seed_is_deterministic_and_idempotent(database: db.Database) -> None:
    await seed_demo(database)
    first = await catalog_snapshot(database)
    await seed_demo(database)
    assert await catalog_snapshot(database) == first
    assert {(row[0], row[1]) for row in first} == {
        *((source.id, source.name) for source in DEMO_SOURCES),
        *((service.id, service.name) for service in DEMO_SERVICES),
    }


def with_settings(settings: Settings, **updates: Any) -> Settings:
    return settings.model_copy(update=updates)


async def test_sessions_carry_server_side_limits(migrated: tuple[Settings, Engine]) -> None:
    settings = with_settings(
        migrated[0],
        db_statement_timeout_ms=1500,
        db_lock_timeout_ms=2500,
        db_idle_in_transaction_timeout_ms=45_000,
    )
    engine = db.create_engine(settings)
    try:
        async with engine.connect() as conn:
            shown = [
                (await conn.execute(text(f"SHOW {name}"))).scalar_one()
                for name in (
                    "statement_timeout",
                    "lock_timeout",
                    "idle_in_transaction_session_timeout",
                    "timezone",
                )
            ]
    finally:
        await engine.dispose()
    assert shown == ["1500ms", "2500ms", "45s", "UTC"]


async def test_statement_timeout_cancels_a_slow_query(migrated: tuple[Settings, Engine]) -> None:
    engine = db.create_engine(with_settings(migrated[0], db_statement_timeout_ms=200))
    try:
        async with engine.connect() as conn:
            with pytest.raises(DBAPIError) as excinfo:
                await conn.execute(text("SELECT pg_sleep(2)"))
    finally:
        await engine.dispose()
    assert excinfo.value.orig.sqlstate == "57014"  # type: ignore[union-attr]  # query_canceled


async def test_pool_is_bounded(migrated: tuple[Settings, Engine]) -> None:
    settings = with_settings(
        migrated[0], db_pool_size=1, db_max_overflow=0, db_pool_timeout_seconds=0.2
    )
    engine = db.create_engine(settings)
    try:
        async with engine.connect():
            with pytest.raises(PoolTimeoutError):
                async with asyncio.timeout(5), engine.connect():
                    pass
    finally:
        await engine.dispose()
