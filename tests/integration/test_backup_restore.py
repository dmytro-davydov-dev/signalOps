"""A PostgreSQL backup restores durable state (P11-T05).

Runs the documented procedure (docs/retention.md#backup-and-restore): `pg_dump
-Fc` inside the Compose `postgres` container, `pg_restore` into an empty
database, then compares every table and checks that deduplication history
still deduplicates. Needs the `docker` CLI and the Compose stack.
"""

import asyncio
import os
import subprocess
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, inspect, make_url, text

from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP
from signalops.config import Settings
from signalops.contracts.common import replay_scope_for
from signalops.persistence import database as db
from signalops.persistence.repositories.reliability import ReceiptRepository
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.test_retention import _world

pytestmark = pytest.mark.integration

ROOT = Path(__file__).parents[2]
USER = os.environ.get("POSTGRES_USER", "signalops")


def _compose(*args: str, stdin: bytes | None = None) -> bytes:
    result = subprocess.run(  # noqa: S603 - fixed docker compose arguments
        ["docker", "compose", "exec", "-T", "postgres", *args],  # noqa: S607
        cwd=ROOT,
        input=stdin,
        capture_output=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")[-2000:]
    return result.stdout


def _name(settings: Settings) -> str:
    name = make_url(settings.validated_database_url()).database
    assert name is not None
    return name


def _snapshot(engine: Engine) -> dict[str, list[tuple[object, ...]]]:
    """Every row of every table in both schemas, in a stable order."""
    tables: dict[str, list[tuple[object, ...]]] = {}
    inspector = inspect(engine)
    with engine.connect() as connection:
        for schema in ("public", "evaluation"):
            for table in inspector.get_table_names(schema=schema):
                rows = connection.execute(
                    text(f'SELECT * FROM "{schema}"."{table}" ORDER BY 1')  # noqa: S608 - catalog names
                ).all()
                tables[f"{schema}.{table}"] = [tuple(row) for row in rows]
    return tables


async def test_a_restored_backup_recovers_durable_state(
    seeded: tuple[Settings, Engine],
    database: db.Database,
    make_scratch_database: Callable[[], AbstractContextManager[Settings]],
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(uuid.uuid4()))
    await _world(database, scope)
    processed = uuid.uuid4()
    async with database.transaction() as session:
        assert await ReceiptRepository(session).record(NORMALIZER_CONSUMER_GROUP, processed)

    dump = await asyncio.to_thread(_compose, "pg_dump", "-U", USER, "-Fc", "-d", _name(seeded[0]))
    assert dump.startswith(b"PGDMP")
    original = await asyncio.to_thread(_snapshot, seeded[1])

    with make_scratch_database() as target:
        await asyncio.to_thread(
            _compose, "pg_restore", "-U", USER, "--exit-on-error", "-d", _name(target), stdin=dump
        )
        restored_engine = create_engine(target.validated_database_url())
        try:
            restored = await asyncio.to_thread(_snapshot, restored_engine)
        finally:
            restored_engine.dispose()
        assert restored.keys() == original.keys()
        assert "public.alembic_version" in restored
        different = [name for name in original if original[name] != restored[name]]
        assert different == []
        assert restored["public.signals"]
        assert restored["public.investigations"]

        engine = db.create_engine(target)
        try:
            async with db.Database.from_engine(engine).transaction() as session:
                receipts = ReceiptRepository(session)
                # Deduplication history came back: a redelivery is still a no-op.
                assert not await receipts.record(NORMALIZER_CONSUMER_GROUP, processed)
                assert await receipts.record(NORMALIZER_CONSUMER_GROUP, uuid.uuid4())
        finally:
            await engine.dispose()
