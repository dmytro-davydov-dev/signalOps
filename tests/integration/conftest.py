import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractContextManager, contextmanager

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, make_url, text

from signalops.config import Settings
from signalops.persistence import database as db
from signalops.persistence.migrations import upgrade_to_head
from signalops.persistence.seed import seed_demo
from tests.integration.schema_support import World


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("SIGNALOPS_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set SIGNALOPS_INTEGRATION=1 with Compose infrastructure up")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def infra_settings() -> Settings:
    """Settings for the Compose stack reached through its loopback host ports."""
    user = os.environ.get("POSTGRES_USER", "signalops")
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ.get("POSTGRES_DB", "signalops")
    pg_port = os.environ.get("POSTGRES_HOST_PORT", "55432")
    redis_port = os.environ.get("REDIS_HOST_PORT", "56379")
    kafka_port = os.environ.get("KAFKA_HOST_PORT", "19094")
    return Settings(
        database_url=SecretStr(f"postgresql+psycopg://{user}:{password}@localhost:{pg_port}/{db}"),
        redis_url=f"redis://localhost:{redis_port}/0",  # type: ignore[arg-type]
        kafka_bootstrap_servers=f"localhost:{kafka_port}",
        readiness_timeout_seconds=3,
    )


@contextmanager
def scratch_database_for(settings: Settings) -> Iterator[Settings]:
    """A throwaway database so schema tests never touch the dev schema."""
    url = make_url(settings.validated_database_url())
    name = f"signalops_test_{uuid.uuid4().hex[:12]}"
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        scratch_url = url.set(database=name).render_as_string(hide_password=False)
        yield settings.model_copy(update={"database_url": SecretStr(scratch_url)})
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


ScratchDatabase = Callable[[], AbstractContextManager[Settings]]


@pytest.fixture(scope="session")
def make_scratch_database(infra_settings: Settings) -> ScratchDatabase:
    return lambda: scratch_database_for(infra_settings)


@pytest.fixture
def scratch_database(infra_settings: Settings) -> Iterator[Settings]:
    with scratch_database_for(infra_settings) as settings:
        yield settings


@pytest.fixture(scope="module")
def migrated(
    make_scratch_database: Callable[[], AbstractContextManager[Settings]],
) -> Iterator[tuple[Settings, Engine]]:
    with make_scratch_database() as settings:
        upgrade_to_head(settings)
        engine = create_engine(settings.validated_database_url())
        try:
            yield settings, engine
        finally:
            engine.dispose()


@pytest.fixture
def engine(migrated: tuple[Settings, Engine]) -> Engine:
    return migrated[1]


@pytest.fixture
def world(engine: Engine) -> World:
    return World(engine)


@pytest.fixture(scope="module")
def seeded(migrated: tuple[Settings, Engine]) -> tuple[Settings, Engine]:
    async def seed() -> None:
        engine = db.create_engine(migrated[0])
        try:
            await seed_demo(db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    asyncio.run(seed())
    return migrated


@pytest.fixture
async def database(seeded: tuple[Settings, Engine]) -> AsyncIterator[db.Database]:
    engine = db.create_engine(seeded[0])
    try:
        yield db.Database.from_engine(engine)
    finally:
        await engine.dispose()


@pytest.fixture
def run_id() -> uuid.UUID:
    """A replay run per test: its scope isolates the test's windows."""
    return uuid.uuid4()
