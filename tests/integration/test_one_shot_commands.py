import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka.admin import AdminClient
from pydantic import SecretStr
from sqlalchemy import create_engine, make_url, text

from signalops.config import Settings
from signalops.persistence.migrations import upgrade_to_head
from signalops.streaming.topics import TopicMismatchError, TopicSpec, ensure_topics

pytestmark = pytest.mark.integration


@pytest.fixture
def scratch_database(infra_settings: Settings) -> Iterator[Settings]:
    """A throwaway database so migration tests never touch the dev schema."""
    url = make_url(infra_settings.validated_database_url())
    name = f"signalops_test_{uuid.uuid4().hex[:12]}"
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        scratch_url = url.set(database=name).render_as_string(hide_password=False)
        yield infra_settings.model_copy(update={"database_url": SecretStr(scratch_url)})
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def test_migrate_reaches_head_and_is_repeatable(scratch_database: Settings) -> None:
    upgrade_to_head(scratch_database)
    upgrade_to_head(scratch_database)
    engine = create_engine(scratch_database.validated_database_url())
    with engine.connect() as conn:
        versions = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
    engine.dispose()
    assert versions == ["0001_baseline"]


def test_ensure_topics_creates_then_verifies_idempotently(infra_settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": infra_settings.kafka_bootstrap_servers})
    suffix = uuid.uuid4().hex[:8]
    specs = (
        TopicSpec(f"test.{suffix}.a", partitions=3, retention_ms=60_000),
        TopicSpec(f"test.{suffix}.b", partitions=1, retention_ms=60_000),
    )
    try:
        assert sorted(ensure_topics(admin, specs)) == sorted(spec.name for spec in specs)
        assert ensure_topics(admin, specs) == []
        partitions = admin.list_topics(timeout=10).topics[specs[0].name].partitions
        assert len(partitions) == 3

        wrong = (TopicSpec(specs[0].name, partitions=5, retention_ms=60_000),)
        with pytest.raises(TopicMismatchError, match="has 3 partitions, expected 5"):
            ensure_topics(admin, wrong)
    finally:
        for future in admin.delete_topics([spec.name for spec in specs]).values():
            future.result()
