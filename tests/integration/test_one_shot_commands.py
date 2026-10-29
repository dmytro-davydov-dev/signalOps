import uuid

import pytest
from alembic.script import ScriptDirectory
from confluent_kafka.admin import AdminClient
from sqlalchemy import create_engine, text

from signalops.config import Settings
from signalops.persistence.migrations import alembic_config, upgrade_to_head
from signalops.streaming.topics import TopicMismatchError, TopicSpec, ensure_topics

pytestmark = pytest.mark.integration


def test_migrate_reaches_head_and_is_repeatable(scratch_database: Settings) -> None:
    upgrade_to_head(scratch_database)
    upgrade_to_head(scratch_database)
    engine = create_engine(scratch_database.validated_database_url())
    with engine.connect() as conn:
        versions = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
    engine.dispose()
    head = ScriptDirectory.from_config(alembic_config(scratch_database)).get_current_head()
    assert versions == [head]


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
