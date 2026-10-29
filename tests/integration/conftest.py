import os

import pytest
from pydantic import SecretStr

from signalops.config import Settings


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("SIGNALOPS_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set SIGNALOPS_INTEGRATION=1 with Compose infrastructure up")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
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
