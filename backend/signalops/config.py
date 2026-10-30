"""Process configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings shared by the API and worker processes.

    Every variable uses the ``SIGNALOPS_`` prefix, for example
    ``SIGNALOPS_DATABASE_URL``. ``database_url`` has no default because it
    carries a credential.
    """

    model_config = SettingsConfigDict(env_prefix="SIGNALOPS_", extra="ignore")

    environment: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    database_url: SecretStr
    redis_url: RedisDsn = RedisDsn("redis://localhost:56379/0")
    kafka_bootstrap_servers: str = "localhost:19094"

    api_host: str = "0.0.0.0"  # noqa: S104 - container listener; Compose publishes on loopback.
    api_port: int = Field(default=8000, ge=1, le=65535)
    cors_origins: list[str] = Field(default_factory=list)

    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=30)

    # Per-process connection pool. Each process holds at most pool_size + max_overflow
    # connections; size the sum across replicas below PostgreSQL's max_connections.
    db_pool_size: int = Field(default=5, ge=1, le=50)
    db_max_overflow: int = Field(default=5, ge=0, le=50)
    db_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    db_connect_timeout_seconds: int = Field(default=5, ge=1, le=60)
    # Server-side limits applied to every session; they bound runaway queries and lock waits.
    db_statement_timeout_ms: int = Field(default=15_000, ge=100, le=600_000)
    db_lock_timeout_ms: int = Field(default=5_000, ge=100, le=600_000)
    db_idle_in_transaction_timeout_ms: int = Field(default=30_000, ge=1_000, le=600_000)

    # Relative to the working directory: the repository root locally, /app in the image.
    migrations_path: str = "migrations"

    # Only the deterministic fake provider exists until P07-T01.
    model_provider: Literal["fake"] = "fake"

    def validated_database_url(self) -> str:
        """Return the database URL after checking it is a PostgreSQL DSN."""
        raw = self.database_url.get_secret_value()
        PostgresDsn(raw)
        return raw


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # values come from the environment
