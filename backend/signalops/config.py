"""Process configuration loaded from environment variables."""

import uuid
from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from signalops.detection.rules import DEFAULT_RULES, DetectionRule, check_unique_rule_ids

DEFAULT_MODELS = {"fake": "fake-1", "anthropic": "claude-opus-5-5"}


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
    # The evaluator's connection (role `signalops_evaluator`), which alone can read
    # ground truth. Unset: evaluation commands and API routes are unavailable.
    evaluation_database_url: SecretStr | None = None
    # Login roles `signalops migrate` creates or updates after the upgrade, one per
    # group role from migration 0017. Unset password: that role is left alone.
    db_runtime_user: str = Field(default="signalops_app", pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    db_runtime_password: SecretStr | None = None
    db_evaluator_user: str = Field(default="signalops_eval", pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    db_evaluator_password: SecretStr | None = None
    redis_url: RedisDsn = RedisDsn("redis://localhost:56379/0")
    kafka_bootstrap_servers: str = "localhost:19094"
    # Upper bound on one message's send, including librdkafka's internal retries.
    # A message not acknowledged by then is reported to the caller as failed.
    kafka_delivery_timeout_ms: int = Field(default=30_000, ge=1_000, le=300_000)
    # Rows the outbox publisher reads per poll, and its wait after an empty or
    # fully failed poll.
    outbox_batch_size: int = Field(default=100, ge=1, le=1_000)
    outbox_idle_interval_ms: int = Field(default=500, ge=50, le=60_000)
    # Transient failures: attempts per record (including the first) before a
    # consumer stops, and the capped exponential backoff between them. The
    # outbox applies the same backoff but never gives up on a transient failure.
    retry_max_attempts: int = Field(default=5, ge=1, le=20)
    retry_base_delay_ms: int = Field(default=1_000, ge=10, le=60_000)
    retry_max_delay_ms: int = Field(default=30_000, ge=10, le=300_000)
    # How often consumers and the outbox publisher log counts, lag, and backlog.
    streaming_stats_interval_ms: int = Field(default=30_000, ge=1_000, le=3_600_000)

    api_host: str = "0.0.0.0"  # noqa: S104 - container listener; Compose publishes on loopback.
    api_port: int = Field(default=8000, ge=1, le=65535)
    cors_origins: list[str] = Field(default_factory=list)

    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=30)

    # Ingestion bearer token per source ID, as a JSON object. A source without
    # a token cannot ingest.
    ingest_tokens: dict[uuid.UUID, SecretStr] = Field(default_factory=dict)
    ingest_max_records: int = Field(default=500, ge=1, le=10_000)
    ingest_max_request_bytes: int = Field(default=4 * 1024 * 1024, ge=64 * 1024, le=64 * 1024**2)
    # Coarse bounds on occurred_at relative to receipt. Records inside them but
    # skewed or late are flagged by the normalizer, not rejected here.
    ingest_max_future_seconds: int = Field(default=24 * 60 * 60, ge=0, le=7 * 24 * 60 * 60)
    ingest_max_age_seconds: int = Field(default=30 * 24 * 60 * 60, ge=60, le=365 * 24 * 60 * 60)
    # Operator bearer token per workspace ID, as a JSON object. The token's
    # workspace bounds every evidence query. No token: operator APIs answer 401.
    # Interim mechanism until P11-T02 (ADR-014); must differ from every ingest token.
    operator_tokens: dict[uuid.UUID, SecretStr] = Field(default_factory=dict)
    # Evidence query bounds: page size, and the time window when the caller
    # gives none and at most.
    query_default_limit: int = Field(default=50, ge=1, le=1_000)
    query_max_limit: int = Field(default=200, ge=1, le=1_000)
    query_default_window_seconds: int = Field(default=60 * 60, ge=60, le=30 * 24 * 60 * 60)
    query_max_window_seconds: int = Field(default=7 * 24 * 60 * 60, ge=60, le=30 * 24 * 60 * 60)
    # Normalizer timing flags (live scope only): occurred_at more than this
    # ahead of ingestion is clock skew (degraded); more than this behind is a
    # late arrival (noted, still accepted).
    normalizer_max_clock_skew_seconds: int = Field(default=300, ge=0, le=24 * 60 * 60)
    normalizer_late_arrival_seconds: int = Field(default=15 * 60, ge=1, le=30 * 24 * 60 * 60)
    # Detector tumbling window length, and its rules as a JSON array of
    # `DetectionRule` objects (replaces the defaults entirely).
    detection_window_seconds: int = Field(default=60, ge=10, le=60 * 60)
    detection_rules: tuple[DetectionRule, ...] = Field(default=DEFAULT_RULES, max_length=64)
    # A sample counts while its clock is within this long after its window ends.
    detection_allowed_lateness_seconds: int = Field(default=30, ge=0, le=60 * 60)
    # How often the closer looks for live windows past their deadline.
    detection_closer_interval_ms: int = Field(default=5000, ge=100, le=60_000)
    # A breach joins an incident seen within this long; a later one starts a new incident.
    incident_correlation_gap_seconds: int = Field(default=600, ge=10, le=24 * 60 * 60)
    # Breach-free time, shown by a sufficient window of every service, that resolves an incident.
    incident_recovery_seconds: int = Field(default=300, ge=10, le=24 * 60 * 60)
    # The most one investigation may use; a request can only lower these.
    investigation_budget_microusd: int = Field(default=3_000_000, ge=1, le=1_000_000_000)
    # Workflow nodes plus model turns; `v3` has 8 nodes and splits the rest among its roles.
    investigation_max_steps: int = Field(default=40, ge=1, le=200)
    investigation_deadline_seconds: int = Field(default=600, ge=60, le=24 * 60 * 60)
    # A new live incident requests one investigation.
    investigation_auto_request: bool = True
    # The workflow new investigations run: `v3` multi-agent, or `v2` single agent.
    investigation_workflow_version: Literal["v2", "v3"] = "v3"
    # Replies an agent may have rejected for shape or support before it abstains.
    agent_max_repairs: int = Field(default=2, ge=0, le=5)
    # Evidence tool calls in one agent step; each result adds to every later prompt.
    agent_max_tool_calls: int = Field(default=10, ge=1, le=64)
    # One evidence tool read; a slower one returns a timeout to the agent.
    tool_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    # How long Redis keeps a tool result.
    tool_cache_seconds: int = Field(default=60, ge=30, le=120)
    # A progress stream reads PostgreSQL at least this often, wake-up or not.
    sse_catchup_seconds: float = Field(default=5.0, ge=0.5, le=60)
    # A progress stream closes after this long; the client reconnects with Last-Event-ID.
    sse_max_stream_seconds: int = Field(default=300, ge=10, le=3600)
    # Open progress streams per API process; one more answers 503.
    sse_max_streams: int = Field(default=200, ge=1, le=10_000)
    # Jobs one worker process runs at once.
    worker_job_concurrency: int = Field(default=2, ge=1, le=32)
    # Specialist branches running at once across all of a worker process's jobs.
    agent_global_branches: int = Field(default=4, ge=1, le=64)
    # A worker that stops heartbeating loses its job this long after the last beat.
    job_lease_seconds: int = Field(default=30, ge=3, le=600)
    # Worker wait after finding no claimable job.
    job_poll_interval_ms: int = Field(default=1000, ge=50, le=60_000)
    # An observed dependency stays valid this long after each observation.
    graph_dependency_ttl_seconds: int = Field(
        default=7 * 24 * 60 * 60, ge=60, le=365 * 24 * 60 * 60
    )
    # Payload keys whose values are replaced before publishing, at any depth.
    # Matched case-insensitively with "-" and " " read as "_". A source's
    # config may add more under "redact_fields".
    ingest_redact_fields: list[str] = Field(
        default_factory=lambda: [
            "access_token",
            "api_key",
            "apikey",
            "authorization",
            "card_number",
            "cookie",
            "credit_card",
            "passwd",
            "password",
            "private_key",
            "refresh_token",
            "secret",
            "set_cookie",
            "ssn",
            "token",
        ]
    )

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

    # `fake` needs no key and no network, and is the default everywhere. `anthropic`
    # makes paid calls and needs a key.
    model_provider: Literal["fake", "anthropic"] = "fake"
    model_api_key: SecretStr | None = None
    # Unset or empty selects the provider's default model.
    model_name: str | None = None
    # Fake provider only: a pause before each reply, so a browser test can watch a
    # running investigation. Ignored by real providers.
    fake_model_delay_seconds: float = Field(default=0.0, ge=0, le=30)
    # One provider call, whatever it is doing; a later retry is a new call.
    model_request_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    # Most tokens one model reply may use, including thinking.
    model_max_output_tokens: int = Field(default=8_000, ge=256, le=64_000)
    model_effort: Literal["low", "medium", "high"] = "high"
    # Calls per request, including the first; each is reserved and possibly billed.
    model_max_attempts: int = Field(default=3, ge=1, le=10)
    # Provider limits per process group, enforced in Redis.
    model_requests_per_minute: int = Field(default=50, ge=1, le=100_000)
    model_tokens_per_minute: int = Field(default=200_000, ge=1, le=100_000_000)
    # The longest a call pauses for a full window or an unreachable Redis.
    model_limit_wait_seconds: float = Field(default=120.0, gt=0, le=600)

    @model_validator(mode="after")
    def _check_query_bounds(self) -> Self:
        if self.query_default_limit > self.query_max_limit:
            raise ValueError("query_default_limit must not exceed query_max_limit")
        if self.query_default_window_seconds > self.query_max_window_seconds:
            raise ValueError(
                "query_default_window_seconds must not exceed query_max_window_seconds"
            )
        return self

    @model_validator(mode="after")
    def _check_detection_rules(self) -> Self:
        check_unique_rule_ids(self.detection_rules)
        return self

    @model_validator(mode="after")
    def _check_incident_timing(self) -> Self:
        recovery = self.incident_recovery_seconds
        if not self.detection_window_seconds <= recovery <= self.incident_correlation_gap_seconds:
            raise ValueError(
                "incident recovery must be between the detection window and the correlation gap"
            )
        return self

    @model_validator(mode="after")
    def _check_credentials_disjoint(self) -> Self:
        # A source token that also opened operator APIs would let a source read evidence.
        ingest = {token.get_secret_value() for token in self.ingest_tokens.values()}
        if any(token.get_secret_value() in ingest for token in self.operator_tokens.values()):
            raise ValueError("an operator token must not equal an ingest token")
        return self

    @model_validator(mode="after")
    def _check_model_provider(self) -> Self:
        key = self.model_api_key
        if self.model_provider == "anthropic" and (key is None or not key.get_secret_value()):
            raise ValueError("model_provider anthropic needs model_api_key")
        return self

    def resolved_model(self) -> str:
        if self.model_name:
            return self.model_name
        return DEFAULT_MODELS[self.model_provider]

    def validated_database_url(self) -> str:
        """Return the database URL after checking it is a PostgreSQL DSN."""
        raw = self.database_url.get_secret_value()
        PostgresDsn(raw)
        return raw

    def evaluator_settings(self) -> "Settings":
        """These settings with the evaluator's connection as the database URL."""
        if self.evaluation_database_url is None:
            raise ValueError("SIGNALOPS_EVALUATION_DATABASE_URL is not set")
        PostgresDsn(self.evaluation_database_url.get_secret_value())
        return self.model_copy(update={"database_url": self.evaluation_database_url})


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # values come from the environment
