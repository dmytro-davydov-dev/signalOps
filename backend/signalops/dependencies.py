"""Infrastructure clients shared by a process and their readiness checks."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol

from confluent_kafka.admin import AdminClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from signalops.adapters.ingestion import (
    DatabaseSourceLookup,
    Publisher,
    ReplaceablePublisher,
    SourceLookup,
)
from signalops.config import Settings
from signalops.contracts.health import DependencyStatus, ReadinessResponse
from signalops.persistence.database import Database, create_engine
from signalops.streaming.producer import EventProducer
from signalops.wakeups import RedisProgressNotifier

logger = logging.getLogger(__name__)


class DependencyCheck(Protocol):
    name: str

    async def check(self) -> None:
        """Return normally when the dependency is usable; raise otherwise."""
        ...


@dataclass
class PostgresCheck:
    engine: AsyncEngine
    name: str = "postgres"

    async def check(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))


@dataclass
class RedisCheck:
    client: Redis
    name: str = "redis"

    async def check(self) -> None:
        await self.client.ping()


@dataclass
class KafkaCheck:
    admin: AdminClient
    timeout_seconds: float
    name: str = "kafka"

    async def check(self) -> None:
        # list_topics blocks inside librdkafka and raises KafkaException on timeout.
        await asyncio.to_thread(self.admin.list_topics, timeout=self.timeout_seconds)


@dataclass
class Resources:
    """Clients opened by the FastAPI lifespan and closed on shutdown."""

    checks: Sequence[DependencyCheck]
    engine: AsyncEngine | None = None
    redis: Redis | None = None
    kafka_admin: AdminClient | None = field(default=None, repr=False)
    # Ingestion; the route answers 503 while either is missing.
    sources: SourceLookup | None = None
    ingest_publisher: Publisher | None = field(default=None, repr=False)
    # Evidence queries; their routes answer 503 while it is missing.
    database: Database | None = None


@asynccontextmanager
async def open_resources(settings: Settings) -> AsyncIterator[Resources]:
    timeout = settings.readiness_timeout_seconds
    # The readiness probe is bounded by its own per-check limit, not the pool timeout.
    engine = create_engine(settings)
    redis = Redis.from_url(
        str(settings.redis_url),
        socket_connect_timeout=timeout,
        socket_timeout=timeout,
    )
    kafka_admin = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "socket.connection.setup.timeout.ms": int(timeout * 1000),
        },
        logger=logging.getLogger("signalops.kafka"),
    )
    # Creating a producer does not connect; deliveries fail per record while Kafka is down.
    publisher = ReplaceablePublisher(
        lambda: EventProducer(settings, client_id="signalops-api-ingest")
    )
    # Cancellations write progress, so open streams hear of them at once.
    database = Database.from_engine(engine, on_progress=RedisProgressNotifier(redis))
    try:
        yield Resources(
            checks=[
                PostgresCheck(engine),
                RedisCheck(redis),
                KafkaCheck(kafka_admin, timeout_seconds=timeout),
            ],
            engine=engine,
            redis=redis,
            kafka_admin=kafka_admin,
            sources=DatabaseSourceLookup(database),
            ingest_publisher=publisher,
            database=database,
        )
    finally:
        # Requests have finished; nothing still queued was reported accepted to a client.
        undelivered = await asyncio.to_thread(publisher.close, timeout)
        if undelivered:
            logger.warning("ingest producer closed undelivered", extra={"count": undelivered})
        await redis.aclose()
        await engine.dispose()


async def _run_check(check: DependencyCheck, limit_seconds: float) -> DependencyStatus:
    started = time.perf_counter()
    error: str | None = None
    try:
        async with asyncio.timeout(limit_seconds):
            await check.check()
    except TimeoutError:
        error = "timeout"
    except Exception as exc:  # noqa: BLE001 - a probe reports any failure as not ready.
        error = type(exc).__name__
        logger.warning("readiness check failed", extra={"dependency": check.name, "error": error})
    latency_ms = round((time.perf_counter() - started) * 1000, 1)
    if error is None:
        return DependencyStatus(status="ok", latency_ms=latency_ms)
    return DependencyStatus(status="error", latency_ms=latency_ms, error=error)


async def check_readiness(
    checks: Sequence[DependencyCheck], limit_seconds: float
) -> ReadinessResponse:
    results = await asyncio.gather(*(_run_check(check, limit_seconds) for check in checks))
    statuses = {check.name: result for check, result in zip(checks, results, strict=True)}
    ready = all(status.status == "ok" for status in statuses.values())
    return ReadinessResponse(status="ready" if ready else "not_ready", checks=statuses)
