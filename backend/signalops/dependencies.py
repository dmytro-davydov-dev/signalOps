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
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from signalops.config import Settings
from signalops.contracts.health import DependencyStatus, ReadinessResponse

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


@asynccontextmanager
async def open_resources(settings: Settings) -> AsyncIterator[Resources]:
    timeout = settings.readiness_timeout_seconds
    engine = create_async_engine(
        settings.validated_database_url(),
        pool_size=5,
        max_overflow=5,
        pool_timeout=timeout,
        pool_pre_ping=True,
        connect_args={"connect_timeout": max(1, int(timeout))},
    )
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
        )
    finally:
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
