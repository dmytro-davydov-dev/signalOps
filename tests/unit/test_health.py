import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi.testclient import TestClient
from pydantic import SecretStr

from signalops.api.app import create_app
from signalops.config import Settings
from signalops.dependencies import DependencyCheck, Resources


@dataclass
class FakeCheck:
    name: str
    error: Exception | None = None
    delay_seconds: float = 0.0

    async def check(self) -> None:
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error


def make_client(checks: Sequence[DependencyCheck]) -> TestClient:
    settings = Settings(
        database_url=SecretStr("postgresql+psycopg://unused@localhost/unused"),
        readiness_timeout_seconds=0.2,
    )

    @asynccontextmanager
    async def fake_resources(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(checks=checks)

    return TestClient(create_app(settings, resource_factory=fake_resources))


def test_liveness_does_not_check_dependencies() -> None:
    with make_client([FakeCheck("postgres", error=ConnectionError())]) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_when_every_dependency_answers() -> None:
    with make_client([FakeCheck("postgres"), FakeCheck("redis"), FakeCheck("kafka")]) as client:
        response = client.get("/health/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert {name: check["status"] for name, check in body["checks"].items()} == {
        "postgres": "ok",
        "redis": "ok",
        "kafka": "ok",
    }


def test_failing_dependency_makes_readiness_503_with_error_category() -> None:
    checks = [
        FakeCheck("postgres"),
        FakeCheck("redis", error=ConnectionError("redis://:secret@redis:6379 refused")),
    ]
    with make_client(checks) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["postgres"]["status"] == "ok"
    assert body["checks"]["redis"] == {
        "status": "error",
        "latency_ms": body["checks"]["redis"]["latency_ms"],
        "error": "ConnectionError",
    }
    assert "secret" not in response.text


def test_hanging_dependency_times_out_instead_of_blocking() -> None:
    with make_client([FakeCheck("kafka", delay_seconds=5)]) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["checks"]["kafka"]["error"] == "timeout"
