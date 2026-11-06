import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from signalops.api.app import create_app
from signalops.api.evidence import InvalidCursorError, InvalidQueryError, check_limit
from signalops.api.incidents import KeysetCursor, query_digest
from signalops.config import Settings
from signalops.contracts.incidents import IncidentStatusUpdate
from signalops.contracts.workflow import IncidentStatus
from signalops.dependencies import Resources
from signalops.persistence.repositories.incidents import Position

AT = datetime(2026, 11, 1, 12, 0, 0, 123456, tzinfo=UTC)
ROW = uuid.UUID("0b7fd24b-3f0c-4a2c-9dc1-2e3b5a1f4c11")


def settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": SecretStr("postgresql+psycopg://unused@localhost/unused"),
    }
    return Settings(**(values | overrides))  # type: ignore[arg-type]


def test_cursor_round_trips_microseconds() -> None:
    cursor = KeysetCursor(Position(AT, ROW), "abc")
    assert KeysetCursor.decode(cursor.encode()) == cursor


@pytest.mark.parametrize(
    "value",
    ["", "not-a-cursor", "e30", "eyJ2IjoyfQ", "eyJ2IjoxLCJwIjpbMSwieCJdLCJxIjoiYSJ9"],
)
def test_bad_cursors_are_rejected(value: str) -> None:
    with pytest.raises(InvalidCursorError):
        KeysetCursor.decode(value)


def test_digest_depends_on_every_field() -> None:
    base = {"workspace_id": ROW, "replay_scope": "live", "status": None}
    assert query_digest(base) == query_digest(dict(reversed(base.items())))
    assert query_digest(base) != query_digest(base | {"status": "open"})
    assert query_digest(base) != query_digest(base | {"replay_scope": "replay:x"})


def test_limit_ceiling_applies_to_explicit_and_default_limits() -> None:
    configured = settings(query_default_limit=150, query_max_limit=200)
    assert check_limit(None, configured, ceiling=100) == 100
    assert check_limit(100, configured, ceiling=100) == 100
    with pytest.raises(InvalidQueryError, match="limit exceeds 100"):
        check_limit(101, configured, ceiling=100)
    assert check_limit(200, configured) == 200


def test_a_lower_configured_limit_wins_over_the_ceiling() -> None:
    with pytest.raises(InvalidQueryError, match="limit exceeds 10"):
        check_limit(11, settings(query_default_limit=5, query_max_limit=10), ceiling=100)


def test_status_update_requires_a_version() -> None:
    assert IncidentStatusUpdate(status=IncidentStatus.OPEN, version=1).version == 1
    with pytest.raises(ValidationError):
        IncidentStatusUpdate.model_validate({"status": "open", "version": 0})


def test_cors_allows_the_status_change() -> None:
    @asynccontextmanager
    async def nothing(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(checks=[])

    app = create_app(settings(cors_origins=["http://ui.example"]), resource_factory=nothing)
    with TestClient(app) as client:
        response = client.options(
            f"/v1/incidents/{ROW}",
            headers={
                "Origin": "http://ui.example",
                "Access-Control-Request-Method": "PATCH",
                "Access-Control-Request-Headers": "authorization,content-type",
            },
        )
    assert response.status_code == 200
    assert "PATCH" in response.headers["access-control-allow-methods"]
