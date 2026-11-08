"""Every route's credential boundary, enumerated from the app itself (P11-T02).

A route added later without operator authentication fails here, because the
test walks `app.routes` rather than a hand-kept list.
"""

import json
import re
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from signalops.api.app import create_app
from signalops.api.credentials import token_digest
from signalops.config import Settings
from signalops.dependencies import Resources

WORKSPACE = uuid.UUID("82631aa1-86f5-4d37-b39f-fac74ebbdac8")
SOURCE = uuid.UUID("b6c72d79-7ec9-4df7-88d0-1d4332c88808")
OPERATOR = "operator-token"
SOURCE_TOKEN = "source-token"  # noqa: S105 - test credential
EXPIRED = "expired-operator-token"

# Routes that need no credential: probes only.
PUBLIC = {"/health/live", "/health/ready"}
INGEST = "/v1/ingest/{source_id}"


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    credentials = tmp_path / "credentials.json"
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    credentials.write_text(
        json.dumps(
            {
                "operators": [
                    {
                        "id": "expired",
                        "workspace_id": str(WORKSPACE),
                        "sha256": token_digest(EXPIRED),
                        "not_after": past,
                    }
                ]
            }
        )
    )
    settings = Settings(
        database_url=SecretStr("postgresql+psycopg://unused@localhost/unused"),
        operator_tokens={WORKSPACE: SecretStr(OPERATOR)},
        ingest_tokens={SOURCE: SecretStr(SOURCE_TOKEN)},
        credentials_file=credentials,
    )

    @asynccontextmanager
    async def no_resources(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(checks=[])

    with TestClient(create_app(settings, resource_factory=no_resources)) as test_client:
        yield test_client


def _routes(client: TestClient) -> list[tuple[str, str]]:
    """Every method and path the app serves, from its OpenAPI document."""
    paths = client.app.openapi()["paths"]  # type: ignore[attr-defined]
    return [(method.upper(), path) for path, item in paths.items() for method in item]


def _concrete(path: str) -> str:
    return re.sub(r"\{[^}]+\}", str(uuid.uuid4()), path)


def _call(client: TestClient, method: str, path: str, token: str | None) -> int:
    headers = {"Idempotency-Key": "k-1"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    url = _concrete(path) if path != INGEST else f"/v1/ingest/{SOURCE}"
    return client.request(method, url, headers=headers, json={}).status_code


def test_every_route_is_public_ingest_or_operator(client: TestClient) -> None:
    paths = {path for _, path in _routes(client)}
    operator = paths - PUBLIC - {INGEST}
    assert INGEST in paths
    # The evaluation, evidence, incident, and investigation routes are all here.
    assert any(path.startswith("/v1/evaluation-runs") for path in operator)
    assert "/v1/signals" in operator
    assert "/v1/incidents/{incident_id}/investigations" in operator


@pytest.mark.parametrize("token", [None, SOURCE_TOKEN, EXPIRED, "wrong"])
def test_operator_routes_refuse_everyone_but_operators(
    client: TestClient, token: str | None
) -> None:
    refused = {
        (method, path): _call(client, method, path, token)
        for method, path in _routes(client)
        if path not in PUBLIC and path != INGEST
    }
    assert refused
    assert set(refused.values()) == {401}, {k: v for k, v in refused.items() if v != 401}


@pytest.mark.parametrize("token", [None, OPERATOR, EXPIRED])
def test_ingestion_refuses_operator_credentials(client: TestClient, token: str | None) -> None:
    assert _call(client, "POST", INGEST, token) == 401


def test_a_valid_operator_passes_authentication(client: TestClient) -> None:
    # No database is configured, so an authenticated read stops at 503, past the credential check.
    assert _call(client, "GET", "/v1/incidents", OPERATOR) == 503


def test_compose_publishes_ports_on_loopback_only() -> None:
    compose = (Path(__file__).parents[2] / "compose.yaml").read_text()
    published = re.findall(r'^\s+- "([^"]*:\d+)"\s*$', compose, flags=re.MULTILINE)
    assert published
    assert all(entry.startswith("127.0.0.1:") for entry in published), published


def test_bodies_over_the_limit_are_refused_before_any_route_runs(client: TestClient) -> None:
    headers = {"Authorization": f"Bearer {OPERATOR}", "Idempotency-Key": "k-2"}
    path = f"/v1/incidents/{uuid.uuid4()}/investigations"
    big = client.post(path, headers=headers, content=b"{" + b" " * (64 * 1024) + b"}")
    assert big.status_code == 413
    # Unauthenticated callers get 413 too: nothing past the limit is read.
    assert client.post(path, content=b"x" * (64 * 1024 + 1)).status_code == 413
    # Ingestion has its own, larger limit.
    small_batch = client.post(
        f"/v1/ingest/{SOURCE}",
        headers={"Authorization": f"Bearer {SOURCE_TOKEN}"},
        content=b'{"records": [' + b" " * (64 * 1024) + b"]}",
    )
    assert small_batch.status_code != 413


def test_each_request_writes_an_audit_line_with_the_credential_id(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level("INFO", logger="signalops.api.request_ids"):
        client.get("/v1/incidents", headers={"Authorization": f"Bearer {OPERATOR}"})
        client.get("/v1/incidents")
    lines = [r for r in caplog.records if r.getMessage() == "api request"]
    assert [(r.status, r.credential_id) for r in lines] == [  # type: ignore[attr-defined]
        (503, f"env:operator:{WORKSPACE}"),
        (401, None),
    ]
    assert all(OPERATOR not in str(vars(r)) for r in lines)


@pytest.fixture(autouse=True)
def _no_sourced_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # pydantic-settings merges dict settings with the environment, so variables
    # sourced from `.env` for integration runs would leak into these checks.
    for name in (
        "SIGNALOPS_OPERATOR_TOKENS",
        "SIGNALOPS_INGEST_TOKENS",
        "SIGNALOPS_CORS_ORIGINS",
        "SIGNALOPS_ENVIRONMENT",
        "SIGNALOPS_CREDENTIALS_FILE",
    ):
        monkeypatch.delenv(name, raising=False)


def _settings(**values: object) -> Settings:
    return Settings(database_url=SecretStr("postgresql://u:p@localhost/db"), **values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "origin", ["*", "http://example.com/", "https://example.com/app", "example.com", "null"]
)
def test_cors_origins_must_be_exact(origin: str) -> None:
    with pytest.raises(ValueError, match="cors origin"):
        _settings(cors_origins=[origin])


def test_exact_cors_origins_are_accepted() -> None:
    assert _settings(cors_origins=["http://localhost:5173", "https://ops.example.com"]).cors_origins


def test_production_takes_credentials_only_from_the_file(tmp_path: Path) -> None:
    file = tmp_path / "credentials.json"
    with pytest.raises(ValueError, match="refuses"):
        _settings(environment="production", operator_tokens={WORKSPACE: SecretStr("t")})
    with pytest.raises(ValueError, match="CREDENTIALS_FILE"):
        _settings(environment="production")
    with pytest.raises(ValueError, match="https"):
        _settings(environment="production", credentials_file=file, cors_origins=["http://a.b"])
    assert _settings(environment="production", credentials_file=file).credentials_file == file


def test_secret_files_supply_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from signalops.config import get_settings

    (tmp_path / "SIGNALOPS_DATABASE_URL").write_text("postgresql://u:from-file@db/signalops")
    monkeypatch.delenv("SIGNALOPS_DATABASE_URL", raising=False)
    monkeypatch.setenv("SIGNALOPS_SECRETS_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        assert "from-file" in get_settings().database_url.get_secret_value()
    finally:
        get_settings.cache_clear()
