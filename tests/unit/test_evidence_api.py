import base64
import json
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from signalops.adapters.ingestion import REDACTED, SourceConfigError
from signalops.api.app import create_app
from signalops.api.credentials import CredentialStore
from signalops.api.evidence import (
    InvalidCursorError,
    InvalidQueryError,
    OutputRedaction,
    SignalCursor,
    check_limit,
    resolve_window,
)
from signalops.config import Settings
from signalops.contracts.queries import TimeWindow
from signalops.dependencies import Resources
from signalops.persistence.models import Signal, Source
from signalops.persistence.repositories.evidence import SignalPosition

WORKSPACE = uuid.UUID("82631aa1-86f5-4d37-b39f-fac74ebbdac8")
OTHER_WORKSPACE = uuid.UUID("9b3c1f4e-2d5a-4b6c-8e7f-0a1b2c3d4e5f")
SOURCE = uuid.UUID("b6c72d79-7ec9-4df7-88d0-1d4332c88808")
NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
KEY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": SecretStr("postgresql+psycopg://unused@localhost/unused"),
        "operator_tokens": {WORKSPACE: SecretStr("operator-token")},
        "ingest_tokens": {SOURCE: SecretStr("source-token")},
    }
    return Settings(**(values | overrides))


def make_client(app_settings: Settings | None = None) -> TestClient:
    @asynccontextmanager
    async def no_database(_: Settings) -> AsyncIterator[Resources]:
        yield Resources(checks=[])

    return TestClient(create_app(app_settings or settings(), resource_factory=no_database))


OPERATOR_TOKEN = "operator-token"  # noqa: S105 - test credential


def auth(token: str = OPERATOR_TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# Credentials


def _workspace(store: CredentialStore, token: str) -> uuid.UUID | None:
    match = store.operator(token)
    return None if match is None else match.principal


def test_operator_token_maps_to_its_workspace() -> None:
    store = CredentialStore({WORKSPACE: SecretStr("one"), OTHER_WORKSPACE: SecretStr("two")}, {})
    assert _workspace(store, "one") == WORKSPACE
    assert _workspace(store, "two") == OTHER_WORKSPACE
    assert _workspace(store, "three") is None
    assert _workspace(CredentialStore({}, {}), "one") is None


def test_operator_token_equal_to_an_ingest_token_is_refused() -> None:
    with pytest.raises(ValidationError, match="operator token must not equal an ingest token"):
        settings(operator_tokens={WORKSPACE: SecretStr("source-token")})


def test_query_defaults_cannot_exceed_maximums() -> None:
    with pytest.raises(ValidationError, match="query_default_limit"):
        settings(query_default_limit=300, query_max_limit=200)
    with pytest.raises(ValidationError, match="query_default_window_seconds"):
        settings(query_default_window_seconds=7200, query_max_window_seconds=3600)


@pytest.mark.parametrize(
    "path", ["/v1/signals", "/v1/traces/abc123", "/v1/sources/quality"], ids=lambda p: p
)
@pytest.mark.parametrize(
    "headers",
    [{}, auth("wrong"), auth("source-token"), {"Authorization": "Basic b3BlcmF0b3I="}],
    ids=["missing", "wrong", "source-token", "basic"],
)
def test_routes_require_an_operator_token(path: str, headers: dict[str, str]) -> None:
    with make_client() as client:
        response = client.get(path, headers=headers)
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_no_operator_tokens_configured_rejects_every_caller() -> None:
    with make_client(settings(operator_tokens={})) as client:
        response = client.get("/v1/signals", headers=auth())
    assert response.status_code == 401


def test_operator_token_cannot_ingest() -> None:
    with make_client() as client:
        response = client.post(f"/v1/ingest/{SOURCE}", headers=auth(), json={"records": []})
    assert response.status_code == 401


# Bounds, checked before any database access


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/signals", {"limit": 201}),
        ("/v1/signals", {"limit": 0}),
        ("/v1/traces/abc123", {"limit": 201}),
        ("/v1/signals", {"since": "2026-10-01T00:00:00Z", "until": "2026-10-30T00:00:00Z"}),
        ("/v1/sources/quality", {"since": "2026-10-01T00:00:00Z"}),
        ("/v1/signals", {"since": "2026-10-30T00:00:00Z", "until": "2026-10-29T00:00:00Z"}),
        ("/v1/signals", {"since": "2026-10-30T00:00:00"}),
        ("/v1/signals", {"since": "1761825600"}),
        ("/v1/signals", {"replay_scope": "Live"}),
        ("/v1/signals", {"replay_scope": ""}),
        ("/v1/signals", {"kind": "metric"}),
        ("/v1/signals", {"quality_status": "unknown"}),
        ("/v1/signals", {"source_id": "not-a-uuid"}),
        ("/v1/traces/bad%20trace", {}),
        ("/v1/traces/" + "a" * 129, {}),
    ],
)
def test_out_of_bounds_queries_are_422(path: str, params: dict[str, Any]) -> None:
    with make_client() as client:
        response = client.get(path, params=params, headers=auth())
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("cursor", ["garbage!", "eyJ2IjoxfQ", base64.b64encode(b"[1]").decode()])
def test_malformed_cursor_is_400(cursor: str) -> None:
    with make_client() as client:
        response = client.get("/v1/signals", params={"cursor": cursor}, headers=auth())
    assert response.status_code == 400
    assert response.json() == {"detail": "invalid cursor"}


def test_valid_query_without_a_database_is_503() -> None:
    with make_client() as client:
        response = client.get("/v1/signals", headers=auth())
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"


# Window, limit, and cursor helpers


def test_window_defaults_and_bounds() -> None:
    default, maximum = timedelta(hours=1), timedelta(days=1)
    window = resolve_window(None, None, now=NOW, default=default, maximum=maximum)
    assert window == TimeWindow(since=NOW - default, until=NOW)
    since = NOW - timedelta(hours=5)
    assert resolve_window(since, None, now=NOW, default=default, maximum=maximum).until == NOW
    until = NOW - timedelta(days=3)
    window = resolve_window(None, until, now=NOW, default=default, maximum=maximum)
    assert window == TimeWindow(since=until - default, until=until)
    with pytest.raises(InvalidQueryError, match="window exceeds 86400 seconds"):
        resolve_window(NOW - timedelta(days=2), None, now=NOW, default=default, maximum=maximum)
    with pytest.raises(InvalidQueryError, match="earlier"):
        resolve_window(NOW, NOW, now=NOW, default=default, maximum=maximum)


def test_limit_defaults_and_maximum() -> None:
    app_settings = settings(query_default_limit=10, query_max_limit=20)
    assert check_limit(None, app_settings) == 10
    assert check_limit(20, app_settings) == 20
    with pytest.raises(InvalidQueryError, match="limit exceeds 20"):
        check_limit(21, app_settings)


def test_cursor_round_trips_and_fits_the_key_pattern() -> None:
    cursor = SignalCursor(
        window=TimeWindow(
            since=NOW - timedelta(days=7, microseconds=123_456),
            until=NOW + timedelta(microseconds=1),
        ),
        position=SignalPosition(NOW - timedelta(microseconds=999_999), uuid.uuid4()),
        query_digest="0" * 32,
    )
    encoded = cursor.encode()
    assert KEY_PATTERN.fullmatch(encoded)
    assert len(encoded) <= 256
    assert SignalCursor.decode(encoded) == cursor


GOOD_WINDOW = [1_761_782_400_000_000, 1_761_786_000_000_000]


@pytest.mark.parametrize(
    "document",
    [
        {"v": 2, "w": GOOD_WINDOW, "p": [GOOD_WINDOW[0], uuid.uuid4().hex], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW[::-1], "p": [GOOD_WINDOW[0], uuid.uuid4().hex], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW},
        {"v": 1, "w": GOOD_WINDOW, "p": ["2026-10-30T00:30:00+00:00", uuid.uuid4().hex], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW, "p": [True, uuid.uuid4().hex], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW, "p": [10**20, uuid.uuid4().hex], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW, "p": [GOOD_WINDOW[0], "not-a-uuid"], "q": "a"},
        {"v": 1, "w": GOOD_WINDOW, "p": [GOOD_WINDOW[0], uuid.uuid4().hex], "q": 7},
    ],
    ids=[
        "version",
        "reversed-window",
        "missing-position",
        "string-time",
        "bool-time",
        "overflow",
        "bad-id",
        "digest-type",
    ],
)
def test_cursor_decode_rejects_bad_documents(document: dict[str, Any]) -> None:
    encoded = base64.urlsafe_b64encode(json.dumps(document).encode()).decode().rstrip("=")
    with pytest.raises(InvalidCursorError):
        SignalCursor.decode(encoded)


# Output redaction


def _signal_row(attributes: dict[str, Any], source_id: uuid.UUID = SOURCE) -> Signal:
    return cast(
        Signal,
        SimpleNamespace(
            id=uuid.uuid4(),
            workspace_id=WORKSPACE,
            source_id=source_id,
            service_id=None,
            source_event_id="e-1",
            replay_scope="live",
            kind="log",
            occurred_at=NOW,
            ingested_at=NOW,
            severity="error",
            message="password=hunter2 in free text",
            trace_id=None,
            attributes=attributes,
            quality_status="degraded",
            quality_flags=["missing_service"],
        ),
    )


def _source(config: dict[str, Any]) -> Source:
    return cast(Source, SimpleNamespace(id=SOURCE, config=config))


def test_output_redaction_applies_process_and_source_fields() -> None:
    redaction = OutputRedaction(["password"], [_source({"redact_fields": ["Customer-Email"]})])
    signal = redaction.signal(
        _signal_row(
            {
                "password": "hunter2",
                "source": {"customer_email": "a@example.com", "items": [{"PASSWORD": "x"}]},
                "status": 500,
            }
        )
    )
    assert signal.attributes == {
        "password": REDACTED,
        "source": {"customer_email": REDACTED, "items": [{"PASSWORD": REDACTED}]},
        "status": 500,
    }
    # Free text gets the tools' masking (P11-T02): sensitive key=value pairs, both lists.
    assert "hunter2" not in signal.message
    assert signal.message.startswith(f"password={REDACTED}")


def test_output_redaction_masks_source_fields_and_bearer_tokens_in_messages() -> None:
    redaction = OutputRedaction(["password"], [_source({"redact_fields": ["api_key"]})])
    row = _signal_row({})
    row.message = "retry with api_key=abc123 and Authorization: Bearer eyJ.x-y, user=bob"
    message = redaction.signal(row).message
    assert "abc123" not in message
    assert "eyJ.x-y" not in message
    assert "user=bob" in message


def test_output_redaction_for_an_unlisted_source_uses_process_fields() -> None:
    redaction = OutputRedaction(["password"], [])
    signal = redaction.signal(_signal_row({"password": "x", "customer_email": "y"}, uuid.uuid4()))
    assert signal.attributes == {"password": REDACTED, "customer_email": "y"}


def test_output_redaction_fails_closed_on_invalid_source_config() -> None:
    with pytest.raises(SourceConfigError):
        OutputRedaction(["password"], [_source({"redact_fields": "customer_email"})])
