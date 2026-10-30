"""Evidence queries through the real app against a migrated scratch database.

Signals are inserted with the repository the normalizer uses, in two
workspaces and two replay scopes, so every query can be checked for what it
must not return as well as what it must.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, text

from signalops.adapters.ingestion import REDACTED
from signalops.api.app import create_app
from signalops.config import Settings
from signalops.contracts.evidence import (
    NormalizedSignal,
    QualityFlag,
    QualityStatus,
    Severity,
    SignalKind,
    SourceKind,
)
from signalops.dependencies import Resources
from signalops.persistence.database import Database, create_engine
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID, seed_demo

pytestmark = pytest.mark.integration

LOGS, TRACES, DEPLOYMENTS = (source.id for source in DEMO_SOURCES)
CHECKOUT, PAYMENT = DEMO_SERVICES[0].id, DEMO_SERVICES[1].id
OTHER_WORKSPACE = uuid.UUID("9b3c1f4e-2d5a-4b6c-8e7f-0a1b2c3d4e5f")
OTHER_SOURCE = uuid.UUID("4c1d2e3f-5a6b-4c7d-8e9f-a0b1c2d3e4f5")
TOKEN, OTHER_TOKEN = "operator-demo", "operator-other"
REPLAY = "replay:7d4c0e8a-1111-4a2b-9c3d-000000000001"
SHARED_TRACE = "trace-shared-1"
# Fixed and recent: inside the ingestion-time window of the quality summary.
BASE = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=30)


def signal(
    source_event_id: str,
    *,
    at: datetime,
    workspace_id: uuid.UUID = DEMO_WORKSPACE_ID,
    source_id: uuid.UUID = LOGS,
    service_id: uuid.UUID | None = CHECKOUT,
    replay_scope: str = "live",
    kind: SignalKind = SignalKind.LOG,
    trace_id: str | None = None,
    attributes: dict[str, Any] | None = None,
    status: QualityStatus = QualityStatus.ACCEPTED,
    flags: tuple[QualityFlag, ...] = (),
    signal_id: uuid.UUID | None = None,
) -> NormalizedSignal:
    return NormalizedSignal(
        id=signal_id or uuid.uuid4(),
        workspace_id=workspace_id,
        source_id=source_id,
        service_id=service_id,
        source_event_id=source_event_id,
        replay_scope=replay_scope,
        kind=kind,
        occurred_at=at,
        ingested_at=BASE,
        severity=Severity.ERROR,
        message=f"message {source_event_id}",
        trace_id=trace_id,
        attributes=attributes or {},
        quality_status=status,
        quality_flags=flags,
    )


# Five checkout logs; two share a timestamp, so the ID tiebreaker decides their order.
PAGED = [
    signal("page-0", at=BASE, signal_id=uuid.UUID(int=10)),
    signal("page-1", at=BASE + timedelta(seconds=1), signal_id=uuid.UUID(int=11)),
    signal("page-2", at=BASE + timedelta(seconds=1), signal_id=uuid.UUID(int=12)),
    signal("page-3", at=BASE + timedelta(seconds=2), signal_id=uuid.UUID(int=13)),
    signal("page-4", at=BASE + timedelta(seconds=3), signal_id=uuid.UUID(int=14)),
]
OTHERS = [
    signal(
        "trace-a",
        at=BASE + timedelta(seconds=5),
        source_id=TRACES,
        service_id=PAYMENT,
        kind=SignalKind.TRACE,
        trace_id=SHARED_TRACE,
    ),
    signal(
        "trace-b",
        at=BASE + timedelta(seconds=4),
        source_id=TRACES,
        kind=SignalKind.TRACE,
        trace_id=SHARED_TRACE,
    ),
    signal(
        "degraded",
        at=BASE + timedelta(seconds=6),
        service_id=None,
        status=QualityStatus.DEGRADED,
        flags=(QualityFlag.MISSING_SERVICE, QualityFlag.LATE_ARRIVAL),
    ),
    signal(
        "quarantined",
        at=BASE + timedelta(seconds=7),
        service_id=None,
        status=QualityStatus.QUARANTINED,
        flags=(QualityFlag.MALFORMED_PAYLOAD,),
        attributes={
            "source": {"password": "hunter2", "customer_email": "a@example.com"},
            "record": {"contract_errors": ["level: invalid"]},
        },
    ),
    # Same trace ID and window in a replay scope and in another workspace.
    signal(
        "replayed",
        at=BASE + timedelta(seconds=4),
        source_id=TRACES,
        replay_scope=REPLAY,
        kind=SignalKind.TRACE,
        trace_id=SHARED_TRACE,
    ),
    signal(
        "foreign",
        at=BASE + timedelta(seconds=4),
        workspace_id=OTHER_WORKSPACE,
        source_id=OTHER_SOURCE,
        service_id=None,
        kind=SignalKind.TRACE,
        trace_id=SHARED_TRACE,
        status=QualityStatus.DEGRADED,
        flags=(QualityFlag.UNKNOWN_SERVICE,),
    ),
]
WINDOW = {
    "since": (BASE - timedelta(minutes=1)).isoformat(),
    "until": (BASE + timedelta(minutes=1)).isoformat(),
}


@pytest.fixture(scope="module")
def stored(migrated: tuple[Settings, Engine]) -> tuple[Settings, Engine]:
    settings, engine = migrated

    async def store() -> None:
        async_engine = create_engine(settings)
        database = Database.from_engine(async_engine)
        try:
            await seed_demo(database)
            async with database.transaction() as session:
                catalog = CatalogRepository(session)
                await catalog.ensure_workspace(OTHER_WORKSPACE, "other")
                await catalog.ensure_source(
                    source_id=OTHER_SOURCE,
                    workspace_id=OTHER_WORKSPACE,
                    name="other-traces",
                    kind=SourceKind.TRACE_SUMMARY,
                )
                repository = EvidenceRepository(session)
                for item in (*PAGED, *OTHERS):
                    assert await repository.insert_signal(item)
        finally:
            await async_engine.dispose()

    asyncio.run(store())
    with engine.begin() as conn:
        # A source-level field added after ingestion: stored unredacted, hidden on output.
        conn.execute(
            text("UPDATE sources SET config = :config WHERE id = :id"),
            {"config": '{"redact_fields": ["customer-email"]}', "id": LOGS},
        )
    return settings, engine


def make_client(settings: Settings, **overrides: Any) -> TestClient:
    settings = settings.model_copy(
        update={
            "operator_tokens": {
                DEMO_WORKSPACE_ID: SecretStr(TOKEN),
                OTHER_WORKSPACE: SecretStr(OTHER_TOKEN),
            },
            **overrides,
        }
    )

    @asynccontextmanager
    async def resources(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = create_engine(app_settings)
        try:
            yield Resources(checks=[], engine=engine, database=Database.from_engine(engine))
        finally:
            await engine.dispose()

    return TestClient(create_app(settings, resource_factory=resources))


@pytest.fixture
def client(stored: tuple[Settings, Engine]) -> Iterator[TestClient]:
    with make_client(stored[0]) as test_client:
        yield test_client


def get(client: TestClient, path: str, token: str = TOKEN, **params: Any) -> Any:
    response = client.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    return response.json()


def ids(items: list[dict[str, Any]]) -> list[str]:
    return [item["source_event_id"] for item in items]


# Scope


def test_signals_stay_in_the_token_workspace_and_requested_replay_scope(
    client: TestClient,
) -> None:
    live = get(client, "/v1/signals", **WINDOW)
    assert live["replay_scope"] == "live"
    assert ids(live["items"]) == [
        "quarantined",
        "degraded",
        "trace-a",
        "trace-b",
        "page-4",
        "page-3",
        "page-2",
        "page-1",
        "page-0",
    ]
    assert {item["workspace_id"] for item in live["items"]} == {str(DEMO_WORKSPACE_ID)}
    assert ids(get(client, "/v1/signals", replay_scope=REPLAY, **WINDOW)["items"]) == ["replayed"]
    assert ids(get(client, "/v1/signals", OTHER_TOKEN, **WINDOW)["items"]) == ["foreign"]


def test_filtering_by_another_workspace_source_returns_nothing(client: TestClient) -> None:
    assert get(client, "/v1/signals", source_id=str(OTHER_SOURCE), **WINDOW)["items"] == []
    assert get(client, "/v1/signals", OTHER_TOKEN, source_id=str(LOGS), **WINDOW)["items"] == []


def test_trace_lookup_is_scoped(client: TestClient) -> None:
    trace = get(client, f"/v1/traces/{SHARED_TRACE}")
    assert ids(trace["signals"]) == ["trace-b", "trace-a"]
    assert trace["truncated"] is False
    replayed = get(client, f"/v1/traces/{SHARED_TRACE}", replay_scope=REPLAY)
    assert ids(replayed["signals"]) == ["replayed"]
    assert ids(get(client, f"/v1/traces/{SHARED_TRACE}", OTHER_TOKEN)["signals"]) == ["foreign"]
    response = client.get(
        "/v1/traces/trace-absent", headers={"Authorization": f"Bearer {OTHER_TOKEN}"}
    )
    assert response.status_code == 404


def test_trace_in_a_scope_without_it_is_404(client: TestClient) -> None:
    response = client.get(
        f"/v1/traces/{SHARED_TRACE}",
        params={"replay_scope": "replay:other"},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert response.status_code == 404


def test_trace_lookup_reports_truncation(client: TestClient) -> None:
    trace = get(client, f"/v1/traces/{SHARED_TRACE}", limit=1)
    assert ids(trace["signals"]) == ["trace-b"]
    assert trace["truncated"] is True


# Filters and pagination


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"service_id": str(PAYMENT)}, ["trace-a"]),
        ({"source_id": str(TRACES)}, ["trace-a", "trace-b"]),
        ({"kind": "trace", "service_id": str(CHECKOUT)}, ["trace-b"]),
        ({"quality_status": "quarantined"}, ["quarantined"]),
        ({"source_id": str(DEPLOYMENTS)}, []),
    ],
)
def test_filters(client: TestClient, params: dict[str, str], expected: list[str]) -> None:
    assert ids(get(client, "/v1/signals", **params, **WINDOW)["items"]) == expected


def test_time_window_is_half_open(client: TestClient) -> None:
    window = {
        "since": (BASE + timedelta(seconds=1)).isoformat(),
        "until": (BASE + timedelta(seconds=3)).isoformat(),
    }
    page = get(client, "/v1/signals", source_id=str(LOGS), **window)
    assert ids(page["items"]) == ["page-3", "page-2", "page-1"]


def test_cursor_pages_without_gaps_or_repeats(client: TestClient) -> None:
    filters = {"service_id": str(CHECKOUT), "kind": "log", **WINDOW}
    seen: list[str] = []
    page = get(client, "/v1/signals", limit=2, **filters)
    window = page["window"]
    while True:
        assert len(page["items"]) <= 2
        assert page["window"] == window
        seen += ids(page["items"])
        if page["next_cursor"] is None:
            break
        # Later pages take the window from the cursor.
        page = get(
            client,
            "/v1/signals",
            limit=2,
            service_id=str(CHECKOUT),
            kind="log",
            cursor=page["next_cursor"],
        )
    assert seen == ["page-4", "page-3", "page-2", "page-1", "page-0"]


def test_default_window_pages_keep_the_first_window(client: TestClient) -> None:
    first = get(client, "/v1/signals", limit=3, source_id=str(LOGS))
    assert first["next_cursor"] is not None
    second = get(client, "/v1/signals", limit=3, source_id=str(LOGS), cursor=first["next_cursor"])
    assert second["window"] == first["window"]
    assert not set(ids(first["items"])) & set(ids(second["items"]))


@pytest.mark.parametrize(
    ("token", "params"),
    [
        (TOKEN, {"service_id": str(PAYMENT)}),
        (TOKEN, {"replay_scope": REPLAY}),
        (TOKEN, {"since": (BASE - timedelta(minutes=2)).isoformat()}),
        (OTHER_TOKEN, {}),
    ],
    ids=["filter", "replay-scope", "window", "workspace"],
)
def test_cursor_from_another_query_is_400(
    client: TestClient, token: str, params: dict[str, str]
) -> None:
    first = get(client, "/v1/signals", limit=2, **WINDOW)
    response = client.get(
        "/v1/signals",
        params={"limit": 2, **WINDOW, **params, "cursor": first["next_cursor"]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "cursor belongs to a different query"}


def test_configured_limit_and_window_bound_queries(stored: tuple[Settings, Engine]) -> None:
    with make_client(
        stored[0],
        query_default_limit=2,
        query_max_limit=3,
        query_default_window_seconds=60,
        query_max_window_seconds=120,
    ) as client:
        assert len(get(client, "/v1/signals", source_id=str(LOGS), **WINDOW)["items"]) == 2
        headers = {"Authorization": f"Bearer {TOKEN}"}
        assert client.get("/v1/signals", params={"limit": 4}, headers=headers).status_code == 422
        wide = {"since": (BASE - timedelta(minutes=5)).isoformat(), "until": BASE.isoformat()}
        assert client.get("/v1/signals", params=wide, headers=headers).status_code == 422
        assert (
            client.get(f"/v1/traces/{SHARED_TRACE}", params={"limit": 4}, headers=headers)
        ).status_code == 422


# Redaction


def test_attributes_are_redacted_on_output(client: TestClient) -> None:
    page = get(client, "/v1/signals", quality_status="quarantined", **WINDOW)
    (item,) = page["items"]
    assert item["attributes"]["source"] == {"password": REDACTED, "customer_email": REDACTED}
    assert item["attributes"]["record"] == {"contract_errors": ["level: invalid"]}
    response = get(client, f"/v1/traces/{SHARED_TRACE}")
    assert "hunter2" not in str(response)


# Quality summary


def test_quality_summary_counts_per_source(client: TestClient) -> None:
    summary = get(client, "/v1/sources/quality")
    by_source = {item["source_id"]: item for item in summary["sources"]}
    assert set(by_source) == {str(source.id) for source in DEMO_SOURCES}
    logs, traces, deployments = (by_source[str(sid)] for sid in (LOGS, TRACES, DEPLOYMENTS))
    assert logs["total"] == 7
    assert logs["by_status"] == {"accepted": 5, "degraded": 1, "quarantined": 1}
    assert logs["by_flag"] == {"missing_service": 1, "late_arrival": 1, "malformed_payload": 1}
    # The replayed and foreign traces are in other scopes.
    assert traces["total"] == 2
    assert traces["by_status"] == {"accepted": 2, "degraded": 0, "quarantined": 0}
    assert traces["by_flag"] == {}
    assert deployments["total"] == 0


def test_quality_summary_respects_scope_and_window(client: TestClient) -> None:
    replay = get(client, "/v1/sources/quality", replay_scope=REPLAY)
    assert {item["name"]: item["total"] for item in replay["sources"]} == {
        source.name: (1 if source.id == TRACES else 0) for source in DEMO_SOURCES
    }
    other = get(client, "/v1/sources/quality", OTHER_TOKEN)
    assert [(item["name"], item["by_flag"]) for item in other["sources"]] == [
        ("other-traces", {"unknown_service": 1})
    ]
    before = {"until": (BASE - timedelta(seconds=1)).isoformat()}
    assert all(
        item["total"] == 0 for item in get(client, "/v1/sources/quality", **before)["sources"]
    )
