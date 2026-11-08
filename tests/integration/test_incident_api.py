"""Incident and graph routes through the real app against Compose PostgreSQL (P05-T05).

Incidents exist in two workspaces and two replay scopes, so each route is
checked for what it must not return. Status-change and graph tests make their
own rows; those in the live scope stay out of listing assertions through `LISTED`.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, select

from signalops.adapters.ingestion import REDACTED
from signalops.api.app import create_app
from signalops.config import Settings
from signalops.contracts.common import replay_scope_for
from signalops.contracts.events import IncidentChange, IncidentChangedPayload, decode_event
from signalops.contracts.evidence import (
    NormalizedSignal,
    QualityStatus,
    Severity,
    SignalKind,
    SourceKind,
)
from signalops.contracts.graph import GraphRelation
from signalops.contracts.workflow import IncidentServiceRole, IncidentSeverity, IncidentStatus
from signalops.dependencies import Resources
from signalops.detection.incidents import (
    Breach,
    IncidentPolicy,
    IncidentVersionError,
    change_status,
    fingerprint_for,
    record_breach,
)
from signalops.detection.rules import DEFAULT_RULES, Window
from signalops.persistence import database as db
from signalops.persistence.models import (
    DetectionTrigger,
    GraphEdge,
    Incident,
    IncidentService,
    IncidentSignal,
    OutboxEvent,
)
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.detection import SeriesKey
from signalops.persistence.repositories.evidence import EvidenceRepository
from signalops.persistence.repositories.graph import GraphRepository
from signalops.persistence.repositories.incidents import IncidentRepository, NewIncident
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID

pytestmark = pytest.mark.integration

LOGS = DEMO_SOURCES[0].id
SERVICES = {service.name: service.id for service in DEMO_SERVICES}
CHECKOUT, PAYMENT = SERVICES["checkout"], SERVICES["payment"]
GATEWAY, INVENTORY = SERVICES["bank-gateway"], SERVICES["inventory"]
OTHER_WORKSPACE = uuid.UUID("9b3c1f4e-2d5a-4b6c-8e7f-0a1b2c3d4e5f")
OTHER_SERVICE = uuid.UUID("7e1d2c3b-4a59-4687-9a0b-1c2d3e4f5a6b")
OTHER_SOURCE = uuid.UUID("4c1d2e3f-5a6b-4c7d-8e9f-a0b1c2d3e4f5")
TOKEN, OTHER_TOKEN = "operator-demo", "operator-other"
REPLAY = replay_scope_for(uuid.UUID("7d4c0e8a-1111-4a2b-9c3d-000000000002"))
LIVE = EvidenceScope(DEMO_WORKSPACE_ID)
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
WINDOW = timedelta(minutes=1)
# Incidents made by status-change tests are high severity, so listings can skip them.
LISTED: dict[str, Any] = {"severity": "low"}


async def add_incident(
    database: db.Database,
    scope: EvidenceScope,
    *,
    at: datetime,
    severity: IncidentSeverity = IncidentSeverity.LOW,
    service_id: uuid.UUID = CHECKOUT,
) -> uuid.UUID:
    async with database.transaction() as session:
        incidents = IncidentRepository(session)
        incident_id = await incidents.create(
            NewIncident(
                scope=scope,
                fingerprint=f"env:{uuid.uuid4().hex[:8]}",
                title=f"High error rate at {at.isoformat()}",
                severity=severity,
                window=Window(at, at + WINDOW),
                detected_at=at + WINDOW,
            )
        )
        await incidents.add_service(
            SeriesKey(scope.workspace_id, service_id, scope.replay_scope, "trace.60s"),
            incident_id,
            IncidentServiceRole.PRIMARY,
        )
    return incident_id


async def cite(
    database: db.Database,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    count: int,
    *,
    attributes: dict[str, Any] | None = None,
) -> list[uuid.UUID]:
    """Signals cited by the incident, stored newest first so order must come from the query."""
    ids = [uuid.uuid4() for _ in range(count)]
    async with database.transaction() as session:
        for offset, signal_id in reversed(list(enumerate(ids))):
            stored = await EvidenceRepository(session).insert_signal(
                NormalizedSignal(
                    id=signal_id,
                    workspace_id=scope.workspace_id,
                    source_id=LOGS,
                    service_id=CHECKOUT,
                    source_event_id=f"cited-{signal_id}",
                    replay_scope=scope.replay_scope,
                    kind=SignalKind.LOG,
                    occurred_at=T0 + timedelta(seconds=offset),
                    ingested_at=T0,
                    severity=Severity.ERROR,
                    message=f"message {offset}",
                    attributes=attributes or {},
                    quality_status=QualityStatus.ACCEPTED,
                )
            )
            assert stored
            session.add(
                IncidentSignal(
                    incident_id=incident_id,
                    signal_id=signal_id,
                    workspace_id=scope.workspace_id,
                    replay_scope=scope.replay_scope,
                    relevance=0.5,
                )
            )
    return ids


@pytest.fixture(scope="module")
def stored(seeded: tuple[Settings, Engine]) -> dict[str, Any]:
    settings, _ = seeded

    async def store() -> dict[str, Any]:
        engine = db.create_engine(settings)
        database = db.Database.from_engine(engine)
        try:
            async with database.transaction() as session:
                catalog = CatalogRepository(session)
                await catalog.ensure_workspace(OTHER_WORKSPACE, "other")
                await catalog.ensure_service(
                    service_id=OTHER_SERVICE,
                    workspace_id=OTHER_WORKSPACE,
                    name="checkout",
                    environment="other",
                )
                await catalog.ensure_source(
                    source_id=OTHER_SOURCE,
                    workspace_id=OTHER_WORKSPACE,
                    name="other-logs",
                    kind=SourceKind.STRUCTURED_LOG,
                )
            listed = [
                await add_incident(database, LIVE, at=T0 + timedelta(hours=hour))
                for hour in range(5)
            ]
            # Same detection time as the newest: the tie falls to the ID.
            listed.append(await add_incident(database, LIVE, at=T0 + timedelta(hours=4)))
            paying = await add_incident(
                database, LIVE, at=T0 - timedelta(hours=1), service_id=PAYMENT
            )
            replayed = await add_incident(database, EvidenceScope(DEMO_WORKSPACE_ID, REPLAY), at=T0)
            foreign = await add_incident(
                database, EvidenceScope(OTHER_WORKSPACE), at=T0, service_id=OTHER_SERVICE
            )
            await cite(database, LIVE, listed[0], 5, attributes={"password": "hunter2"})
            async with database.transaction() as session:
                session.add(
                    DetectionTrigger(
                        workspace_id=DEMO_WORKSPACE_ID,
                        service_id=CHECKOUT,
                        replay_scope="live",
                        incident_id=listed[0],
                        rule_id="trace_error_rate",
                        rule_version=2,
                        metric_name="trace.60s",
                        window_start=T0,
                        window_end=T0 + WINDOW,
                        observed=0.6,
                        threshold=0.2,
                        sample_count=20,
                        error_count=12,
                    )
                )
            return {"listed": listed, "paying": paying, "replayed": replayed, "foreign": foreign}
        finally:
            await engine.dispose()

    return asyncio.run(store())


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
        engine = db.create_engine(app_settings)
        try:
            yield Resources(checks=[], engine=engine, database=db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    return TestClient(create_app(settings, resource_factory=resources))


@pytest.fixture
def client(seeded: tuple[Settings, Engine], stored: dict[str, Any]) -> Iterator[TestClient]:
    with make_client(seeded[0]) as test_client:
        yield test_client


def get(client: TestClient, path: str, token: str = TOKEN, **params: Any) -> Any:
    response = client.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    return response.json()


def status_of(client: TestClient, path: str, token: str = TOKEN, **params: Any) -> int:
    headers = {"Authorization": f"Bearer {token}"}
    return client.get(path, params=params, headers=headers).status_code


def patch(client: TestClient, incident_id: uuid.UUID, state: str, version: int) -> Any:
    return client.patch(
        f"/v1/incidents/{incident_id}",
        json={"status": state, "version": version},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )


def test_every_route_requires_an_operator_token(client: TestClient) -> None:
    incident_id = uuid.uuid4()
    assert client.get("/v1/incidents").status_code == 401
    assert client.get(f"/v1/incidents/{incident_id}").status_code == 401
    assert client.get(f"/v1/incidents/{incident_id}/evidence").status_code == 401
    assert client.get(f"/v1/services/{CHECKOUT}/graph").status_code == 401
    response = client.patch(f"/v1/incidents/{incident_id}", json={"status": "open", "version": 1})
    assert response.status_code == 401


def test_list_pages_newest_first_with_no_repeats(
    client: TestClient, stored: dict[str, Any]
) -> None:
    pages, cursor = [], None
    while True:
        params = LISTED | {"limit": 4} | ({"cursor": cursor} if cursor else {})
        page = get(client, "/v1/incidents", **params)
        pages.append(page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    items = [item for page in pages for item in page]
    expected = sorted((item["detected_at"], item["id"]) for item in items)[::-1]
    assert [(item["detected_at"], item["id"]) for item in items] == expected
    assert len({item["id"] for item in items}) == len(items)
    assert set(map(str, (*stored["listed"], stored["paying"]))) <= {i["id"] for i in items}


def test_a_row_inserted_during_paging_does_not_repeat_a_row(
    seeded: tuple[Settings, Engine], client: TestClient, stored: dict[str, Any]
) -> None:
    first = get(client, "/v1/incidents", limit=2, **LISTED)

    newest = run(
        client,
        seeded[0],
        lambda database: add_incident(database, LIVE, at=T0 + timedelta(days=30)),
    )
    second = get(client, "/v1/incidents", limit=100, cursor=first["next_cursor"], **LISTED)
    seen = {item["id"] for item in first["items"]}
    assert not seen & {item["id"] for item in second["items"]}
    assert str(newest) not in {item["id"] for item in second["items"]}


def test_list_filters_and_scope(client: TestClient, stored: dict[str, Any]) -> None:
    by_service = get(client, "/v1/incidents", service_id=str(PAYMENT))["items"]
    assert str(stored["paying"]) in {item["id"] for item in by_service}
    assert str(stored["listed"][0]) not in {item["id"] for item in by_service}
    paying = next(item for item in by_service if item["id"] == str(stored["paying"]))
    assert {(s["service_id"], s["role"]) for s in paying["services"]} >= {(str(PAYMENT), "primary")}
    assert paying["services"][0]["role"] == "primary"

    assert get(client, "/v1/incidents", severity="critical")["items"] == []
    assert get(client, "/v1/incidents", status="resolved", service_id=str(GATEWAY))["items"] == []

    live = {item["id"] for item in get(client, "/v1/incidents", limit=100)["items"]}
    assert str(stored["replayed"]) not in live
    assert str(stored["foreign"]) not in live

    replay = get(client, "/v1/incidents", replay_scope=REPLAY)["items"]
    assert [item["id"] for item in replay] == [str(stored["replayed"])]
    foreign = get(client, "/v1/incidents", token=OTHER_TOKEN)["items"]
    assert [item["id"] for item in foreign] == [str(stored["foreign"])]


def test_a_cursor_only_works_for_its_own_query(client: TestClient) -> None:
    cursor = get(client, "/v1/incidents", limit=1, **LISTED)["next_cursor"]
    assert status_of(client, "/v1/incidents", cursor=cursor, severity="high") == 400
    assert status_of(client, "/v1/incidents", cursor=cursor, replay_scope=REPLAY) == 400
    assert status_of(client, "/v1/incidents", cursor=cursor, token=OTHER_TOKEN) == 400
    assert status_of(client, "/v1/incidents", cursor="not-a-cursor") == 400


def test_list_limit_follows_the_contract_not_only_the_setting(client: TestClient) -> None:
    # The configured maximum is 200; the page contract allows 100.
    assert status_of(client, "/v1/incidents", limit=100) == 200
    assert status_of(client, "/v1/incidents", limit=101) == 422


def test_detail_has_services_evidence_and_triggers(
    client: TestClient, stored: dict[str, Any]
) -> None:
    detail = get(client, f"/v1/incidents/{stored['listed'][0]}")
    assert [(s["name"], s["role"]) for s in detail["services"]] == [("checkout", "primary")]
    assert len(detail["evidence"]) == 5
    occurred = [ref["occurred_at"] for ref in detail["evidence"]]
    assert occurred == sorted(occurred)
    assert detail["triggers_truncated"] is False
    assert [(t["rule_id"], t["observed"]) for t in detail["triggers"]] == [
        ("trace_error_rate", 0.6)
    ]


def test_detail_stays_in_its_workspace_and_scope(
    client: TestClient, stored: dict[str, Any]
) -> None:
    assert status_of(client, f"/v1/incidents/{stored['foreign']}") == 404
    assert status_of(client, f"/v1/incidents/{stored['replayed']}") == 404
    assert status_of(client, f"/v1/incidents/{stored['replayed']}", replay_scope=REPLAY) == 200
    assert status_of(client, f"/v1/incidents/{stored['foreign']}/evidence") == 404
    assert status_of(client, f"/v1/incidents/{uuid.uuid4()}") == 404


def test_evidence_pages_oldest_first_and_redacts(
    client: TestClient, stored: dict[str, Any]
) -> None:
    path = f"/v1/incidents/{stored['listed'][0]}/evidence"
    first = get(client, path, limit=2)
    second = get(client, path, limit=2, cursor=first["next_cursor"])
    third = get(client, path, limit=2, cursor=second["next_cursor"])
    assert third["next_cursor"] is None
    items = [*first["items"], *second["items"], *third["items"]]
    assert [item["signal"]["message"] for item in items] == [f"message {i}" for i in range(5)]
    assert {item["signal"]["attributes"]["password"] for item in items} == {REDACTED}
    assert {item["relevance"] for item in items} == {0.5}

    other = f"/v1/incidents/{stored['listed'][1]}/evidence"
    assert status_of(client, other, cursor=first["next_cursor"]) == 400


def test_an_incident_without_evidence_has_an_empty_timeline(
    client: TestClient, stored: dict[str, Any]
) -> None:
    page = get(client, f"/v1/incidents/{stored['listed'][1]}/evidence")
    assert page["items"] == []
    assert page["next_cursor"] is None


def _events(engine: Engine, incident_id: uuid.UUID) -> list[IncidentChangedPayload]:
    with engine.connect() as conn:
        rows = conn.execute(
            select(OutboxEvent.envelope)
            .where(OutboxEvent.aggregate_id == incident_id)
            .order_by(OutboxEvent.aggregate_version)
        )
        decoded = [decode_event(json.dumps(row).encode()).payload for (row,) in rows]
    assert all(isinstance(payload, IncidentChangedPayload) for payload in decoded)
    return decoded  # type: ignore[return-value]


@pytest.fixture
def changing(
    client: TestClient, seeded: tuple[Settings, Engine], stored: dict[str, Any]
) -> uuid.UUID:
    """A live incident of its own, high severity so that listing tests skip it."""

    return run(
        client,
        seeded[0],
        lambda database: add_incident(
            database, LIVE, at=T0 + timedelta(days=40), severity=IncidentSeverity.HIGH
        ),
    )


def test_resolve_and_reopen_bump_the_version_and_publish(
    client: TestClient, seeded: tuple[Settings, Engine], changing: uuid.UUID
) -> None:
    resolved = patch(client, changing, "resolved", 1)
    assert resolved.status_code == 200, resolved.text
    body = resolved.json()
    assert (body["status"], body["version"]) == ("resolved", 2)
    assert body["resolved_at"] is not None
    assert [s["name"] for s in body["services"]] == ["checkout"]

    reopened = patch(client, changing, "open", 2).json()
    assert (reopened["status"], reopened["version"], reopened["resolved_at"]) == ("open", 3, None)

    events = _events(seeded[1], changing)
    assert [(e.incident_version, e.change, e.status) for e in events] == [
        (2, IncidentChange.RESOLVED, IncidentStatus.RESOLVED),
        (3, IncidentChange.REOPENED, IncidentStatus.OPEN),
    ]


def test_a_stale_version_changes_nothing(
    client: TestClient, seeded: tuple[Settings, Engine], changing: uuid.UUID
) -> None:
    assert patch(client, changing, "resolved", 1).status_code == 200
    stale = patch(client, changing, "open", 1)
    assert stale.status_code == 409
    assert "version 2" in stale.json()["detail"]
    assert get(client, f"/v1/incidents/{changing}")["status"] == "resolved"
    assert len(_events(seeded[1], changing)) == 1


def test_a_retried_status_change_is_harmless(
    client: TestClient, seeded: tuple[Settings, Engine], changing: uuid.UUID
) -> None:
    assert patch(client, changing, "open", 1).json()["version"] == 1
    assert _events(seeded[1], changing) == []


def test_status_change_is_live_only_and_in_workspace(
    client: TestClient, stored: dict[str, Any]
) -> None:
    assert patch(client, stored["replayed"], "resolved", 1).status_code == 404
    assert patch(client, stored["foreign"], "resolved", 1).status_code == 404
    replay = get(client, f"/v1/incidents/{stored['replayed']}", replay_scope=REPLAY)
    assert (replay["status"], replay["version"]) == ("open", 1)


@pytest.mark.parametrize("body", [{"status": "open"}, {"status": "closed", "version": 1}])
def test_invalid_status_body_is_rejected(
    client: TestClient, changing: uuid.UUID, body: dict[str, Any]
) -> None:
    response = client.patch(
        f"/v1/incidents/{changing}", json=body, headers={"Authorization": f"Bearer {TOKEN}"}
    )
    assert response.status_code == 422


async def test_concurrent_operators_cannot_both_win(
    database: db.Database, run_id: uuid.UUID
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    incident_id = await add_incident(database, scope, at=T0)

    async def resolve() -> bool:
        try:
            async with database.transaction() as session:
                await change_status(
                    session,
                    scope,
                    incident_id,
                    IncidentStatus.RESOLVED,
                    version=1,
                    now=datetime.now(UTC),
                )
            return True
        except IncidentVersionError:
            return False

    # Asking for the same status at version 1: the loser sees version 2.
    results = await asyncio.gather(*(resolve() for _ in range(5)))
    assert results.count(True) == 1
    async with database.transaction() as session:
        incident = await session.get(Incident, incident_id)
        events = await session.scalars(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == incident_id)
        )
        assert incident is not None
        assert incident.version == 2
        assert len(events.all()) == 1


async def test_a_later_breach_reopens_what_an_operator_resolved(
    database: db.Database, run_id: uuid.UUID
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))
    series = SeriesKey(DEMO_WORKSPACE_ID, CHECKOUT, scope.replay_scope, "trace.60s")
    policy = IncidentPolicy()
    rule = DEFAULT_RULES[0]

    async def breach(window: Window) -> None:
        async with database.transaction() as session:
            await record_breach(session, policy, Breach(series, window, rule, None, window.end))

    await breach(Window(T0, T0 + WINDOW))
    async with database.transaction() as session:
        incident = (await session.scalars(select(Incident).where(scope.clause(Incident)))).one()
        await change_status(
            session,
            scope,
            incident.id,
            IncidentStatus.RESOLVED,
            version=incident.version,
            now=T0 + 2 * WINDOW,
        )

    await breach(Window(T0 + 2 * WINDOW, T0 + 3 * WINDOW))
    async with database.transaction() as session:
        incident = (
            await session.scalars(
                select(Incident)
                .where(scope.clause(Incident))
                .execution_options(populate_existing=True)
            )
        ).one()
    assert incident.status == IncidentStatus.OPEN
    assert incident.resolved_at is None
    assert incident.fingerprint == fingerprint_for("demo")


async def _link(
    database: db.Database,
    scope: EvidenceScope,
    pairs: Sequence[tuple[uuid.UUID, uuid.UUID]],
    *,
    valid_from: datetime,
    valid_to: datetime | None = None,
) -> None:
    async with database.transaction() as session:
        graph = GraphRepository(session)
        nodes = await graph.ensure_service_nodes(scope, {id for pair in pairs for id in pair})
        session.add_all(
            GraphEdge(
                workspace_id=scope.workspace_id,
                replay_scope=scope.replay_scope,
                from_node_id=nodes[caller],
                to_node_id=nodes[callee],
                relation=GraphRelation.DEPENDS_ON,
                valid_from=valid_from,
                valid_to=valid_to,
                confidence=1.0,
            )
            for caller, callee in pairs
        )


def run[T](
    client: TestClient, settings: Settings, action: Callable[[db.Database], Awaitable[T]]
) -> T:
    async def call() -> T:
        engine = db.create_engine(settings)
        try:
            return await action(db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    assert client.portal is not None
    return client.portal.call(call)


@pytest.fixture
def graph_scope() -> str:
    return replay_scope_for(uuid.uuid4())


def _edges(entries: Sequence[dict[str, Any]]) -> set[tuple[str, str, int]]:
    return {(e["from_service_id"], e["to_service_id"], e["hops"]) for e in entries}


def test_graph_shows_both_directions_with_names(
    client: TestClient, seeded: tuple[Settings, Engine], graph_scope: str
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, graph_scope)
    pairs = [(CHECKOUT, PAYMENT), (PAYMENT, GATEWAY), (CHECKOUT, INVENTORY)]
    run(client, seeded[0], lambda database: _link(database, scope, pairs, valid_from=T0))

    graph = get(
        client,
        f"/v1/services/{PAYMENT}/graph",
        replay_scope=graph_scope,
        as_of=(T0 + WINDOW).isoformat(),
    )
    assert _edges(graph["downstream"]) == {(str(PAYMENT), str(GATEWAY), 1)}
    assert _edges(graph["upstream"]) == {(str(CHECKOUT), str(PAYMENT), 1)}
    assert {s["name"] for s in graph["services"]} == {"payment", "bank-gateway", "checkout"}
    assert graph["truncated"] is False

    wide = get(
        client,
        f"/v1/services/{CHECKOUT}/graph",
        replay_scope=graph_scope,
        as_of=(T0 + WINDOW).isoformat(),
    )
    assert (str(PAYMENT), str(GATEWAY), 2) in _edges(wide["downstream"])
    near = get(
        client,
        f"/v1/services/{CHECKOUT}/graph",
        replay_scope=graph_scope,
        as_of=(T0 + WINDOW).isoformat(),
        hops=1,
    )
    assert {edge[2] for edge in _edges(near["downstream"])} == {1}


def test_graph_excludes_what_came_later(
    client: TestClient, seeded: tuple[Settings, Engine], graph_scope: str
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, graph_scope)

    async def link(database: db.Database) -> None:
        await _link(database, scope, [(CHECKOUT, PAYMENT)], valid_from=T0)
        await _link(database, scope, [(CHECKOUT, GATEWAY)], valid_from=T0 + 2 * WINDOW)

    run(client, seeded[0], link)
    path = f"/v1/services/{CHECKOUT}/graph"
    early = get(client, path, replay_scope=graph_scope, as_of=(T0 + WINDOW).isoformat())
    assert _edges(early["downstream"]) == {(str(CHECKOUT), str(PAYMENT), 1)}
    assert {s["name"] for s in early["services"]} == {"checkout", "payment"}
    before = get(client, path, replay_scope=graph_scope, as_of=(T0 - WINDOW).isoformat())
    assert before["downstream"] == []
    assert [s["name"] for s in before["services"]] == ["checkout"]
    assert all(edge["valid_to"] is None for edge in early["downstream"])


def test_graph_of_a_cycle_terminates(
    client: TestClient, seeded: tuple[Settings, Engine], graph_scope: str
) -> None:
    scope = EvidenceScope(DEMO_WORKSPACE_ID, graph_scope)
    pairs = [(CHECKOUT, PAYMENT), (PAYMENT, GATEWAY), (GATEWAY, CHECKOUT)]
    run(client, seeded[0], lambda database: _link(database, scope, pairs, valid_from=T0))
    graph = get(
        client,
        f"/v1/services/{CHECKOUT}/graph",
        replay_scope=graph_scope,
        as_of=(T0 + WINDOW).isoformat(),
    )
    # A path never re-enters its start, so the closing edge shows only from the other side.
    assert _edges(graph["downstream"]) == {
        (str(CHECKOUT), str(PAYMENT), 1),
        (str(PAYMENT), str(GATEWAY), 2),
    }
    assert _edges(graph["upstream"]) == {
        (str(GATEWAY), str(CHECKOUT), 1),
        (str(PAYMENT), str(GATEWAY), 2),
    }
    assert graph["truncated"] is False


def test_graph_of_an_unobserved_service_is_empty(client: TestClient, graph_scope: str) -> None:
    graph = get(client, f"/v1/services/{CHECKOUT}/graph", replay_scope=graph_scope)
    assert (graph["downstream"], graph["upstream"]) == ([], [])
    assert [s["name"] for s in graph["services"]] == ["checkout"]


def test_graph_bounds_and_workspace(client: TestClient) -> None:
    path = f"/v1/services/{CHECKOUT}/graph"
    assert status_of(client, path, hops=4) == 422
    assert status_of(client, path, hops=0) == 422
    assert status_of(client, path, as_of="2026-11-01T12:00:00") == 422
    assert status_of(client, f"/v1/services/{OTHER_SERVICE}/graph") == 404
    assert status_of(client, f"/v1/services/{CHECKOUT}/graph", token=OTHER_TOKEN) == 404
    assert status_of(client, f"/v1/services/{uuid.uuid4()}/graph") == 404


def test_a_database_outage_is_503(seeded: tuple[Settings, Engine]) -> None:
    @asynccontextmanager
    async def down(app_settings: Settings) -> AsyncIterator[Resources]:
        engine = db.create_engine(
            app_settings.model_copy(
                update={
                    "database_url": SecretStr("postgresql+psycopg://x:y@localhost:1/none"),
                }
            )
        )
        try:
            yield Resources(checks=[], engine=engine, database=db.Database.from_engine(engine))
        finally:
            await engine.dispose()

    settings = seeded[0].model_copy(
        update={"operator_tokens": {DEMO_WORKSPACE_ID: SecretStr(TOKEN)}}
    )
    with TestClient(create_app(settings, resource_factory=down)) as outage:
        assert status_of(outage, "/v1/incidents") == 503
        assert status_of(outage, f"/v1/services/{CHECKOUT}/graph") == 503


def test_detail_reports_truncated_triggers(
    client: TestClient, seeded: tuple[Settings, Engine]
) -> None:
    async def add(database: db.Database) -> uuid.UUID:
        incident_id = await add_incident(
            database, LIVE, at=T0 + timedelta(days=50), severity=IncidentSeverity.HIGH
        )
        async with database.transaction() as session:
            session.add_all(
                DetectionTrigger(
                    workspace_id=DEMO_WORKSPACE_ID,
                    service_id=CHECKOUT,
                    replay_scope="live",
                    incident_id=incident_id,
                    rule_id="trace_error_rate",
                    rule_version=2,
                    metric_name="trace.60s",
                    window_start=T0 + timedelta(days=50) + index * WINDOW,
                    window_end=T0 + timedelta(days=50) + (index + 1) * WINDOW,
                    observed=0.6,
                    threshold=0.2,
                    sample_count=20,
                    error_count=12,
                )
                for index in range(201)
            )
        return incident_id

    incident_id = run(client, seeded[0], add)
    detail = get(client, f"/v1/incidents/{incident_id}")
    assert len(detail["triggers"]) == 200
    assert detail["triggers_truncated"] is True


def test_service_join_covers_every_role(
    client: TestClient, seeded: tuple[Settings, Engine]
) -> None:
    async def add(database: db.Database) -> uuid.UUID:
        incident_id = await add_incident(
            database, LIVE, at=T0 + timedelta(days=60), severity=IncidentSeverity.HIGH
        )
        async with database.transaction() as session:
            session.add(
                IncidentService(
                    incident_id=incident_id,
                    service_id=GATEWAY,
                    workspace_id=DEMO_WORKSPACE_ID,
                    role=IncidentServiceRole.AFFECTED,
                )
            )
        return incident_id

    incident_id = run(client, seeded[0], add)
    services = get(client, f"/v1/incidents/{incident_id}")["services"]
    assert {(s["name"], s["role"]) for s in services} == {
        ("bank-gateway", "affected"),
        ("checkout", "primary"),
    }
