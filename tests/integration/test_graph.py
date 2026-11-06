"""Graph projection and as-of traversal against Compose PostgreSQL (P05-T04).

Each test has its own replay scope, so graphs never meet across tests.
Traversal tests link services directly; projection tests go through the handler.
"""

import asyncio
import random
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from itertools import pairwise

import pytest
from sqlalchemy import select

from signalops.contracts.common import ContractError, replay_scope_for
from signalops.contracts.events import EventEnvelope, EventPayload
from signalops.contracts.evidence import QualityFlag, QualityStatus, SignalKind
from signalops.contracts.graph import GraphNodeType, GraphRelation
from signalops.contracts.tools import MAX_TOOL_ROWS, MAX_TRAVERSAL_HOPS
from signalops.detection.detector import DETECTOR_CONSUMER_GROUP
from signalops.detection.graph import GRAPH_CONSUMER_GROUP, GraphPolicy, GraphProjector
from signalops.persistence import database as db
from signalops.persistence.models import ConsumerReceipt, Deployment, GraphEdge, GraphNode
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.graph import (
    MAX_WALK_PATHS,
    GraphDirection,
    GraphRepository,
)
from signalops.persistence.repositories.scope import EvidenceScope
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from signalops.streaming.consumer import ProcessOutcome, TransactionalProcessor
from tests.integration.test_detector import (
    PAYMENT,
    SERVICE_IDS,
    T0,
    _normalized_demo,
    _processor,
    _redelivered,
    _store,
)

pytestmark = pytest.mark.integration

CHECKOUT = SERVICE_IDS["checkout"]
GATEWAY = SERVICE_IDS["bank-gateway"]
INVENTORY = SERVICE_IDS["inventory"]
ORDERS = SERVICE_IDS["orders-db"]
TTL = timedelta(hours=1)
POLICY = GraphPolicy(dependency_ttl_seconds=3600)


def _scope(run_id: uuid.UUID) -> EvidenceScope:
    return EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id))


def _projector(database: db.Database) -> TransactionalProcessor:
    return TransactionalProcessor(database, GRAPH_CONSUMER_GROUP, GraphProjector(POLICY))


async def _project(
    database: db.Database, envelopes: Sequence[EventEnvelope[EventPayload]]
) -> list[ProcessOutcome]:
    processor = _projector(database)
    return [await processor.process(envelope) for envelope in envelopes]


async def _call(
    database: db.Database,
    run_id: uuid.UUID,
    at: datetime,
    *downstream: str,
    service_id: uuid.UUID = PAYMENT,
) -> EventEnvelope[EventPayload]:
    return await _store(database, run_id, at, service_id=service_id, downstream=downstream)


async def _deploy(
    database: db.Database,
    run_id: uuid.UUID,
    at: datetime,
    revision: str,
    *,
    service_id: uuid.UUID = PAYMENT,
) -> EventEnvelope[EventPayload]:
    envelope = await _store(database, run_id, at, kind=SignalKind.DEPLOYMENT, service_id=service_id)
    signal_id = envelope.payload.signal_id  # type: ignore[attr-defined]
    async with database.transaction() as session:
        session.add(
            Deployment(
                workspace_id=DEMO_WORKSPACE_ID,
                service_id=service_id,
                signal_id=signal_id,
                replay_scope=replay_scope_for(run_id),
                revision=revision,
                deployed_at=at,
                attributes={"previous_revision": "v0"},
            )
        )
    return envelope


async def _edges(
    database: db.Database, run_id: uuid.UUID, relation: GraphRelation = GraphRelation.DEPENDS_ON
) -> list[GraphEdge]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(GraphEdge)
            .where(_scope(run_id).clause(GraphEdge), GraphEdge.relation == relation)
            .order_by(GraphEdge.valid_from)
        )
        return list(rows)


async def _downstream(
    database: db.Database, run_id: uuid.UUID, service_id: uuid.UUID, as_of: datetime
) -> set[uuid.UUID]:
    async with database.transaction() as session:
        walk = await GraphRepository(session).dependencies(_scope(run_id), service_id, as_of=as_of)
    return {entry.to_service_id for entry in walk.items}


async def _running(
    database: db.Database, run_id: uuid.UUID, as_of: datetime, service_id: uuid.UUID = PAYMENT
) -> list[str]:
    async with database.transaction() as session:
        entries = await GraphRepository(session).deployments_as_of(
            _scope(run_id), {service_id}, as_of=as_of
        )
    return [entry.revision for entry in entries]


async def _link(
    database: db.Database,
    run_id: uuid.UUID,
    pairs: Sequence[tuple[uuid.UUID, uuid.UUID]],
    *,
    valid_from: datetime = T0,
    valid_to: datetime | None = None,
) -> None:
    """Dependency edges written directly, for tests of the traversal alone."""
    scope = _scope(run_id)
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


async def _new_services(database: db.Database, count: int) -> list[uuid.UUID]:
    ids = [uuid.uuid4() for _ in range(count)]
    async with database.transaction() as session:
        for service_id in ids:
            await CatalogRepository(session).ensure_service(
                service_id=service_id,
                workspace_id=DEMO_WORKSPACE_ID,
                name=f"svc-{service_id.hex[:12]}",
                environment="demo",
            )
    return ids


# Dependencies


async def test_repeated_observations_coalesce_with_provenance(
    database: db.Database, run_id: uuid.UUID
) -> None:
    calls = [
        await _call(database, run_id, T0 + timedelta(minutes=m), "bank-gateway") for m in (0, 5, 9)
    ]
    await _project(database, calls)

    [edge] = await _edges(database, run_id)
    assert edge.valid_from == T0
    assert edge.valid_to == T0 + timedelta(minutes=9) + TTL
    assert edge.provenance_signal_id == calls[0].payload.signal_id  # type: ignore[attr-defined]
    assert edge.confidence == 1.0
    assert edge.properties["observations"] == 3
    assert edge.properties["last_signal_id"] == str(
        calls[2].payload.signal_id  # type: ignore[attr-defined]
    )


async def test_a_trace_observes_each_downstream_once(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(
        database,
        [
            await _call(
                database,
                run_id,
                T0,
                "inventory",
                "orders-db",
                "payment",
                "orders-db",
                service_id=CHECKOUT,
            )
        ],
    )
    assert await _downstream(database, run_id, CHECKOUT, T0) == {INVENTORY, ORDERS, PAYMENT}
    assert all(edge.properties["observations"] == 1 for edge in await _edges(database, run_id))


async def test_a_dependency_no_trace_confirms_expires(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(database, [await _call(database, run_id, T0, "bank-gateway")])

    assert await _downstream(database, run_id, PAYMENT, T0 + TTL - timedelta(seconds=1)) == {
        GATEWAY
    }
    assert await _downstream(database, run_id, PAYMENT, T0 + TTL) == set()


async def test_a_dependency_returning_after_the_ttl_is_a_new_edge(
    database: db.Database, run_id: uuid.UUID
) -> None:
    later = T0 + TTL * 3
    await _project(
        database,
        [
            await _call(database, run_id, T0, "bank-gateway"),
            await _call(database, run_id, later, "bank-gateway"),
        ],
    )

    first, second = await _edges(database, run_id)
    assert (first.valid_from, first.valid_to) == (T0, T0 + TTL)
    assert (second.valid_from, second.valid_to) == (later, later + TTL)
    assert await _downstream(database, run_id, PAYMENT, T0 + TTL * 2) == set()


async def test_an_observation_bridging_two_edges_merges_them(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(
        database,
        [
            await _call(database, run_id, T0, "bank-gateway"),
            await _call(database, run_id, T0 + TTL * 2, "bank-gateway"),
        ],
    )
    assert len(await _edges(database, run_id)) == 2

    await _project(database, [await _call(database, run_id, T0 + TTL, "bank-gateway")])

    [edge] = await _edges(database, run_id)
    assert (edge.valid_from, edge.valid_to) == (T0, T0 + TTL * 3)
    assert edge.properties["observations"] == 3


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_arrival_order_does_not_change_the_edges(database: db.Database, seed: int) -> None:
    offsets = (0, 20, 45, 200, 230, 500)
    ordered_run, shuffled_run = uuid.uuid4(), uuid.uuid4()
    ordered = [
        await _call(database, ordered_run, T0 + timedelta(minutes=m), "bank-gateway")
        for m in offsets
    ]
    shuffled = [
        await _call(database, shuffled_run, T0 + timedelta(minutes=m), "bank-gateway")
        for m in offsets
    ]
    random.Random(seed).shuffle(shuffled)  # noqa: S311 - test data

    await _project(database, ordered)
    await _project(database, shuffled)

    def shape(edges: Sequence[GraphEdge]) -> list[tuple[datetime, datetime | None, int]]:
        return [(e.valid_from, e.valid_to, e.properties["observations"]) for e in edges]

    assert shape(await _edges(database, ordered_run)) == shape(await _edges(database, shuffled_run))
    assert len(await _edges(database, ordered_run)) == 3


async def test_redelivery_changes_nothing(database: db.Database, run_id: uuid.UUID) -> None:
    call = await _call(database, run_id, T0, "bank-gateway")

    assert await _project(database, [call, call]) == [
        ProcessOutcome.APPLIED,
        ProcessOutcome.DUPLICATE,
    ]

    [edge] = await _edges(database, run_id)
    assert edge.properties["observations"] == 1


async def test_same_signal_under_another_event_keeps_the_interval(
    database: db.Database, run_id: uuid.UUID
) -> None:
    call = await _call(database, run_id, T0, "bank-gateway")
    await _project(database, [call, _redelivered(call)])

    [edge] = await _edges(database, run_id)
    assert (edge.valid_from, edge.valid_to) == (T0, T0 + TTL)
    assert edge.provenance_signal_id == call.payload.signal_id  # type: ignore[attr-defined]


async def test_concurrent_projectors_share_one_edge(
    database: db.Database, run_id: uuid.UUID
) -> None:
    calls = [
        await _call(database, run_id, T0 + timedelta(seconds=s), "bank-gateway", "inventory")
        for s in range(12)
    ]
    processors = [_projector(database) for _ in calls]
    await asyncio.gather(
        *(processor.process(call) for processor, call in zip(processors, calls, strict=True))
    )

    edges = await _edges(database, run_id)
    assert len(edges) == 2
    assert [edge.properties["observations"] for edge in edges] == [12, 12]


async def test_opposite_traces_create_shared_nodes_without_deadlock(
    database: db.Database, run_id: uuid.UUID
) -> None:
    first = [
        await _call(database, run_id, T0, "inventory", "orders-db", service_id=PAYMENT),
        await _call(database, run_id, T0, "orders-db", "payment", service_id=INVENTORY),
        await _call(database, run_id, T0, "payment", "inventory", service_id=ORDERS),
    ]
    await asyncio.wait_for(
        asyncio.gather(*(_projector(database).process(call) for call in first)), timeout=10
    )
    assert len(await _edges(database, run_id)) == 6


async def test_only_trustworthy_catalogued_calls_project(
    database: db.Database, run_id: uuid.UUID
) -> None:
    other_environment = uuid.uuid4()
    async with database.transaction() as session:
        await CatalogRepository(session).ensure_service(
            service_id=other_environment,
            workspace_id=DEMO_WORKSPACE_ID,
            name=f"elsewhere-{run_id.hex[:12]}",
            environment="staging",
        )
    envelopes = [
        await _call(database, run_id, T0, "not-in-the-catalog", "payment"),
        await _call(database, run_id, T0, f"elsewhere-{run_id.hex[:12]}"),
        await _store(
            database,
            run_id,
            T0,
            status=QualityStatus.DEGRADED,
            flags=(QualityFlag.CLOCK_SKEW,),
            downstream=("bank-gateway",),
        ),
        await _store(database, run_id, T0, kind=SignalKind.LOG),
    ]

    assert await _project(database, envelopes) == [ProcessOutcome.APPLIED] * 4
    assert await _edges(database, run_id) == []


async def test_unknown_signal_is_a_contract_error(database: db.Database, run_id: uuid.UUID) -> None:
    call = await _call(database, run_id, T0, "bank-gateway")
    payload = call.payload.model_copy(update={"signal_id": uuid.uuid4()})
    with pytest.raises(ContractError):
        await _projector(database).process(call.model_copy(update={"payload": payload}))
    assert await _edges(database, run_id) == []


async def test_projector_and_detector_consume_independently(
    database: db.Database, run_id: uuid.UUID
) -> None:
    call = await _call(database, run_id, T0, "bank-gateway")
    await _processor(database).process(call)
    assert await _edges(database, run_id) == []

    assert await _project(database, [call]) == [ProcessOutcome.APPLIED]

    assert len(await _edges(database, run_id)) == 1
    async with database.transaction() as session:
        groups = await session.scalars(
            select(ConsumerReceipt.consumer_group).where(ConsumerReceipt.event_id == call.event_id)
        )
        assert set(groups) == {DETECTOR_CONSUMER_GROUP, GRAPH_CONSUMER_GROUP}


# Deployments


async def test_deployment_gets_a_node_and_an_edge_to_its_service(
    database: db.Database, run_id: uuid.UUID
) -> None:
    deployment = await _deploy(database, run_id, T0, "v1")
    await _project(database, [deployment])

    [edge] = await _edges(database, run_id, GraphRelation.DEPLOYED_TO)
    assert edge.valid_to is None
    assert edge.provenance_signal_id == deployment.payload.signal_id  # type: ignore[attr-defined]
    async with database.transaction() as session:
        nodes = {
            node.id: node
            for node in await session.scalars(
                select(GraphNode).where(_scope(run_id).clause(GraphNode))
            )
        }
    assert nodes[edge.from_node_id].node_type == GraphNodeType.DEPLOYMENT
    assert nodes[edge.to_node_id].service_id == PAYMENT


async def test_historical_query_excludes_future_deployments(
    database: db.Database, run_id: uuid.UUID
) -> None:
    day = timedelta(days=1)
    await _project(
        database,
        [await _deploy(database, run_id, T0 + day * n, f"v{n}") for n in (0, 1, 2)],
    )

    assert await _running(database, run_id, T0 - timedelta(seconds=1)) == []
    assert await _running(database, run_id, T0) == ["v0"]
    assert await _running(database, run_id, T0 + day - timedelta(seconds=1)) == ["v0"]
    assert await _running(database, run_id, T0 + day) == ["v1"]
    assert await _running(database, run_id, T0 + day * 9) == ["v2"]


async def test_historical_answer_does_not_disclose_the_next_deployment(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(database, [await _deploy(database, run_id, T0, "v1")])
    await _project(database, [await _deploy(database, run_id, T0 + TTL, "v2")])

    async with database.transaction() as session:
        [entry] = await GraphRepository(session).deployments_as_of(
            _scope(run_id), {PAYMENT}, as_of=T0 + timedelta(minutes=1)
        )
    assert entry.deployed_at == T0
    assert entry.previous_revision == "v0"
    assert "valid_to" not in entry.model_dump()


async def test_deployments_arriving_out_of_order_get_the_same_intervals(
    database: db.Database, run_id: uuid.UUID
) -> None:
    day = timedelta(days=1)
    first = await _deploy(database, run_id, T0, "v1")
    second = await _deploy(database, run_id, T0 + day, "v2")
    third = await _deploy(database, run_id, T0 + day * 2, "v3")

    await _project(database, [third, first, second])

    edges = await _edges(database, run_id, GraphRelation.DEPLOYED_TO)
    assert [(edge.valid_from, edge.valid_to) for edge in edges] == [
        (T0, T0 + day),
        (T0 + day, T0 + day * 2),
        (T0 + day * 2, None),
    ]


async def test_deployments_of_other_services_do_not_end_each_other(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(
        database,
        [
            await _deploy(database, run_id, T0, "payment-1"),
            await _deploy(database, run_id, T0 + TTL, "checkout-1", service_id=CHECKOUT),
        ],
    )
    assert await _running(database, run_id, T0 + TTL * 2) == ["payment-1"]
    assert await _running(database, run_id, T0 + TTL * 2, CHECKOUT) == ["checkout-1"]


async def test_redelivered_deployment_changes_nothing(
    database: db.Database, run_id: uuid.UUID
) -> None:
    deployment = await _deploy(database, run_id, T0, "v1")
    await _project(database, [deployment, _redelivered(deployment)])
    assert len(await _edges(database, run_id, GraphRelation.DEPLOYED_TO)) == 1


async def test_deployments_at_one_instant_both_stay_valid(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(
        database,
        [await _deploy(database, run_id, T0, "v1"), await _deploy(database, run_id, T0, "v2")],
    )
    assert sorted(await _running(database, run_id, T0)) == ["v1", "v2"]


# Traversal


async def test_historical_query_excludes_future_dependencies(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _link(database, run_id, [(CHECKOUT, PAYMENT)])
    await _link(database, run_id, [(PAYMENT, GATEWAY)], valid_from=T0 + TTL)

    assert await _downstream(database, run_id, CHECKOUT, T0) == {PAYMENT}
    assert await _downstream(database, run_id, CHECKOUT, T0 + TTL) == {PAYMENT, GATEWAY}


async def test_a_path_cannot_pass_through_an_expired_edge(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _link(database, run_id, [(CHECKOUT, PAYMENT)], valid_to=T0 + TTL)
    await _link(database, run_id, [(PAYMENT, GATEWAY)])

    assert await _downstream(database, run_id, CHECKOUT, T0 + TTL) == set()


async def test_cycle_terminates_and_reports_each_edge_once(
    database: db.Database, run_id: uuid.UUID
) -> None:
    a, b, c = CHECKOUT, PAYMENT, GATEWAY
    await _link(database, run_id, [(a, b), (b, c), (c, a), (b, a)])

    async with database.transaction() as session:
        walk = await asyncio.wait_for(
            GraphRepository(session).dependencies(_scope(run_id), a, as_of=T0), timeout=5
        )

    assert [(e.from_service_id, e.to_service_id, e.hops) for e in walk.items] == [
        (a, b, 1),
        (b, c, 2),
    ]
    assert not walk.truncated


async def test_traversal_stops_at_three_hops(database: db.Database, run_id: uuid.UUID) -> None:
    chain = await _new_services(database, 6)
    await _link(database, run_id, list(pairwise(chain)))

    async with database.transaction() as session:
        graph = GraphRepository(session)
        walk = await graph.dependencies(_scope(run_id), chain[0], as_of=T0)
        short = await graph.dependencies(_scope(run_id), chain[0], as_of=T0, max_hops=1)
        with pytest.raises(ValueError, match="max_hops"):
            await graph.dependencies(_scope(run_id), chain[0], as_of=T0, max_hops=4)

    assert [entry.to_service_id for entry in walk.items] == chain[1:4]
    assert [entry.hops for entry in walk.items] == [1, 2, 3]
    assert [entry.to_service_id for entry in short.items] == chain[1:2]


async def test_shortest_distance_wins_for_an_edge_on_two_paths(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _link(
        database,
        run_id,
        [(CHECKOUT, PAYMENT), (CHECKOUT, INVENTORY), (INVENTORY, PAYMENT), (PAYMENT, GATEWAY)],
    )

    async with database.transaction() as session:
        walk = await GraphRepository(session).dependencies(_scope(run_id), CHECKOUT, as_of=T0)

    hops = {(e.from_service_id, e.to_service_id): e.hops for e in walk.items}
    assert hops[(PAYMENT, GATEWAY)] == 2
    assert hops[(CHECKOUT, PAYMENT)] == 1


async def test_dense_cyclic_graph_is_bounded_and_reports_truncation(
    database: db.Database, run_id: uuid.UUID
) -> None:
    services = await _new_services(database, 14)
    await _link(database, run_id, [(a, b) for a in services for b in services if a != b])

    async with database.transaction() as session:
        walk = await asyncio.wait_for(
            GraphRepository(session).dependencies(_scope(run_id), services[0], as_of=T0),
            timeout=5,
        )
        limited = await GraphRepository(session).dependencies(
            _scope(run_id), services[0], as_of=T0, limit=5, max_hops=1
        )

    assert walk.truncated
    assert 0 < len(walk.items) <= MAX_TOOL_ROWS
    assert limited.truncated
    assert [entry.hops for entry in limited.items] == [1] * 5


async def test_walk_reads_no_more_paths_than_its_bound(
    database: db.Database, run_id: uuid.UUID
) -> None:
    services = await _new_services(database, 14)
    await _link(database, run_id, [(a, b) for a in services for b in services if a != b])
    scope = _scope(run_id)

    async with database.transaction() as session:
        graph = GraphRepository(session)
        nodes = await graph.ensure_service_nodes(scope, {services[0]})
        paths = await graph._walk(
            scope, nodes[services[0]], T0, GraphDirection.DOWNSTREAM, MAX_TRAVERSAL_HOPS
        )

    assert len(paths) == MAX_WALK_PATHS + 1


async def test_upstream_walk_reports_callers(database: db.Database, run_id: uuid.UUID) -> None:
    await _link(database, run_id, [(CHECKOUT, PAYMENT), (PAYMENT, GATEWAY), (GATEWAY, INVENTORY)])

    async with database.transaction() as session:
        walk = await GraphRepository(session).dependencies(
            _scope(run_id), GATEWAY, as_of=T0, direction=GraphDirection.UPSTREAM
        )

    assert [(e.from_service_id, e.to_service_id, e.hops) for e in walk.items] == [
        (PAYMENT, GATEWAY, 1),
        (CHECKOUT, PAYMENT, 2),
    ]


async def test_graphs_stay_in_their_replay_scope_and_workspace(
    database: db.Database, run_id: uuid.UUID
) -> None:
    await _project(database, [await _call(database, run_id, T0, "bank-gateway")])

    other_run = uuid.uuid4()
    assert await _downstream(database, other_run, PAYMENT, T0) == set()
    async with database.transaction() as session:
        walk = await GraphRepository(session).dependencies(
            EvidenceScope(uuid.uuid4(), replay_scope_for(run_id)), PAYMENT, as_of=T0
        )
    assert walk.items == ()


async def test_service_without_a_node_has_no_dependencies(
    database: db.Database, run_id: uuid.UUID
) -> None:
    assert await _downstream(database, run_id, PAYMENT, T0) == set()


# Demo data


async def test_demo_fault_projects_the_call_path_in_any_order(database: db.Database) -> None:
    runs = uuid.uuid4(), uuid.uuid4()
    orders = [
        await _normalized_demo(database, T0, timedelta(minutes=5), seed=781637803, run_id=run_id)
        for run_id in runs
    ]
    random.Random(7).shuffle(orders[1])  # noqa: S311 - test data
    for envelopes in orders:
        await _project(database, envelopes)

    as_of = T0 + timedelta(minutes=5)
    expected = {INVENTORY, ORDERS, PAYMENT, GATEWAY}
    assert await _downstream(database, runs[0], CHECKOUT, as_of) == expected
    assert await _downstream(database, runs[1], CHECKOUT, as_of) == expected
    assert await _downstream(database, runs[0], CHECKOUT, T0 - timedelta(seconds=1)) == set()

    first, second = await _edges(database, runs[0]), await _edges(database, runs[1])

    def shape(edges: Sequence[GraphEdge]) -> list[tuple[datetime, datetime | None, int]]:
        return sorted((e.valid_from, e.valid_to, e.properties["observations"]) for e in edges)

    assert shape(first) == shape(second)
