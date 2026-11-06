"""Graph and reliability tables against real PostgreSQL (P02-T03)."""

import json
import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, delete, text
from sqlalchemy.exc import IntegrityError

from signalops.config import Settings
from signalops.contracts.evidence import SignalKind
from signalops.contracts.graph import GraphNodeType, GraphRelation
from signalops.contracts.workflow import IncidentSeverity
from signalops.persistence.migrations import upgrade_to_head
from signalops.persistence.models import (
    ConsumerReceipt,
    Deployment,
    GraphEdge,
    GraphNode,
    Incident,
    OutboxEvent,
)
from tests.integration.schema_support import (
    T0,
    World,
    constraint_name,
    insert,
    insert_row,
    violated_constraint,
)

pytestmark = pytest.mark.integration


class Graph(World):
    """A workspace with evidence, plus helpers for graph rows."""

    def service_node(self, replay_scope: str = "live", /, **overrides: Any) -> uuid.UUID:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "replay_scope": replay_scope,
            "node_type": GraphNodeType.SERVICE,
            "external_key": str(self.service_id),
            "service_id": self.service_id,
        }
        values.update(overrides)
        return insert(self.engine, GraphNode, **values)

    def runbook_node(self, key: str = "runbooks/checkout.md", **overrides: Any) -> uuid.UUID:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "node_type": GraphNodeType.RUNBOOK,
            "external_key": key,
        }
        values.update(overrides)
        return insert(self.engine, GraphNode, **values)

    def incident(self, replay_scope: str = "live") -> uuid.UUID:
        return insert(
            self.engine,
            Incident,
            workspace_id=self.workspace_id,
            replay_scope=replay_scope,
            fingerprint=f"checkout:error-rate:{uuid.uuid4()}",
            correlation_window_start=T0,
            title="Checkout error rate above threshold",
            severity=IncidentSeverity.HIGH,
            detected_at=T0,
            first_seen_at=T0,
            last_seen_at=T0,
        )

    def deployment(self, replay_scope: str = "live") -> uuid.UUID:
        signal_id = self.signal(kind=SignalKind.DEPLOYMENT, replay_scope=replay_scope)
        return insert(
            self.engine,
            Deployment,
            workspace_id=self.workspace_id,
            service_id=self.service_id,
            signal_id=signal_id,
            replay_scope=replay_scope,
            revision="v42",
            deployed_at=T0,
        )

    def edge_values(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "replay_scope": "live",
            "relation": GraphRelation.DOCUMENTED_BY,
            "valid_from": T0,
            "confidence": 0.9,
        }
        # Fresh endpoints, so several edges can share one workspace.
        for endpoint in ("from_node_id", "to_node_id"):
            if endpoint not in overrides:
                values[endpoint] = self.runbook_node(key=f"runbooks/{uuid.uuid4()}.md")
        values.update(overrides)
        return values

    def edge(self, **overrides: Any) -> uuid.UUID:
        return insert(self.engine, GraphEdge, **self.edge_values(**overrides))


@pytest.fixture
def graph(engine: Engine) -> Graph:
    return Graph(engine)


# Graph nodes


def test_entity_nodes_reference_their_rows(graph: Graph) -> None:
    graph.service_node()
    incident_id = graph.incident()
    graph.service_node(
        node_type=GraphNodeType.INCIDENT,
        external_key=str(incident_id),
        service_id=None,
        incident_id=incident_id,
    )
    deployment_id = graph.deployment()
    graph.service_node(
        node_type=GraphNodeType.DEPLOYMENT,
        external_key=str(deployment_id),
        service_id=None,
        deployment_id=deployment_id,
    )
    graph.runbook_node()


def test_duplicate_node_is_rejected(graph: Graph) -> None:
    graph.service_node()
    with pytest.raises(IntegrityError) as excinfo:
        graph.service_node()
    assert constraint_name(excinfo.value) == "uq_graph_nodes_identity"


def test_same_entity_can_have_a_node_per_replay_scope(graph: Graph) -> None:
    graph.service_node("live")
    graph.service_node("replay-run-a")


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"service_id": None}, id="service-without-reference"),
        pytest.param({"external_key": "checkout"}, id="key-not-the-referenced-id"),
        pytest.param({"incident_id": uuid.uuid4()}, id="second-reference"),
        pytest.param({"node_type": GraphNodeType.RUNBOOK}, id="runbook-with-reference"),
    ],
)
def test_entity_reference_must_match_node_type(graph: Graph, overrides: dict[str, Any]) -> None:
    with pytest.raises(IntegrityError) as excinfo:
        graph.service_node(**overrides)
    assert constraint_name(excinfo.value) == "ck_graph_nodes_entity_reference"


def test_node_cannot_reference_another_workspace(graph: Graph, engine: Engine) -> None:
    other = World(engine)
    with pytest.raises(IntegrityError) as excinfo:
        graph.service_node(external_key=str(other.service_id), service_id=other.service_id)
    assert constraint_name(excinfo.value) == "fk_graph_nodes_workspace_id_services"


@pytest.mark.parametrize("kind", ["incident", "deployment"])
def test_node_cannot_reference_another_replay_scope(graph: Graph, kind: str) -> None:
    row_id = graph.incident("replay-run-a") if kind == "incident" else graph.deployment("other")
    with pytest.raises(IntegrityError) as excinfo:
        graph.service_node(
            node_type=kind,
            external_key=str(row_id),
            service_id=None,
            **{f"{kind}_id": row_id},
        )
    assert constraint_name(excinfo.value) == f"fk_graph_nodes_workspace_id_{kind}s"


def test_node_type_rejects_unknown_values(graph: Graph) -> None:
    with pytest.raises(IntegrityError) as excinfo:
        graph.runbook_node(node_type="metric")
    assert constraint_name(excinfo.value) == "ck_graph_nodes_node_type"


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"external_key": ""}, "ck_graph_nodes_external_key_length"),
        ({"external_key": "x" * 513}, "ck_graph_nodes_external_key_length"),
        ({"replay_scope": ""}, "ck_graph_nodes_replay_scope_length"),
        ({"properties": []}, "ck_graph_nodes_properties_object"),
    ],
)
def test_node_checks(graph: Graph, overrides: dict[str, Any], constraint: str) -> None:
    with pytest.raises(IntegrityError) as excinfo:
        graph.runbook_node(**overrides)
    assert constraint_name(excinfo.value) == constraint


# Graph edges


@pytest.mark.parametrize("endpoint", ["from", "to"])
def test_dangling_edge_is_rejected(graph: Graph, endpoint: str) -> None:
    values = graph.edge_values(**{f"{endpoint}_node_id": uuid.uuid4()})
    assert (
        violated_constraint(graph.engine, GraphEdge, **values) == f"fk_graph_edges_{endpoint}_node"
    )


def test_node_with_edges_cannot_be_deleted(graph: Graph) -> None:
    node_id = graph.service_node()
    graph.edge(from_node_id=node_id)
    with pytest.raises(IntegrityError) as excinfo, graph.engine.begin() as conn:
        conn.execute(delete(GraphNode).where(GraphNode.id == node_id))
    assert constraint_name(excinfo.value) == "fk_graph_edges_from_node"


def test_edge_cannot_join_replay_scopes(graph: Graph) -> None:
    replay_node = graph.service_node("replay-run-a")
    values = graph.edge_values(to_node_id=replay_node)
    assert violated_constraint(graph.engine, GraphEdge, **values) == "fk_graph_edges_to_node"


def test_edge_cannot_join_workspaces(graph: Graph, engine: Engine) -> None:
    other = Graph(engine)
    values = graph.edge_values(to_node_id=other.runbook_node())
    assert violated_constraint(engine, GraphEdge, **values) == "fk_graph_edges_to_node"


def test_edge_provenance_must_share_scope(graph: Graph) -> None:
    graph.edge(provenance_signal_id=graph.signal())
    graph.edge(provenance_signal_id=None)
    values = graph.edge_values(provenance_signal_id=graph.signal(replay_scope="replay-run-a"))
    assert violated_constraint(graph.engine, GraphEdge, **values) == (
        "fk_graph_edges_workspace_id_signals"
    )


def test_open_and_closed_validity_intervals_are_accepted(graph: Graph) -> None:
    graph.edge(valid_to=None)
    graph.edge(valid_to=T0 + timedelta(hours=1))


def test_edges_of_one_relationship_cannot_overlap(graph: Graph) -> None:
    pair = {
        "from_node_id": graph.runbook_node(key=f"runbooks/{uuid.uuid4()}.md"),
        "to_node_id": graph.runbook_node(key=f"runbooks/{uuid.uuid4()}.md"),
    }
    graph.edge(**pair, valid_to=T0 + timedelta(hours=1))
    graph.edge(**pair, valid_from=T0 + timedelta(hours=1), valid_to=None)
    overlapping = graph.edge_values(**pair, valid_from=T0 + timedelta(hours=2))
    assert violated_constraint(graph.engine, GraphEdge, **overlapping) == "ex_graph_edges_validity"


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"valid_to": T0}, "ck_graph_edges_validity_interval"),
        ({"valid_to": T0 - timedelta(seconds=1)}, "ck_graph_edges_validity_interval"),
        ({"confidence": -0.01}, "ck_graph_edges_confidence_range"),
        ({"confidence": 1.01}, "ck_graph_edges_confidence_range"),
        ({"relation": "CAUSES"}, "ck_graph_edges_relation"),
        ({"properties": "x"}, "ck_graph_edges_properties_object"),
    ],
)
def test_edge_checks(graph: Graph, overrides: dict[str, Any], constraint: str) -> None:
    values = graph.edge_values(**overrides)
    assert violated_constraint(graph.engine, GraphEdge, **values) == constraint


def test_self_loop_is_rejected(graph: Graph) -> None:
    node_id = graph.service_node()
    values = graph.edge_values(from_node_id=node_id, to_node_id=node_id)
    assert violated_constraint(graph.engine, GraphEdge, **values) == "ck_graph_edges_no_self_loop"


@pytest.mark.parametrize("relation", list(GraphRelation))
def test_every_relation_is_accepted(graph: Graph, relation: GraphRelation) -> None:
    graph.edge(relation=relation)


def test_edge_indexes_serve_both_directions(graph: Graph) -> None:
    with graph.engine.connect() as conn:
        indexes = {
            row.indexname: row.indexdef
            for row in conn.execute(
                text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'graph_edges'")
            )
        }
    assert "(from_node_id, relation)" in indexes["ix_graph_edges_from_node_id_relation"]
    assert "(to_node_id, relation)" in indexes["ix_graph_edges_to_node_id_relation"]


# Consumer receipts


def test_duplicate_receipt_is_rejected(engine: Engine) -> None:
    event_id = uuid.uuid4()
    insert_row(engine, ConsumerReceipt, consumer_group="normalizer", event_id=event_id)
    assert (
        violated_constraint(engine, ConsumerReceipt, consumer_group="normalizer", event_id=event_id)
        == "pk_consumer_receipts"
    )


def test_same_event_can_be_processed_by_each_group(engine: Engine) -> None:
    event_id = uuid.uuid4()
    insert_row(engine, ConsumerReceipt, consumer_group="normalizer", event_id=event_id)
    insert_row(engine, ConsumerReceipt, consumer_group="detector", event_id=event_id)


def test_receipt_group_must_be_named(engine: Engine) -> None:
    assert (
        violated_constraint(engine, ConsumerReceipt, consumer_group="", event_id=uuid.uuid4())
        == "ck_consumer_receipts_consumer_group_length"
    )


# Outbox


def outbox_values(**overrides: Any) -> dict[str, Any]:
    aggregate_id = overrides.get("aggregate_id", uuid.uuid4())
    values: dict[str, Any] = {
        "aggregate_id": aggregate_id,
        "aggregate_version": 1,
        "topic": "signalops.incidents.v1",
        "message_key": str(aggregate_id),
        "envelope": {"event_id": str(uuid.uuid4()), "schema_version": 1},
    }
    values.update(overrides)
    return values


def test_duplicate_outbox_event_is_rejected(engine: Engine) -> None:
    values = outbox_values()
    insert(engine, OutboxEvent, **values)
    assert (
        violated_constraint(engine, OutboxEvent, **values)
        == "uq_outbox_events_aggregate_id_aggregate_version_topic"
    )


def test_one_aggregate_version_may_go_to_several_topics(engine: Engine) -> None:
    values = outbox_values()
    insert(engine, OutboxEvent, **values)
    insert(engine, OutboxEvent, **{**values, "topic": "signalops.investigation-requests.v1"})
    insert(engine, OutboxEvent, **{**values, "aggregate_version": 2})


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"aggregate_version": 0}, "ck_outbox_events_aggregate_version_positive"),
        ({"topic": ""}, "ck_outbox_events_topic_length"),
        ({"message_key": ""}, "ck_outbox_events_message_key_nonempty"),
        ({"envelope": ["x"]}, "ck_outbox_events_envelope_object"),
        ({"attempts": -1}, "ck_outbox_events_attempts_nonnegative"),
        (
            {"created_at": T0, "published_at": T0 - timedelta(seconds=1)},
            "ck_outbox_events_published_after_created",
        ),
    ],
)
def test_outbox_checks(engine: Engine, overrides: dict[str, Any], constraint: str) -> None:
    assert violated_constraint(engine, OutboxEvent, **outbox_values(**overrides)) == constraint


def test_new_outbox_event_is_pending(engine: Engine) -> None:
    event_id = insert(engine, OutboxEvent, **outbox_values())
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT published_at, attempts FROM outbox_events WHERE id = :id"),
            {"id": event_id},
        ).one()
    assert row.published_at is None
    assert row.attempts == 0


# Same filter and order as OutboxRepository.pending.
PENDING_LOOKUP = text(
    "SELECT id, aggregate_id, aggregate_version, topic, message_key, envelope "
    "FROM outbox_events WHERE published_at IS NULL "
    "ORDER BY created_at, aggregate_version LIMIT 100"
)


def plan_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [plan]
    for child in plan.get("Plans", []):
        nodes.extend(plan_nodes(child))
    return nodes


def test_pending_outbox_lookup_uses_partial_index(scratch_database: Settings) -> None:
    """With mostly published rows, the publisher query reads only the partial index.

    Uses its own database so the shared module database's pending rows do not count.
    """
    upgrade_to_head(scratch_database)
    engine = create_engine(scratch_database.validated_database_url())
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO outbox_events "
                    "(aggregate_id, aggregate_version, topic, message_key, envelope, "
                    " created_at, published_at) "
                    "SELECT gen_random_uuid(), 1, 'signalops.incidents.v1', 'k', '{}'::jsonb, "
                    "       now() - make_interval(secs => n), "
                    "       CASE WHEN n % 1000 = 0 THEN NULL ELSE now() END "
                    "FROM generate_series(1, 20000) AS n"
                )
            )
            conn.execute(text("ANALYZE outbox_events"))
        with engine.connect() as conn:
            explained = conn.execute(text(f"EXPLAIN (FORMAT JSON) {PENDING_LOOKUP}")).scalar_one()
            pending = conn.execute(PENDING_LOOKUP).all()
    finally:
        engine.dispose()
    plan = (explained if isinstance(explained, list) else json.loads(explained))[0]["Plan"]
    scans = [node for node in plan_nodes(plan) if "Scan" in node["Node Type"]]
    assert [(node["Node Type"], node.get("Index Name")) for node in scans] == [
        ("Index Scan", "ix_outbox_events_created_at")
    ]
    assert len(pending) == 20
