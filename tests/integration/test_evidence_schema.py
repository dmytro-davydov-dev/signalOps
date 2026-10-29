"""Source and evidence tables against real PostgreSQL (P02-T01)."""

from typing import Any

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, func, select

from signalops.config import Settings
from signalops.contracts.evidence import QualityStatus, Severity, SignalKind, SourceKind
from signalops.persistence.base import Base
from signalops.persistence.migrations import alembic_config
from signalops.persistence.models import (
    Deployment,
    Service,
    Signal,
    SignalAggregate,
    Source,
)
from tests.integration.schema_support import T0, World, insert, violated_constraint

pytestmark = pytest.mark.integration


def test_migrations_match_models(engine: Engine) -> None:
    with engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    assert diff == []


def test_downgrade_to_baseline_and_upgrade_again(scratch_database: Settings) -> None:
    config = alembic_config(scratch_database)
    command.upgrade(config, "head")
    command.downgrade(config, "0001_baseline")
    command.upgrade(config, "head")


def test_duplicate_source_identity_in_one_replay_scope_is_rejected(world: World) -> None:
    world.signal(source_event_id="evt-1")
    name = violated_constraint(
        world.engine, Signal, **world.signal_values(source_event_id="evt-1", message="retry")
    )
    assert name == "uq_signals_source_id_replay_scope_source_event_id"


def test_same_fixture_can_exist_in_separate_replay_runs(world: World) -> None:
    for scope in ("live", "replay-run-a", "replay-run-b"):
        world.signal(source_event_id="evt-fixture-1", replay_scope=scope)
    with world.engine.connect() as conn:
        count = conn.execute(
            select(func.count())
            .select_from(Signal)
            .where(Signal.source_id == world.source_id, Signal.source_event_id == "evt-fixture-1")
        ).scalar_one()
    assert count == 3


def test_identical_messages_with_distinct_event_ids_are_separate(world: World) -> None:
    world.signal(source_event_id="evt-a", message="same text")
    world.signal(source_event_id="evt-b", message="same text")


def test_replay_scope_defaults_to_live(world: World) -> None:
    signal_id = world.signal()
    with world.engine.connect() as conn:
        scope = conn.execute(select(Signal.replay_scope).where(Signal.id == signal_id)).scalar_one()
    assert scope == "live"


def test_unresolved_service_is_allowed_as_degraded_evidence(world: World) -> None:
    world.signal(service_id=None, quality_status=QualityStatus.DEGRADED, quality_flags=["x"])


def test_signal_cannot_cite_another_workspace(world: World, engine: Engine) -> None:
    other = World(engine)
    assert (
        violated_constraint(engine, Signal, **world.signal_values(source_id=other.source_id))
        == "fk_signals_workspace_id_sources"
    )
    assert (
        violated_constraint(engine, Signal, **world.signal_values(service_id=other.service_id))
        == "fk_signals_workspace_id_services"
    )


def test_deployment_must_match_its_signal(world: World, engine: Engine) -> None:
    signal_id = world.signal(kind=SignalKind.DEPLOYMENT, severity=Severity.INFO)
    base = {
        "workspace_id": world.workspace_id,
        "service_id": world.service_id,
        "signal_id": signal_id,
        "revision": "payment@1.4.2",
        "deployed_at": T0,
    }
    other_service = insert(
        engine, Service, workspace_id=world.workspace_id, name="payment", environment="demo"
    )
    assert (
        violated_constraint(engine, Deployment, **(base | {"service_id": other_service}))
        == "fk_deployments_workspace_id_signals"
    )
    assert (
        violated_constraint(engine, Deployment, **(base | {"replay_scope": "replay-run-a"}))
        == "fk_deployments_workspace_id_signals"
    )
    insert(engine, Deployment, **base)
    assert violated_constraint(engine, Deployment, **base) == "uq_deployments_signal_id"


@pytest.mark.parametrize(
    ("column", "values"),
    [("kind", SignalKind), ("severity", Severity), ("quality_status", QualityStatus)],
)
def test_signal_enumerations(world: World, column: str, values: type[SignalKind]) -> None:
    for value in values:
        world.signal(**{column: value})
    name = violated_constraint(world.engine, Signal, **world.signal_values(**{column: "bogus"}))
    assert name == f"ck_signals_{column}"


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"source_event_id": ""}, "ck_signals_source_event_id_length"),
        ({"source_event_id": "x" * 513}, "ck_signals_source_event_id_length"),
        ({"replay_scope": ""}, "ck_signals_replay_scope_length"),
        ({"attributes": ["not", "an", "object"]}, "ck_signals_attributes_object"),
        ({"quality_flags": {"not": "an array"}}, "ck_signals_quality_flags_array"),
    ],
)
def test_signal_field_checks(world: World, overrides: dict[str, Any], constraint: str) -> None:
    assert (
        violated_constraint(world.engine, Signal, **world.signal_values(**overrides)) == constraint
    )


def test_source_kind_and_name_uniqueness(world: World) -> None:
    for kind in SourceKind:
        insert(world.engine, Source, workspace_id=world.workspace_id, name=f"s-{kind}", kind=kind)
    bad_kind = {"workspace_id": world.workspace_id, "name": "metrics", "kind": "metric"}
    assert violated_constraint(world.engine, Source, **bad_kind) == "ck_sources_kind"
    duplicate = {"workspace_id": world.workspace_id, "name": "checkout-logs", "kind": "deployment"}
    assert violated_constraint(world.engine, Source, **duplicate) == "uq_sources_workspace_id_name"


def test_service_identity_is_name_plus_environment(world: World) -> None:
    insert(
        world.engine, Service, workspace_id=world.workspace_id, name="checkout", environment="prod"
    )
    duplicate = {"workspace_id": world.workspace_id, "name": "checkout", "environment": "demo"}
    assert (
        violated_constraint(world.engine, Service, **duplicate)
        == "uq_services_workspace_id_name_environment"
    )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"window_end": T0}, "ck_signal_aggregates_window_order"),
        ({"sample_count": -1, "error_count": 0}, "ck_signal_aggregates_counts_range"),
        ({"error_count": -1}, "ck_signal_aggregates_counts_range"),
        ({"error_count": 11}, "ck_signal_aggregates_counts_range"),
    ],
)
def test_aggregate_checks(world: World, overrides: dict[str, Any], constraint: str) -> None:
    values = world.aggregate_values(**overrides)
    assert violated_constraint(world.engine, SignalAggregate, **values) == constraint


def test_aggregate_window_is_unique_per_replay_scope(world: World) -> None:
    insert(world.engine, SignalAggregate, **world.aggregate_values())
    insert(world.engine, SignalAggregate, **world.aggregate_values(replay_scope="replay-run-a"))
    assert (
        violated_constraint(world.engine, SignalAggregate, **world.aggregate_values())
        == "uq_signal_aggregates_window"
    )
