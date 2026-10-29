"""Helpers for schema tests that insert rows into a migrated scratch database."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import Engine, Table
from sqlalchemy.exc import IntegrityError

from signalops.contracts.evidence import QualityStatus, Severity, SignalKind, SourceKind
from signalops.persistence.base import Base
from signalops.persistence.models import Service, Signal, Source, Workspace

T0 = datetime(2026, 10, 16, 10, 40, tzinfo=UTC)


def insert_row(engine: Engine, model: type[Base], /, **values: Any) -> None:
    table = model.__table__
    assert isinstance(table, Table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(**values))


def insert(engine: Engine, model: type[Base], /, **values: Any) -> uuid.UUID:
    """Insert into a table with a UUID `id` and return it."""
    values.setdefault("id", uuid.uuid4())
    insert_row(engine, model, **values)
    return uuid.UUID(str(values["id"]))


def constraint_name(error: IntegrityError) -> str:
    name = error.orig.diag.constraint_name  # type: ignore[union-attr]
    assert isinstance(name, str)
    return name


def violated_constraint(engine: Engine, model: type[Base], /, **values: Any) -> str:
    with pytest.raises(IntegrityError) as excinfo:
        insert_row(engine, model, **values)
    return constraint_name(excinfo.value)


class World:
    """One workspace with a source and a service, plus helpers for evidence rows."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.workspace_id = insert(engine, Workspace, name="demo")
        self.source_id = insert(
            engine,
            Source,
            workspace_id=self.workspace_id,
            name="checkout-logs",
            kind=SourceKind.STRUCTURED_LOG,
        )
        self.service_id = insert(
            engine, Service, workspace_id=self.workspace_id, name="checkout", environment="demo"
        )

    def signal_values(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "source_id": self.source_id,
            "service_id": self.service_id,
            "source_event_id": f"evt-{uuid.uuid4()}",
            "kind": SignalKind.LOG,
            "occurred_at": T0,
            "ingested_at": T0 + timedelta(seconds=1),
            "severity": Severity.ERROR,
            "message": "upstream timeout calling payment",
            "quality_status": QualityStatus.ACCEPTED,
        }
        values.update(overrides)
        return values

    def signal(self, **overrides: Any) -> uuid.UUID:
        return insert(self.engine, Signal, **self.signal_values(**overrides))

    def aggregate_values(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "service_id": self.service_id,
            "metric_name": "http.errors",
            "window_start": T0,
            "window_end": T0 + timedelta(minutes=1),
            "sample_count": 10,
            "error_count": 2,
        }
        values.update(overrides)
        return values
