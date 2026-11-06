"""Workspaces, sources, services, and the evidence derived from them.

Workspace-owned relationships use composite foreign keys that include
`workspace_id`, so a row can never reference a parent in another workspace.
Each parent exposes `UNIQUE(workspace_id, id)` as the target for those keys.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.common import MAX_REPLAY_SCOPE_LENGTH, MAX_SOURCE_EVENT_ID_LENGTH
from signalops.contracts.evidence import (
    QualityStatus,
    Severity,
    SignalKind,
    SourceKind,
)
from signalops.detection.rules import WindowClosure
from signalops.persistence.base import Base
from signalops.persistence.models.common import (
    created_at,
    json_attributes,
    json_object,
    nonempty,
    one_of,
    replay_scope,
    uuid_pk,
)


class Workspace(Base):
    __tablename__ = "workspaces"
    __table_args__ = (CheckConstraint(nonempty("name"), name="name_nonempty"),)

    id: Mapped[uuid.UUID] = uuid_pk()
    name: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = created_at()


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("workspace_id", "name"),
        UniqueConstraint("workspace_id", "id"),
        CheckConstraint(nonempty("name"), name="name_nonempty"),
        CheckConstraint(one_of("kind", SourceKind), name="kind"),
        # Holds secret references only, never credential values.
        CheckConstraint(json_object("config"), name="config_object"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"))
    name: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    config: Mapped[dict[str, Any]] = json_attributes()
    enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    created_at: Mapped[datetime] = created_at()


class Service(Base):
    __tablename__ = "services"
    __table_args__ = (
        UniqueConstraint("workspace_id", "name", "environment"),
        UniqueConstraint("workspace_id", "id"),
        CheckConstraint(nonempty("name"), name="name_nonempty"),
        CheckConstraint(nonempty("environment"), name="environment_nonempty"),
        CheckConstraint(json_object("attributes"), name="attributes_object"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"))
    name: Mapped[str] = mapped_column(Text)
    environment: Mapped[str] = mapped_column(Text)
    attributes: Mapped[dict[str, Any]] = json_attributes()
    created_at: Mapped[datetime] = created_at()


class Signal(Base):
    __tablename__ = "signals"
    __table_args__ = (
        # The source and service keys also pin the signal's workspace.
        ForeignKeyConstraint(["workspace_id", "source_id"], ["sources.workspace_id", "sources.id"]),
        # MATCH SIMPLE: a NULL service_id (unresolved identity) skips this check.
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        UniqueConstraint("source_id", "replay_scope", "source_event_id"),
        # Target for deployments, which must agree with their signal on all four.
        UniqueConstraint("workspace_id", "replay_scope", "service_id", "id"),
        # Target for incident evidence, which must share the incident's replay scope.
        UniqueConstraint("workspace_id", "replay_scope", "id"),
        CheckConstraint(
            f"char_length(source_event_id) BETWEEN 1 AND {MAX_SOURCE_EVENT_ID_LENGTH}",
            name="source_event_id_length",
        ),
        CheckConstraint(
            f"char_length(replay_scope) BETWEEN 1 AND {MAX_REPLAY_SCOPE_LENGTH}",
            name="replay_scope_length",
        ),
        CheckConstraint(one_of("kind", SignalKind), name="kind"),
        CheckConstraint(one_of("severity", Severity), name="severity"),
        CheckConstraint(one_of("quality_status", QualityStatus), name="quality_status"),
        CheckConstraint(json_object("attributes"), name="attributes_object"),
        CheckConstraint("jsonb_typeof(quality_flags) = 'array'", name="quality_flags_array"),
        # Evidence queries (0008): keyset pages within a scope, optionally per
        # service, scanned in either direction; quality counts per source.
        Index(
            "ix_signals_workspace_id_replay_scope_occurred_at",
            "workspace_id",
            "replay_scope",
            "occurred_at",
            "id",
        ),
        Index(
            "ix_signals_service_id_replay_scope_occurred_at",
            "service_id",
            "replay_scope",
            "occurred_at",
            "id",
        ),
        Index(
            "ix_signals_source_id_replay_scope_ingested_at",
            "source_id",
            "replay_scope",
            "ingested_at",
        ),
        Index("ix_signals_trace_id", "trace_id", postgresql_where=text("trace_id IS NOT NULL")),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    source_id: Mapped[uuid.UUID]
    service_id: Mapped[uuid.UUID | None]
    source_event_id: Mapped[str] = mapped_column(Text)
    replay_scope: Mapped[str] = replay_scope()
    kind: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    severity: Mapped[str] = mapped_column(Text)
    message: Mapped[str] = mapped_column(Text)
    trace_id: Mapped[str | None] = mapped_column(Text)
    attributes: Mapped[dict[str, Any]] = json_attributes()
    quality_status: Mapped[str] = mapped_column(Text)
    quality_flags: Mapped[list[str]] = mapped_column(
        JSONB, default=list, server_default=text("'[]'::jsonb")
    )


class Deployment(Base):
    __tablename__ = "deployments"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        # The deployment's workspace, replay scope, and service must match its signal.
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "service_id", "signal_id"],
            [
                "signals.workspace_id",
                "signals.replay_scope",
                "signals.service_id",
                "signals.id",
            ],
        ),
        UniqueConstraint("signal_id"),
        # Target for graph nodes in the same replay scope.
        UniqueConstraint("workspace_id", "replay_scope", "id"),
        CheckConstraint(nonempty("revision"), name="revision_nonempty"),
        CheckConstraint(json_object("attributes"), name="attributes_object"),
        Index(
            "ix_deployments_service_id_replay_scope_deployed_at",
            "service_id",
            "replay_scope",
            text("deployed_at DESC"),
        ),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    service_id: Mapped[uuid.UUID]
    signal_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = replay_scope()
    revision: Mapped[str] = mapped_column(Text)
    deployed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    attributes: Mapped[dict[str, Any]] = json_attributes()


class SignalAggregate(Base):
    __tablename__ = "signal_aggregates"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        # Explicit name: the conventional one exceeds PostgreSQL's 63-character limit.
        UniqueConstraint(
            "service_id",
            "replay_scope",
            "metric_name",
            "window_start",
            name="uq_signal_aggregates_window",
        ),
        CheckConstraint(nonempty("metric_name"), name="metric_name_nonempty"),
        CheckConstraint("window_end > window_start", name="window_order"),
        # Also implies a nonnegative sample_count.
        CheckConstraint("0 <= error_count AND error_count <= sample_count", name="counts_range"),
        CheckConstraint(json_object("histogram"), name="histogram_object"),
        CheckConstraint("late_count >= 0", name="late_count_nonnegative"),
        CheckConstraint("(closed_at IS NULL) = (closed_by IS NULL)", name="closure_complete"),
        CheckConstraint(one_of("closed_by", WindowClosure), name="closed_by_known"),
        # Finds windows due for closure.
        Index(
            "ix_signal_aggregates_open",
            "replay_scope",
            "window_end",
            postgresql_where=text("closed_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    service_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = replay_scope()
    metric_name: Mapped[str] = mapped_column(Text)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    sample_count: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    error_count: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    value_sum: Mapped[float] = mapped_column(Double, default=0.0, server_default=text("0"))
    histogram: Mapped[dict[str, Any]] = json_attributes()
    # Samples past the lateness deadline: never in the counts above, never evaluated.
    late_count: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_by: Mapped[str | None] = mapped_column(Text)
