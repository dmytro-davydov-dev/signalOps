"""Detector state beside `signal_aggregates`: counted samples and rule triggers.

`detection_samples` records which signals a window series already counts. Its
key is the business key of a contribution, so a signal reaching the detector
again under another event ID still adds to its window once. Samples follow
their signal's retention (`ON DELETE CASCADE`); aggregates outlive them.

`detection_triggers` records each time a rule crossed its threshold for a
service's window. One row per (service, scope, rule, window); the incident
writer (P05-T03) groups them.

`detection_clocks` holds each service's simulated clock in a replay scope: the
latest `occurred_at` counted. A service's signals share a partition, so its
clock advances in that partition's order regardless of other services.
"""

import uuid
from datetime import datetime

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
    false,
)
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.common import LIVE_REPLAY_SCOPE, MAX_REPLAY_SCOPE_LENGTH
from signalops.persistence.base import Base
from signalops.persistence.models.common import (
    created_at,
    length_between,
    nonempty,
    replay_scope,
    uuid_pk,
)


class DetectionSample(Base):
    __tablename__ = "detection_samples"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        CheckConstraint(nonempty("metric_name"), name="metric_name_nonempty"),
        CheckConstraint("value IS NULL OR value >= 0", name="value_nonnegative"),
        # Recomputing a window from its samples (late data, P05-T02).
        Index(
            "ix_detection_samples_window",
            "service_id",
            "replay_scope",
            "metric_name",
            "window_start",
        ),
    )

    signal_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("signals.id", ondelete="CASCADE"), primary_key=True
    )
    metric_name: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[uuid.UUID]
    service_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = replay_scope()
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    is_error: Mapped[bool]
    value: Mapped[float | None] = mapped_column(Double)
    # Past the window's lateness deadline: kept as evidence, not in the aggregate's counts.
    late: Mapped[bool] = mapped_column(default=False, server_default=false())
    recorded_at: Mapped[datetime] = created_at()


class DetectionTrigger(Base):
    __tablename__ = "detection_triggers"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        # Also serves the cooldown lookup: one rule's recent windows for a service.
        UniqueConstraint(
            "service_id",
            "replay_scope",
            "rule_id",
            "window_start",
            name="uq_detection_triggers_window",
        ),
        CheckConstraint(
            length_between("replay_scope", 1, MAX_REPLAY_SCOPE_LENGTH), name="replay_scope_length"
        ),
        CheckConstraint(nonempty("rule_id"), name="rule_id_nonempty"),
        CheckConstraint("rule_version >= 1", name="rule_version_positive"),
        CheckConstraint(nonempty("metric_name"), name="metric_name_nonempty"),
        CheckConstraint("window_end > window_start", name="window_order"),
        CheckConstraint("0 <= error_count AND error_count <= sample_count", name="counts_range"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    service_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = replay_scope()
    rule_id: Mapped[str] = mapped_column(Text)
    rule_version: Mapped[int]
    metric_name: Mapped[str] = mapped_column(Text)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    observed: Mapped[float] = mapped_column(Double)
    threshold: Mapped[float] = mapped_column(Double)
    sample_count: Mapped[int] = mapped_column(BigInteger)
    error_count: Mapped[int] = mapped_column(BigInteger)
    triggered_at: Mapped[datetime] = created_at()


class DetectionClock(Base):
    __tablename__ = "detection_clocks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        CheckConstraint(
            length_between("replay_scope", 1, MAX_REPLAY_SCOPE_LENGTH), name="replay_scope_length"
        ),
        # Live windows close by wall clock.
        CheckConstraint(f"replay_scope <> '{LIVE_REPLAY_SCOPE}'", name="replay_only"),
    )

    service_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    replay_scope: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[uuid.UUID]
    clock_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
