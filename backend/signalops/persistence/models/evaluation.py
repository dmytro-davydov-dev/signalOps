"""Evaluation scenarios, runs, replays, and cases, in the `evaluation` schema.

The schema holds ground truth: a scenario's injection spec and expected outcome,
and the expected outcome copied onto each case. The runtime database role has no
privilege on the schema (migration 0017), so a worker, its tools, and the API's
runtime connection cannot read it even with a crafted query. Only the evaluator
role can.

A run evaluates one split of scenarios with several system variants. Each
scenario is replayed once per run into its own replay scope, and every variant
and trial investigates the same incident there, so their evidence is identical.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.common import MAX_ERROR_DETAIL_LENGTH, MAX_KEY_LENGTH, MAX_NAME_LENGTH
from signalops.contracts.evaluation import (
    CaseStatus,
    ReplayStatus,
    RunStatus,
    ScenarioFamily,
    ScenarioSplit,
    SystemVariant,
)
from signalops.persistence.base import Base
from signalops.persistence.models.common import (
    created_at,
    json_object,
    length_between,
    nonempty,
    one_of,
    uuid_pk,
)

EVALUATION_SCHEMA = "evaluation"
SCHEMA_ARGS = {"schema": EVALUATION_SCHEMA}


def _optional_object(column: str) -> str:
    return f"{column} IS NULL OR {json_object(column)}"


def _error_length(column: str = "error") -> str:
    return f"{column} IS NULL OR {length_between(column, 1, MAX_ERROR_DETAIL_LENGTH)}"


class EvaluationScenario(Base):
    """One versioned scenario variant and its ground truth. Immutable once stored."""

    __tablename__ = "scenarios"
    __table_args__ = (
        UniqueConstraint("name", "version"),
        CheckConstraint(length_between("name", 1, MAX_NAME_LENGTH), name="name_length"),
        CheckConstraint(nonempty("version"), name="version_nonempty"),
        CheckConstraint(one_of("family", ScenarioFamily), name="family"),
        CheckConstraint(one_of("split", ScenarioSplit), name="split"),
        CheckConstraint(nonempty("dataset_ref"), name="dataset_ref_nonempty"),
        CheckConstraint("dataset_hash ~ '^[0-9a-f]{64}$'", name="dataset_hash_sha256"),
        CheckConstraint("record_count >= 0", name="record_count_nonnegative"),
        CheckConstraint(json_object("injection_spec"), name="injection_spec_object"),
        CheckConstraint(json_object("expected_outcome"), name="expected_outcome_object"),
        SCHEMA_ARGS,
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    name: Mapped[str] = mapped_column(Text)
    version: Mapped[str] = mapped_column(Text)
    family: Mapped[str] = mapped_column(Text)
    split: Mapped[str] = mapped_column(Text)
    synthetic: Mapped[bool] = mapped_column(Boolean)
    dataset_ref: Mapped[str] = mapped_column(Text)
    dataset_hash: Mapped[str] = mapped_column(Text)
    record_count: Mapped[int] = mapped_column(Integer)
    injection_spec: Mapped[dict[str, Any]] = mapped_column(JSONB)
    expected_outcome: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = created_at()


class EvaluationRun(Base):
    """One benchmark execution and everything needed to reproduce and compare it."""

    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(length_between("label", 1, MAX_KEY_LENGTH), name="label_length"),
        CheckConstraint(one_of("split", ScenarioSplit), name="split"),
        CheckConstraint(one_of("status", RunStatus), name="status"),
        CheckConstraint("jsonb_typeof(variants) = 'array'", name="variants_array"),
        CheckConstraint("trials BETWEEN 1 AND 20", name="trials_range"),
        CheckConstraint("dataset_hash ~ '^[0-9a-f]{64}$'", name="dataset_hash_sha256"),
        CheckConstraint(json_object("workflow_versions"), name="workflow_versions_object"),
        CheckConstraint(json_object("prompt_versions"), name="prompt_versions_object"),
        CheckConstraint(json_object("model_config"), name="model_config_object"),
        CheckConstraint(json_object("pricing_config"), name="pricing_config_object"),
        CheckConstraint(json_object("limits"), name="limits_object"),
        CheckConstraint(_optional_object("metrics"), name="metrics_object"),
        CheckConstraint(_error_length(), name="error_length"),
        CheckConstraint(
            "(status IN ('completed', 'failed')) = (completed_at IS NOT NULL)",
            name="completed_at_status",
        ),
        CheckConstraint(
            "(lease_owner IS NULL) = (lease_expires_at IS NULL)", name="lease_complete"
        ),
        Index("ix_runs_status_created_at", "status", "created_at"),
        Index("ix_runs_workspace_id_created_at", "workspace_id", "created_at"),
        SCHEMA_ARGS,
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"))
    label: Mapped[str] = mapped_column(Text)
    split: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=RunStatus.QUEUED, server_default=RunStatus.QUEUED
    )
    variants: Mapped[list[str]] = mapped_column(JSONB)
    trials: Mapped[int] = mapped_column(Integer)
    # Recorded for reproducibility; a provider seed alone does not make output repeatable.
    seed: Mapped[int] = mapped_column(BigInteger)
    # SHA-256 over the split's scenario names, versions, and dataset hashes.
    dataset_hash: Mapped[str] = mapped_column(Text)
    workflow_versions: Mapped[dict[str, Any]] = mapped_column(JSONB)
    prompt_versions: Mapped[dict[str, Any]] = mapped_column(JSONB)
    model_config: Mapped[dict[str, Any]] = mapped_column(JSONB)
    pricing_config: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Budget, steps, and deadline every variant gets for each case.
    limits: Mapped[dict[str, Any]] = mapped_column(JSONB)
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = created_at()
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))


class EvaluationReplay(Base):
    """One scenario replayed for a run. Its ID is the replay run ID of its scope."""

    __tablename__ = "replays"
    __table_args__ = (
        UniqueConstraint("run_id", "scenario_id"),
        UniqueConstraint("replay_scope"),
        CheckConstraint(one_of("status", ReplayStatus), name="status"),
        CheckConstraint(nonempty("replay_scope"), name="replay_scope_nonempty"),
        CheckConstraint(_optional_object("report"), name="report_object"),
        Index("ix_replays_scenario_id", "scenario_id"),
        SCHEMA_ARGS,
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.runs.id"))
    scenario_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.scenarios.id"))
    replay_scope: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=ReplayStatus.PENDING, server_default=ReplayStatus.PENDING
    )
    # Reconciliation: accepted, normalized, detected, and flushed counts.
    report: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime] = created_at()
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class EvaluationCase(Base):
    """One variant's trial on one scenario: what was expected, observed, and scored."""

    __tablename__ = "cases"
    __table_args__ = (
        UniqueConstraint("run_id", "case_key"),
        CheckConstraint(length_between("case_key", 1, MAX_KEY_LENGTH), name="case_key_length"),
        CheckConstraint(one_of("variant", SystemVariant), name="variant"),
        CheckConstraint("trial >= 1", name="trial_positive"),
        CheckConstraint(one_of("status", CaseStatus), name="status"),
        CheckConstraint(json_object("expected_outcome"), name="expected_outcome_object"),
        CheckConstraint(_optional_object("observed_outcome"), name="observed_outcome_object"),
        CheckConstraint(_optional_object("scores"), name="scores_object"),
        CheckConstraint(_error_length(), name="error_length"),
        CheckConstraint(
            "(status IN ('scored', 'failed')) = (completed_at IS NOT NULL)",
            name="completed_at_status",
        ),
        Index("ix_cases_replay_id", "replay_id"),
        Index("ix_cases_scenario_id", "scenario_id"),
        Index("ix_cases_investigation_id", "investigation_id"),
        SCHEMA_ARGS,
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.runs.id"))
    replay_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.replays.id"))
    scenario_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.scenarios.id"))
    case_key: Mapped[str] = mapped_column(Text)
    variant: Mapped[str] = mapped_column(Text)
    trial: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(
        Text, default=CaseStatus.PENDING, server_default=CaseStatus.PENDING
    )
    investigation_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("investigations.id"))
    # The scenario's expected outcome without its evidence labels.
    expected_outcome: Mapped[dict[str, Any]] = mapped_column(JSONB)
    observed_outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    scores: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = created_at()
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class EvaluationAudit(Base):
    """A person's judgement of one cited signal in a case: does it support the diagnosis?

    Human audits sample semantic support that deterministic labels cannot see. They
    are reported beside the automatic scores, never mixed into them.
    """

    __tablename__ = "audits"
    __table_args__ = (
        UniqueConstraint("case_id", "signal_id", "auditor"),
        CheckConstraint(length_between("auditor", 1, MAX_NAME_LENGTH), name="auditor_length"),
        CheckConstraint("note IS NULL OR char_length(note) BETWEEN 1 AND 2000", name="note_length"),
        SCHEMA_ARGS,
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("evaluation.cases.id"))
    signal_id: Mapped[uuid.UUID]
    supported: Mapped[bool] = mapped_column(Boolean)
    auditor: Mapped[str] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = created_at()
