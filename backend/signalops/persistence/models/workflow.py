"""Incidents, investigations, durable jobs, agent steps, calls, and progress events.

Incident evidence and investigations carry `workspace_id` and `replay_scope` in
composite foreign keys, so an incident can only cite signals from its own replay
run and an investigation always shares its incident's scope. Rows below the
investigation (jobs, steps, calls, progress) inherit scope through it.

Allowed status transitions are enforced by triggers created in the migration,
from the maps in `signalops.contracts.workflow`; Alembic does not track them.
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
    Identity,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from signalops.contracts.common import MAX_KEY_LENGTH, MAX_REPLAY_SCOPE_LENGTH
from signalops.contracts.investigations import DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS
from signalops.contracts.workflow import (
    TERMINAL_INVESTIGATION_STATUSES,
    AgentRole,
    CostBasis,
    IncidentServiceRole,
    IncidentSeverity,
    IncidentStatus,
    InvestigationStatus,
    JobStatus,
    LlmCallStatus,
    StepStatus,
    ToolCallStatus,
    ToolName,
)
from signalops.persistence.base import Base
from signalops.persistence.models.common import (
    created_at,
    json_attributes,
    json_object,
    length_between,
    nonempty,
    one_of,
    replay_scope,
    uuid_pk,
)


def nonnegative(column: str) -> str:
    return f"{column} >= 0"


def optional_nonnegative(column: str) -> str:
    return f"{column} IS NULL OR {column} >= 0"


def optional_json_object(column: str) -> str:
    return f"{column} IS NULL OR {json_object(column)}"


def optional_json() -> Mapped[dict[str, Any] | None]:
    # none_as_null stores Python None as SQL NULL rather than the JSON value null.
    return mapped_column(JSONB(none_as_null=True))


def counter(default: int = 0) -> Mapped[int]:
    return mapped_column(Integer, default=default, server_default=text(str(default)))


def big_counter(default: int = 0) -> Mapped[int]:
    return mapped_column(BigInteger, default=default, server_default=text(str(default)))


def scope_column() -> Mapped[str]:
    """Replay scope copied from a parent; no default, so callers must state it."""
    return mapped_column(Text)


TERMINAL_INVESTIGATION = one_of(
    "status", [s for s in InvestigationStatus if s in TERMINAL_INVESTIGATION_STATUSES]
)


def diagnosis_status_rule() -> str:
    """CHECK text requiring the diagnosis that the contract mapping prescribes.

    IS NOT DISTINCT FROM makes a missing diagnosis fail the comparison instead of
    yielding NULL, which a CHECK would accept. Unknown statuses are left to the
    status enumeration check.
    """
    with_diagnosis = [
        f"(status = '{status}' AND diagnosis ->> 'status' IS NOT DISTINCT FROM '{diagnosis}')"
        for status, diagnosis in DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.items()
        if diagnosis is not None
    ]
    diagnosed = [s for s, d in DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS.items() if d is not None]
    without = f"(NOT ({one_of('status', diagnosed)}) AND diagnosis IS NULL)"
    return " OR ".join([*with_diagnosis, without])


class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (
        # Explicit name: the conventional one exceeds PostgreSQL's 63-character limit.
        UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "fingerprint",
            "correlation_window_start",
            name="uq_incidents_correlation",
        ),
        UniqueConstraint("workspace_id", "id"),
        # Target for incident evidence and investigations in the same replay scope.
        UniqueConstraint("workspace_id", "replay_scope", "id"),
        CheckConstraint(
            length_between("replay_scope", 1, MAX_REPLAY_SCOPE_LENGTH),
            name="replay_scope_length",
        ),
        CheckConstraint(nonempty("fingerprint"), name="fingerprint_nonempty"),
        CheckConstraint(nonempty("title"), name="title_nonempty"),
        CheckConstraint(one_of("severity", IncidentSeverity), name="severity"),
        CheckConstraint(one_of("status", IncidentStatus), name="status"),
        CheckConstraint("first_seen_at <= last_seen_at", name="seen_order"),
        CheckConstraint(
            "(status = 'resolved') = (resolved_at IS NOT NULL)", name="resolved_at_status"
        ),
        CheckConstraint("version >= 1", name="version_positive"),
        Index("ix_incidents_status_detected_at", "status", text("detected_at DESC")),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"))
    replay_scope: Mapped[str] = replay_scope()
    fingerprint: Mapped[str] = mapped_column(Text)
    correlation_window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    title: Mapped[str] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=IncidentStatus.OPEN, server_default=IncidentStatus.OPEN
    )
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = counter(1)

    # ORM updates fail with StaleDataError when another writer bumped the version.
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012 (SQLAlchemy hook)


class IncidentService(Base):
    __tablename__ = "incident_services"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "incident_id"], ["incidents.workspace_id", "incidents.id"]
        ),
        ForeignKeyConstraint(
            ["workspace_id", "service_id"], ["services.workspace_id", "services.id"]
        ),
        CheckConstraint(one_of("role", IncidentServiceRole), name="role"),
    )

    incident_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    service_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    workspace_id: Mapped[uuid.UUID]
    role: Mapped[str] = mapped_column(Text)


class IncidentSignal(Base):
    __tablename__ = "incident_signals"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
        ),
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "signal_id"],
            ["signals.workspace_id", "signals.replay_scope", "signals.id"],
        ),
        CheckConstraint("relevance BETWEEN 0 AND 1", name="relevance_range"),
    )

    incident_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    signal_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    workspace_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = scope_column()
    relevance: Mapped[float] = mapped_column(Double)


class Investigation(Base):
    __tablename__ = "investigations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
        ),
        UniqueConstraint("request_key"),
        CheckConstraint(
            length_between("request_key", 1, MAX_KEY_LENGTH), name="request_key_length"
        ),
        CheckConstraint(one_of("status", InvestigationStatus), name="status"),
        CheckConstraint(nonempty("workflow_version"), name="workflow_version_nonempty"),
        CheckConstraint(nonnegative("budget_microusd"), name="budget_nonnegative"),
        CheckConstraint(nonnegative("reserved_microusd"), name="reserved_nonnegative"),
        CheckConstraint(nonnegative("spent_microusd"), name="spent_nonnegative"),
        CheckConstraint("max_steps > 0", name="max_steps_positive"),
        CheckConstraint("deadline_at > created_at", name="deadline_after_created"),
        CheckConstraint(
            f"({TERMINAL_INVESTIGATION}) = (completed_at IS NOT NULL)",
            name="completed_at_status",
        ),
        CheckConstraint(optional_json_object("diagnosis"), name="diagnosis_object"),
        CheckConstraint(diagnosis_status_rule(), name="diagnosis_status"),
        CheckConstraint("version >= 1", name="version_positive"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    workspace_id: Mapped[uuid.UUID]
    incident_id: Mapped[uuid.UUID]
    replay_scope: Mapped[str] = scope_column()
    # Stable idempotency key from the API request or the detector trigger.
    request_key: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=InvestigationStatus.QUEUED, server_default=InvestigationStatus.QUEUED
    )
    workflow_version: Mapped[str] = mapped_column(Text)
    # Tools never read evidence after this instant.
    evidence_cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # The reservation query enforces reserved + spent <= budget. No CHECK does,
    # because actual provider charges above a reservation must stay recordable.
    budget_microusd: Mapped[int] = mapped_column(BigInteger)
    reserved_microusd: Mapped[int] = big_counter()
    spent_microusd: Mapped[int] = big_counter()
    max_steps: Mapped[int] = mapped_column(Integer)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = created_at()
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    diagnosis: Mapped[dict[str, Any] | None] = optional_json()
    version: Mapped[int] = counter(1)

    # ORM updates fail with StaleDataError when another writer bumped the version.
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012 (SQLAlchemy hook)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        # One logical job per investigation, however often the request is delivered.
        UniqueConstraint("investigation_id"),
        CheckConstraint(one_of("status", JobStatus), name="status"),
        CheckConstraint(nonnegative("attempts"), name="attempts_nonnegative"),
        CheckConstraint("max_attempts > 0", name="max_attempts_positive"),
        CheckConstraint("attempts <= max_attempts", name="attempts_within_max"),
        CheckConstraint(nonnegative("lease_generation"), name="lease_generation_nonnegative"),
        # Exactly the running jobs hold a lease.
        CheckConstraint(
            "(status = 'running') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)",
            name="lease_status",
        ),
        CheckConstraint(
            "(lease_owner IS NULL) = (lease_until IS NULL)", name="lease_fields_together"
        ),
        CheckConstraint(
            f"lease_owner IS NULL OR {nonempty('lease_owner')}", name="lease_owner_nonempty"
        ),
        CheckConstraint(optional_json_object("last_error"), name="last_error_object"),
        Index("ix_jobs_status_available_at", "status", "available_at"),
        Index("ix_jobs_lease_until", "lease_until", postgresql_where=text("status = 'running'")),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    investigation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("investigations.id"))
    status: Mapped[str] = mapped_column(
        Text, default=JobStatus.QUEUED, server_default=JobStatus.QUEUED
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    attempts: Mapped[int] = counter()
    max_attempts: Mapped[int] = counter(3)
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_generation: Mapped[int] = big_counter()
    last_error: Mapped[dict[str, Any] | None] = optional_json()
    created_at: Mapped[datetime] = created_at()


class AgentStep(Base):
    __tablename__ = "agent_steps"
    __table_args__ = (
        # Deterministic step keys make each logical step unique per investigation.
        UniqueConstraint("investigation_id", "step_key"),
        CheckConstraint(length_between("step_key", 1, MAX_KEY_LENGTH), name="step_key_length"),
        CheckConstraint(one_of("agent_role", AgentRole), name="agent_role"),
        CheckConstraint(one_of("status", StepStatus), name="status"),
        CheckConstraint(nonnegative("attempt"), name="attempt_nonnegative"),
        CheckConstraint(nonnegative("lease_generation"), name="lease_generation_nonnegative"),
        CheckConstraint(json_object("input"), name="input_object"),
        CheckConstraint(optional_json_object("output"), name="output_object"),
        CheckConstraint(optional_json_object("checkpoint"), name="checkpoint_object"),
        CheckConstraint("status = 'pending' OR started_at IS NOT NULL", name="started_at_status"),
        CheckConstraint(
            "(status IN ('completed', 'failed', 'skipped')) = (completed_at IS NOT NULL)",
            name="completed_at_status",
        ),
        # Completed output is what recovery reuses.
        CheckConstraint("status <> 'completed' OR output IS NOT NULL", name="completed_output"),
        Index("ix_agent_steps_investigation_id_status", "investigation_id", "status"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    investigation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("investigations.id"))
    step_key: Mapped[str] = mapped_column(Text)
    agent_role: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=StepStatus.PENDING, server_default=StepStatus.PENDING
    )
    attempt: Mapped[int] = counter()
    input: Mapped[dict[str, Any]] = json_attributes()
    output: Mapped[dict[str, Any] | None] = optional_json()
    # Structured findings and operational state only, never private model reasoning.
    checkpoint: Mapped[dict[str, Any] | None] = optional_json()
    # Generation of the job lease that last wrote this step.
    lease_generation: Mapped[int] = big_counter()
    created_at: Mapped[datetime] = created_at()
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ToolCall(Base):
    __tablename__ = "tool_calls"
    __table_args__ = (
        UniqueConstraint("agent_step_id", "call_key"),
        CheckConstraint(length_between("call_key", 1, MAX_KEY_LENGTH), name="call_key_length"),
        CheckConstraint(one_of("tool_name", ToolName), name="tool_name"),
        CheckConstraint(one_of("status", ToolCallStatus), name="status"),
        CheckConstraint(json_object("arguments"), name="arguments_object"),
        CheckConstraint(optional_nonnegative("duration_ms"), name="duration_nonnegative"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    agent_step_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agent_steps.id"))
    call_key: Mapped[str] = mapped_column(Text)
    tool_name: Mapped[str] = mapped_column(Text)
    arguments: Mapped[dict[str, Any]] = json_attributes()
    result_ref: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = created_at()


class LlmCall(Base):
    __tablename__ = "llm_calls"
    __table_args__ = (
        # Each retry is a new attempt with its own reservation.
        UniqueConstraint("agent_step_id", "attempt"),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        CheckConstraint(nonempty("provider"), name="provider_nonempty"),
        CheckConstraint(nonempty("model"), name="model_nonempty"),
        CheckConstraint(nonempty("prompt_version"), name="prompt_version_nonempty"),
        CheckConstraint(nonempty("request_hash"), name="request_hash_nonempty"),
        CheckConstraint(one_of("status", LlmCallStatus), name="status"),
        CheckConstraint(
            f"cost_basis IS NULL OR {one_of('cost_basis', CostBasis)}", name="cost_basis"
        ),
        CheckConstraint(optional_nonnegative("input_tokens"), name="input_tokens_nonnegative"),
        CheckConstraint(optional_nonnegative("output_tokens"), name="output_tokens_nonnegative"),
        CheckConstraint(nonnegative("reserved_microusd"), name="reserved_nonnegative"),
        CheckConstraint(optional_nonnegative("cost_microusd"), name="cost_nonnegative"),
        CheckConstraint(optional_nonnegative("latency_ms"), name="latency_nonnegative"),
        # A cost basis is recorded exactly when the call has an outcome.
        CheckConstraint("(status = 'pending') = (cost_basis IS NULL)", name="cost_basis_status"),
        # Known costs carry an amount; an unknown cost keeps its reservation instead.
        CheckConstraint(
            "(cost_basis IN ('actual', 'estimated')) = (cost_microusd IS NOT NULL)",
            name="cost_amount",
        ),
        CheckConstraint("status <> 'unknown' OR cost_basis = 'unknown'", name="unknown_cost"),
        CheckConstraint(
            "status <> 'succeeded' OR cost_basis IN ('actual', 'estimated')",
            name="succeeded_cost",
        ),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    agent_step_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agent_steps.id"))
    attempt: Mapped[int] = mapped_column(Integer)
    provider: Mapped[str] = mapped_column(Text)
    model: Mapped[str] = mapped_column(Text)
    prompt_version: Mapped[str] = mapped_column(Text)
    # Local fingerprint of the request; it does not deduplicate at the provider.
    request_hash: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, default=LlmCallStatus.PENDING, server_default=LlmCallStatus.PENDING
    )
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    reserved_microusd: Mapped[int] = mapped_column(BigInteger)
    cost_microusd: Mapped[int | None] = mapped_column(BigInteger)
    cost_basis: Mapped[str | None] = mapped_column(Text)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    provider_request_id: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = created_at()


class ProgressEvent(Base):
    __tablename__ = "progress_events"
    __table_args__ = (
        CheckConstraint(length_between("event_type", 1, 128), name="event_type_length"),
        CheckConstraint(json_object("payload"), name="payload_object"),
        Index("ix_progress_events_investigation_id_id", "investigation_id", "id"),
    )

    # The SSE cursor: monotonically increasing, never reused.
    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    investigation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("investigations.id"))
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = json_attributes()
    created_at: Mapped[datetime] = created_at()
