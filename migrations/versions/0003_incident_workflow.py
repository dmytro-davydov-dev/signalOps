"""Incident and workflow tables: incidents, evidence links, investigations, jobs, steps, calls.

Incident evidence and investigations share the incident's workspace and replay scope
through composite foreign keys. One job per investigation (UNIQUE investigation_id) and
one row per logical step (UNIQUE investigation_id, step_key).

Status transitions are enforced by the `enforce_status_transition` trigger function,
whose arguments list the allowed `from>to` pairs. They mirror the maps in
`signalops.contracts.workflow`; an integration test compares the two.

Revision ID: 0003_incident_workflow
Revises: 0002_source_evidence
Create Date: 2026-10-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_incident_workflow"
down_revision: str | None = "0002_source_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Allowed status changes besides keeping the same status; terminal states have none.
STATUS_TRANSITIONS: dict[str, list[str]] = {
    "incidents": ["open>resolved", "resolved>open"],
    "investigations": [
        "queued>running",
        "queued>failed",
        "queued>cancelled",
        "running>completed",
        "running>insufficient_evidence",
        "running>failed",
        "running>cancelled",
        "running>budget_exceeded",
    ],
    "jobs": [
        "queued>running",
        "queued>failed",
        "queued>cancelled",
        "running>queued",
        "running>succeeded",
        "running>failed",
        "running>cancelled",
    ],
    "agent_steps": [
        "pending>running",
        "pending>skipped",
        "running>completed",
        "running>failed",
        "failed>running",
    ],
}


def create_status_transition_triggers() -> None:
    op.execute(
        """
        CREATE FUNCTION enforce_status_transition() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT ((OLD.status || '>' || NEW.status) = ANY (TG_ARGV)) THEN
                RAISE EXCEPTION 'invalid % status transition: % -> %',
                    TG_TABLE_NAME, OLD.status, NEW.status
                    USING ERRCODE = 'check_violation',
                          CONSTRAINT = 'ck_' || TG_TABLE_NAME || '_status_transition',
                          TABLE = TG_TABLE_NAME,
                          COLUMN = 'status';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    for table, transitions in STATUS_TRANSITIONS.items():
        arguments = ", ".join(f"'{transition}'" for transition in transitions)
        op.execute(
            f"CREATE TRIGGER status_transition BEFORE UPDATE OF status ON {table} "
            "FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) "
            f"EXECUTE FUNCTION enforce_status_transition({arguments})"
        )


def upgrade() -> None:
    # Target for incident_signals, which must share the incident's replay scope.
    op.create_unique_constraint(
        op.f("uq_signals_workspace_id_replay_scope_id"),
        "signals",
        ["workspace_id", "replay_scope", "id"],
    )
    op.create_table(
        "incidents",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), server_default="live", nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("correlation_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="open", nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.CheckConstraint(
            "(status = 'resolved') = (resolved_at IS NOT NULL)",
            name=op.f("ck_incidents_resolved_at_status"),
        ),
        sa.CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')", name=op.f("ck_incidents_severity")
        ),
        sa.CheckConstraint("status IN ('open', 'resolved')", name=op.f("ck_incidents_status")),
        sa.CheckConstraint(
            "char_length(fingerprint) > 0", name=op.f("ck_incidents_fingerprint_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(replay_scope) BETWEEN 1 AND 128",
            name=op.f("ck_incidents_replay_scope_length"),
        ),
        sa.CheckConstraint("char_length(title) > 0", name=op.f("ck_incidents_title_nonempty")),
        sa.CheckConstraint("first_seen_at <= last_seen_at", name=op.f("ck_incidents_seen_order")),
        sa.CheckConstraint("version >= 1", name=op.f("ck_incidents_version_positive")),
        sa.ForeignKeyConstraint(
            ["workspace_id"], ["workspaces.id"], name=op.f("fk_incidents_workspace_id_workspaces")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_incidents")),
        sa.UniqueConstraint("workspace_id", "id", name=op.f("uq_incidents_workspace_id_id")),
        sa.UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "fingerprint",
            "correlation_window_start",
            name="uq_incidents_correlation",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "replay_scope",
            "id",
            name=op.f("uq_incidents_workspace_id_replay_scope_id"),
        ),
    )
    op.create_index(
        "ix_incidents_status_detected_at",
        "incidents",
        ["status", sa.literal_column("detected_at DESC")],
        unique=False,
    )
    op.create_table(
        "incident_services",
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "role IN ('primary', 'affected')", name=op.f("ck_incident_services_role")
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "incident_id"],
            ["incidents.workspace_id", "incidents.id"],
            name=op.f("fk_incident_services_workspace_id_incidents"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "service_id"],
            ["services.workspace_id", "services.id"],
            name=op.f("fk_incident_services_workspace_id_services"),
        ),
        sa.PrimaryKeyConstraint("incident_id", "service_id", name=op.f("pk_incident_services")),
    )
    op.create_table(
        "investigations",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), nullable=False),
        sa.Column("request_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="queued", nullable=False),
        sa.Column("workflow_version", sa.Text(), nullable=False),
        sa.Column("evidence_cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("budget_microusd", sa.BigInteger(), nullable=False),
        sa.Column(
            "reserved_microusd", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("spent_microusd", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_steps", sa.Integer(), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("diagnosis", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.CheckConstraint(
            "(status IN ('completed', 'insufficient_evidence', 'failed', 'cancelled', "
            "'budget_exceeded')) = (completed_at IS NOT NULL)",
            name=op.f("ck_investigations_completed_at_status"),
        ),
        sa.CheckConstraint(
            "diagnosis IS NULL OR jsonb_typeof(diagnosis) = 'object'",
            name=op.f("ck_investigations_diagnosis_object"),
        ),
        sa.CheckConstraint(
            "diagnosis IS NULL OR status IN ('completed', 'insufficient_evidence')",
            name=op.f("ck_investigations_diagnosis_status"),
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'insufficient_evidence', 'failed', "
            "'cancelled', 'budget_exceeded')",
            name=op.f("ck_investigations_status"),
        ),
        sa.CheckConstraint(
            "budget_microusd >= 0", name=op.f("ck_investigations_budget_nonnegative")
        ),
        sa.CheckConstraint(
            "char_length(request_key) BETWEEN 1 AND 256",
            name=op.f("ck_investigations_request_key_length"),
        ),
        sa.CheckConstraint(
            "char_length(workflow_version) > 0",
            name=op.f("ck_investigations_workflow_version_nonempty"),
        ),
        sa.CheckConstraint(
            "deadline_at > created_at", name=op.f("ck_investigations_deadline_after_created")
        ),
        sa.CheckConstraint("max_steps > 0", name=op.f("ck_investigations_max_steps_positive")),
        sa.CheckConstraint(
            "reserved_microusd >= 0", name=op.f("ck_investigations_reserved_nonnegative")
        ),
        sa.CheckConstraint("spent_microusd >= 0", name=op.f("ck_investigations_spent_nonnegative")),
        sa.CheckConstraint("version >= 1", name=op.f("ck_investigations_version_positive")),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
            name=op.f("fk_investigations_workspace_id_incidents"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_investigations")),
        sa.UniqueConstraint("request_key", name=op.f("uq_investigations_request_key")),
    )
    op.create_table(
        "agent_steps",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column("step_key", sa.Text(), nullable=False),
        sa.Column("agent_role", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("attempt", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "input",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("checkpoint", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed', 'skipped')) = (completed_at IS NOT NULL)",
            name=op.f("ck_agent_steps_completed_at_status"),
        ),
        sa.CheckConstraint(
            "agent_role IN ('investigator', 'coordinator', 'telemetry_investigator', "
            "'deployment_investigator', 'dependency_investigator', 'synthesizer', 'verifier')",
            name=op.f("ck_agent_steps_agent_role"),
        ),
        sa.CheckConstraint(
            "checkpoint IS NULL OR jsonb_typeof(checkpoint) = 'object'",
            name=op.f("ck_agent_steps_checkpoint_object"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(input) = 'object'", name=op.f("ck_agent_steps_input_object")
        ),
        sa.CheckConstraint(
            "output IS NULL OR jsonb_typeof(output) = 'object'",
            name=op.f("ck_agent_steps_output_object"),
        ),
        sa.CheckConstraint(
            "status <> 'completed' OR output IS NOT NULL",
            name=op.f("ck_agent_steps_completed_output"),
        ),
        sa.CheckConstraint(
            "status = 'pending' OR started_at IS NOT NULL",
            name=op.f("ck_agent_steps_started_at_status"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'skipped')",
            name=op.f("ck_agent_steps_status"),
        ),
        sa.CheckConstraint("attempt >= 0", name=op.f("ck_agent_steps_attempt_nonnegative")),
        sa.CheckConstraint(
            "char_length(step_key) BETWEEN 1 AND 256", name=op.f("ck_agent_steps_step_key_length")
        ),
        sa.CheckConstraint(
            "lease_generation >= 0", name=op.f("ck_agent_steps_lease_generation_nonnegative")
        ),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_agent_steps_investigation_id_investigations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_steps")),
        sa.UniqueConstraint(
            "investigation_id", "step_key", name=op.f("uq_agent_steps_investigation_id_step_key")
        ),
    )
    op.create_index(
        "ix_agent_steps_investigation_id_status",
        "agent_steps",
        ["investigation_id", "status"],
        unique=False,
    )
    op.create_table(
        "incident_signals",
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("replay_scope", sa.Text(), nullable=False),
        sa.Column("relevance", sa.Double(), nullable=False),
        sa.CheckConstraint(
            "relevance BETWEEN 0 AND 1", name=op.f("ck_incident_signals_relevance_range")
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "incident_id"],
            ["incidents.workspace_id", "incidents.replay_scope", "incidents.id"],
            name=op.f("fk_incident_signals_workspace_id_incidents"),
        ),
        sa.ForeignKeyConstraint(
            ["workspace_id", "replay_scope", "signal_id"],
            ["signals.workspace_id", "signals.replay_scope", "signals.id"],
            name=op.f("fk_incident_signals_workspace_id_signals"),
        ),
        sa.PrimaryKeyConstraint("incident_id", "signal_id", name=op.f("pk_incident_signals")),
    )
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.Text(), server_default="queued", nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default=sa.text("3"), nullable=False),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'running') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)",
            name=op.f("ck_jobs_lease_status"),
        ),
        sa.CheckConstraint(
            "last_error IS NULL OR jsonb_typeof(last_error) = 'object'",
            name=op.f("ck_jobs_last_error_object"),
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name=op.f("ck_jobs_status"),
        ),
        sa.CheckConstraint(
            "(lease_owner IS NULL) = (lease_until IS NULL)",
            name=op.f("ck_jobs_lease_fields_together"),
        ),
        sa.CheckConstraint("attempts <= max_attempts", name=op.f("ck_jobs_attempts_within_max")),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_jobs_attempts_nonnegative")),
        sa.CheckConstraint(
            "lease_generation >= 0", name=op.f("ck_jobs_lease_generation_nonnegative")
        ),
        sa.CheckConstraint(
            "lease_owner IS NULL OR char_length(lease_owner) > 0",
            name=op.f("ck_jobs_lease_owner_nonempty"),
        ),
        sa.CheckConstraint("max_attempts > 0", name=op.f("ck_jobs_max_attempts_positive")),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_jobs_investigation_id_investigations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
        sa.UniqueConstraint("investigation_id", name=op.f("uq_jobs_investigation_id")),
    )
    op.create_index(
        "ix_jobs_lease_until",
        "jobs",
        ["lease_until"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index("ix_jobs_status_available_at", "jobs", ["status", "available_at"], unique=False)
    op.create_table(
        "progress_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("investigation_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name=op.f("ck_progress_events_payload_object")
        ),
        sa.CheckConstraint(
            "char_length(event_type) BETWEEN 1 AND 128",
            name=op.f("ck_progress_events_event_type_length"),
        ),
        sa.ForeignKeyConstraint(
            ["investigation_id"],
            ["investigations.id"],
            name=op.f("fk_progress_events_investigation_id_investigations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_progress_events")),
    )
    op.create_index(
        "ix_progress_events_investigation_id_id",
        "progress_events",
        ["investigation_id", "id"],
        unique=False,
    )
    op.create_table(
        "llm_calls",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("agent_step_id", sa.Uuid(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("reserved_microusd", sa.BigInteger(), nullable=False),
        sa.Column("cost_microusd", sa.BigInteger(), nullable=True),
        sa.Column("cost_basis", sa.Text(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("provider_request_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(cost_basis IN ('actual', 'estimated')) = (cost_microusd IS NOT NULL)",
            name=op.f("ck_llm_calls_cost_amount"),
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (cost_basis IS NULL)",
            name=op.f("ck_llm_calls_cost_basis_status"),
        ),
        sa.CheckConstraint(
            "cost_basis IS NULL OR cost_basis IN ('actual', 'estimated', 'unknown')",
            name=op.f("ck_llm_calls_cost_basis"),
        ),
        sa.CheckConstraint(
            "status <> 'succeeded' OR cost_basis IN ('actual', 'estimated')",
            name=op.f("ck_llm_calls_succeeded_cost"),
        ),
        sa.CheckConstraint(
            "status <> 'unknown' OR cost_basis = 'unknown'", name=op.f("ck_llm_calls_unknown_cost")
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed', 'unknown')",
            name=op.f("ck_llm_calls_status"),
        ),
        sa.CheckConstraint("attempt >= 1", name=op.f("ck_llm_calls_attempt_positive")),
        sa.CheckConstraint("char_length(model) > 0", name=op.f("ck_llm_calls_model_nonempty")),
        sa.CheckConstraint(
            "char_length(prompt_version) > 0", name=op.f("ck_llm_calls_prompt_version_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(provider) > 0", name=op.f("ck_llm_calls_provider_nonempty")
        ),
        sa.CheckConstraint(
            "char_length(request_hash) > 0", name=op.f("ck_llm_calls_request_hash_nonempty")
        ),
        sa.CheckConstraint(
            "cost_microusd IS NULL OR cost_microusd >= 0",
            name=op.f("ck_llm_calls_cost_nonnegative"),
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name=op.f("ck_llm_calls_input_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0", name=op.f("ck_llm_calls_latency_nonnegative")
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name=op.f("ck_llm_calls_output_tokens_nonnegative"),
        ),
        sa.CheckConstraint(
            "reserved_microusd >= 0", name=op.f("ck_llm_calls_reserved_nonnegative")
        ),
        sa.ForeignKeyConstraint(
            ["agent_step_id"],
            ["agent_steps.id"],
            name=op.f("fk_llm_calls_agent_step_id_agent_steps"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_llm_calls")),
        sa.UniqueConstraint(
            "agent_step_id", "attempt", name=op.f("uq_llm_calls_agent_step_id_attempt")
        ),
    )
    op.create_table(
        "tool_calls",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("agent_step_id", sa.Uuid(), nullable=False),
        sa.Column("call_key", sa.Text(), nullable=False),
        sa.Column("tool_name", sa.Text(), nullable=False),
        sa.Column(
            "arguments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("result_ref", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(arguments) = 'object'", name=op.f("ck_tool_calls_arguments_object")
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed')", name=op.f("ck_tool_calls_status")
        ),
        sa.CheckConstraint(
            "tool_name IN ('query_logs', 'get_trace_summary', 'get_deployments', "
            "'get_service_dependencies', 'retrieve_runbooks')",
            name=op.f("ck_tool_calls_tool_name"),
        ),
        sa.CheckConstraint(
            "char_length(call_key) BETWEEN 1 AND 256", name=op.f("ck_tool_calls_call_key_length")
        ),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name=op.f("ck_tool_calls_duration_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["agent_step_id"],
            ["agent_steps.id"],
            name=op.f("fk_tool_calls_agent_step_id_agent_steps"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tool_calls")),
        sa.UniqueConstraint(
            "agent_step_id", "call_key", name=op.f("uq_tool_calls_agent_step_id_call_key")
        ),
    )
    create_status_transition_triggers()


def downgrade() -> None:
    op.execute("DROP FUNCTION enforce_status_transition() CASCADE")
    op.drop_table("tool_calls")
    op.drop_table("llm_calls")
    op.drop_index("ix_progress_events_investigation_id_id", table_name="progress_events")
    op.drop_table("progress_events")
    op.drop_index("ix_jobs_status_available_at", table_name="jobs")
    op.drop_index(
        "ix_jobs_lease_until", table_name="jobs", postgresql_where=sa.text("status = 'running'")
    )
    op.drop_table("jobs")
    op.drop_table("incident_signals")
    op.drop_index("ix_agent_steps_investigation_id_status", table_name="agent_steps")
    op.drop_table("agent_steps")
    op.drop_table("investigations")
    op.drop_table("incident_services")
    op.drop_index("ix_incidents_status_detected_at", table_name="incidents")
    op.drop_table("incidents")
    op.drop_constraint(op.f("uq_signals_workspace_id_replay_scope_id"), "signals", type_="unique")
