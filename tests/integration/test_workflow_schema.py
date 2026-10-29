"""Incident and workflow tables against real PostgreSQL (P02-T02)."""

import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

import pytest
from sqlalchemy import Engine, Table, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from signalops.contracts.workflow import (
    INCIDENT_TRANSITIONS,
    INVESTIGATION_TRANSITIONS,
    JOB_TRANSITIONS,
    STEP_TRANSITIONS,
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
from signalops.persistence.models import (
    AgentStep,
    Incident,
    IncidentService,
    IncidentSignal,
    Investigation,
    Job,
    LlmCall,
    ProgressEvent,
    Service,
    ToolCall,
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

LATER = T0 + timedelta(minutes=5)


def incident_state(status: str) -> dict[str, Any]:
    return {"resolved_at": LATER if status == IncidentStatus.RESOLVED else None}


def investigation_state(status: str) -> dict[str, Any]:
    terminal = status in TERMINAL_INVESTIGATION_STATUSES
    return {"completed_at": LATER if terminal else None}


def job_state(status: str) -> dict[str, Any]:
    running = status == JobStatus.RUNNING
    return {
        "lease_owner": "worker-1" if running else None,
        "lease_until": LATER if running else None,
    }


def step_state(status: str) -> dict[str, Any]:
    finished = status in {StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED}
    return {
        "started_at": None if status == StepStatus.PENDING else T0,
        "completed_at": LATER if finished else None,
        "output": {"findings": []} if status == StepStatus.COMPLETED else None,
    }


class Scenario(World):
    """A workspace with evidence, plus helpers for incident and workflow rows."""

    def incident_values(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "fingerprint": f"checkout:error-rate:{uuid.uuid4()}",
            "correlation_window_start": T0,
            "title": "Checkout error rate above threshold",
            "severity": IncidentSeverity.HIGH,
            "detected_at": T0 + timedelta(minutes=1),
            "first_seen_at": T0,
            "last_seen_at": T0 + timedelta(minutes=1),
        }
        values.update(overrides)
        return values

    def incident(self, **overrides: Any) -> uuid.UUID:
        return insert(self.engine, Incident, **self.incident_values(**overrides))

    def investigation_values(self, incident_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "incident_id": incident_id,
            "replay_scope": "live",
            "request_key": f"req-{uuid.uuid4()}",
            "workflow_version": "v1",
            "evidence_cutoff_at": T0 + timedelta(minutes=1),
            "budget_microusd": 500_000,
            "max_steps": 12,
            "created_at": T0 + timedelta(minutes=1),
            "deadline_at": T0 + timedelta(minutes=11),
        }
        values.update(overrides)
        return values

    def investigation(self, incident_id: uuid.UUID | None = None, **overrides: Any) -> uuid.UUID:
        incident_id = incident_id or self.incident()
        return insert(
            self.engine, Investigation, **self.investigation_values(incident_id, **overrides)
        )

    def step_values(self, investigation_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "investigation_id": investigation_id,
            "step_key": f"investigate:{uuid.uuid4()}",
            "agent_role": AgentRole.INVESTIGATOR,
        }
        values.update(overrides)
        return values

    def step(self, investigation_id: uuid.UUID | None = None, **overrides: Any) -> uuid.UUID:
        investigation_id = investigation_id or self.investigation()
        return insert(self.engine, AgentStep, **self.step_values(investigation_id, **overrides))

    def llm_call_values(self, agent_step_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "agent_step_id": agent_step_id,
            "attempt": 1,
            "provider": "fake",
            "model": "fake-model",
            "prompt_version": "investigator-v1",
            "request_hash": "sha256:0",
            "reserved_microusd": 20_000,
        }
        values.update(overrides)
        return values


@pytest.fixture
def scenario(engine: Engine) -> Scenario:
    return Scenario(engine)


def update_status(engine: Engine, model: type[Base], row_id: uuid.UUID, /, **values: Any) -> None:
    table = model.__table__
    assert isinstance(table, Table)
    with engine.begin() as conn:
        conn.execute(update(table).where(table.c.id == row_id).values(**values))


def violated_update(engine: Engine, model: type[Base], row_id: uuid.UUID, /, **values: Any) -> str:
    with pytest.raises(IntegrityError) as excinfo:
        update_status(engine, model, row_id, **values)
    return constraint_name(excinfo.value)


# Uniqueness: logical identities


def test_one_investigation_cannot_have_two_jobs(scenario: Scenario) -> None:
    investigation_id = scenario.investigation()
    insert(scenario.engine, Job, investigation_id=investigation_id)
    name = violated_constraint(scenario.engine, Job, investigation_id=investigation_id)
    assert name == "uq_jobs_investigation_id"


def test_duplicate_logical_step_is_rejected(scenario: Scenario) -> None:
    investigation_id = scenario.investigation()
    scenario.step(investigation_id, step_key="investigate")
    name = violated_constraint(
        scenario.engine,
        AgentStep,
        **scenario.step_values(investigation_id, step_key="investigate", attempt=1),
    )
    assert name == "uq_agent_steps_investigation_id_step_key"
    # The same deterministic key is independent in another investigation.
    scenario.step(scenario.investigation(), step_key="investigate")


def test_duplicate_request_key_is_rejected(scenario: Scenario) -> None:
    incident_id = scenario.incident()
    scenario.investigation(incident_id, request_key="detector:incident-1:v1")
    name = violated_constraint(
        scenario.engine,
        Investigation,
        **scenario.investigation_values(incident_id, request_key="detector:incident-1:v1"),
    )
    assert name == "uq_investigations_request_key"


def test_incident_is_unique_per_correlation_window_and_replay_scope(scenario: Scenario) -> None:
    scenario.incident(fingerprint="checkout:error-rate")
    duplicate = scenario.incident_values(fingerprint="checkout:error-rate")
    assert violated_constraint(scenario.engine, Incident, **duplicate) == "uq_incidents_correlation"
    scenario.incident(fingerprint="checkout:error-rate", correlation_window_start=LATER)
    scenario.incident(fingerprint="checkout:error-rate", replay_scope="replay-run-a")


def test_duplicate_tool_call_and_llm_attempt_are_rejected(scenario: Scenario) -> None:
    step_id = scenario.step(status=StepStatus.RUNNING, started_at=T0)
    tool_call = {
        "agent_step_id": step_id,
        "call_key": "query_logs:0",
        "tool_name": ToolName.QUERY_LOGS,
        "status": ToolCallStatus.RUNNING,
    }
    insert(scenario.engine, ToolCall, **tool_call)
    assert (
        violated_constraint(scenario.engine, ToolCall, **tool_call)
        == "uq_tool_calls_agent_step_id_call_key"
    )
    insert(scenario.engine, LlmCall, **scenario.llm_call_values(step_id))
    assert (
        violated_constraint(scenario.engine, LlmCall, **scenario.llm_call_values(step_id))
        == "uq_llm_calls_agent_step_id_attempt"
    )
    insert(scenario.engine, LlmCall, **scenario.llm_call_values(step_id, attempt=2))


# Workspace and replay-scope safety


def test_incident_evidence_must_share_replay_scope(scenario: Scenario) -> None:
    incident_id = scenario.incident()
    live_signal = scenario.signal()
    replay_signal = scenario.signal(replay_scope="replay-run-a")
    link = {"incident_id": incident_id, "workspace_id": scenario.workspace_id, "relevance": 0.9}
    insert_row(scenario.engine, IncidentSignal, **link, signal_id=live_signal, replay_scope="live")
    assert (
        violated_constraint(
            scenario.engine, IncidentSignal, **link, signal_id=replay_signal, replay_scope="live"
        )
        == "fk_incident_signals_workspace_id_signals"
    )
    assert (
        violated_constraint(
            scenario.engine,
            IncidentSignal,
            **link,
            signal_id=replay_signal,
            replay_scope="replay-run-a",
        )
        == "fk_incident_signals_workspace_id_incidents"
    )


def test_incident_services_stay_in_workspace(scenario: Scenario, engine: Engine) -> None:
    incident_id = scenario.incident()
    other = World(engine)
    row = {"incident_id": incident_id, "workspace_id": scenario.workspace_id}
    insert_row(engine, IncidentService, **row, service_id=scenario.service_id, role="primary")
    assert (
        violated_constraint(
            engine, IncidentService, **row, service_id=other.service_id, role="affected"
        )
        == "fk_incident_services_workspace_id_services"
    )
    payment = insert(
        engine, Service, workspace_id=scenario.workspace_id, name="payment", environment="demo"
    )
    assert (
        violated_constraint(engine, IncidentService, **row, service_id=payment, role="cause")
        == "ck_incident_services_role"
    )
    for role in IncidentServiceRole:
        if role != IncidentServiceRole.PRIMARY:
            insert_row(engine, IncidentService, **row, service_id=payment, role=role)


def test_investigation_shares_incident_scope(scenario: Scenario) -> None:
    incident_id = scenario.incident()
    mismatched = scenario.investigation_values(incident_id, replay_scope="replay-run-a")
    assert (
        violated_constraint(scenario.engine, Investigation, **mismatched)
        == "fk_investigations_workspace_id_incidents"
    )


# Enumerations


@pytest.mark.parametrize(
    ("model", "column"),
    [
        (Incident, "status"),
        (Incident, "severity"),
        (Investigation, "status"),
        (Job, "status"),
        (AgentStep, "status"),
        (AgentStep, "agent_role"),
        (ToolCall, "tool_name"),
        (ToolCall, "status"),
        (LlmCall, "status"),
    ],
)
def test_enumerations_reject_unknown_values(
    scenario: Scenario, model: type[Base], column: str
) -> None:
    builders: dict[type[Base], Callable[[], dict[str, Any]]] = {
        Incident: scenario.incident_values,
        Investigation: lambda: scenario.investigation_values(scenario.incident()),
        Job: lambda: {"investigation_id": scenario.investigation()},
        AgentStep: lambda: scenario.step_values(scenario.investigation(), started_at=T0),
        ToolCall: lambda: {
            "agent_step_id": scenario.step(),
            "call_key": "c",
            "tool_name": ToolName.QUERY_LOGS,
            "status": ToolCallStatus.RUNNING,
        },
        LlmCall: lambda: scenario.llm_call_values(scenario.step(), cost_basis="unknown"),
    }
    values = builders[model]() | {column: "bogus"}
    name = violated_constraint(scenario.engine, model, **values)
    assert name == f"ck_{model.__tablename__}_{column}"


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (AgentStep, AgentRole),
        (ToolCall, ToolName),
    ],
)
def test_every_enumeration_member_is_accepted(
    scenario: Scenario, model: type[Base], values: type[StrEnum]
) -> None:
    step_id = scenario.step()
    for member in values:
        if model is AgentStep:
            scenario.step(agent_role=member)
        else:
            insert(
                scenario.engine,
                ToolCall,
                agent_step_id=step_id,
                call_key=f"{member}:0",
                tool_name=member,
                status=ToolCallStatus.SUCCEEDED,
                duration_ms=3,
            )


# State consistency


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"budget_microusd": -1}, "ck_investigations_budget_nonnegative"),
        ({"reserved_microusd": -1}, "ck_investigations_reserved_nonnegative"),
        ({"spent_microusd": -1}, "ck_investigations_spent_nonnegative"),
        ({"max_steps": 0}, "ck_investigations_max_steps_positive"),
        ({"deadline_at": T0}, "ck_investigations_deadline_after_created"),
        ({"status": "completed"}, "ck_investigations_completed_at_status"),
        ({"completed_at": LATER}, "ck_investigations_completed_at_status"),
        ({"diagnosis": {"status": "diagnosed"}}, "ck_investigations_diagnosis_status"),
        (
            {"status": "completed", "completed_at": LATER, "diagnosis": ["x"]},
            "ck_investigations_diagnosis_object",
        ),
        ({"request_key": ""}, "ck_investigations_request_key_length"),
        ({"version": 0}, "ck_investigations_version_positive"),
    ],
)
def test_investigation_checks(
    scenario: Scenario, overrides: dict[str, Any], constraint: str
) -> None:
    values = scenario.investigation_values(scenario.incident(), **overrides)
    assert violated_constraint(scenario.engine, Investigation, **values) == constraint


def test_insufficient_evidence_may_carry_a_diagnosis(scenario: Scenario) -> None:
    scenario.investigation(
        status=InvestigationStatus.INSUFFICIENT_EVIDENCE,
        completed_at=LATER,
        diagnosis={"status": "insufficient_evidence", "root_cause": None},
    )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"status": "running"}, "ck_jobs_lease_status"),
        ({"lease_owner": "worker-1", "lease_until": LATER}, "ck_jobs_lease_status"),
        ({"lease_owner": "worker-1"}, "ck_jobs_lease_fields_together"),
        (
            {"status": "running", "lease_owner": "", "lease_until": LATER},
            "ck_jobs_lease_owner_nonempty",
        ),
        ({"attempts": 4, "max_attempts": 3}, "ck_jobs_attempts_within_max"),
        ({"attempts": -1}, "ck_jobs_attempts_nonnegative"),
        ({"max_attempts": 0}, "ck_jobs_max_attempts_positive"),
        ({"lease_generation": -1}, "ck_jobs_lease_generation_nonnegative"),
        ({"last_error": ["boom"]}, "ck_jobs_last_error_object"),
    ],
)
def test_job_checks(scenario: Scenario, overrides: dict[str, Any], constraint: str) -> None:
    values = {"investigation_id": scenario.investigation()} | overrides
    assert violated_constraint(scenario.engine, Job, **values) == constraint


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"status": "running"}, "ck_agent_steps_started_at_status"),
        (
            {"status": "completed", "started_at": T0, "output": {}},
            "ck_agent_steps_completed_at_status",
        ),
        (
            {"status": "completed", "started_at": T0, "completed_at": LATER},
            "ck_agent_steps_completed_output",
        ),
        ({"completed_at": LATER}, "ck_agent_steps_completed_at_status"),
        ({"step_key": ""}, "ck_agent_steps_step_key_length"),
        ({"input": []}, "ck_agent_steps_input_object"),
        ({"attempt": -1}, "ck_agent_steps_attempt_nonnegative"),
    ],
)
def test_step_checks(scenario: Scenario, overrides: dict[str, Any], constraint: str) -> None:
    values = scenario.step_values(scenario.investigation(), **overrides)
    assert violated_constraint(scenario.engine, AgentStep, **values) == constraint


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"cost_basis": "actual", "cost_microusd": 1}, "ck_llm_calls_cost_basis_status"),
        ({"status": "succeeded"}, "ck_llm_calls_cost_basis_status"),
        ({"status": "succeeded", "cost_basis": "unknown"}, "ck_llm_calls_succeeded_cost"),
        ({"status": "succeeded", "cost_basis": "actual"}, "ck_llm_calls_cost_amount"),
        (
            {"status": "unknown", "cost_basis": "estimated", "cost_microusd": 5},
            "ck_llm_calls_unknown_cost",
        ),
        (
            {"status": "unknown", "cost_basis": "unknown", "cost_microusd": 5},
            "ck_llm_calls_cost_amount",
        ),
        ({"status": "failed", "cost_basis": "invoiced"}, "ck_llm_calls_cost_basis"),
        ({"reserved_microusd": -1}, "ck_llm_calls_reserved_nonnegative"),
        ({"input_tokens": -1}, "ck_llm_calls_input_tokens_nonnegative"),
        ({"attempt": 0}, "ck_llm_calls_attempt_positive"),
    ],
)
def test_llm_call_cost_checks(
    scenario: Scenario, overrides: dict[str, Any], constraint: str
) -> None:
    values = scenario.llm_call_values(scenario.step(), **overrides)
    assert violated_constraint(scenario.engine, LlmCall, **values) == constraint


def test_llm_call_outcomes_record_cost_basis(scenario: Scenario) -> None:
    step_id = scenario.step()
    outcomes: list[dict[str, Any]] = [
        {"status": LlmCallStatus.PENDING},
        {"status": LlmCallStatus.SUCCEEDED, "cost_basis": CostBasis.ACTUAL, "cost_microusd": 900},
        {"status": LlmCallStatus.SUCCEEDED, "cost_basis": CostBasis.ESTIMATED, "cost_microusd": 1},
        {"status": LlmCallStatus.FAILED, "cost_basis": CostBasis.ACTUAL, "cost_microusd": 0},
        # Ambiguous timeout: no amount, the reservation stays in place.
        {"status": LlmCallStatus.UNKNOWN, "cost_basis": CostBasis.UNKNOWN},
    ]
    for attempt, outcome in enumerate(outcomes, start=1):
        values = scenario.llm_call_values(step_id, attempt=attempt)
        call_id = insert(scenario.engine, LlmCall, **values)
        update_status(scenario.engine, LlmCall, call_id, **outcome)


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"first_seen_at": LATER}, "ck_incidents_seen_order"),
        ({"status": "resolved"}, "ck_incidents_resolved_at_status"),
        ({"resolved_at": LATER}, "ck_incidents_resolved_at_status"),
        ({"fingerprint": ""}, "ck_incidents_fingerprint_nonempty"),
        ({"replay_scope": ""}, "ck_incidents_replay_scope_length"),
    ],
)
def test_incident_checks(scenario: Scenario, overrides: dict[str, Any], constraint: str) -> None:
    values = scenario.incident_values(**overrides)
    assert violated_constraint(scenario.engine, Incident, **values) == constraint


def test_relevance_is_bounded(scenario: Scenario) -> None:
    link = {
        "incident_id": scenario.incident(),
        "signal_id": scenario.signal(),
        "workspace_id": scenario.workspace_id,
        "replay_scope": "live",
    }
    assert (
        violated_constraint(scenario.engine, IncidentSignal, **link, relevance=1.5)
        == "ck_incident_signals_relevance_range"
    )


# Status transitions: the triggers must enforce exactly the contract maps.


def check_transitions[S: StrEnum](
    engine: Engine,
    model: type[Base],
    transitions: Mapping[S, frozenset[S]],
    state: Callable[[str], dict[str, Any]],
    create: Callable[[str], uuid.UUID],
) -> None:
    mismatches: list[str] = []
    for old in transitions:
        for new in transitions:
            row_id = create(old)
            allowed = new == old or new in transitions[old]
            accepted = True
            rejected_by: str | None = None
            try:
                update_status(engine, model, row_id, status=new, **state(new))
            except IntegrityError as error:
                rejected_by = constraint_name(error)
                accepted = False
            if rejected_by not in (None, f"ck_{model.__tablename__}_status_transition"):
                mismatches.append(f"{old}->{new}: rejected by {rejected_by}")
            if accepted != allowed:
                mismatches.append(f"{old}->{new}: accepted={accepted}, allowed={allowed}")
    assert mismatches == []


def test_incident_transitions(scenario: Scenario) -> None:
    check_transitions(
        scenario.engine,
        Incident,
        INCIDENT_TRANSITIONS,
        incident_state,
        lambda status: scenario.incident(status=status, **incident_state(status)),
    )


def test_investigation_transitions(scenario: Scenario) -> None:
    incident_id = scenario.incident()
    check_transitions(
        scenario.engine,
        Investigation,
        INVESTIGATION_TRANSITIONS,
        investigation_state,
        lambda status: scenario.investigation(
            incident_id, status=status, **investigation_state(status)
        ),
    )


def test_job_transitions(scenario: Scenario) -> None:
    check_transitions(
        scenario.engine,
        Job,
        JOB_TRANSITIONS,
        job_state,
        lambda status: insert(
            scenario.engine,
            Job,
            investigation_id=scenario.investigation(),
            status=status,
            **job_state(status),
        ),
    )


def test_step_transitions(scenario: Scenario) -> None:
    investigation_id = scenario.investigation()
    check_transitions(
        scenario.engine,
        AgentStep,
        STEP_TRANSITIONS,
        step_state,
        lambda status: scenario.step(investigation_id, status=status, **step_state(status)),
    )


def test_terminal_investigation_cannot_restart(scenario: Scenario) -> None:
    investigation_id = scenario.investigation(
        status=InvestigationStatus.CANCELLED, completed_at=LATER
    )
    assert (
        violated_update(
            scenario.engine,
            Investigation,
            investigation_id,
            status=InvestigationStatus.RUNNING,
            completed_at=None,
        )
        == "ck_investigations_status_transition"
    )


# Optimistic versions and progress cursor


def test_concurrent_incident_update_is_rejected_by_version(scenario: Scenario) -> None:
    incident_id = scenario.incident()
    with Session(scenario.engine) as first, Session(scenario.engine) as second:
        mine = first.get_one(Incident, incident_id)
        theirs = second.get_one(Incident, incident_id)
        mine.last_seen_at = LATER
        first.commit()
        theirs.status = IncidentStatus.RESOLVED
        theirs.resolved_at = LATER
        with pytest.raises(StaleDataError):
            second.commit()
    with scenario.engine.connect() as conn:
        row = conn.execute(
            select(Incident.version, Incident.status).where(Incident.id == incident_id)
        ).one()
    assert tuple(row) == (2, IncidentStatus.OPEN)


def test_progress_event_ids_increase(scenario: Scenario) -> None:
    investigation_id = scenario.investigation()
    table = ProgressEvent.__table__
    assert isinstance(table, Table)
    with scenario.engine.begin() as conn:
        ids = [
            conn.execute(
                table.insert()
                .values(investigation_id=investigation_id, event_type="step.completed")
                .returning(table.c.id)
            ).scalar_one()
            for _ in range(3)
        ]
        count = conn.execute(
            select(func.count())
            .select_from(table)
            .where(table.c.investigation_id == investigation_id)
        ).scalar_one()
    assert ids == sorted(set(ids))
    assert count == 3


def test_timestamps_are_timezone_aware(scenario: Scenario) -> None:
    investigation_id = scenario.investigation()
    with scenario.engine.connect() as conn:
        created: datetime = conn.execute(
            select(Investigation.created_at).where(Investigation.id == investigation_id)
        ).scalar_one()
    assert created.tzinfo is not None
