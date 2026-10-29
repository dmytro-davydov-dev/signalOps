"""Incident and investigation workflow enumerations and allowed status transitions.

These drive the persistence CHECK constraints and status-transition triggers.
Migrations hardcode the values, so changing a member or a transition requires a
new revision; an integration test compares the database rules with these maps.
"""

from enum import StrEnum


class IncidentStatus(StrEnum):
    OPEN = "open"
    RESOLVED = "resolved"


class IncidentSeverity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IncidentServiceRole(StrEnum):
    # The service whose detection rule fired.
    PRIMARY = "primary"
    # A service correlated with the incident; not a causal claim.
    AFFECTED = "affected"


class InvestigationStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BUDGET_EXCEEDED = "budget_exceeded"


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class AgentRole(StrEnum):
    # The single tool-using agent of the first slice.
    INVESTIGATOR = "investigator"
    COORDINATOR = "coordinator"
    TELEMETRY_INVESTIGATOR = "telemetry_investigator"
    DEPLOYMENT_INVESTIGATOR = "deployment_investigator"
    DEPENDENCY_INVESTIGATOR = "dependency_investigator"
    SYNTHESIZER = "synthesizer"
    VERIFIER = "verifier"


class ToolName(StrEnum):
    QUERY_LOGS = "query_logs"
    GET_TRACE_SUMMARY = "get_trace_summary"
    GET_DEPLOYMENTS = "get_deployments"
    GET_SERVICE_DEPENDENCIES = "get_service_dependencies"
    RETRIEVE_RUNBOOKS = "retrieve_runbooks"


class ToolCallStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class LlmCallStatus(StrEnum):
    # Cost reserved, request in flight.
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    # Definite failure reported by the provider.
    FAILED = "failed"
    # Ambiguous outcome such as a timeout; the provider may still have charged.
    UNKNOWN = "unknown"


class CostBasis(StrEnum):
    ACTUAL = "actual"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"


TERMINAL_INVESTIGATION_STATUSES = frozenset(
    {
        InvestigationStatus.COMPLETED,
        InvestigationStatus.INSUFFICIENT_EVIDENCE,
        InvestigationStatus.FAILED,
        InvestigationStatus.CANCELLED,
        InvestigationStatus.BUDGET_EXCEEDED,
    }
)

TERMINAL_JOB_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})

# Status changes allowed besides keeping the same status. Terminal states have none.
INCIDENT_TRANSITIONS: dict[IncidentStatus, frozenset[IncidentStatus]] = {
    IncidentStatus.OPEN: frozenset({IncidentStatus.RESOLVED}),
    # A recurrence inside the same correlation window reopens the incident.
    IncidentStatus.RESOLVED: frozenset({IncidentStatus.OPEN}),
}

INVESTIGATION_TRANSITIONS: dict[InvestigationStatus, frozenset[InvestigationStatus]] = {
    InvestigationStatus.QUEUED: frozenset(
        {InvestigationStatus.RUNNING, InvestigationStatus.FAILED, InvestigationStatus.CANCELLED}
    ),
    InvestigationStatus.RUNNING: TERMINAL_INVESTIGATION_STATUSES,
    **{status: frozenset() for status in TERMINAL_INVESTIGATION_STATUSES},
}

JOB_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.FAILED, JobStatus.CANCELLED}),
    # Back to queued for a delayed retry; a reclaimed lease stays running.
    JobStatus.RUNNING: TERMINAL_JOB_STATUSES | {JobStatus.QUEUED},
    **{status: frozenset() for status in TERMINAL_JOB_STATUSES},
}

STEP_TRANSITIONS: dict[StepStatus, frozenset[StepStatus]] = {
    StepStatus.PENDING: frozenset({StepStatus.RUNNING, StepStatus.SKIPPED}),
    StepStatus.RUNNING: frozenset({StepStatus.COMPLETED, StepStatus.FAILED}),
    # A failed step may be retried as a new attempt.
    StepStatus.FAILED: frozenset({StepStatus.RUNNING}),
    # Completed output is reused after recovery, so it never changes state again.
    StepStatus.COMPLETED: frozenset(),
    StepStatus.SKIPPED: frozenset(),
}
