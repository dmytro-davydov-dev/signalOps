"""The persisted investigation workflow, run by `JobRunner` under a lease (ADR-022).

**Nodes.** A node has a stable key, which is its step key, a role, and a typed
output. A node runs in no transaction, so a slow one holds no connection or lock.
It reads earlier output through `NodeContext` and has no other inputs.

**Steps.** The engine opens a step, runs the node, and completes the step with its
output, each write fenced by the lease in its own transaction. Output is validated
as it will be read back, so what is stored can be reused.

**Recovery.** A new attempt reloads the steps, reuses the completed ones, and runs
the rest, reopening a step its predecessor left running or failed. A node may run
twice when a worker dies inside it: reuse is durable, execution is at least once.
Completed output that no longer validates, or that another workflow version wrote,
fails the job, because a completed step can never run again.

**Stops.** Opening a step checks for a cancellation or a passed deadline in the same
transaction, so no node starts after either commits. The gateway makes the same check
before each model call inside a node (ADR-023).

**Outcome.** A workflow ends in a node whose output is the `Diagnosis`. The engine
returns it, reused if a crash came after that step, and `JobRunner` ends the job and
investigation with it in one transaction (ADR-024). So a replay of a finished
workflow reaches the outcome already stored.

**Versions.** An investigation runs the workflow version it was requested with. A
change to the nodes or their output needs a new entry in `WORKFLOWS`.

The nodes run in sequence. In `v1`, `investigate` makes no model call and always
abstains. In `v2`, it is the tool-using agent of `agents/investigator.py`, whose
output `conclude` turns into the diagnosis. A node that makes model calls reaches
them through the gateway, which makes the stop check before each one.
"""

import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, cast

from pydantic import ValidationError

from signalops.agents.investigations import WORKFLOW_VERSION
from signalops.agents.investigator import AgentRuntime, investigate_incident
from signalops.agents.jobs import JobStoppedError, PermanentJobError, error_record
from signalops.agents.tools import ToolScope
from signalops.contracts.checkpoints import StepCheckpoint, decode_checkpoint
from signalops.contracts.common import ContractError, ContractModel
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus, RootCause
from signalops.contracts.steps import (
    MAX_BRIEF_EVIDENCE,
    MAX_BRIEF_SERVICES,
    IncidentBrief,
    InvestigatorFindings,
    ProposedDiagnosis,
    ServiceBrief,
)
from signalops.contracts.workflow import AgentRole, StepStatus
from signalops.persistence.database import Database
from signalops.persistence.models import AgentStep, Investigation
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.incidents import IncidentRepository
from signalops.persistence.repositories.jobs import JobLease, JobRepository
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkflowRun:
    """What the workflow may know about its investigation, copied from the row."""

    investigation_id: uuid.UUID
    incident_id: uuid.UUID
    scope: EvidenceScope
    evidence_cutoff_at: datetime
    workflow_version: str
    max_steps: int

    @classmethod
    def of(cls, investigation: Investigation) -> "WorkflowRun":
        return cls(
            investigation.id,
            investigation.incident_id,
            EvidenceScope(investigation.workspace_id, investigation.replay_scope),
            investigation.evidence_cutoff_at,
            investigation.workflow_version,
            investigation.max_steps,
        )


class NodeContext:
    def __init__(
        self, database: Database, run: WorkflowRun, runtime: AgentRuntime | None = None
    ) -> None:
        self.database = database
        self.run = run
        self.runtime = runtime
        self._outputs: dict[str, ContractModel] = {}
        self._lease: JobLease | None = None
        self._step_key = ""
        self._step_count = 0

    def at(self, lease: JobLease, step_key: str, step_count: int) -> "NodeContext":
        """This context for one step, sharing the outputs kept so far."""
        step = NodeContext(self.database, self.run, self.runtime)
        step._outputs = self._outputs
        step._lease = lease
        step._step_key = step_key
        step._step_count = step_count
        return step

    @property
    def lease(self) -> JobLease:
        if self._lease is None:
            raise RuntimeError("the context is not at a step")
        return self._lease

    @property
    def step_key(self) -> str:
        return self._step_key

    @property
    def step_count(self) -> int:
        """Steps in this workflow. `max_steps` beyond them is left for model turns."""
        return self._step_count

    def output[T: ContractModel](self, node: "Node[T]") -> T:
        """The output of an earlier node."""
        return cast("T", self._outputs[node.key])

    def keep[T: ContractModel](self, node: "Node[T]", output: T) -> None:
        self._outputs[node.key] = output

    @property
    def finished(self) -> list[str]:
        return list(self._outputs)


@dataclass(frozen=True)
class Node[T: ContractModel]:
    key: str
    role: AgentRole
    output: type[T]
    run: Callable[[NodeContext], Awaitable[T]]


@dataclass(frozen=True)
class Workflow:
    version: str
    # The nodes before the conclusion.
    nodes: tuple[Node[Any], ...]
    conclusion: Node[Diagnosis]

    @property
    def steps(self) -> tuple[Node[Any], ...]:
        return (*self.nodes, self.conclusion)


async def load_context(context: NodeContext) -> IncidentBrief:
    run = context.run
    async with context.database.transaction() as session:
        incident = await IncidentRepository(session).get(run.scope, run.incident_id)
    if incident is None:
        raise PermanentJobError("the incident is outside the investigation's scope")
    return IncidentBrief(
        incident_id=incident.id,
        title=incident.title,
        detected_at=incident.detected_at,
        evidence_cutoff_at=run.evidence_cutoff_at,
    )


async def investigate(_context: NodeContext) -> InvestigatorFindings:
    return InvestigatorFindings(missing_evidence=("No model-backed investigator is configured",))


async def conclude(context: NodeContext) -> Diagnosis:
    return Diagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary=f"No diagnosis for: {context.output(LOAD_CONTEXT).title}",
        root_cause=None,
        missing_evidence=context.output(INVESTIGATE).missing_evidence,
        workflow_version=context.run.workflow_version,
    )


async def load_incident(context: NodeContext) -> IncidentBrief:
    run = context.run
    cutoff = run.evidence_cutoff_at
    async with context.database.transaction() as session:
        incidents = IncidentRepository(session)
        incident = await incidents.get(run.scope, run.incident_id)
        if incident is None:
            raise PermanentJobError("the incident is outside the investigation's scope")
        services = await CatalogRepository(session).list_services(
            run.scope.workspace_id, limit=MAX_BRIEF_SERVICES
        )
        cited = await incidents.evidence_known_by(
            run.scope, run.incident_id, known_by=cutoff, limit=MAX_BRIEF_EVIDENCE
        )
    return IncidentBrief(
        incident_id=incident.id,
        title=incident.title,
        detected_at=incident.detected_at,
        evidence_cutoff_at=cutoff,
        window_start=incident.correlation_window_start,
        services=tuple(ServiceBrief(service_id=row.id, name=row.name) for row in services),
        evidence_signal_ids=tuple(cited),
    )


async def investigate_with_agent(context: NodeContext) -> ProposedDiagnosis:
    runtime = context.runtime
    if runtime is None:
        raise PermanentJobError("this worker has no model runtime")
    run = context.run
    return await investigate_incident(
        runtime,
        context.lease,
        context.step_key,
        ToolScope(run.scope, run.evidence_cutoff_at),
        context.output(LOAD_INCIDENT),
        max_turns=run.max_steps - context.step_count,
    )


async def conclude_findings(context: NodeContext) -> Diagnosis:
    run = context.run
    proposal = context.output(INVESTIGATE_AGENT)
    cause = proposal.root_cause
    root_cause = None
    if cause is not None:
        async with context.database.transaction() as session:
            services = await CatalogRepository(session).get_services(
                run.scope.workspace_id, [cause.service_id]
            )
        if not services:
            return _unsupported(proposal, run, "The named service is not in this workspace")
        root_cause = RootCause(**cause.model_dump(mode="python"), service_name=services[0].name)
    return Diagnosis(
        status=proposal.status,
        summary=proposal.summary,
        root_cause=root_cause,
        alternatives=proposal.alternatives,
        suggested_actions=proposal.suggested_actions,
        limitations=proposal.limitations,
        missing_evidence=proposal.missing_evidence,
        workflow_version=run.workflow_version,
    )


def _unsupported(proposal: ProposedDiagnosis, run: WorkflowRun, reason: str) -> Diagnosis:
    return Diagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary=proposal.summary,
        root_cause=None,
        limitations=proposal.limitations,
        missing_evidence=(reason,),
        workflow_version=run.workflow_version,
    )


LOAD_CONTEXT: Final = Node("load_context", AgentRole.COORDINATOR, IncidentBrief, load_context)
INVESTIGATE: Final = Node("investigate", AgentRole.INVESTIGATOR, InvestigatorFindings, investigate)
CONCLUDE: Final = Node("conclude", AgentRole.SYNTHESIZER, Diagnosis, conclude)

LOAD_INCIDENT: Final = Node("load_context", AgentRole.COORDINATOR, IncidentBrief, load_incident)
INVESTIGATE_AGENT: Final = Node(
    "investigate", AgentRole.INVESTIGATOR, ProposedDiagnosis, investigate_with_agent
)
CONCLUDE_FINDINGS: Final = Node("conclude", AgentRole.SYNTHESIZER, Diagnosis, conclude_findings)

WORKFLOWS: Final[Mapping[str, Workflow]] = {
    # The fake sequence of Phase 6; finished and running investigations keep it.
    "v1": Workflow("v1", (LOAD_CONTEXT, INVESTIGATE), CONCLUDE),
    WORKFLOW_VERSION: Workflow(
        WORKFLOW_VERSION, (LOAD_INCIDENT, INVESTIGATE_AGENT), CONCLUDE_FINDINGS
    ),
}


def reused_output[T: ContractModel](node: Node[T], step: AgentStep, workflow_version: str) -> T:
    """Validated output of a completed step. The message stored names no evidence."""
    try:
        checkpoint = decode_checkpoint(step.checkpoint or {})
        if not checkpoint.reusable_for(workflow_version):
            raise PermanentJobError(
                f"step {node.key} was written by workflow {checkpoint.workflow_version}"
            )
        return node.output.model_validate(step.output)
    except (ContractError, ValidationError) as error:
        raise PermanentJobError(f"step {node.key} cannot be reused") from error


class WorkflowEngine:
    """`JobHandler` that runs an investigation's unfinished nodes."""

    def __init__(
        self,
        database: Database,
        workflows: Mapping[str, Workflow] = WORKFLOWS,
        runtime: AgentRuntime | None = None,
    ) -> None:
        self._database = database
        self._workflows = workflows
        self._runtime = runtime

    async def __call__(self, lease: JobLease) -> Diagnosis:
        async with self._database.transaction() as session:
            run = WorkflowRun.of(await session.get_one(Investigation, lease.investigation_id))
            stored = await JobRepository(session).steps_of(lease.investigation_id)
        workflow = self._workflows.get(run.workflow_version)
        if workflow is None:
            raise PermanentJobError(f"unknown workflow version {run.workflow_version}")
        steps = {step.step_key: step for step in stored}
        context = NodeContext(self._database, run, self._runtime)
        for index, node in enumerate(workflow.steps):
            step = steps.get(node.key)
            if step is not None and step.status == StepStatus.COMPLETED:
                context.keep(node, reused_output(node, step, run.workflow_version))
                logger.info("step reused", extra={"step_key": node.key, "attempt": lease.attempt})
            elif index >= run.max_steps:
                raise PermanentJobError(f"max_steps {run.max_steps} reached before {node.key}")
            else:
                await self._execute(lease, node, context.at(lease, node.key, len(workflow.steps)))
        return context.output(workflow.conclusion)

    async def _execute[T: ContractModel](
        self, lease: JobLease, node: Node[T], context: NodeContext
    ) -> None:
        checkpoint = StepCheckpoint(
            workflow_version=context.run.workflow_version,
            step_key=node.key,
            agent_role=node.role,
            attempt=lease.attempt,
            lease_generation=lease.generation,
        )
        step_input = {"after": context.finished}
        async with self._database.transaction() as session:
            jobs = JobRepository(session)
            await jobs.hold(lease)
            if await jobs.stop_reason(lease.investigation_id) is not None:
                raise JobStoppedError(node.key)
            opened = await jobs.save_checkpoint(
                lease, node.key, node.role, step_input, checkpoint.model_dump(mode="json")
            )
        if not opened:
            raise PermanentJobError(f"step {node.key} finished in another state")
        try:
            output = (await node.run(context)).model_dump(mode="json")
            validated = node.output.model_validate(output)
        except Exception as error:
            failed = checkpoint.model_copy(update={"state": {"error": error_record(error)}})
            async with self._database.transaction() as session:
                await JobRepository(session).fail_step(
                    lease, node.key, failed.model_dump(mode="json"), error_record(error)["category"]
                )
            raise
        async with self._database.transaction() as session:
            completed = await JobRepository(session).complete_step(
                lease, node.key, output, checkpoint.model_dump(mode="json")
            )
        if not completed:
            raise PermanentJobError(f"step {node.key} finished in another state")
        context.keep(node, validated)
