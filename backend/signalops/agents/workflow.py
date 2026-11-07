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
transaction, so no node starts after either commits. Phase 7 must make the same
check before each model call inside a node (ADR-023).

**Versions.** An investigation runs the workflow version it was requested with. A
change to the nodes or their output needs a new entry in `WORKFLOWS`.

The nodes run in sequence. `investigate` stands in for the model-backed investigator
of Phase 7 and always abstains, so nothing here calls a provider or writes a
diagnosis to the investigation (P06-T05).
"""

import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, cast

from pydantic import ValidationError

from signalops.agents.investigations import WORKFLOW_VERSION
from signalops.agents.jobs import JobStoppedError, PermanentJobError, error_record
from signalops.contracts.checkpoints import StepCheckpoint, decode_checkpoint
from signalops.contracts.common import ContractError, ContractModel
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.steps import IncidentBrief, InvestigatorFindings
from signalops.contracts.workflow import AgentRole, StepStatus
from signalops.persistence.database import Database
from signalops.persistence.models import AgentStep, Investigation
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
    def __init__(self, database: Database, run: WorkflowRun) -> None:
        self.database = database
        self.run = run
        self._outputs: dict[str, ContractModel] = {}

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
    nodes: tuple[Node[Any], ...]


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


LOAD_CONTEXT: Final = Node("load_context", AgentRole.COORDINATOR, IncidentBrief, load_context)
INVESTIGATE: Final = Node("investigate", AgentRole.INVESTIGATOR, InvestigatorFindings, investigate)
CONCLUDE: Final = Node("conclude", AgentRole.SYNTHESIZER, Diagnosis, conclude)

WORKFLOWS: Final[Mapping[str, Workflow]] = {
    WORKFLOW_VERSION: Workflow(WORKFLOW_VERSION, (LOAD_CONTEXT, INVESTIGATE, CONCLUDE)),
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

    def __init__(self, database: Database, workflows: Mapping[str, Workflow] = WORKFLOWS) -> None:
        self._database = database
        self._workflows = workflows

    async def __call__(self, lease: JobLease) -> None:
        async with self._database.transaction() as session:
            run = WorkflowRun.of(await session.get_one(Investigation, lease.investigation_id))
            stored = await JobRepository(session).steps_of(lease.investigation_id)
        workflow = self._workflows.get(run.workflow_version)
        if workflow is None:
            raise PermanentJobError(f"unknown workflow version {run.workflow_version}")
        steps = {step.step_key: step for step in stored}
        context = NodeContext(self._database, run)
        for index, node in enumerate(workflow.nodes):
            step = steps.get(node.key)
            if step is not None and step.status == StepStatus.COMPLETED:
                context.keep(node, reused_output(node, step, run.workflow_version))
                logger.info("step reused", extra={"step_key": node.key, "attempt": lease.attempt})
            elif index >= run.max_steps:
                raise PermanentJobError(f"max_steps {run.max_steps} reached before {node.key}")
            else:
                await self._execute(lease, node, context)

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
                    lease, node.key, failed.model_dump(mode="json")
                )
            raise
        async with self._database.transaction() as session:
            completed = await JobRepository(session).complete_step(
                lease, node.key, output, checkpoint.model_dump(mode="json")
            )
        if not completed:
            raise PermanentJobError(f"step {node.key} finished in another state")
        context.keep(node, validated)
