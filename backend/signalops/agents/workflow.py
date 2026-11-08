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
output `conclude` turns into the diagnosis. `v3` is the multi-agent workflow of
`agents/multi_agent.py`. A node that makes model calls reaches them through the
gateway, which makes the stop check before each one.
"""

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from typing import Any, Final

from pydantic import ValidationError

from signalops.agents.briefs import LOAD_INCIDENT
from signalops.agents.findings import verify_findings
from signalops.agents.investigator import AgentRuntime, investigate_incident
from signalops.agents.jobs import JobStoppedError, PermanentJobError, error_record
from signalops.agents.multi_agent import CONCLUDE_VERIFIED, V3_NODES, WORKFLOW_V3
from signalops.agents.nodes import Node as Node
from signalops.agents.nodes import NodeContext as NodeContext
from signalops.agents.nodes import Parallel as Parallel
from signalops.agents.nodes import Workflow as Workflow
from signalops.agents.nodes import WorkflowRun as WorkflowRun
from signalops.agents.read_cache import ToolScope
from signalops.contracts.checkpoints import StepCheckpoint, decode_checkpoint
from signalops.contracts.common import ContractError, ContractModel
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus, RootCause
from signalops.contracts.steps import (
    IncidentBrief,
    InvestigatorFindings,
    ProposedDiagnosis,
)
from signalops.contracts.workflow import AgentRole, StepStatus
from signalops.gateway.errors import BudgetExceededError
from signalops.persistence.database import Database
from signalops.persistence.models import AgentStep, Investigation
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.incidents import IncidentRepository
from signalops.persistence.repositories.jobs import JobLease, JobRepository, LeaseLostError

logger = logging.getLogger(__name__)


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


async def investigate_with_agent(context: NodeContext) -> ProposedDiagnosis:
    runtime = context.runtime
    if runtime is None:
        raise PermanentJobError("this worker has no model runtime")
    run = context.run
    scope = ToolScope(run.scope, run.evidence_cutoff_at)

    async def check(proposal: ProposedDiagnosis) -> str | None:
        return await verify_findings(context.database, scope, proposal)

    return await investigate_incident(
        runtime,
        context.lease,
        context.step_key,
        scope,
        context.output(LOAD_INCIDENT),
        max_turns=run.max_steps - context.step_count,
        check=check,
    )


async def conclude_findings(context: NodeContext) -> Diagnosis:
    run = context.run
    proposal = context.output(INVESTIGATE_AGENT)
    cause = proposal.root_cause
    # Checked again here, because this is the last step before the outcome is stored.
    scope = ToolScope(run.scope, run.evidence_cutoff_at)
    problem = await verify_findings(context.database, scope, proposal)
    if problem is not None:
        return _unsupported(proposal, run, "The cited evidence could not be verified")
    root_cause = None
    if cause is not None:
        async with context.database.transaction() as session:
            services = await CatalogRepository(session).get_services(
                run.scope.workspace_id, [cause.service_id]
            )
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

INVESTIGATE_AGENT: Final = Node(
    "investigate", AgentRole.INVESTIGATOR, ProposedDiagnosis, investigate_with_agent
)
CONCLUDE_FINDINGS: Final = Node("conclude", AgentRole.SYNTHESIZER, Diagnosis, conclude_findings)

WORKFLOWS: Final[Mapping[str, Workflow]] = {
    # The fake sequence of Phase 6; finished and running investigations keep it.
    "v1": Workflow("v1", (LOAD_CONTEXT, INVESTIGATE), CONCLUDE),
    "v2": Workflow("v2", (LOAD_INCIDENT, INVESTIGATE_AGENT), CONCLUDE_FINDINGS),
    WORKFLOW_V3: Workflow(WORKFLOW_V3, V3_NODES, CONCLUDE_VERIFIED),
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


_SEVERITY_ORDER: Final = (JobStoppedError, LeaseLostError, BudgetExceededError, PermanentJobError)


def _severity(failure: BaseException) -> int:
    if not isinstance(failure, Exception):
        # A cancellation or interpreter exit outranks any job error.
        return -1
    for rank, kind in enumerate(_SEVERITY_ORDER):
        if isinstance(failure, kind):
            return rank
    return len(_SEVERITY_ORDER)


class WorkflowEngine:
    """`JobHandler` that runs an investigation's unfinished nodes."""

    def __init__(
        self,
        database: Database,
        workflows: Mapping[str, Workflow] = WORKFLOWS,
        runtime: AgentRuntime | None = None,
        *,
        branch_slots: asyncio.Semaphore | None = None,
    ) -> None:
        self._database = database
        self._workflows = workflows
        self._runtime = runtime
        # Shared by every job this engine runs, so it bounds the whole process.
        self._slots: asyncio.Semaphore | contextlib.nullcontext[None] = (
            branch_slots or contextlib.nullcontext()
        )

    async def __call__(self, lease: JobLease) -> Diagnosis:
        async with self._database.transaction() as session:
            run = WorkflowRun.of(await session.get_one(Investigation, lease.investigation_id))
            stored = await JobRepository(session).steps_of(lease.investigation_id)
        workflow = self._workflows.get(run.workflow_version)
        if workflow is None:
            raise PermanentJobError(f"unknown workflow version {run.workflow_version}")
        steps = {step.step_key: step for step in stored}
        context = NodeContext(self._database, run, self._runtime)
        count = len(workflow.steps)
        index = 0
        for stage in workflow.stages:
            group = stage.nodes if isinstance(stage, Parallel) else (stage,)
            pending: list[Node[Any]] = []
            for node in group:
                step = steps.get(node.key)
                if step is not None and step.status == StepStatus.COMPLETED:
                    context.keep(node, reused_output(node, step, run.workflow_version))
                    logger.info(
                        "step reused", extra={"step_key": node.key, "attempt": lease.attempt}
                    )
                elif index >= run.max_steps:
                    raise PermanentJobError(f"max_steps {run.max_steps} reached before {node.key}")
                else:
                    pending.append(node)
                index += 1
            if isinstance(stage, Parallel):
                await self._branches(lease, pending, context, count)
            else:
                for node in pending:
                    await self._execute(lease, node, context.at(lease, node.key, count))
        return context.output(workflow.conclusion)

    async def _branches(
        self, lease: JobLease, nodes: list[Node[Any]], context: NodeContext, count: int
    ) -> None:
        """Run a parallel stage's unfinished nodes, each once a global slot is free.

        Every branch runs to its end, so a sibling's failure loses no finished work.
        Then the most decisive failure is raised: a stop or lost lease first, then
        an exhausted budget, a permanent error, and a retryable one last.
        """

        async def branch(node: Node[Any]) -> None:
            async with self._slots:
                await self._execute(lease, node, context.at(lease, node.key, count))

        outcomes = await asyncio.gather(*(branch(node) for node in nodes), return_exceptions=True)
        failures = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
        if not failures:
            return
        if len(failures) > 1:
            logger.warning(
                "parallel branches failed",
                extra={"errors": sorted(type(failure).__name__ for failure in failures)},
            )
        raise min(failures, key=_severity)

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
