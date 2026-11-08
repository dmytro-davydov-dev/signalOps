"""The pieces a workflow is made of: its run, its nodes, and their shared context.

See `workflow` for how the engine runs them (ADR-022).
"""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from signalops.contracts.common import ContractModel
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.workflow import AgentRole
from signalops.persistence.database import Database
from signalops.persistence.models import Investigation
from signalops.persistence.repositories.jobs import JobLease
from signalops.persistence.repositories.scope import EvidenceScope

if TYPE_CHECKING:
    from signalops.agents.investigator import AgentRuntime


@dataclass(frozen=True)
class WorkflowRun:
    """What the workflow may know about its investigation, copied from the row."""

    investigation_id: uuid.UUID
    incident_id: uuid.UUID
    scope: EvidenceScope
    evidence_cutoff_at: datetime
    workflow_version: str
    max_steps: int
    budget_microusd: int = 0

    @classmethod
    def of(cls, investigation: Investigation) -> "WorkflowRun":
        return cls(
            investigation.id,
            investigation.incident_id,
            EvidenceScope(investigation.workspace_id, investigation.replay_scope),
            investigation.evidence_cutoff_at,
            investigation.workflow_version,
            investigation.max_steps,
            investigation.budget_microusd,
        )


class NodeContext:
    def __init__(
        self, database: Database, run: WorkflowRun, runtime: "AgentRuntime | None" = None
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
