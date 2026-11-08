"""One bounded agent: model turns and evidence tools until it submits (ADR-028, ADR-031).

Every role that calls a model runs this loop with its own prompt, tools, brief,
and submit tool. Each turn is a gateway call, so a stop or an exhausted budget
ends the loop at the next reservation. Tools run in the step's lease and scope.

**Authority.** The role's tools are the only tools offered and the only tools run.
A call to any other name, including another role's tool, is an error result for
the model. Nothing the model or the evidence says changes that.

**What was read.** The loop keeps the signal IDs and runbook versions that tool
results returned, so a role's check can refuse a citation it never read.

**Repair.** A reply that cannot be used goes back to the model with field locations
only, a bounded number of times. The loop then ends without output, and the caller
abstains: an unsupported answer is never stored.

**Shares.** A role given a `BudgetLimit` stops when its next call does not fit, and
the caller reports that; only a call drawing on the whole budget can end the
investigation `budget_exceeded`.

**Restart.** The conversation lives in memory, because a provider's thinking blocks
are not stored. A node that restarts begins it again, and each call is a new billable
reservation. Tools are read-only, so re-reading changes nothing.
"""

import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ValidationError

from signalops.agents.read_cache import ToolScope
from signalops.agents.tools import tool_definitions
from signalops.contracts.common import describe_validation_error
from signalops.contracts.roles import AgentEnding, ToolCallRef
from signalops.contracts.workflow import ToolName
from signalops.gateway.errors import BudgetShareExhaustedError
from signalops.gateway.gateway import BudgetLimit
from signalops.gateway.types import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ModelRole,
    StopReason,
    TextBlock,
    ToolDefinition,
    ToolResultBlock,
    ToolUseBlock,
)
from signalops.persistence.repositories.jobs import JobLease

if TYPE_CHECKING:
    from signalops.agents.investigator import AgentRuntime

logger = logging.getLogger(__name__)

MAX_TOOL_CALLS_PER_TURN: Final = 5


@dataclass
class Reads:
    """What this loop's tool results returned so far."""

    signal_ids: set[uuid.UUID] = field(default_factory=set)
    references: set[str] = field(default_factory=set)
    tool_calls: list[ToolCallRef] = field(default_factory=list)

    def take(self, tool: ToolName, call_key: str, content: str) -> None:
        if call_key and all(ref.call_key != call_key for ref in self.tool_calls):
            self.tool_calls.append(ToolCallRef(tool=tool, call_key=call_key))
        try:
            document = json.loads(content)
        except ValueError:
            return
        for item in document.get("items", ()):
            if not isinstance(item, dict):
                continue
            for name in ("signal_id", "provenance_signal_id"):
                value = item.get(name)
                if isinstance(value, str):
                    try:
                        self.signal_ids.add(uuid.UUID(value))
                    except ValueError:
                        continue
            if isinstance(item.get("runbook_key"), str) and isinstance(item.get("version"), str):
                self.references.add(f"{item['runbook_key']}@{item['version']}")


@dataclass(frozen=True)
class RoleTask[T: BaseModel]:
    """One role's loop: its prompt, its tools, and what it must submit."""

    prompt_version: str
    system: str
    brief: str
    # None offers all tools; an empty tuple offers none.
    tools: Collection[ToolName] | None
    submit: ToolDefinition
    output: type[T]
    check: Callable[[T, Reads], Awaitable[str | None]]
    # Shown to the model before a repair, then the field locations.
    invalid_prefix: str = "Invalid output"
    # A share and money held back for later roles; None draws on the whole budget.
    budget: BudgetLimit | None = None


@dataclass(frozen=True)
class LoopResult[T: BaseModel]:
    # None unless `ended_by` is SUBMITTED.
    output: T | None
    ended_by: AgentEnding
    turns: int
    reads: Reads


def _user(*blocks: TextBlock | ToolResultBlock) -> ModelMessage:
    return ModelMessage(role=ModelRole.USER, content=blocks)


class AgentLoop[T: BaseModel]:
    def __init__(
        self,
        runtime: "AgentRuntime",
        lease: JobLease,
        step_key: str,
        scope: ToolScope,
        task: RoleTask[T],
    ) -> None:
        self._runtime = runtime
        self._lease = lease
        self._step_key = step_key
        self._scope = scope
        self._task = task
        self._messages: list[ModelMessage] = [_user(TextBlock(text=task.brief))]
        self._allowed = frozenset(ToolName) if task.tools is None else frozenset(task.tools)
        self._definitions = (*tool_definitions(task.tools), task.submit)
        self._repairs = 0
        self._tool_calls = 0
        self._turns = 0
        self._reads = Reads()

    async def run(self, max_turns: int) -> LoopResult[T]:
        if max_turns <= 0:
            return self._ended(AgentEnding.NO_TURNS)
        for _turn in range(max_turns):
            try:
                response = await self._ask()
            except BudgetShareExhaustedError:
                return self._ended(AgentEnding.BUDGET_SHARE)
            self._turns += 1
            if response.stop_reason is StopReason.REFUSAL:
                return self._ended(AgentEnding.REFUSED)
            finished = await self._take(response)
            if finished is not None:
                return LoopResult(finished, AgentEnding.SUBMITTED, self._turns, self._reads)
            if self._repairs > self._runtime.limits.max_repairs:
                return self._ended(AgentEnding.REPAIR_LIMIT)
        return self._ended(AgentEnding.TURN_LIMIT)

    def _ended(self, ending: AgentEnding) -> LoopResult[T]:
        return LoopResult(None, ending, self._turns, self._reads)

    async def _ask(self) -> ModelResponse:
        limits = self._runtime.limits
        request = ModelRequest(
            model=self._runtime.model,
            prompt_version=self._task.prompt_version,
            system=self._task.system,
            messages=tuple(self._messages),
            tools=self._definitions,
            max_output_tokens=limits.max_output_tokens,
            effort=limits.effort,
        )
        return await self._runtime.gateway.complete(
            self._lease, self._step_key, request, budget=self._task.budget
        )

    async def _take(self, response: ModelResponse) -> T | None:
        if response.stop_reason is StopReason.MAX_TOKENS:
            # A cut-off turn may hold a partial tool call, so it is not kept or acted on.
            self._repairs += 1
            self._messages.append(
                _user(TextBlock(text="Your reply was cut off. Reply again, more briefly."))
            )
            return None
        self._messages.append(ModelMessage(role=ModelRole.ASSISTANT, content=response.content))
        uses = response.tool_uses()
        submit = self._task.submit.name
        if not uses:
            self._repairs += 1
            self._messages.append(
                _user(TextBlock(text=f"Finish by calling {submit}, or call an evidence tool."))
            )
            return None
        results: list[ToolResultBlock] = []
        for position, use in enumerate(uses):
            if use.name == submit:
                output, problem = await self._submitted(use)
                if output is not None:
                    return output
                results.append(self._error(use, problem))
            elif position >= MAX_TOOL_CALLS_PER_TURN:
                results.append(self._error(use, "Too many tool calls in one turn."))
            elif self._tool_calls >= self._runtime.limits.max_tool_calls:
                results.append(self._error(use, "The tool call limit is reached. Submit now."))
            else:
                results.append(await self._run_tool(use))
        self._messages.append(_user(*results))
        return None

    async def _submitted(self, use: ToolUseBlock) -> tuple[T | None, str]:
        try:
            output = self._task.output.model_validate(use.input)
        except ValidationError as error:
            self._repairs += 1
            return None, f"{self._task.invalid_prefix}: {describe_validation_error(error)}"
        problem = await self._task.check(output, self._reads)
        if problem is not None:
            self._repairs += 1
            return None, problem
        return output, ""

    @staticmethod
    def _error(use: ToolUseBlock, message: str) -> ToolResultBlock:
        return ToolResultBlock(tool_use_id=use.id, content=message, is_error=True)

    async def _run_tool(self, use: ToolUseBlock) -> ToolResultBlock:
        try:
            tool = ToolName(use.name)
        except ValueError:
            return self._error(use, "There is no such tool.")
        if tool not in self._allowed:
            # Offered to another role only; refused here, whatever the request says.
            return self._error(use, "There is no such tool.")
        self._tool_calls += 1
        outcome = await self._runtime.tools.run(
            self._lease, self._step_key, self._scope, tool, use.input
        )
        if not outcome.is_error:
            self._reads.take(tool, outcome.call_key, outcome.content)
        return ToolResultBlock(
            tool_use_id=use.id, content=outcome.content, is_error=outcome.is_error
        )
