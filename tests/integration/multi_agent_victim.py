"""A `v3` worker process that `test_multi_agent_recovery.py` kills at a chosen point.

`VICTIM_MODE=branch`: the deployment branch hangs inside its first model call, after
the gateway reserved it, while the other branches run to completion.
`VICTIM_MODE=persist`: every step completes, then the worker hangs before the
transaction that ends the job and the investigation.

Everything else is the real workflow, gateway, tools, and fake roles.
"""

import asyncio
import os

from signalops.agents.investigator import AgentLimits, AgentRuntime
from signalops.agents.jobs import JobPolicy, JobRunner
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import WorkflowEngine
from signalops.config import get_settings
from signalops.contracts.diagnosis import Diagnosis
from signalops.gateway.fake import FAKE_MODEL, FakeProvider
from signalops.gateway.fake_roles import role_step
from signalops.gateway.gateway import ModelGateway
from signalops.gateway.types import ModelRequest, ModelResponse
from signalops.persistence.database import Database, create_engine
from signalops.persistence.repositories.jobs import JobLease

LIMITS = AgentLimits(max_output_tokens=1_000, effort=None, max_repairs=1, max_tool_calls=6)


class HangingDeployment:
    """Fake roles, except that the deployment branch's first call never returns."""

    name = "fake"

    def __init__(self) -> None:
        self._inner = FakeProvider(responder=role_step)

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if request.prompt_version == "deployment-v1":
            await asyncio.Event().wait()
        return await self._inner.complete(request)


async def main() -> None:
    settings = get_settings()
    database = Database.from_engine(create_engine(settings))
    mode = os.environ["VICTIM_MODE"]
    provider = HangingDeployment() if mode == "branch" else FakeProvider(responder=role_step)
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider),
        tools=ToolExecutor(database, TextRedactor(("password",))),
        model=FAKE_MODEL,
        limits=LIMITS,
    )
    engine = WorkflowEngine(database, runtime=runtime)

    async def handler(lease: JobLease) -> Diagnosis:
        diagnosis = await engine(lease)
        if mode == "persist":
            # The conclusion step is stored; the job and investigation are not ended.
            await asyncio.Event().wait()
        return diagnosis

    runner = JobRunner(database, handler, JobPolicy.from_settings(settings))
    await runner.run(asyncio.Event())


if __name__ == "__main__":
    asyncio.run(main())
