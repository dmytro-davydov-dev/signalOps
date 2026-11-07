"""A worker process that finishes `load_context`, then hangs in `investigate`.

`test_workflow_recovery.py` kills it there. Its workflow differs from v1 only in
that one node's behavior, so the steps it leaves are the ones v1 resumes.
"""

import asyncio
from dataclasses import replace

from signalops.agents.jobs import JobPolicy, JobRunner
from signalops.agents.workflow import (
    CONCLUDE,
    INVESTIGATE,
    LOAD_CONTEXT,
    NodeContext,
    Workflow,
    WorkflowEngine,
)
from signalops.config import get_settings
from signalops.contracts.steps import InvestigatorFindings
from signalops.persistence.database import Database, create_engine


async def hang(_context: NodeContext) -> InvestigatorFindings:
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


async def main() -> None:
    settings = get_settings()
    database = Database.from_engine(create_engine(settings))
    workflow = Workflow("v1", (LOAD_CONTEXT, replace(INVESTIGATE, run=hang), CONCLUDE))
    runner = JobRunner(
        database, WorkflowEngine(database, {"v1": workflow}), JobPolicy.from_settings(settings)
    )
    await runner.run(asyncio.Event())


if __name__ == "__main__":
    asyncio.run(main())
