"""Helpers for workflow `v3` tests: replayed incidents, queued investigations, fake roles."""

import json
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from signalops.agents.investigator import AgentLimits, AgentRuntime
from signalops.agents.tools import TextRedactor, ToolExecutor
from signalops.agents.workflow import WorkflowEngine
from signalops.contracts.roles import SpecialistFindings
from signalops.demo.generator import Scenario
from signalops.gateway.fake import FAKE_MODEL, FakeStep
from signalops.gateway.fake_roles import role_step
from signalops.gateway.gateway import ModelGateway
from signalops.gateway.provider import ModelProvider
from signalops.gateway.types import ModelRequest, TextBlock
from signalops.persistence import database as db
from signalops.persistence.models import AgentStep
from signalops.persistence.repositories.investigations import (
    InvestigationRepository,
    NewInvestigation,
)
from signalops.persistence.repositories.scope import EvidenceScope
from tests.integration.test_investigator import RETRIES, replayed_incident

V3_LIMITS = AgentLimits(max_output_tokens=1_000, effort=None, max_repairs=1, max_tool_calls=6)
V3_BUDGET = 5_000_000
V3_STEPS = [
    "load_context",
    "plan",
    "telemetry",
    "deployment",
    "dependency",
    "synthesize",
    "verify",
    "conclude",
]

Responder = Callable[[ModelRequest], FakeStep]


def roles_with(overrides: Mapping[str, Responder] | None = None) -> Responder:
    """The fake roles, with some prompt versions answered another way."""
    chosen = dict(overrides or {})

    def respond(request: ModelRequest) -> FakeStep:
        override = chosen.get(request.prompt_version)
        return override(request) if override is not None else role_step(request)

    return respond


def v3_engine(
    database: db.Database, provider: ModelProvider, limits: AgentLimits = V3_LIMITS
) -> WorkflowEngine:
    runtime = AgentRuntime(
        gateway=ModelGateway(database, provider, policy=RETRIES),
        tools=ToolExecutor(database, TextRedactor(("password", "token", "api_key"))),
        model=FAKE_MODEL,
        limits=limits,
    )
    return WorkflowEngine(database, runtime=runtime)


async def queued_v3(
    database: db.Database,
    scope: EvidenceScope,
    incident_id: uuid.UUID,
    *,
    max_steps: int = 40,
    budget: int = V3_BUDGET,
    deadline: timedelta = timedelta(minutes=10),
) -> uuid.UUID:
    now = datetime.now(UTC)
    async with database.transaction() as session:
        repository = InvestigationRepository(session)
        investigation = await repository.create(
            NewInvestigation(
                scope=scope,
                incident_id=incident_id,
                request_key=f"test:{uuid.uuid4()}",
                workflow_version="v3",
                budget_microusd=budget,
                max_steps=max_steps,
                created_at=now,
                deadline_at=now + deadline,
            )
        )
        assert investigation is not None
        assert await repository.enqueue(investigation.id)
        return investigation.id


async def timeout_incident(database: db.Database, seed: int) -> tuple[EvidenceScope, uuid.UUID]:
    scope, incident_id = await replayed_incident(database, Scenario.DOWNSTREAM_TIMEOUT, seed)
    assert incident_id is not None
    return scope, incident_id


async def steps_by_key(database: db.Database, investigation_id: uuid.UUID) -> dict[str, AgentStep]:
    async with database.transaction() as session:
        rows = await session.scalars(
            select(AgentStep).where(AgentStep.investigation_id == investigation_id)
        )
        return {row.step_key: row for row in rows}


def findings_of(step: AgentStep) -> SpecialistFindings:
    return SpecialistFindings.model_validate(step.output)


def brief_of(request: ModelRequest) -> dict[str, Any]:
    first = request.messages[0].content[0]
    assert isinstance(first, TextBlock)
    loaded: dict[str, Any] = json.loads(first.text)
    return loaded


def offered(request: ModelRequest) -> set[str]:
    return {tool.name for tool in request.tools}
