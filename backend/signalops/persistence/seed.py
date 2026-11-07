"""Deterministic development seed: the demo workspace, its sources, and its services.

IDs are fixed literals that match `fixtures/contracts/v1`, so fixture events refer to
rows that exist after seeding. Seeding is idempotent and runs in one transaction.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from signalops.contracts.evidence import SourceKind
from signalops.persistence.database import Database
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.graph import GraphRepository, RunbookVersion
from signalops.persistence.repositories.scope import EvidenceScope


@dataclass(frozen=True)
class SeedSource:
    id: uuid.UUID
    name: str
    kind: SourceKind


@dataclass(frozen=True)
class SeedService:
    id: uuid.UUID
    name: str
    environment: str


DEMO_WORKSPACE_ID = uuid.UUID("82631aa1-86f5-4d37-b39f-fac74ebbdac8")
DEMO_WORKSPACE_NAME = "demo"
DEMO_ENVIRONMENT = "demo"

DEMO_SOURCES = (
    SeedSource(
        uuid.UUID("b6c72d79-7ec9-4df7-88d0-1d4332c88808"),
        "demo-structured-logs",
        SourceKind.STRUCTURED_LOG,
    ),
    SeedSource(
        uuid.UUID("1dee9492-8198-4ab9-af5e-9bcc709660fd"),
        "demo-trace-summaries",
        SourceKind.TRACE_SUMMARY,
    ),
    SeedSource(
        uuid.UUID("ad03a959-871e-4eeb-bb3f-dc63a69372aa"),
        "demo-deployments",
        SourceKind.DEPLOYMENT,
    ),
)

DEMO_SERVICES = (
    SeedService(uuid.UUID("dc2bf08d-6c3c-44ea-855a-d7d42eeefb36"), "checkout", DEMO_ENVIRONMENT),
    SeedService(uuid.UUID("5f3e2d1c-0b9a-4876-9543-210fedcba987"), "payment", DEMO_ENVIRONMENT),
    SeedService(uuid.UUID("72a0b9e1-770e-4b5c-8b54-f518c82d942a"), "inventory", DEMO_ENVIRONMENT),
    SeedService(
        uuid.UUID("57988918-06d9-4f40-82a2-48ed0c9c0adf"), "bank-gateway", DEMO_ENVIRONMENT
    ),
    SeedService(uuid.UUID("0c4f6b1e-3a8d-4e27-9b5c-6d2a1f8e7c30"), "orders-db", DEMO_ENVIRONMENT),
)


RUNBOOK_PUBLISHED_AT = datetime(2026, 10, 1, tzinfo=UTC)
DEMO_RUNBOOKS = (
    (
        RunbookVersion(
            key="downstream-timeouts",
            version="v1",
            title="Payment calls to a downstream gateway time out",
            content=(
                "Compare the latency of the downstream service with the caller's timeout. "
                "If the downstream trace durations rise first, the caller's errors are a "
                "symptom. Check recent deployments of the downstream service before "
                "changing the caller."
            ),
            provenance="demo runbook, synthetic",
            published_at=RUNBOOK_PUBLISHED_AT,
        ),
        ("payment", "bank-gateway"),
    ),
    (
        RunbookVersion(
            key="slow-order-queries",
            version="v1",
            title="Slow queries on the orders database",
            content=(
                "Occasional slow queries are ordinary. Treat them as a cause only when "
                "their rate rises with the incident and the callers' durations rise with them."
            ),
            provenance="demo runbook, synthetic",
            published_at=RUNBOOK_PUBLISHED_AT,
        ),
        ("orders-db", "checkout"),
    ),
)


async def seed_demo(database: Database) -> None:
    """Create the demo catalog, or verify that an existing one matches it."""
    async with database.transaction() as session:
        catalog = CatalogRepository(session)
        await catalog.ensure_workspace(DEMO_WORKSPACE_ID, DEMO_WORKSPACE_NAME)
        for source in DEMO_SOURCES:
            await catalog.ensure_source(
                source_id=source.id,
                workspace_id=DEMO_WORKSPACE_ID,
                name=source.name,
                kind=source.kind,
            )
        for service in DEMO_SERVICES:
            await catalog.ensure_service(
                service_id=service.id,
                workspace_id=DEMO_WORKSPACE_ID,
                name=service.name,
                environment=service.environment,
            )
        graph = GraphRepository(session)
        scope = EvidenceScope(DEMO_WORKSPACE_ID)
        ids = {service.name: service.id for service in DEMO_SERVICES}
        for runbook, services in DEMO_RUNBOOKS:
            await graph.ensure_runbook(scope, runbook, [ids[name] for name in services])
