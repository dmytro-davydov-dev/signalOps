# Development guide

> Status: **proposed**. The repository is not bootstrapped yet (next task: P01-T01). The commands below are conventions from [CLAUDE.md](../CLAUDE.md), not verified scripts. Phase 1 replaces this note with the canonical commands.

## Repository layout (planned)

```text
signalops/
  apps/web/                   React + TypeScript (pnpm)
  backend/signalops/
    api/                      HTTP and SSE
    contracts/                Pydantic models and exported JSON Schemas
    adapters/                 Logs, traces, deployments
    streaming/                Kafka consumers and outbox publisher
    detection/                Rules and durable window state
    agents/                   Workflow nodes and tools
    gateway/                  Model calls, rate limits, budgets
    persistence/              SQLAlchemy models and repositories
    evaluation/               Replay and scoring
  migrations/                 Alembic revisions
  fixtures/                   Versioned deterministic scenarios
  tests/                      Contract, unit, integration, recovery, evaluation
  infra/                      Compose and observability configuration
  docs/                       This documentation
```

## Prerequisites (expected)

- Docker with Compose v2
- [uv](https://docs.astral.sh/uv/) for Python
- Node.js with [pnpm](https://pnpm.io/) for the frontend

No model provider key is needed. Fake-provider mode is the default.

## Bootstrap commands (verified)

The Python project is defined by the root `pyproject.toml` (package source in `backend/signalops`, Python 3.12 via `.python-version`). The frontend package lives in `apps/web`.

```bash
# Backend (repository root)
uv sync
uv run signalops --version

# Frontend (apps/web)
pnpm install --frozen-lockfile
pnpm build
```

## Proposed commands

```bash
# Infrastructure (repository root)
docker compose config --quiet
docker compose up

# Backend (from its configured project directory)
uv run ruff check .
uv run ruff format --check .
uv run mypy signalops
uv run pytest

# Frontend (apps/web)
pnpm lint
pnpm typecheck
pnpm test --run
pnpm build
```

## Configuration

- Environment variables are documented in an `.env.example` file with placeholders, arriving in P01-T05.
- Never commit real secrets. Source configuration stores secret references, not credentials.

## Conventions

- **Contracts first.** Pydantic models for events, DTOs, checkpoints, tool results, and diagnoses. Breaking changes get a new version.
- **Domain logic stays separate** from HTTP handlers and Kafka plumbing. Abstractions should match actual reuse.
- **Type annotations and explicit error categories** everywhere. No broad exception handling that silently acknowledges failed work.
- **Timestamps** are UTC-aware. Source event IDs are stable, domain IDs are UUIDs, and money is stored in integer units.
- **Migrations:** every constraint and index goes through an Alembic revision. Review generated revisions before applying them.
- **Frontend:** server data lives in the query layer, interaction state in components and hooks. Handle loading, empty, stale, disconnected, failed, and insufficient-evidence states.
- **No new infrastructure** (Kubernetes, a graph database, an agent framework, a new service) without a concrete requirement recorded in [decisions.md](decisions.md).

## Testing expectations

- Use real PostgreSQL and Kafka for transaction, offset, and redelivery guarantees. Mocks do not count as evidence.
- Use deterministic fake-provider failures for throttling, timeout, invalid output, and insufficient evidence.
- Add failure and concurrency tests alongside each feature rather than deferring them to Phase 11. See [reliability.md](reliability.md#verification-checklist).
- Never report a skipped check or a mock result as a measured success.

## Working with the roadmap

1. Read [development_roadmap.md](development_roadmap.md): the progress ledger, dependencies, and the active task.
2. Pick the user-selected task or the next unblocked one, and mark it in progress in the ledger.
3. Implement it, run the relevant checks, and evaluate the task's acceptance condition.
4. Check the box only when acceptance passes. Update the ledger and the work log, and record a commit hash only if a commit exists.
