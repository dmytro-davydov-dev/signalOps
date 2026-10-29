# Development guide

> Status: Phase 1 in progress. Commands marked *verified* run in this repository and in CI; the rest are still proposed.

## Repository layout

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

## Commands

The Python project is defined by the root `pyproject.toml` (package source in `backend/signalops`, Python 3.12 via `.python-version`), so backend commands run from the repository root. The frontend package lives in `apps/web`.

### Backend (repository root, verified)

```bash
uv sync --locked              # install from uv.lock
uv run signalops --version
uv run ruff check .           # lint
uv run ruff format --check .  # formatting
uv run mypy                   # strict type check of backend/signalops and tests
uv run pytest                 # tests
```

### Frontend (`apps/web`, verified)

```bash
pnpm install --frozen-lockfile
pnpm lint          # ESLint, type-aware, zero warnings allowed
pnpm typecheck     # tsc -b --noEmit
pnpm test          # Vitest, single run (pnpm test:watch for watch mode)
pnpm build         # type check + production build
```

CI (`.github/workflows/ci.yml`) runs exactly these commands on pushes to `main` and on pull requests.

### Infrastructure (repository root, verified)

```bash
cp .env.example .env                 # once; placeholders only
docker compose config --quiet        # validate
docker compose up -d --wait          # PostgreSQL, Redis, Kafka with health checks
docker compose --profile tools build backend   # shared backend image
docker compose down                  # stop; named volumes are kept
```

| Service | Image | Host access (loopback only) |
| --- | --- | --- |
| PostgreSQL | `postgres:17.11-alpine` | `127.0.0.1:${POSTGRES_HOST_PORT:-55432}` |
| Redis | `redis:7.4.12-alpine` | `127.0.0.1:${REDIS_HOST_PORT:-56379}` |
| Kafka (KRaft, single node) | `apache/kafka:4.3.0` | `localhost:${KAFKA_HOST_PORT:-19094}` |
| Backend image | `python:3.12.16-slim` + uv 0.12.5 | n/a |

- Containers reach each other on the private `signalops` network as `postgres:5432`, `redis:6379`, and `kafka:9092`.
- Host ports exist so that host-run integration tests can use real infrastructure. They are bound to `127.0.0.1` and default to non-standard ports to avoid clashing with other local stacks.
- Kafka topic auto-creation is disabled; topics are created explicitly.
- Data lives in the named volumes `postgres-data`, `redis-data`, and `kafka-data`. `docker compose down -v` deletes them.
- Single-node Kafka (replication factor 1) and local volumes do not protect against host or disk loss.

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
