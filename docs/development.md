# Development guide

> Status: Phase 1 complete. All commands below are verified in this repository.

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

## Prerequisites

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
uv run pytest                 # unit tests; integration tests are skipped
uv run signalops export-schemas          # regenerate JSON Schemas, OpenAPI, generated UI contracts
uv run signalops export-schemas --check  # exit 1 if they are out of date (pytest checks this too)

# Integration tests against the running Compose infrastructure
set -a; . ./.env; set +a
SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration
```

Run the API on the host against Compose infrastructure:

```bash
set -a; . ./.env; set +a
export SIGNALOPS_DATABASE_URL="postgresql+psycopg://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_HOST_PORT/$POSTGRES_DB"
export SIGNALOPS_REDIS_URL="redis://localhost:$REDIS_HOST_PORT/0"
export SIGNALOPS_KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_HOST_PORT"
SIGNALOPS_API_PORT=8011 uv run signalops api   # 8010 is taken by the api container
```

### Frontend (`apps/web`, verified)

```bash
pnpm install --frozen-lockfile
pnpm lint          # ESLint, type-aware, zero warnings allowed
pnpm typecheck     # tsc -b --noEmit
pnpm test          # Vitest, single run (pnpm test:watch for watch mode)
pnpm build         # type check + production build
pnpm dev           # Vite dev server; proxies /health/ and /v1/ to SIGNALOPS_API_URL (default http://127.0.0.1:8010)
```

CI (`.github/workflows/ci.yml`) runs exactly these commands on pushes to `main` and on pull requests.

### Infrastructure (repository root, verified)

```bash
cp .env.example .env                 # once; placeholders only
docker compose config --quiet        # validate
docker compose up -d --build --wait  # infrastructure, one-shot jobs, api, frontend
docker compose --profile tools build backend   # shared backend image only
docker compose run --rm migrate      # re-run migrations on demand
docker compose run --rm init-topics  # re-run topic provisioning on demand
docker compose down                  # stop; named volumes are kept
```

| Service | Image | Host access (loopback only) |
| --- | --- | --- |
| PostgreSQL | `postgres:17.11-alpine` | `127.0.0.1:${POSTGRES_HOST_PORT:-55432}` |
| Redis | `redis:7.4.12-alpine` | `127.0.0.1:${REDIS_HOST_PORT:-56379}` |
| Kafka (KRaft, single node) | `apache/kafka:4.3.0` | `localhost:${KAFKA_HOST_PORT:-19094}` |
| API (`signalops api`) | backend image: `python:3.12.16-slim` + uv 0.12.5 | `http://127.0.0.1:${API_HOST_PORT:-8010}` |
| Frontend | `node:24.19.0-alpine` build, `nginx:1.28.3-alpine` serve | `http://127.0.0.1:${WEB_HOST_PORT:-8090}` |

Useful URLs once the stack is up:

- UI: `http://127.0.0.1:8090/` (system health at `/status`)
- Liveness: `http://127.0.0.1:8010/health/live` (process is up; no dependency checks)
- Readiness: `http://127.0.0.1:8010/health/ready` (200 when PostgreSQL, Redis, and Kafka all answer within the timeout; 503 with per-dependency error categories otherwise)
- OpenAPI: `http://127.0.0.1:8010/docs`

Startup order: `postgres` and `kafka` become healthy → the one-shot `migrate` (`signalops migrate`, Alembic upgrade to head) and `init-topics` (`signalops init-topics`) jobs run to completion → `api` starts → `frontend` starts. Both jobs are idempotent and run on every `up`. `init-topics` creates missing topics from `backend/signalops/streaming/topics.py` and fails if an existing topic has a different partition count, instead of altering it, because that would change key ordering.

### Profiles

The default `docker compose up` needs no paid model key and does not start the OpenTelemetry Demo. Optional profiles are reserved for later tasks; they currently contain no services:

| Profile | Will contain | Added in |
| --- | --- | --- |
| `demo` | Deterministic demo producer; optional instrumented demo application | P04-T01, P10-T03 |
| `evaluation` | Evaluation runner for isolated replays | P10-T02 |
| `observability` | Metrics and dashboards | P11-T01 |
| `tools` | The bare backend image (`signalops --version`), used to build or run ad hoc commands | P01-T03 (exists) |

### Migrations

```bash
set -a; . ./.env; set +a
export SIGNALOPS_DATABASE_URL="postgresql+psycopg://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_HOST_PORT/$POSTGRES_DB"
uv run alembic current
uv run alembic revision --autogenerate -m "describe change"   # review the file before applying
uv run alembic upgrade head
```

Revisions live in `migrations/versions/` (`0001_baseline` is empty; `0002_source_evidence` adds the source and evidence tables; `0003_incident_workflow` adds incident and workflow tables and status-transition triggers; `0004_graph_reliability` adds graph, receipt, and outbox tables; `0005_diagnosis_status_mapping` ties the stored diagnosis to the investigation status). ORM models live in `backend/signalops/persistence/models/`, must use `signalops.persistence.base.Base` (whose naming convention keeps constraint names stable), and must be imported from `signalops.persistence.models` so Alembic sees them. Generate revisions against a throwaway database at the previous head, review them, and keep names under 63 characters (pass an explicit `name=` when the convention would exceed it). The integration test `test_migrations_match_models` fails if models and revisions drift. Autogenerate ignores triggers and functions, so add them by hand and cover them with tests.

nginx serves the built UI and proxies `/health/` and `/v1/` to the API, resolving the `api` container per request so a stopped API returns 502 immediately. UI routes therefore never use the `/health/` or `/v1/` prefixes.

- Containers reach each other on the private `signalops` network as `postgres:5432`, `redis:6379`, and `kafka:9092`.
- Host ports exist so that host-run integration tests can use real infrastructure. They are bound to `127.0.0.1` and default to non-standard ports to avoid clashing with other local stacks.
- Kafka topic auto-creation is disabled; topics are created explicitly.
- Data lives in the named volumes `postgres-data`, `redis-data`, and `kafka-data`. `docker compose down -v` deletes them.
- Single-node Kafka (replication factor 1) and local volumes do not protect against host or disk loss.

## Configuration

Backend processes read `SIGNALOPS_`-prefixed variables (`backend/signalops/config.py`). Compose sets them for containers from `.env`.

| Variable | Default | Notes |
| --- | --- | --- |
| `SIGNALOPS_DATABASE_URL` | none (required) | `postgresql+psycopg://…`; contains a credential, never logged |
| `SIGNALOPS_REDIS_URL` | `redis://localhost:56379/0` | |
| `SIGNALOPS_KAFKA_BOOTSTRAP_SERVERS` | `localhost:19094` | `kafka:9092` inside Compose |
| `SIGNALOPS_LOG_LEVEL` | `INFO` | JSON logs on stdout |
| `SIGNALOPS_ENVIRONMENT` | `development` | `development`, `test`, `production` |
| `SIGNALOPS_API_HOST` / `SIGNALOPS_API_PORT` | `0.0.0.0` / `8000` | |
| `SIGNALOPS_CORS_ORIGINS` | empty | JSON list; only needed when the UI is served from another origin |
| `SIGNALOPS_READINESS_TIMEOUT_SECONDS` | `2.0` | per-dependency probe limit |
| `SIGNALOPS_MODEL_PROVIDER` | `fake` | only `fake` is accepted until P07-T01; no model key is read |
| `SIGNALOPS_MIGRATIONS_PATH` | `migrations` | Alembic environment, relative to the working directory |

Compose-level variables in `.env` (see `.env.example`): `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (required, no default), the loopback host ports `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`, `KAFKA_HOST_PORT`, `API_HOST_PORT`, `WEB_HOST_PORT`, and `SIGNALOPS_LOG_LEVEL`.

The frontend reads `VITE_API_BASE_URL` at build time (empty means same origin).

- Never commit real secrets. Source configuration stores secret references, not credentials.

## Conventions

- **Contracts first.** Pydantic models for events, DTOs, checkpoints, tool results, and diagnoses. After changing one, run `uv run signalops export-schemas` and commit the regenerated files. Breaking changes get a new version ([events.md](events.md#versioning-rules)). Do not edit files under `contracts/schemas/` or `apps/web/src/api/generated/` by hand.
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
