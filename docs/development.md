# Development guide

> Status: Phases 1–7 complete. All commands below are verified in this repository.

## Repository layout

```text
signalops/
  apps/web/                   React + TypeScript (pnpm)
  backend/signalops/
    api/                      HTTP and SSE
    contracts/                Pydantic models and exported JSON Schemas
    adapters/                 Logs, traces, deployments
    streaming/                Kafka consumers and outbox publisher
    detection/                Rules, durable window state, and incident correlation
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

No model provider key is needed. Fake-provider mode is the default, and `fixtures/gateway/v1` holds its failure scripts.

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
uv run signalops seed                    # create or verify the demo catalog (needs SIGNALOPS_DATABASE_URL)
uv run signalops demo-generate downstream-timeout --out signals.jsonl --labels labels.json  # synthetic dataset
uv run signalops normalizer              # normalize signals.raw.v1 into evidence until SIGINT/SIGTERM
uv run signalops detector                # update detection windows from signals.normalized.v1 until SIGINT/SIGTERM
uv run signalops graph-projector         # project signals.normalized.v1 into the temporal graph until SIGINT/SIGTERM
uv run signalops dispatcher              # turn investigations.requested.v1 into durable jobs until SIGINT/SIGTERM
uv run signalops worker                  # claim investigation jobs and run their workflow until SIGINT/SIGTERM
uv run signalops detector-closer         # close live detection windows by wall clock until SIGINT/SIGTERM
uv run signalops detector-flush --workspace-id W --replay-run-id R  # close a replay's remaining windows, then exit
uv run signalops outbox-publisher        # publish pending outbox rows until SIGINT/SIGTERM
uv run signalops redrive-record --topic T --partition P --offset O  # republish one quarantined record
uv run signalops redrive-outbox EVENT_ID # make one quarantined outbox row due again
printf '%s\n' "$TOKEN" | uv run signalops hash-token  # digest for a credentials file entry
uv run signalops retention [--apply] [--scope S]  # report (or apply) retention cleanup; see docs/retention.md
uv run signalops benchmark replay|sustained --out FILE  # throughput run against Compose; see docs/benchmarks.md
uv run signalops operations-snapshot     # print job queues, leases, outbox age, unknown model calls, and Kafka lag as JSON
SIGNALOPS_METRICS_PORT=9100 uv run signalops metrics-exporter  # serve that state as Prometheus gauges until SIGINT/SIGTERM

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
pnpm e2e           # Playwright browser journey; needs the Compose stack (see "Browser journey check")
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

- UI: `http://127.0.0.1:8090/` (incidents at `/incidents`, which asks for a token from `SIGNALOPS_OPERATOR_TOKENS`; system health at `/status`). Add `?replay_scope=replay:<run>` to `/incidents` to read a replay
- Liveness: `http://127.0.0.1:8010/health/live` (process is up; no dependency checks)
- Readiness: `http://127.0.0.1:8010/health/ready` (200 when PostgreSQL, Redis, and Kafka all answer within the timeout; 503 with per-dependency error categories otherwise)
- OpenAPI: `http://127.0.0.1:8010/docs`

Startup order: `postgres` and `kafka` become healthy → the one-shot `migrate` (`signalops migrate`, Alembic upgrade to head) and `init-topics` (`signalops init-topics`) jobs run to completion → `api`, `normalizer`, and `outbox-publisher` start → `frontend` starts. Both jobs are idempotent and run on every `up`. `init-topics` creates missing topics from `backend/signalops/streaming/topics.py` and fails if an existing topic has a different partition count, instead of altering it, because that would change key ordering.

### Browser journey check (R1, P08-T04)

One Playwright test (`apps/web/e2e/journey.e2e.ts`) drives a real browser through the first portfolio slice against the Compose stack and the fake provider. It ingests a real-time `downstream-timeout` fault with the demo poster, waits for the detector's incident, opens it from the list, checks its evidence timeline, data quality, and dependency graph, starts an investigation with a double click and checks that only one started, reloads mid-run so the progress stream resumes with `Last-Event-ID`, checks that the page shows every step of the durable history, inspects the diagnosis or abstention and its estimated cost, and cancels a second run. It makes no paid call and takes about five minutes, mostly waiting for detection windows to close.

```bash
# Repository root, from a clean checkout. Docker with Compose v2, uv, and Node 24 with pnpm.
cp .env.example .env
# In .env: replace the token placeholders in SIGNALOPS_INGEST_TOKENS and
# SIGNALOPS_OPERATOR_TOKENS, and set SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3 so a run
# stays open long enough to reconnect to and cancel.
docker compose up -d --build --wait
docker compose --profile demo run --rm demo-seed   # the demo catalog, once

cd apps/web
pnpm install --frozen-lockfile
pnpm exec playwright install chromium   # or set E2E_BROWSER_CHANNEL=chrome to use an installed Chrome
E2E_OPERATOR_TOKEN='<operator-token>' pnpm e2e
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `E2E_OPERATOR_TOKEN` | required | The operator token from `SIGNALOPS_OPERATOR_TOKENS` |
| `E2E_BASE_URL` | `http://127.0.0.1:8090` | The frontend; it proxies `/v1` to the API |
| `E2E_INGEST_COMMAND` | `docker compose --profile demo run --rm demo demo-post downstream-timeout --realtime --duration-s 150 --api-url http://api:8000` | Shell command, run from the repository root, that ingests the fault |
| `E2E_BROWSER_CHANNEL` | unset (Playwright's Chromium) | `chrome` for an installed Google Chrome |
| `E2E_EVALUATION_RUN_ID` | unset (the newest completed run) | Evaluation run that `evaluation.e2e.ts` reviews |

A second test (`apps/web/e2e/evaluation.e2e.ts`, P10 gate) reviews an evaluation run that already completed: it opens the run, checks the fake-provider notice, the three-variant comparison, and detection, filters to incorrect cases, opens a failed case with citations, checks the expected and observed outcomes and the labelled evidence, and follows the link to the investigation in its replay scope. It saves screenshots to `apps/web/e2e-results/`. Queue a run first (`POST /v1/evaluation-runs` or the Evaluation page, with `docker compose --profile evaluation up -d evaluation-worker` running), then `pnpm exec playwright test evaluation` runs only this test.

The report is written to `apps/web/e2e-report/` and failure traces to `apps/web/e2e-results/`. CI does not run this check, because it needs the full stack; `pnpm test` does not include it.

### Profiles

The default `docker compose up` needs no paid model key and does not start the OpenTelemetry Demo. Optional profiles hold services that are not started by default:

| Profile | Contains | Added in |
| --- | --- | --- |
| `demo` | `demo-seed` (runs `signalops seed`) and `demo` (posts one generated `downstream-timeout` run to the API); later an optional instrumented demo application | P04-T02 (exists), P10-T03 |
| `evaluation` | Evaluation runner for isolated replays | P10-T02 |
| `observability` | `metrics-exporter` (durable-state gauges), Prometheus (`127.0.0.1:59090`), Grafana with the provisioned dashboard (`127.0.0.1:53000`); see docs/observability.md | P11-T01 |
| `tools` | The bare backend image (`signalops --version`), used to build or run ad hoc commands | P01-T03 (exists) |

### Migrations

```bash
set -a; . ./.env; set +a
export SIGNALOPS_DATABASE_URL="postgresql+psycopg://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_HOST_PORT/$POSTGRES_DB"
uv run alembic current
uv run alembic revision --autogenerate -m "describe change"   # review the file before applying
uv run alembic upgrade head
```

Revisions live in `migrations/versions/` (`0001_baseline` is empty; `0002_source_evidence` adds the source and evidence tables; `0003_incident_workflow` adds incident and workflow tables and status-transition triggers; `0004_graph_reliability` adds graph, receipt, and outbox tables; `0005_diagnosis_status_mapping` ties the stored diagnosis to the investigation status; `0006_baseline_indexes` adds foreign-key and scoped-listing indexes; `0007_outbox_retry` adds outbox retry and quarantine state; `0013_idempotency_keys` adds investigation request keys; `0014_investigation_cancellation` adds `cancel_requested_at`; `0015_investigation_outcomes` allows one ending per investigation and settles investigations left running; `0016_llm_call_accounting` records prices, cache tokens, and the lease of each model call; `0017_evaluation` and `0018_evaluation_audits` add the evaluation schema; `0019_retention_indexes` adds the indexes retention cleanup uses). ORM models live in `backend/signalops/persistence/models/`, must use `signalops.persistence.base.Base` (whose naming convention keeps constraint names stable), and must be imported from `signalops.persistence.models` so Alembic sees them. Generate revisions against a throwaway database at the previous head, review them, and keep names under 63 characters (pass an explicit `name=` when the convention would exceed it). The integration test `test_migrations_match_models` fails if models and revisions drift. Autogenerate ignores triggers and functions, so add them by hand and cover them with tests.

nginx serves the built UI and proxies `/health/` and `/v1/` to the API, resolving the `api` container per request so a stopped API returns 502 immediately. UI routes therefore never use the `/health/` or `/v1/` prefixes.

- Containers reach each other on the private `signalops` network as `postgres:5432`, `redis:6379`, and `kafka:9092`.
- Host ports exist so that host-run integration tests can use real infrastructure. They are bound to `127.0.0.1` and default to non-standard ports to avoid clashing with other local stacks.
- Kafka topic auto-creation is disabled; topics are created explicitly.
- Every long-running backend process serves Prometheus metrics on port 9100 inside the network (`SIGNALOPS_METRICS_PORT`); it is never published to the host.
- Data lives in the named volumes `postgres-data`, `redis-data`, and `kafka-data` (and `prometheus-data`, `grafana-data` with the observability profile). `docker compose down -v` deletes them.
- Single-node Kafka (replication factor 1) and local volumes do not protect against host or disk loss.

## Configuration

Backend processes read `SIGNALOPS_`-prefixed variables (`backend/signalops/config.py`). Compose sets them for containers from `.env`.

| Variable | Default | Notes |
| --- | --- | --- |
| `SIGNALOPS_DATABASE_URL` | none (required) | `postgresql+psycopg://…`; contains a credential, never logged. Compose connects application processes as `signalops_app` (runtime role) and `migrate` as the owner |
| `SIGNALOPS_EVALUATION_DATABASE_URL` | unset | The evaluator's connection (`signalops_eval`), the only one that can read ground truth. Unset: evaluation commands and routes are unavailable |
| `SIGNALOPS_DB_RUNTIME_USER`, `SIGNALOPS_DB_RUNTIME_PASSWORD` | `signalops_app`, unset | Login role `signalops migrate` creates or updates in group `signalops_runtime`; unset password skips it. Compose falls back to `POSTGRES_PASSWORD` locally |
| `SIGNALOPS_DB_EVALUATOR_USER`, `SIGNALOPS_DB_EVALUATOR_PASSWORD` | `signalops_eval`, unset | The same for group `signalops_evaluator` |
| `SIGNALOPS_REDIS_URL` | `redis://localhost:56379/0` | |
| `SIGNALOPS_KAFKA_BOOTSTRAP_SERVERS` | `localhost:19094` | `kafka:9092` inside Compose |
| `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS` | `30000` | librdkafka `message.timeout.ms`: one message's send including internal retries; after it the caller gets a `timeout` result |
| `SIGNALOPS_OUTBOX_BATCH_SIZE` | `100` | Pending outbox rows read per publisher poll |
| `SIGNALOPS_OUTBOX_IDLE_INTERVAL_MS` | `500` | Publisher wait after a poll that published nothing (empty or all failed) |
| `SIGNALOPS_RETRY_MAX_ATTEMPTS` | `5` | Attempts per consumed record (including the first) for transient failures before the consumer stops without committing. The outbox never gives up on transient failures |
| `SIGNALOPS_RETRY_BASE_DELAY_MS` / `SIGNALOPS_RETRY_MAX_DELAY_MS` | `1000` / `30000` | Full-jitter backoff: retry n waits a random time up to min(max, base × 2^(n−1)) |
| `SIGNALOPS_STREAMING_STATS_INTERVAL_MS` | `30000` | Interval of the `consumer stats` and `outbox stats` log lines (see [reliability.md](reliability.md#streaming-stats)) |
| `SIGNALOPS_RETENTION_SIGNALS_DAYS` | `7` | Uncited signals older than this are deleted by `signalops retention --apply` |
| `SIGNALOPS_RETENTION_AGGREGATES_DAYS` | `32` | Closed detection windows; must exceed the ingestion age plus future bounds |
| `SIGNALOPS_RETENTION_OUTBOX_DAYS` | `7` | Published outbox rows |
| `SIGNALOPS_RETENTION_PROGRESS_DAYS` | `30` | Progress history of investigations that ended earlier |
| `SIGNALOPS_METRICS_PORT` | unset (Compose: `9100`) | Prometheus listener for this process; unset means none ([observability.md](observability.md)) |
| `SIGNALOPS_METRICS_HOST` | `0.0.0.0` | Listener address |
| `SIGNALOPS_METRICS_REFRESH_SECONDS` | `15` | How often `metrics-exporter` reads PostgreSQL and Kafka |
| `SIGNALOPS_LOG_LEVEL` | `INFO` | JSON logs on stdout |
| `SIGNALOPS_ENVIRONMENT` | `development` | `development`, `test`, `production` |
| `SIGNALOPS_API_HOST` / `SIGNALOPS_API_PORT` | `0.0.0.0` / `8000` | |
| `SIGNALOPS_CORS_ORIGINS` | empty | JSON list of exact `scheme://host[:port]` origins; only needed when the UI is served from another origin |
| `SIGNALOPS_CREDENTIALS_FILE` | unset | Hashed, named operator and source credentials, reloaded on change ([security.md](security.md)) |
| `SIGNALOPS_CREDENTIALS_RELOAD_SECONDS` | `5` | Least interval between credential file checks |
| `SIGNALOPS_API_MAX_BODY_BYTES` | `65536` | Body limit on every route but ingestion |
| `SIGNALOPS_SECRETS_DIR` | unset | Directory of files named after variables (for example `SIGNALOPS_DATABASE_URL`) that supply their values |
| `SIGNALOPS_READINESS_TIMEOUT_SECONDS` | `2.0` | per-dependency probe limit |
| `SIGNALOPS_INGEST_TOKENS` | `{}` | JSON object of source ID to bearer token for `POST /v1/ingest/{source_id}`; a source without a token cannot ingest. Contains credentials, never logged. See `.env.example` |
| `SIGNALOPS_OPERATOR_TOKENS` | `{}` | JSON object of workspace ID to bearer token for the evidence, incident, and graph routes; the token's workspace bounds every query. Empty means every operator request is `401`. Must not share a value with `SIGNALOPS_INGEST_TOKENS` (startup fails). Credentials, never logged. Interim mechanism ([ADR-014](decisions.md#adr-014-interim-operator-tokens-scoped-to-one-workspace)) |
| `SIGNALOPS_QUERY_DEFAULT_LIMIT` / `SIGNALOPS_QUERY_MAX_LIMIT` | `50` / `200` | Evidence query page size; a larger `limit` is `422` |
| `SIGNALOPS_QUERY_DEFAULT_WINDOW_SECONDS` / `SIGNALOPS_QUERY_MAX_WINDOW_SECONDS` | `3600` / `604800` | Evidence query time window when none is given, and its maximum span (`422` above it) |
| `SIGNALOPS_INGEST_MAX_RECORDS` | `500` | Records per ingestion request (`413` above it) |
| `SIGNALOPS_INGEST_MAX_REQUEST_BYTES` | `4194304` | Ingestion request body limit (`413` above it) |
| `SIGNALOPS_INGEST_MAX_FUTURE_SECONDS` / `SIGNALOPS_INGEST_MAX_AGE_SECONDS` | `86400` / `2592000` | Records with `occurred_at` further ahead of or behind receipt are rejected `timestamp_out_of_range` |
| `SIGNALOPS_NORMALIZER_MAX_CLOCK_SKEW_SECONDS` / `SIGNALOPS_NORMALIZER_LATE_ARRIVAL_SECONDS` | `300` / `900` | Normalizer flags `clock_skew` (degraded) and `late_arrival` (noted) on live signals (see [events.md](events.md#normalizer-normalizer-v1)) |
| `SIGNALOPS_DETECTION_WINDOW_SECONDS` | `60` | Detector tumbling window length (10–3600); a new length starts new aggregate series |
| `SIGNALOPS_DETECTION_ALLOWED_LATENESS_SECONDS` | `30` | How long after a window ends a sample still counts (0–3600); see [events.md](events.md#closure-and-lateness) |
| `SIGNALOPS_DETECTION_CLOSER_INTERVAL_MS` | `5000` | Closer wait after a batch that was not full (100–60000) |
| `SIGNALOPS_INCIDENT_CORRELATION_GAP_SECONDS` | `600` | A breach joins an incident seen within this long; a later one starts a new incident (10–86400) |
| `SIGNALOPS_INCIDENT_RECOVERY_SECONDS` | `300` | Breach-free time, shown by a sufficient window of every service, that resolves an incident; between the window length and the gap |
| `SIGNALOPS_INVESTIGATION_BUDGET_MICROUSD` / `SIGNALOPS_INVESTIGATION_MAX_STEPS` / `SIGNALOPS_INVESTIGATION_DEADLINE_SECONDS` | `3000000` / `40` / `600` | The most one investigation may use (1–1000000000, 1–200, 60–86400); a request can only lower them. Steps count workflow nodes plus model turns |
| `SIGNALOPS_INVESTIGATION_WORKFLOW_VERSION` | `v3` | The workflow new investigations run: `v3` multi-agent (ADR-031) or `v2` single agent (ADR-028) |
| `SIGNALOPS_INVESTIGATION_AUTO_REQUEST` | `true` | A new live incident requests one investigation |
| `SIGNALOPS_TOOL_TIMEOUT_SECONDS` | `5` | One evidence tool read (up to 60); a slower one returns a timeout to the agent |
| `SIGNALOPS_TOOL_CACHE_SECONDS` | `60` | How long Redis keeps a tool result (30–120) |
| `SIGNALOPS_SSE_CATCHUP_SECONDS` / `SIGNALOPS_SSE_MAX_STREAM_SECONDS` / `SIGNALOPS_SSE_MAX_STREAMS` | `5` / `300` / `200` | A progress stream reads PostgreSQL at least this often (0.5–60) and closes after this long (10–3600), and one API process serves at most this many (1–10000; `503` beyond) |
| `SIGNALOPS_JOB_LEASE_SECONDS` | `30` | A worker that stops heartbeating loses its job this long after the last beat (3–600). Job retries use `SIGNALOPS_RETRY_BASE_DELAY_MS` and `SIGNALOPS_RETRY_MAX_DELAY_MS` |
| `SIGNALOPS_JOB_POLL_INTERVAL_MS` | `1000` | Worker wait after finding no claimable job (50–60000) |
| `SIGNALOPS_GRAPH_DEPENDENCY_TTL_SECONDS` | `604800` | An observed dependency stays valid this long after each observation (60–31536000) |
| `SIGNALOPS_DETECTION_RULES` | three rules (`detection/rules.py`) | JSON array of `DetectionRule` objects replacing the defaults: `id`, `version`, `metric` (`trace`, `log`), `kind` (`error_rate`, `latency_quantile`), `threshold`, `quantile`, `min_samples`, `cooldown_seconds`, `severity` (default `medium`). A latency threshold must be a histogram bucket bound. Bump `version` when a rule's settings change (see [events.md](events.md#detector-detector-v1)) |
| `SIGNALOPS_INGEST_REDACT_FIELDS` | password, token, authorization, cookie, … (`config.py`) | JSON list of payload keys whose values are replaced before publishing; a source's `config.redact_fields` adds more |
| `SIGNALOPS_MODEL_PROVIDER` | `fake` | `fake` needs no key or network. `anthropic` makes paid calls and needs `SIGNALOPS_MODEL_API_KEY`; startup fails without one. Compose passes these to `investigation-worker` only |
| `SIGNALOPS_MODEL_API_KEY` | unset | Credential, never logged |
| `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS` | `0` | Fake provider only: seconds before each reply (0–30); the browser journey check uses 3. Compose passes it to `investigation-worker` |
| `SIGNALOPS_MODEL_NAME` | `fake-1` / `claude-opus-5-5` | The provider's default model when unset |
| `SIGNALOPS_MODEL_REQUEST_TIMEOUT_SECONDS` | `60` | One provider call; the provider client never retries on its own |
| `SIGNALOPS_MODEL_MAX_OUTPUT_TOKENS` / `SIGNALOPS_MODEL_EFFORT` | `8000` / `high` | Tokens per reply including thinking (256–64000), and effort (`low`, `medium`, `high`) |
| `SIGNALOPS_AGENT_MAX_REPAIRS` / `SIGNALOPS_AGENT_MAX_TOOL_CALLS` | `2` / `10` | Rejected replies before the agent abstains, and evidence tool calls per step |
| `SIGNALOPS_WORKER_JOB_CONCURRENCY` / `SIGNALOPS_AGENT_GLOBAL_BRANCHES` | `2` / `4` | Jobs one worker process runs at once (1–32), and specialist branches running at once across them (1–64); a branch waits for a slot before its step opens (ADR-032) |
| `SIGNALOPS_MODEL_MAX_ATTEMPTS` | `3` | Calls per request, including the first (1–10). Each one is reserved separately and may be billed |
| `SIGNALOPS_MODEL_REQUESTS_PER_MINUTE` / `SIGNALOPS_MODEL_TOKENS_PER_MINUTE` | `50` / `200000` | Provider limits enforced in Redis across all workers |
| `SIGNALOPS_MODEL_LIMIT_WAIT_SECONDS` | `120` | The longest a call pauses for a full window or an unreachable Redis before the job retries |
| `SIGNALOPS_MIGRATIONS_PATH` | `migrations` | Alembic environment, relative to the working directory |
| `SIGNALOPS_DB_POOL_SIZE` / `SIGNALOPS_DB_MAX_OVERFLOW` | `5` / `5` | per process; keep the sum over all processes below PostgreSQL `max_connections` (100 by default) |
| `SIGNALOPS_DB_POOL_TIMEOUT_SECONDS` | `5.0` | wait for a pooled connection before `sqlalchemy.exc.TimeoutError` |
| `SIGNALOPS_DB_CONNECT_TIMEOUT_SECONDS` | `5` | TCP connect limit |
| `SIGNALOPS_DB_STATEMENT_TIMEOUT_MS` | `15000` | server-side `statement_timeout` for every session |
| `SIGNALOPS_DB_LOCK_TIMEOUT_MS` | `5000` | server-side `lock_timeout` |
| `SIGNALOPS_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS` | `30000` | ends sessions left idle inside a transaction |

Compose-level variables in `.env` (see `.env.example`): `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (required, no default), the loopback host ports `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`, `KAFKA_HOST_PORT`, `API_HOST_PORT`, `WEB_HOST_PORT`, `SIGNALOPS_LOG_LEVEL`, `SIGNALOPS_INGEST_TOKENS`, and `SIGNALOPS_OPERATOR_TOKENS` (passed to every backend container).

The frontend reads `VITE_API_BASE_URL` at build time (empty means same origin).

- Never commit real secrets. Source configuration stores secret references, not credentials.

## Conventions

- **Contracts first.** Pydantic models for events, DTOs, checkpoints, tool results, and diagnoses. After changing one, run `uv run signalops export-schemas` and commit the regenerated files. Breaking changes get a new version ([events.md](events.md#versioning-rules)). Do not edit files under `contracts/schemas/` or `apps/web/src/api/generated/` by hand.
- **Domain logic stays separate** from HTTP handlers and Kafka plumbing. Abstractions should match actual reuse.
- **Type annotations and explicit error categories** everywhere. No broad exception handling that silently acknowledges failed work.
- **Timestamps** are UTC-aware. Source event IDs are stable, domain IDs are UUIDs, and money is stored in integer units.
- **Migrations:** every constraint and index goes through an Alembic revision. Review generated revisions before applying them.
- **Database access:** processes build their engine with `persistence.database.create_engine` (bounded pool, server-side timeouts, UTC sessions). A unit of work is `async with database.transaction() as session:`; it commits when the block ends and rolls back every write when it raises. Repositories (`persistence/repositories/`) take that session and never commit, roll back, or open their own transactions. Keep the block free of model calls, broker publication, and other slow I/O.
- **Seed data:** `signalops seed` (`persistence/seed.py`) creates the demo workspace, one source per adapter kind, and the services `checkout`, `payment`, `inventory`, `bank-gateway`, `orders-db` with fixed IDs that match `fixtures/contracts/v1`, plus two synthetic runbooks. It is idempotent and raises `CatalogConflictError` instead of overwriting a row whose identity matches but whose values differ.
- **Demo data:** `signalops demo-generate SCENARIO` (`demo/generator.py`) writes a deterministic synthetic dataset for the seeded demo sources: checkout, inventory, orders-db, payment, and bank-gateway traffic with noise, trace-linked logs and trace summaries, and a versioned deployment. Scenarios are `healthy` and `downstream-timeout` (bank-gateway latency makes payment time out between 40% and 80% of the run). Defaults: seed 42, start `2026-10-16T10:30:00Z`, 600 s. `--out` gets JSON Lines of ingestion records (`source_id`, `source_event_id`, `occurred_at`, `payload`); `--labels` gets the evaluator labels in a separate file, and nothing that identifies the scenario or fault is written into a record. `fixtures/scenarios/v1/*.labels.json` pin each scenario's dataset hash; a unit test regenerates and compares. If a generator change is intended, regenerate those files with `--labels` (and bump `GENERATOR_VERSION` when old datasets must stay distinguishable).
- **Posting demo data:** `signalops demo-post SCENARIO [--api-url URL] [--seed N] [--start T] [--duration-s S] [--batch-size N] [--realtime]` (`demo/poster.py`) generates a run and posts it per source with the tokens from `SIGNALOPS_INGEST_TOKENS`. Failed records are resent with the same IDs (5 attempts, exponential backoff); rejected ones are counted and make the command exit 1. Defaults: start = now minus the duration (so timestamps pass the ingestion bounds) and a fresh random seed per run, because source event IDs derive from scenario and seed and a rerun with the same seed would be deduplicated as the same evidence. Pass `--seed` and `--start` for a reproducible run. A default run backfills the dataset in one go, so it is evidence but past the detector's lateness bound, and its windows fire nothing. To see rules fire, add `--realtime`: the run starts now and takes `--duration-s` to post (fault at 40–80 % of it; about 5 minutes is enough). In Compose: set `SIGNALOPS_INGEST_TOKENS` in `.env`, then `docker compose --profile demo up demo` (or `docker compose --profile demo run --rm demo demo-post healthy --api-url http://api:8000`).
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
