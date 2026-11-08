# SignalOps development roadmap

Version 1 · 29 October 2026 · Status: planned

## Purpose

Implement SignalOps, an incident intelligence platform that converts noisy logs, trace summaries, and deployment events into evidence-backed diagnoses. This roadmap expands `SignalOps_Implementation_Plan.md` dated 16 October 2026 into executable phases and tasks. It describes planned work; no implementation or verification is claimed complete.

**Stack:** React and TypeScript; Python and FastAPI; Kafka; PostgreSQL; Redis; Docker Compose. Use Pydantic, SQLAlchemy, Alembic, a Python Kafka client, and an explicit persisted Python workflow. Keep source adapters, workers, and gateway modules in one backend codebase with separate process commands.

**Architecture:** sources → ingestion API → Kafka raw signals → normalizer → PostgreSQL and outbox → Kafka normalized signals → detector → incident and investigation request → dispatcher → durable PostgreSQL job → agent worker → diagnosis and progress events → API and React. The outbox publisher bridges committed database changes to Kafka. Redis provides recoverable caches, rate limits, and UI wake-ups.

PostgreSQL owns evidence, workflow progress, budgets, and the initial knowledge graph. Kafka is the durable event backbone. Redis does not own evidence or job completion. Agents investigate selected incidents rather than processing every raw signal through an LLM.

## How to use this roadmap

- Before starting work, read this roadmap, the source implementation plan, and relevant architecture decisions. Check the progress ledger for unfinished work and blockers.
- Select one task whose prerequisites are complete. Mark it in progress in the ledger; keep its checkbox unchecked until its acceptance checks pass.
- On completion, check the task, record the commit and validation evidence, and update the next task. Record deviations and their reasons alongside the affected task.
- Every task lists implementation work and a concrete acceptance condition. Phase gates require integration evidence as well as task completion.
- Keep commits small and reviewable. Suggested commit messages are examples, not commands that must run verbatim.
- Use deterministic fixtures and a fake model provider for ordinary development. Real model evaluation is an explicit, budgeted activity.

### Progress ledger

| Field | Current value |
| --- | --- |
| Active phase | Phase 10 Evaluation infrastructure (in progress); Phase 9 gate passed on the fake provider |
| Active task | P10-T02 (in progress) |
| Last completed task | P10-T01 |
| Latest verified commit | `53db2c6` (gate fix); the `docs(P09)` gate commit follows |
| Validation evidence | Phase 9 gate: `SIGNALOPS_INTEGRATION=1 uv run pytest` 1260 passed; on Compose, a `v3` investigation survived `docker compose kill investigation-worker` mid-branch, reusing completed branches and keeping the interrupted call `unknown` with its reservation; `pnpm e2e` passed on `v3`. A live-only bug (runbook references) was found and fixed at the gate. No model was called. Earlier evidence is in the work log |
| Blockers | none |
| Next task | P10-T03 |

### Scope and release boundaries

One local workspace; three initial adapters; read-only tools; human-applied remediation. Single-node Compose is a development environment, not production high availability. Add metrics as a fourth adapter after the first end-to-end release. A million-event replay is a benchmark target, not an already demonstrated capacity. Thousands of connectors, dedicated graph storage, Kubernetes, and autonomous remediation are deferred.

| Release | Includes | Gate |
| --- | --- | --- |
| R0 Data platform | Phases 1–5 | Fault signals produce a durable incident and inspectable evidence |
| R1 First portfolio slice | Phases 1–8 | One agent diagnoses a fault with evidence in the UI |
| R2 Multi-agent platform | Phase 9 | Durable specialist workflow survives a worker crash |
| R3 Evaluated portfolio release | Phases 10–12 | Held-out comparisons, load results, and reproducible demonstration |

### Phase sequence and effort

Estimates are focused developer days, excluding learning time and external delays. The expanded scope is approximately 36–61 days. This revises the earlier high-level estimate to account for explicit UI, security, recovery, and evaluation work. Re-estimate after R1.

| Phase | Focus | Depends on | Estimate |
| --- | --- | --- | --- |
| 1 | Project foundation | None | 2–3 days |
| 2 | Persistence and contracts | 1 | 3–5 days |
| 3 | Streaming reliability | 2 | 3–5 days |
| 4 | Ingestion and data quality | 3 | 3–5 days |
| 5 | Detection and knowledge graph | 4 | 4–6 days |
| 6 | Durable jobs and workflow runtime | 3 and 5 | 3–5 days |
| 7 | Gateway and first agent | 6 | 4–7 days |
| 8 | Incident workspace and live UI | 5 and 7 | 3–5 days |
| 9 | Multi-agent investigations | 7 and 8 | 3–5 days |
| 10 | Evaluation infrastructure | 9 | 4–7 days |
| 11 | Operational hardening and scale | 10 | 3–5 days |
| 12 | Portfolio packaging | 11 | 1–3 days |

Start the React shell in Phase 1 and add API-backed incident views in Phase 5; Phase 8 completes the UI. Add reliability checks alongside their owning feature rather than postponing all tests to Phase 11.

## Phase 1 Project foundation

**Dependencies:** None.

**Outcome:** A clean checkout starts the core infrastructure and API without model credentials.

### P01-T01 Create the repository layout

- [x] Complete this task.

**Implementation:** Create apps/web, backend/signalops, migrations, fixtures, tests, infra, and docs. Add README, .gitignore, Python project configuration, frontend package configuration, and lockfiles.

**Acceptance:** Backend and frontend imports/builds work from documented commands; generated files and secrets are excluded.

**Notes (2026-10-29):** The Python project is the root `pyproject.toml` (hatchling, package source `backend/signalops`, Python 3.12 pinned in `.python-version`, `signalops` console entry point) rather than a `backend/`-scoped project, so lint/type/test commands run from the root and cover `tests/` and `migrations/` too. Frontend is Vite 7 + React 19 + TypeScript 5.9 with pnpm 9; newer majors were deliberately not adopted. Planning files remain in `docs/` (see decisions.md Q11).

### P01-T02 Set up development checks

- [x] Complete this task.

**Implementation:** Configure Python formatting/linting and type checks, TypeScript checking, frontend linting, pytest, and a frontend test runner. Add CI for these checks and the frontend production build.

**Acceptance:** A clean checkout passes CI; intentionally invalid code is rejected by the relevant check.

**Notes (2026-10-29):** Backend: ruff (lint + format), mypy `--strict` over `backend/signalops` and `tests`, pytest. Frontend: ESLint 10 flat config with `typescript-eslint` strict type-checked rules, React hooks and React refresh plugins (ESLint 9 was installed first but is deprecated); Vitest 3 + Testing Library on jsdom. `pnpm test` runs once (`vitest run`) because pnpm does not forward `pnpm test --run`; `pnpm test:watch` is the watch mode. CI is `.github/workflows/ci.yml`. It has not run on GitHub yet because the repository has no remote; validated by running the same steps in a clean copy of the tracked files. Canonical commands are recorded in `docs/development.md` and `CLAUDE.md`. `.claude/settings.json` still allowlists only the `* `-suffixed forms (for example `uv run mypy *`), so the argument-less canonical commands may prompt; changing that file is left to the user.

### P01-T03 Create Docker Compose infrastructure

- [x] Complete this task.

**Implementation:** Add PostgreSQL, Redis, single-node Kafka in KRaft mode, named volumes, private networking, health checks, and one shared backend image. Pin compatible versions after a startup spike; document selected versions.

**Acceptance:** Kafka works from container clients; database and Redis are reachable internally; recreating containers retains database records.

**Notes (2026-10-29):** `compose.yaml` lives at the repository root so `docker compose` works there; image build files live in `infra/docker/`. Pinned after a pull spike: `postgres:17.11-alpine`, `redis:7.4.12-alpine`, `apache/kafka:4.3.0` (KRaft, internal listener `kafka:9092`, controller `9093`, host listener `localhost:19094`, auto topic creation off), backend `python:3.12.16-slim` with uv 0.12.5, running as a non-root user. Deviation: stateful services also publish loopback-only host ports (defaults 55432/56379/19094, overridable in `.env`) so host-run integration tests in later phases can use real PostgreSQL/Kafka; nothing binds to public interfaces. The PostgreSQL password has no default and must come from `.env` (`cp .env.example .env`). Application services join Compose in their own tasks (API in P01-T04, workers in their phases); the backend image is reachable now through the `tools` profile.

### P01-T04 Bootstrap FastAPI and React

- [x] Complete this task.

**Implementation:** Implement API configuration, structured logging, liveness/readiness endpoints, and lifespan-managed clients. Add React routing, layout, API client configuration, loading/error components, and a health view.

**Acceptance:** Readiness reflects missing required dependencies; UI renders an API error usefully rather than crashing.

**Notes (2026-10-29):**
- Backend: `pydantic-settings` configuration with the `SIGNALOPS_` prefix (`database_url` required, `model_provider` restricted to `fake`); JSON logging on stdout, uvicorn included; `create_app()` factory whose lifespan opens an async SQLAlchemy engine (psycopg, bounded pool), an async Redis client, and a confluent-kafka `AdminClient`, and closes them on shutdown. `GET /health/live` never touches dependencies. `GET /health/ready` probes PostgreSQL, Redis, and Kafka concurrently with a per-check limit and returns 503 with error categories only (exception class or `timeout`), never driver messages, which can contain hosts or credentials.
- Kafka client chosen: `confluent-kafka` (decisions.md Q1). Frontend libraries: TanStack Query 5 and React Router 7 (Q3).
- Frontend: layout with navigation, overview, `/status` health view, not-found page, route error boundary, typed `getJson` client with error kinds (`network`, `http`, `invalid_response`) and a 10 s timeout, loading/error/stale states. A failed refetch keeps the last data visible and marks it stale (TanStack v5 sets `status: "error"` while keeping data).
- Compose gains `api` (backend image, `signalops api`) and `frontend` (nginx serving the build, proxying `/health/` and `/v1/`). The UI route is `/status`, not `/health`, because nginx treats `/health/` as an API prefix. nginx resolves `api` per request so a stopped API yields an immediate 502 instead of a 14 s DNS stall (measured before the fix).
- Starlette deprecates `httpx` for `TestClient`; tests use `httpx2`. `pytest-asyncio` runs async integration tests.
- Not done here: no real-browser check of the UI. The error rendering is verified by component tests and by curl against nginx.

### P01-T05 Define development profiles and commands

- [x] Complete this task.

**Implementation:** Add one-shot migration and topic-init commands plus optional demo, evaluation, and observability profiles. Document environment variables and use fake model mode by default.

**Acceptance:** The default startup does not require the full OpenTelemetry Demo or a paid model key.

**Notes (2026-10-29):**
- `signalops migrate` runs Alembic to head programmatically (URL passed as a config attribute, never written to a file); `migrations/` has an `env.py`, a typed revision template, and an empty `0001_baseline`. `alembic.ini` supports `uv run alembic ...` for developers. `persistence/base.py` defines the declarative `Base` with a constraint naming convention.
- `signalops init-topics` creates the six plan topics (signal topics 3 partitions, domain topics 1; retention raw 3 days, others 7 days) and fails instead of altering an existing topic whose partition count differs. Retention for the four domain topics and `processing.failed.v1` is not specified in the plan; 7 days was chosen to match normalized signals. This overlaps P03-T01, which still owns producer settings, keys, and delivery callbacks.
- Compose runs `migrate` and `init-topics` as one-shot services; `api` waits for both to complete successfully. All backend services share one environment anchor with `SIGNALOPS_MODEL_PROVIDER=fake`; no model key exists anywhere in configuration.
- **Deviation:** the `demo`, `evaluation`, and `observability` profiles are documented as reserved names (docs/development.md) but contain no services yet, because the programs they would run (demo producer P04-T01, evaluation runner P10-T02, dashboards P11-T01) do not exist. Placeholder services were not added. Each owning task adds its service under the reserved profile. The existing `tools` profile exposes the bare backend image.
- Removed the fixed Compose network name so separate projects (for example a clean-start check) do not share a network.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `chore: bootstrap SignalOps workspace`. Split this into task-sized commits as needed.

**Phase 1 gate (2026-10-29): passed.** All five tasks are checked with evidence in the work log. Outcome demonstrated: a clean copy starts PostgreSQL, Redis, Kafka, migrations, topic provisioning, the API, and the UI without model credentials (P01-T05 work log). Open items carried forward: CI has not run on GitHub because there is no remote; no real-browser UI run yet; reserved profiles are empty until their owning tasks.

## Phase 2 Persistence and contracts

**Dependencies:** Phase 1.

**Outcome:** Migrations and contract fixtures establish a durable, versioned domain model.

### P02-T01 Implement source and evidence tables

- [x] Complete this task.

**Implementation:** Create workspaces, sources, services, signals, deployments, and signal_aggregates. Add UTC timestamps, JSONB attributes, kind/status checks, foreign keys, and uniqueness on source_id, replay_scope, source_event_id.

**Acceptance:** Duplicate source identities in one replay scope are rejected; the same fixture can exist in separate replay runs.

**Notes (2026-10-29):**
- Revision `0002_source_evidence`, generated with autogenerate against a throwaway database at `0001_baseline` and reviewed. ORM models in `persistence/models/evidence.py`; enumerations (`SourceKind`, `SignalKind`, `Severity`, `QualityStatus`) in `contracts/evidence.py` drive the CHECK constraints. Migrations hardcode the values, so widening an enumeration needs a new revision.
- Workspace safety: composite foreign keys including `workspace_id` (parents expose `UNIQUE(workspace_id, id)`). A deployment must agree with its signal on workspace, replay scope, and service through one composite key to `signals`. No cascades.
- **Deviations:** `deployments.replay_scope`, plus `workspace_id` on `deployments` and `signal_aggregates` (not in the plan's column lists), so scoped queries and composite keys work without joins. Source kinds are `structured_log`, `trace_summary`, `deployment`; signal kinds `log`, `trace`, `deployment` (metrics come with the deferred fourth adapter). The aggregate count rule is one CHECK (`0 <= error_count <= sample_count`) and its unique constraint is named `uq_signal_aggregates_window` to stay under 63 characters.
- Left to later tasks: Pydantic payload contracts and field size limits beyond `source_event_id`/`replay_scope` (P02-T04); repositories, transaction helpers, seed data (P02-T06); detector rule/version columns on aggregates (P05-T01).

### P02-T02 Implement incident and workflow tables

- [x] Complete this task.

**Implementation:** Create incidents, incident_services, incident_signals, investigations, jobs, agent_steps, tool_calls, llm_calls, and progress_events. Add request uniqueness, optimistic versions, leases, and nonnegative budget constraints.

**Acceptance:** Invalid states and duplicate logical steps fail; one investigation cannot create two logical jobs.

**Notes (2026-10-29):**
- Revision `0003_incident_workflow`, autogenerated against a throwaway database at `0002_source_evidence` and reviewed. The generated revision created the new `signals` unique key after the table that references it; reordered by hand (and dropped last on downgrade). ORM models in `persistence/models/workflow.py`; shared column helpers moved to `persistence/models/common.py`. Enumerations and transition maps in `contracts/workflow.py`.
- Status transitions are enforced by a PL/pgSQL trigger (`enforce_status_transition`, allowed `from>to` pairs passed as trigger arguments) on incidents, investigations, jobs, and agent_steps; rejections raise `check_violation` named `ck_<table>_status_transition`. Chosen over repository-only checks so a buggy or stale writer cannot regress a terminal state. Autogenerate does not track triggers; an integration test compares all state pairs with the Python maps, and a deliberately edited map made it fail.
- State consistency CHECKs: leases set exactly while a job is running; terminal investigations and finished steps carry `completed_at`; completed steps carry output; diagnosis only on `completed`/`insufficient_evidence`; LLM cost basis rules (pending has none, `unknown` has no amount and keeps its reservation, `succeeded` needs a known cost).
- `incidents.version` and `investigations.version` are SQLAlchemy `version_id_col`s; a test shows a concurrent ORM update failing with `StaleDataError`.
- Found while testing: nullable JSONB columns stored Python `None` as JSON `null` and failed their object checks; fixed with `JSONB(none_as_null=True)`.
- **Deviations:** scope columns (`workspace_id`, `replay_scope`) on investigations and incident links for composite keys; `investigations.evidence_cutoff_at`; `llm_calls.cost_basis`; `created_at` on jobs, steps, and calls; nullable `agent_steps.started_at`. No `reserved + spent <= budget` CHECK, because actual charges above a reservation must remain recordable; the reservation query (P07) enforces the ceiling. Incident status (`open`, `resolved`), severity (`low`…`critical`), and service roles (`primary`, `affected`) are initial choices not given by the plan. Details in docs/data-model.md.
- Test layout: `tests/` and `tests/integration/` became packages so schema tests share `tests/integration/schema_support.py`; the scratch-database `migrated`/`engine`/`world` fixtures moved to `conftest.py`.
- Left to later tasks: cancellation-request column and fenced conditional writes (P06-T02, P06-T04); progress event types (P06-T05); diagnosis status mapping (P02-T05); Pydantic contracts for diagnoses and checkpoints (P02-T04).

### P02-T03 Implement graph and reliability tables

- [x] Complete this task.

**Implementation:** Create graph_nodes, graph_edges, consumer_receipts, and outbox_events with the indexes from the source plan. Add temporal interval checks and workspace-safe relationships.

**Acceptance:** Dangling graph edges are rejected; duplicate receipts are rejected; pending outbox lookup uses its intended index.

**Notes (2026-10-29):**
- Revision `0004_graph_reliability`, autogenerated against a throwaway database at `0003_incident_workflow` and reviewed. As in 0003, the new `deployments` unique key was generated after the table that references it; moved first by hand (dropped last on downgrade). ORM models in `persistence/models/graph.py` and `persistence/models/reliability.py`; enumerations (`GraphNodeType`, `GraphRelation`) in `contracts/graph.py`.
- Edge endpoints reference `graph_nodes(workspace_id, replay_scope, id)` with explicit FK names (`fk_graph_edges_from_node`, `fk_graph_edges_to_node`), since the naming convention would give both the same name. Self-loops are rejected by CHECK; longer cycles are the traversal's job (P05-T04).
- Found while testing: the first entity-reference CHECK compared `external_key = service_id::text`, which is NULL (and therefore passes) when the reference is missing; changed to `IS NOT DISTINCT FROM`.
- The index acceptance test inserts 20,000 outbox rows (20 pending) into its own scratch database, runs `ANALYZE`, and asserts the publisher query's `EXPLAIN` contains only an index scan on `ix_outbox_events_created_at`. Manually confirmed that dropping the index yields a seq scan plus sort.
- **Deviations:** `replay_scope` on graph nodes and edges (replay isolation for traversals); typed `service_id`/`deployment_id`/`incident_id` node references with `external_key` pinned to the referenced ID (no unchecked polymorphic IDs); `created_at` on nodes and edges; `UNIQUE(workspace_id, replay_scope, id)` on `deployments`; length/shape checks on receipt group, outbox topic, key, envelope, and `published_at >= created_at`. The node identity constraint is named `uq_graph_nodes_identity` (the conventional name reaches 63 characters). Details in docs/data-model.md.
- Left to later tasks: edge uniqueness and observation coalescing (P05-T04); outbox retry/error columns (P03-T04); receipt retention cleanup and its `processed_at` index (P03 or P11); envelope contract (P02-T04).

### P02-T04 Define typed application contracts

- [x] Complete this task.

**Implementation:** Create Pydantic models for envelopes, raw adapter inputs, normalized evidence, incident DTOs, workflow checkpoints, tool results, and diagnoses. Define optional UUID source_id for non-source domain events and nullable UUID causation_id; replace illustrative placeholders with valid fixtures.

**Acceptance:** Valid examples round-trip; unsupported versions, malformed IDs, oversized fields, and invalid timestamps fail predictably.

**Notes (2026-10-29):**
- Modules in `backend/signalops/contracts/`: `common.py` (base model, `UtcDatetime`, bounded string/JSON types, `ContractError` + `ContractErrorCategory`, shared length limits), `events.py` (`EventHeader`, generic `EventEnvelope[P]`, six v1 payloads, `(event_type, schema_version)` registry, `decode_event`, size-checked `encode`), `adapters.py` (three raw record models), `evidence.py` (adds `QualityFlag`, `NormalizedSignal`), `incidents.py`, `checkpoints.py` (`StepCheckpoint`, `decode_checkpoint`), `tools.py` (discriminated result union, `ToolFailure`), `diagnosis.py`.
- Valid fixtures in `fixtures/contracts/v1/` (one scenario with consistent UUIDs) replace the placeholder IDs from the plan; docs examples now point to them. Fixtures are stored in canonical form, so the round-trip test compares the dump with the file exactly.
- `LIVE_REPLAY_SCOPE`, `MAX_REPLAY_SCOPE_LENGTH`, `MAX_SOURCE_EVENT_ID_LENGTH`, and `MAX_KEY_LENGTH` moved to `contracts/common.py`; persistence models import them. Values unchanged; `alembic check` reports no drift.
- Compatibility choices: event envelopes and payloads ignore unknown fields (a v1 consumer tolerates additive producer fields); raw adapter records move unknown fields into `attributes` and reject a name that appears in both; application-produced contracts (diagnosis, checkpoint, tool results, DTOs, normalized signal) forbid unknown fields, so model output cannot smuggle a free-form reasoning field. Timestamps must be offset-bearing RFC 3339 strings (Pydantic's numeric Unix-timestamp coercion is disabled) and are normalized to UTC.
- **Deviations:** `root_cause` identifies the service by `service_id` plus `service_name` instead of the plan's name-only `service`, so the verifier can check it against scope. Diagnosis adds `missing_evidence` (required for `insufficient_evidence`). Envelope `replay_run_id` maps to replay scope `replay:<uuid>` (`replay_scope_for`); P10 may refine this. Size limits chosen here: envelope 256 KiB, raw record 64 KiB, message 8 KiB, attributes/checkpoint state 16 KiB serialized, tool results 200 rows, diagnosis lists 10 items and 1–50 evidence IDs.
- Left to later tasks: JSON Schema/OpenAPI export and the diagnosis-to-investigation status mapping (P02-T05); ingestion request/response DTOs (P04-T02); tool argument models (P07-T03); node-specific step output models (P06-T03); progress event types (P06-T05).

### P02-T05 Define API and event compatibility rules

- [x] Complete this task.

**Implementation:** Export JSON Schemas and OpenAPI into versioned files. Define additive optional changes, unknown-field preservation in source attributes, and new-version handling for breaking changes. Document diagnosis status versus investigation terminal status mapping.

**Acceptance:** A completed investigation may contain diagnosed output; insufficient_evidence maps consistently across API, persistence, and UI.

**Notes (2026-10-29):**
- Status mapping (decision Q6): one to one. `completed` ⇔ `diagnosed`, `insufficient_evidence` ⇔ `insufficient_evidence`, no diagnosis for any other status. Single source: `DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS` in `contracts/investigations.py`, used by `InvestigationView` (API body), `InvestigationCompletedPayload`, the persistence CHECK text, and the generated `apps/web/src/api/generated/investigationStatus.ts`, which `apps/web/src/api/investigationStatus.ts` presents ("Insufficient evidence" has its own `abstained` tone, distinct from failures).
- Revision `0005_diagnosis_status_mapping` (hand-written; autogenerate ignores CHECK changes) replaces `ck_investigations_diagnosis_status`. The no-diagnosis clause is phrased as "status not in the diagnosis-bearing set" so an unknown status is still reported by `ck_investigations_status` (PostgreSQL reports CHECKs in name order). A completed investigation without a diagnosis is now rejected.
- `signalops export-schemas` (`contracts/export.py`) writes 20 JSON Schemas under `backend/signalops/contracts/schemas/` (`<family>/<name>.v<N>.json`, draft 2020-12, sorted keys), `schemas/openapi.v1.json`, `schemas/investigations/status-mapping.v1.json`, and the generated TS module. `--check` reports stale or orphaned files; a pytest test does the same, so CI needs no new step.
- `contracts/compatibility.py` reports structural breaks (removed property, newly required property, type change, removed enum/const value, removed union alternative), resolving `$ref`s. `export-schemas` refuses to rewrite an existing schema with a break and writes nothing unless `--allow-breaking`. Tightened bounds are not detected (documented as a review responsibility). Removing a field from an adapter record model is flagged even though old data would land in `attributes`; the check is deliberately conservative.
- Compatibility rules documented in docs/events.md ("Versioning rules"); mapping in docs/api.md and docs/data-model.md.
- **Deviations:** the investigation API body (`InvestigationView`) and investigation fixtures were added here, ahead of P06/P07, because the acceptance needs an API-side contract. `InvestigationCompletedPayload` now enforces the mapping (a tightening of a v1 payload that no producer emits yet). OpenAPI currently lists only the health routes; planned route bodies are exported as standalone schemas.
- Left to later tasks: the investigation endpoint itself (P06/P07) and its UI views (P08); usage cost-basis fields on `InvestigationView` (P07-T02).

### P02-T06 Add migrations and repository boundaries

- [x] Complete this task.

**Implementation:** Create Alembic revisions, deterministic seed data, transaction helpers, and bounded database pools. Add baseline indexes and a schema migration test against real PostgreSQL.

**Acceptance:** An empty database upgrades to head; repository transactions roll back failed writes without leaving partial state.

**Notes (2026-10-30):**
- `persistence/database.py`: `create_engine(settings)` builds the async engine with a bounded pool (`SIGNALOPS_DB_POOL_SIZE` 5 + `SIGNALOPS_DB_MAX_OVERFLOW` 5, `SIGNALOPS_DB_POOL_TIMEOUT_SECONDS` 5), pre-ping, a connect timeout, and server-side `statement_timeout` 15 s, `lock_timeout` 5 s, `idle_in_transaction_session_timeout` 30 s, and `timezone=UTC` set through libpq `options`. The API lifespan now uses it instead of its inline pool (which reused the 2 s readiness timeout for pool and connect waits). `Database.transaction()` is the unit of work: commit at block end, roll back on any exception; sessions use `expire_on_commit=False`.
- Repository boundary: repositories take a session inside the caller's transaction and never commit. First one is `persistence/repositories/catalog.py` (`ensure_workspace`, `ensure_source`, `ensure_service`): `INSERT … ON CONFLICT DO NOTHING`, then read back by natural key, raising `CatalogConflictError` when the stored row has another id or kind.
- `signalops seed` (`persistence/seed.py`) creates the demo workspace, three sources (one per adapter kind), and four services with fixed UUIDs; the workspace, structured-log source, `checkout`, and `payment` IDs are the ones already used by `fixtures/contracts/v1` (a unit test keeps them aligned). One transaction, idempotent.
- Revision `0006_baseline_indexes` (hand-written, `alembic check` clean): `incidents(workspace_id, replay_scope, detected_at DESC)`, `investigations(incident_id, created_at DESC)`, `incident_services(service_id)`, `incident_signals(signal_id)`, partial `graph_nodes(service_id|deployment_id|incident_id)` and `graph_edges(provenance_signal_id)`. Found by listing foreign keys with no index led by one of their identifying columns; an integration test now enforces that rule.
- **Deviations:** the existing revisions 0001–0005 already satisfied "create Alembic revisions"; this task adds only 0006. Seed data is the catalog only: evidence and incident scenarios belong to the fixture generator (P10-T01) and demo producer (P04-T01). No error translation layer over `IntegrityError` yet; P03-T02 adds the categories receipts need.
- Left to later tasks: further repositories as their writers arrive (receipts/outbox P03-T02, signals P04, incidents P05, jobs P06); signal query indexes that include `replay_scope`, after P04-T05 fixes the query shapes.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: define persistence models and event contracts`. Split this into task-sized commits as needed.

**Phase 2 gate (2026-10-30): passed.** All six tasks are checked with evidence in the work log. Outcome demonstrated against real PostgreSQL: an empty database upgrades to `0006_baseline_indexes`, downgrade to baseline and upgrade again succeed, `alembic check` reports no drift on the dev database, contract fixtures round-trip and match the seeded catalog, and repository transactions roll back failed writes. Open items carried forward: OpenAPI lists only health routes until the investigation and incident endpoints exist (P05–P08).

## Phase 3 Streaming reliability

**Dependencies:** Phase 2.

**Outcome:** Database effects and Kafka offsets remain consistent under redelivery and publisher crashes.

### P03-T01 Provision Kafka topics and producer settings

- [x] Complete this task.

**Implementation:** Create signals.raw.v1, signals.normalized.v1, incidents.changed.v1, investigations.requested.v1, investigations.completed.v1, and processing.failed.v1. Configure keys, retention, producer idempotence, acknowledgements, and delivery callbacks.

**Acceptance:** Producer acknowledgement failures reach the caller; partition keys are stable and documented.

**Notes (2026-10-30):**
- Topics and retention already existed from P01-T05 (`streaming/topics.py`, `signalops init-topics`); unchanged here.
- `streaming/topics.py` adds `TOPIC_BY_EVENT_TYPE`, `topic_for`, and `message_key`. Key formats are in docs/events.md: parts joined with `:`, absent optional parts omitted (raw signal without `service_hint` → `{source_id}`; unresolved normalized signal → `{workspace_id}`; unparseable failure original → `{topic}:{partition}:{offset}`). The plan says only "source_id plus service hint" and "workspace_id plus service_id"; the separator and the omission rule are this task's choice.
- `streaming/producer.py`: `EventProducer.publish(messages)` returns one `DeliveryResult` per message in input order, with partition/offset or a `DeliveryErrorCategory` (`timeout`, `transient`, `rejected`, `queue_full`, `fatal`). Settings: idempotence, `acks=all`, 5 in flight, `message.timeout.ms` from new `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS` (30 s), `linger.ms=5`, `murmur2_random` partitioner (Java-compatible mapping). A message with no callback by the end of flush is reported as `timeout`, never delivered. After a `fatal` result the instance refuses further publishes (`ProducerUnusableError`).
- Observed: an unknown topic (auto-creation is off) surfaces as `timeout` after the full delivery timeout, because librdkafka waits for the topic to appear; an oversized message is refused locally at `produce()` and reported as `rejected`.
- **Deviation:** no message headers. Consumers decode the envelope, so `event_type`/`event_id` headers would duplicate it; add them if a consumer needs to route before decoding.
- Left to later tasks: wiring the producer into ingestion (P04-T02, run in a worker thread from async code) and the outbox publisher (P03-T03); producer metrics (P03-T05).

### P03-T02 Build the consumer transaction pattern

- [x] Complete this task.

**Implementation:** Disable automatic commits. Process one record at a time per partition initially; insert receipt, domain effect, and outbox records in one database transaction, then commit the next Kafka offset.

**Acceptance:** A crash after DB commit but before offset commit causes redelivery with no duplicate logical effect; offsets never skip unfinished earlier records.

**Notes (2026-10-30):**
- `persistence/repositories/reliability.py`: `ReceiptRepository.record` (`INSERT … ON CONFLICT DO NOTHING RETURNING`, False on duplicate) and `OutboxRepository.add`, which stores `topic_for`/`message_key` and the encoded envelope, rejecting an oversized envelope before commit. The outbox row `id` is the envelope's `event_id`, so a second staging of the same event fails on the primary key.
- `streaming/consumer.py`: `TransactionalProcessor` (receipt, then the `EventHandler` effect, in one `Database.transaction()`) and `ConsumerRunner`, which polls in a worker thread, processes one record to completion, then commits offset + 1 synchronously. Config: `enable.auto.commit=false`, `enable.auto.offset.store=false`, `auto.offset.reset=earliest`.
- One record at a time per consumer, not per partition: simpler than per-partition queues; revisit when measurements show a need. Parallelism would need the contiguous-frontier commit described in docs/reliability.md.
- A failed offset commit is logged and processing continues; the effect is durable, and a later commit or a receipt-hit redelivery covers it.
- **Deviation:** a decode or handler failure stops the runner without committing (`RecordProcessingError` with stage and position) instead of quarantining. P03-T04 owns quarantine and retry; until then a poison record halts its consumer, which is the safe direction.
- No process entry command yet: the first real consumer is the normalizer in Phase 4. Rebalance callbacks and graceful shutdown are P03-T05.

### P03-T03 Implement the outbox publisher

- [x] Complete this task.

**Implementation:** Poll committed pending rows, publish using the stored event ID/key, and mark delivery after acknowledgement. Start with one publisher and preserve aggregate version order. Avoid holding a database transaction open during broker calls.

**Acceptance:** A crash after publication but before marking delivery republishes safely; failed delivery leaves the row pending.

**Notes (2026-10-30):**
- `streaming/outbox.py`: `OutboxPublisher.publish_once` reads up to `SIGNALOPS_OUTBOX_BATCH_SIZE` pending rows in one short transaction, publishes outside any transaction through `EventProducer` (in a worker thread), and marks results in a second short transaction per round. `run(stop)` loops and waits `SIGNALOPS_OUTBOX_IDLE_INTERVAL_MS` only after a poll that published nothing. Entry command `signalops outbox-publisher` (SIGINT/SIGTERM stop it); Compose service `outbox-publisher` with `restart: unless-stopped`.
- Messages carry the stored topic, key, and envelope; the envelope is re-serialized from JSONB, so key order can differ from the original bytes. Consumers decode by field, so this is harmless.
- Order: rows go out in rounds, each aggregate's lowest pending version first; an aggregate with a failed row sits out the rest of the batch. `OutboxRepository.add` now sets `created_at = clock_timestamp()` instead of the transaction-start `now()`, so a version allocated later under the aggregate's row lock also sorts later across batches. That depends on writers allocating versions under a lock; P05 writers must do so.
- `attempts` is incremented on each recorded attempt, including the one that marks the row published. A crash after the ack records no attempt.
- **Deviation:** no single-publisher enforcement (for example an advisory lock). Compose runs one instance; a second instance would duplicate and could reorder. Revisit if the publisher is ever scaled.
- A permanently rejected row stays pending and blocks its aggregate, retried every idle interval. Quarantine and backoff are P03-T04. Outbox age and lag metrics are P03-T05.

### P03-T04 Implement retry and failure handling

- [x] Complete this task.

**Implementation:** Separate transient infrastructure failures from permanent validation failures. Add bounded retry with jitter, structured attempt metadata, redacted failure events, and a controlled redrive command. Pause/retry partitions without violating poll deadlines.

**Acceptance:** Permanent failures are explicitly quarantined; failure-event publication errors do not silently acknowledge the original.

**Notes (2026-10-30):**
- `streaming/retry.py`: `RetryPolicy` (full-jitter capped exponential backoff; `SIGNALOPS_RETRY_MAX_ATTEMPTS` 5, `SIGNALOPS_RETRY_BASE_DELAY_MS` 1000, `SIGNALOPS_RETRY_MAX_DELAY_MS` 30000).
- `streaming/failures.py`: classification, `failure_envelope`, `failure_event_id` (UUIDv5 of group and position), and the `signalops-redrive-attempt-id` Kafka header. `OutgoingMessage` gained `headers` and an optional `event_id`.
- `ConsumerRunner` now takes `consumer_group`, `failure_publisher`, and `retry_policy`. Permanent = `ContractError` from decoding or from the handler; the failure event is published synchronously through `EventProducer` (not the outbox), and the offset is committed only after its acknowledgement. No receipt is written for a quarantined record, so a redrive is processed by the group that rejected it. Everything else is transient: pause the partition, seek back, keep polling, resume after the backoff. Poll timeouts shrink to the next due retry. A record fetched past a pending retry (prefetch) is rewound, not processed.
- After `max_attempts` transient failures the runner raises without committing. **Decision:** exhausted transient failures stop the consumer instead of quarantining, so a database or broker outage can never mass-quarantine valid records; the restart policy brings the process back at the same offset. A handler bug therefore halts its partition until fixed; a handler that knows content is invalid must raise `ContractError`.
- **Decision:** failure events whose original has no readable `workspace_id` use the nil UUID instead of making `workspace_id` optional on the envelope (a contract change for every consumer). `replay_run_id` is preserved when readable.
- No new `ContractErrorCategory` for handler rejections yet: handlers reuse the contract categories (normally `invalid_payload`). An additive value can be added when Phase 4 needs a distinct one; `processing.failed.v1` has no consumer yet.
- Outbox (migration `0007_outbox_retry`): `next_attempt_at`, `quarantined_at`, `last_error`, check constraints, and a partial `(aggregate_id, aggregate_version)` index on pending rows. `pending` excludes a row while it or an earlier pending version of its aggregate is deferred or quarantined. Transient delivery failures are deferred and never abandoned; `rejected` quarantines. `record_failed_attempts` was replaced by `schedule_retry` and `quarantine`.
- Redrive: `signalops redrive-record --topic --partition --offset` reads the original by position with a non-committing assigned consumer and republishes it with a new attempt ID; it refuses `processing.failed.v1`, unretained or out-of-range offsets, and keyless or empty records. `signalops redrive-outbox EVENT_ID` clears a quarantine. There is no command to discard an outbox row.
- Rebalance: retry state is per partition in memory and is not cleared on revoke. After a reassignment a stale entry can rewind to an already-committed offset; receipts absorb the repeat. P03-T05 owns rebalance callbacks. Resolved in P03-T05 (revoke drops the entry and lifts the pause).
- Left: failure metrics and outbox age (P03-T05); a consumer entry command arrives with the normalizer (P04); `processing.failed.v1` has no reader or triage UI.

### P03-T05 Add streaming observability and lifecycle

- [x] Complete this task.

**Implementation:** Measure lag, consumed/produced counts, receipt deduplications, processing latency, retry counts, and outbox age. Implement graceful shutdown and safe rebalance behavior.

**Acceptance:** Stopping a worker mid-batch resumes from a safe offset; backlog growth is visible in logs or metrics.

**Notes (2026-10-30):**
- Observability is structured logs, not a metrics endpoint: `consumer stats` and `outbox stats` every `SIGNALOPS_STREAMING_STATS_INTERVAL_MS` (30 s) and on shutdown. Fields are listed in docs/reliability.md#streaming-stats. No new dependency; exported metrics and dashboards stay with P11-T01.
- Consumer: `ConsumerStats` counts consumed, applied, duplicates (receipt hits), quarantined, retries scheduled, and processing latency (poll return to offset commit, committed outcomes only). Lag per assigned partition is the high watermark minus the committed offset, queried from the broker at report time (two short broker calls per interval); without a committed offset the low watermark stands in. `null` on timeout or broker error.
- Outbox: `PublishSummary` totals per interval plus `OutboxRepository.backlog()` (unpublished count, quarantined count, oldest row age via the partial pending index).
- Lifecycle: `ConsumerRunner.run` checks `stop` between records and closes the consumer (leaving the group) in `finally`; the final stats line is skipped when the runner stops on an error. `OutboxPublisher.publish_once(stop)` stops between rounds. Compose gives `outbox-publisher` a 40 s stop grace period, longer than the 30 s delivery timeout.
- Rebalance: `create_consumer` no longer subscribes; `ConsumerRunner.subscribe(topics)` registers `on_assign`/`on_revoke`/`on_lost`. Revoke and lost drop the partition's pending retry and resume it if paused. **Found while testing:** librdkafka keeps an application pause across a revoke, so before this fix a partition paused for retry and then reassigned to the same member was never fetched again (a latent P03-T04 stall). No commit on revoke: records are committed synchronously before the next poll, so nothing is in progress when callbacks run.
- The stats report runs only between records, so a single slow record delays the next line; a stuck handler shows up as missing stats lines rather than rising lag in the log.
- Left: no consumer entry command until the normalizer (P04-T02), which should wire SIGTERM/SIGINT the way `outbox-publisher` does; a failed offset commit is still not retried on revoke (receipts absorb the redelivery); no producer-level delivery counters beyond the publisher's summary.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: implement Kafka consumers and transactional outbox`. Split this into task-sized commits as needed.

## Phase 4 Ingestion and data quality

**Dependencies:** Phase 3.

**Outcome:** Three heterogeneous adapters produce normalized, scoped evidence with auditable quality outcomes.

### P04-T01 Build a deterministic demo producer

- [x] Complete this task.

**Implementation:** Generate checkout, payment, and database signals with realistic noise, stable event IDs, trace relationships, and versioned deployment events. Start with healthy and downstream-timeout fixtures.

**Acceptance:** A fixed seed produces reproducible data; the runtime sees signals without evaluator labels.

**Notes (2026-10-30):**
- `demo/generator.py`: `generate(scenario, seed, start, duration)` returns `DemoDataset(records, labels)`. Records are ingestion batch items (`source_id`, `source_event_id`, `occurred_at`, `payload`) for the three seeded demo sources; every payload is built through its adapter contract. `signalops demo-generate SCENARIO --out FILE [--labels FILE] [--seed] [--start] [--duration-s]` writes JSON Lines and, separately, the labels.
- Traffic: one checkout request every 1.5–4.5 s calls inventory, orders-db, and payment → bank-gateway. Each request emits a trace summary per service under one trace ID; logs carry that trace ID and a span ID. Noise: background warnings (flag cache, cache hit ratio, GC pauses, connection-reset retries), debug health checks, slow queries (3%), missing SKUs (1%), rare isolated payment timeouts (0.4%), and one unrelated inventory deployment at 15% of the run with a `previous_revision`.
- Scenarios: `healthy`, and `downstream-timeout`, where between 40% and 80% of the run 75% of bank-gateway calls outlast payment's 2000 ms timeout and the rest are slow. Labels: window, root cause `bank-gateway`, affected services, `expected_incident`, record count, and the dataset's SHA-256.
- Determinism: `random.Random(seed)` using only `random()` and `getrandbits`; millisecond timestamps; canonical JSON (sorted keys, compact). `source_event_id` is `{service}-{log|span|deploy}-{sha256(scenario:seed:n)[:20]}`, unique across datasets without naming the scenario.
- Label separation: labels never go into a record. Unit tests reject label words or scenario names anywhere in the serialized signals and check that every record shape (source, service, attribute keys) in the fault dataset also occurs in healthy traffic, so a field cannot mark fault records.
- **Decision:** only labels and the dataset hash are committed (`fixtures/scenarios/v1/*.labels.json`); records are regenerated from the seed (about 1,000 records per 10-minute run). The fixture test regenerates and compares the hash.
- **Decision:** added `orders-db` to the seeded catalog so database signals resolve to a known service instead of being flagged unknown.
- **Deviation:** no Kafka or HTTP publishing and no Compose `demo` service yet. Records target `POST /v1/ingest/{source_id}`, which P04-T02 builds; the poster and the `demo` profile service belong there. Bypassing ingestion by writing raw envelopes to Kafka would skip credentials and redaction.
- Left: P04-T04 malformed/skewed/late fixtures; remaining fault families and held-out variants (P10-T03).

### P04-T02 Implement authenticated batch ingestion

- [x] Complete this task.

**Implementation:** Build POST /v1/ingest/{source_id}; validate source credentials, batch size, timestamps, payload bounds, and redaction. Acknowledge each accepted item only after Kafka acknowledgement. Define partial-success and ambiguous-timeout behavior.

**Acceptance:** Broker outage does not produce a false acceptance; client retry uses the same source event ID.

**Notes (2026-10-30):**
- Route `api/ingest.py`; logic in `adapters/ingestion.py` (`ingest_batch`, `SourceTokens`, `Redactor`, `DatabaseSourceLookup`, `ReplaceablePublisher`); bodies in `contracts/ingestion.py` (`IngestRecord`, `IngestRequest`, `IngestRecordResult`, `IngestResponse`), exported under `contracts/schemas/ingestion/` and in OpenAPI. Behavior is specified in docs/api.md#ingestion.
- Order of checks: bearer token (401) → source row (404 unknown, 403 disabled, 500 invalid config, 503 database down) → body size and record count (413) → batch shape (422) → per-record validation → redaction → publish.
- Partial success: each record is `accepted` (Kafka acknowledged), `rejected` with a reason (`invalid_record`, `oversized`, `timestamp_out_of_range`, `duplicate_in_batch`), or `failed` (`ambiguous_timeout` for a delivery timeout, where the broker may have written it; `broker_unavailable` otherwise). 200 when nothing failed; 503 + `Retry-After` with the same body when anything failed. Resending the whole batch is safe.
- **Decision (ADR-011):** raw `event_id` = UUIDv5(source, replay scope, `source_event_id`), so retries after an ambiguous timeout carry the same ID and consumer receipts deduplicate them. Tokens come from `SIGNALOPS_INGEST_TOKENS` (source ID → token, compared as SHA-256 digests in constant time), not from `sources.config`, which holds only non-secret `redact_fields`. Invalid source config fails closed with 500 instead of skipping redaction.
- **Decision:** ingestion does not validate payloads against the adapter contracts and rejects only coarse timestamp outliers (±1 day ahead, 30 days behind by default). Malformed payloads, skew, and late arrival are the normalizer's quality outcomes (P04-T03/T04), so they stay auditable as evidence.
- Producer: one `EventProducer` per API process, called from a worker thread. `ReplaceablePublisher` creates a new producer after a fatal idempotence error; the batch that hit it is reported failed.
- Demo: `signalops demo-post` and the Compose `demo` profile (`demo-seed`, `demo`) finish the poster deferred from P04-T01. Default seed is random per run because source event IDs derive from scenario and seed; a rerun with a fixed seed would be deduplicated.
- **Deviation:** the per-source 429 rate limit in docs/api.md is not implemented; the task's implementation list does not include it and no task owns it yet. Recorded under remaining issues.
- Left: rate limiting; free-text redaction inside messages (field names only); token rotation without restart (P11-T02); the normalizer consuming `signals.raw.v1` (P04-T03).

### P04-T03 Implement three source adapters

- [x] Complete this task.

**Implementation:** Normalize structured logs, trace summaries, and deployments into the common evidence model. Resolve service plus environment; preserve source attributes and provenance.

**Acceptance:** All three fixture formats produce queryable evidence; contradictory identity is flagged rather than guessed.

**Notes (2026-10-30):**
- `adapters/sources.py` (pure): `adapt(kind, record)` validates with the adapter contract and maps severity, message, trace ID, and kind-specific fields; `reported_identity` collects service/environment claims from the record's fields and well-known attributes and lists disagreeing keys. `adapters/normalizer.py`: `Normalizer`, the `EventHandler` for `signals.raw.v1`; `EvidenceRepository` (`insert_signal`, `insert_deployment`); `CatalogRepository.find_service_id`. Command `signalops normalizer` (group `normalizer-v1`) and the Compose `normalizer` service. Rules are in docs/events.md#normalizer-normalizer-v1.
- **Decision (ADR-012):** exact `(service, environment)` catalog match only. Missing → `degraded`/`missing_service`; not in catalog → `degraded`/`unknown_service`; disagreeing claims → `quarantined`/`contradictory_identity`, with the keys in `provenance.identity_conflicts`. `service_id` is NULL in all three. An unresolved deployment is stored as a signal without a `deployments` row.
- **Decision:** `signals.attributes` = `{source, record, provenance}` so source keys cannot overwrite derived fields; `provenance` keeps the reported identity, raw event ID, adapter version, and correlation ID.
- Idempotency: signal ID = UUIDv5(source, replay scope, `source_event_id`); deployment and normalized event IDs derive from it. `insert_signal` uses `ON CONFLICT` on the business key, so a record already stored (another group, or a receipt-less redrive) stages no second deployment or event.
- **Deviation:** records that fail their adapter contract still raise `ContractError` and go to `processing.failed.v1` (no receipt, no evidence). Turning them into quarantined evidence with `malformed_payload` is P04-T04's classification work; the docs say so.
- Left: malformed/skew/late classification and missing-ID fallback (P04-T04); per-source identity binding (ADR-012 revisit condition); query API (P04-T05). Attributes over 16 KiB after adding provenance are quarantined to `processing.failed.v1` rather than truncated.

### P04-T04 Implement normalization quality policies

- [x] Complete this task.

**Implementation:** Classify accepted, degraded, and quarantined records. Detect malformed payloads, missing IDs, clock skew, late arrival, and unknown services. Define adapter-specific stable identity fallback when needed.

**Acceptance:** Identical message text with distinct source event IDs remains separate evidence; duplicates with stable IDs collapse correctly.

**Notes (2026-10-30):**
- `adapters/quality.py` (pure): `FLAG_EFFECTS` and `assess(flags)` give the status from the flags; `TimingPolicy` raises `clock_skew`/`late_arrival` for the live scope from the envelope's `occurred_at` and `ingested_at` (settings `SIGNALOPS_NORMALIZER_MAX_CLOCK_SKEW_SECONDS` 300, `SIGNALOPS_NORMALIZER_LATE_ARRIVAL_SECONDS` 900); `derive_source_event_id` is the per-kind fallback identity. `Normalizer(timing)` now combines identity, timing, and derived-ID flags and stores malformed records as quarantined evidence. Rules are in docs/events.md#normalizer-normalizer-v1 and docs/api.md#ingestion.
- **Decision (ADR-013):** fixed effect per flag (quarantine `malformed_payload`/`contradictory_identity`; degrade `missing_service`/`unknown_service`/`clock_skew`; note `late_arrival`/`derived_event_id`/`truncated_message`), most severe wins. Late evidence stays `accepted`; detection-window lateness is the detector's rule (P05).
- **Decision:** missing IDs are handled at ingestion, so the raw event ID, receipts, and business key all use the derived ID. `IngestRecord.source_event_id` is optional (additive), `RawSignalPayload.source_event_id_derived` is new (default `false`), reject reason `missing_source_event_id` and flag `derived_event_id` are new enum values. Trace: `trace_id`+`service`+`operation`; deployment: `service`+`environment`+`revision`+`occurred_at`; log: `occurred_at` + whole redacted payload, never the message alone. Exported schemas regenerated; the `signal_raw` contract fixture gains the new field.
- **Found while exporting:** `contracts/compatibility.py` matched a primitive by its field title, so widening `source_event_id` to nullable read as "alternative removed". Plain primitives now match by type; enums and objects still match by title. New case `titled-nullable-added`.
- Malformed: kind from the source, severity `info`, no service or trace ID, contract errors in `attributes.record.contract_errors`, raw record under `attributes.source` unless the attributes would exceed 16 KiB (then `provenance.record_omitted_bytes`). This also covers the P04-T03 case of oversized attributes, which no longer goes to `processing.failed.v1`. Only envelope faults (workspace or source-kind mismatch) still fail.
- **Deviation:** P04-T01 deferred "malformed/skewed/late fixtures" here. The demo generator still emits clean records; the quality cases are built inline in `tests/integration/test_normalizer.py` and `tests/unit/test_quality.py`. A data-quality noise option for the generator is left to the fixture generator (P10-T01).
- Left: replay timing against a simulated clock (P10); `truncated_message` is reserved and unused; quality counts per source are P04-T05.

### P04-T05 Expose evidence queries and quality summaries

- [x] Complete this task.

**Implementation:** Add bounded source/service/time filters, cursor pagination, trace lookup, and source quality counts. Use repository scope helpers for workspace and replay boundaries.

**Acceptance:** Evidence queries cannot read another scope, exceed configured limits, or leak redacted fields.

**Notes (2026-10-30):**
- Routes (`api/evidence.py`): `GET /v1/signals` (window on `occurred_at`, filters `source_id`/`service_id`/`kind`/`quality_status`, newest first, keyset cursor), `GET /v1/traces/{trace_id}` (oldest first, `truncated`), `GET /v1/sources/quality` (per source: total, every status, flags present; window on `ingested_at`). Bodies `SignalPage`, `TraceEvidence`, `QualitySummary` in `contracts/queries.py`, exported under `contracts/schemas/evidence/`. Rules in docs/api.md#evidence-queries.
- Scope helper: `EvidenceScope(workspace_id, replay_scope).clause(model)` in `persistence/repositories/scope.py`; `EvidenceRepository.list_signals`/`trace_signals`/`quality_counts` filter with it first. `CatalogRepository.list_sources` added.
- **Decision (ADR-014):** no operator auth existed (Q9 is P11-T02), and "cannot read another scope" needs a principal. `SIGNALOPS_OPERATOR_TOKENS` (workspace ID → token, SHA-256 constant-time) gives one; the token's workspace is the only workspace a query can use. Settings refuse an operator token equal to an ingest token. Unset → every operator request 401. Q9 stays open.
- Bounds: `SIGNALOPS_QUERY_DEFAULT_LIMIT`/`MAX_LIMIT` 50/200 and `SIGNALOPS_QUERY_DEFAULT_WINDOW_SECONDS`/`MAX_WINDOW_SECONDS` 1 h/7 d; exceeding them is 422, not clamped. Cursors are versioned base64url JSON (integer microseconds, hex IDs, so they fit `Key`'s 256 characters) carrying the resolved window and a digest of workspace, scope, filters, and window; another query's cursor is 400.
- Redaction: `attributes` re-redacted on output with the current process list plus the source's `redact_fields`; invalid source config is 500. Messages are returned as stored (ingestion does not scrub free text either).
- Migration `0008_evidence_query_indexes`: `signals(workspace_id, replay_scope, occurred_at, id)`; the plan's service and source indexes replaced by `(service_id, replay_scope, occurred_at, id)` and `(source_id, replay_scope, ingested_at)`. This closes the P02-T06 note about scoped signal indexes. `EXPLAIN` on the dev database: the scoped page is a backward index-only scan with the `(occurred_at, id)` row comparison as an index condition; quality counts use the source index.
- Compose passes `SIGNALOPS_OPERATOR_TOKENS`; `.env.example` has a placeholder. CORS allows the `Authorization` header.
- Left: no single-signal or deployment lookup routes (P05-T05 evidence references can add `GET /v1/signals/{id}`); no per-user identity, audit, or token rotation (P11-T02); replay datasets are readable by any operator of the workspace; free-text message scrubbing.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Phase 4 gate (2026-10-30): passed.** All five tasks are checked with evidence in the work log. Outcome demonstrated against the Compose stack: records from all three adapters (structured logs, trace summaries, deployments) went through `POST /v1/ingest`, Kafka, and the normalizer into PostgreSQL with receipts and outbox events (P04-T03/T04), and the P04-T05 live check read them back through the scoped API: 1036 signals over 6 pages with no repeats, quality counts per source showing accepted, degraded (`missing_service`, `clock_skew`), and quarantined (`malformed_payload`) outcomes. Open items carried forward: ingestion rate limiting, free-text redaction, generator quality fixtures (P10-T01), and the operator mechanism (Q9, P11-T02).

**Suggested phase commit:** `feat: ingest and normalize telemetry sources`. Split this into task-sized commits as needed.

## Phase 5 Detection and knowledge graph

**Dependencies:** Phase 4.

**Outcome:** An injected fault produces one correlated incident with temporal dependencies and inspectable evidence.

### P05-T01 Implement durable detection windows

- [x] Complete this task.

**Implementation:** Persist sample/error counts and latency histograms per service, replay scope, rule, and time window. Deduplicate before aggregate updates. Configure minimum samples, thresholds, cooldowns, and rule versions.

**Acceptance:** Duplicate delivery does not inflate error rate; low-sample windows do not trigger misleading incidents.

**Notes (2026-10-30):**
- `signalops detector` (Compose `detector`, group `detector-v1`) consumes `signals.normalized.v1`; the handler is `detection/detector.py` and the pure rules, windows, and histogram are in `detection/rules.py`. It counts `log`/`trace` signals that are `accepted` with a resolved service, loading the stored row in the envelope's workspace and scope. Details in docs/events.md#detector-detector-v1 and ADR-015.
- **Decision (ADR-015), deviation from the plan's tables:** migration `0009_detection_state` adds `detection_samples` (PK signal + series, cascades with its signal) as the dedupe key before an aggregate changes, and `detection_triggers` (unique per service, scope, rule, window) as the record of a rule firing. Aggregates are keyed per metric series, not per rule: rules over the same metric share one row (`trace.60s`, `log.60s`), and triggers carry the rule ID and version.
- Aggregate updates are additive upserts (`histogram || jsonb_build_object(bucket, n + 1)`); trigger decisions take a transaction-scoped advisory lock per (service, scope, rule). Cooldowns compare window starts (event time).
- Settings: `SIGNALOPS_DETECTION_WINDOW_SECONDS` (60) and `SIGNALOPS_DETECTION_RULES` (JSON; defaults `trace_error_rate` ≥ 0.2 / 15 samples, `trace_latency_p95` > 1000 ms / 20, `log_error_rate` ≥ 0.5 / 10, all with a 300 s cooldown). Settings refuse duplicate rule IDs; a latency threshold must be a bucket bound.
- **Found while testing:** the first version read p95 as the bucket upper bound and compared with ≥, so a 625 ms request counted as 1000 ms and a single slow request fired the rule in a 10-sample window (a demo false positive before the fault). Fixed by the exact count-against-bound test and `min_samples` 20 for p95. Offline calibration over 100 healthy and 100 fault demo runs (generator only, no services): 2 healthy runs fire the latency rule (two isolated timeouts in one 20-sample window), and 1 fault run misses `bank-gateway` latency. Recorded in ADR-015.
- `cli.py`: the normalizer and detector share `_run_consumer`.
- Left: rules evaluate on every sample, before window closure, with no lateness bound (P05-T02); triggers create no incidents or events yet (P05-T03); no API for aggregates or triggers (P05-T05); retention cleanup for aggregates and triggers is not implemented.

### P05-T02 Handle late data and window closure

- [x] Complete this task.

**Implementation:** Implement configurable 30-second allowed lateness, durable closure/recovery state, and explicit late flags. Use wall-clock closure live and a simulated clock plus final flush during replay.

**Acceptance:** Shuffled events within the lateness bound yield the expected incident; older arrivals follow the documented historical policy.

**Notes (2026-11-06):**
- **Decision (ADR-016, resolves Q7), deviation from P05-T01:** rules now run at window closure instead of on every sample, because per-sample evaluation lets arrival order decide. Worst-case detection is window plus lateness, 90 s by default.
- Live lateness uses the signal's `ingested_at`, so consumer lag does not make a signal late. Replay uses a durable per-service clock (`detection_clocks`), because a scope-wide clock would let one partition close another's windows.
- `signalops detector-closer` (new Compose service) closes live windows by wall clock; `signalops detector-flush` ends a replay. Migration `0010_window_closure` adds `late_count`, `closed_at`, `closed_by`, `detection_samples.late`, and `detection_clocks`, and closes earlier windows as `backfill` without evaluating them.
- Older arrivals: `detection_samples.late` and `late_count` only; they reach no rule. An on-time sample that finds its window closed joins it and re-evaluates; triggers are not withdrawn.
- **Acceptance scope:** incidents do not exist until P05-T03, so "expected incident" was verified at trigger level: shuffled arrivals produce the same triggers as ordered ones (synthetic windows, three seeds; the demo fault dataset, one seed). P05-T03 should repeat this at incident level.
- **Behavior change:** a default `demo-post` backfill, and the Compose `demo` service that runs it, is now mostly past the bound and fires only in its newest windows. `demo-post --realtime` (new, `demo/poster.py`) posts as timestamps arrive; a 300 s run took about 5 minutes.
- **Recalibrated offline:** over the same 100 healthy and 100 fault generator runs, per-sample and closure evaluation give identical results (2 healthy runs fire latency, 1 fault run misses latency). Closure adds order independence, not fewer false positives.
- **Local configuration:** `.env` lacks ingestion tokens for two demo sources, so `demo-post` fails with `no ingestion token for sources`. The Compose validation used temporary tokens held in one process's environment, and `api` was restarted without them afterward. `.env` was not changed.
- Left: the replay API path and automatic flush (P10-T02; replay tests set `replay_run_id` on the envelope); no API for late counts or closure state (P05-T05); after detector downtime a trigger from a partly consumed window is not withdrawn; replay does not bound far-future timestamps; retention for clocks, aggregates, and triggers.

### P05-T03 Create correlated incidents atomically

- [x] Complete this task.

**Implementation:** Define fingerprint and correlation-window keys, attach evidence and services, track recovery, and write incident events through the outbox. Initially allow manual investigation requests; add automatic requests after Phase 6.

**Acceptance:** Repeated triggers in one correlation window produce one logical incident; genuine later incidents remain distinguishable.

**Notes (2026-11-06):**
- **Decision (ADR-017):** the fingerprint is `env:{environment}` and a correlation window is a session bounded by `SIGNALOPS_INCIDENT_CORRELATION_GAP_SECONDS` (600), not a time bucket. The catalog has no dependencies until P05-T04, and one fault fires on several services. Two unrelated faults in one environment inside the gap merge.
- `detection/incidents.py` runs in the transaction that closes the window, so a trigger, its incident change, and the `incident.changed` outbox row commit together. An advisory lock per replay scope, taken before the per-rule lock, serializes correlation.
- Every breach reaches the incident, including those a cooldown suppresses; otherwise a long fault would split and look recovered.
- **Recovery:** an incident resolves when every service has a window that could have fired, did not, and ended at least `SIGNALOPS_INCIDENT_RECOVERY_SECONDS` (300) after `last_seen_at`. Silence is not recovery. A breach within the gap reopens the same incident.
- Migration `0011_incident_correlation` adds `detection_triggers.incident_id`, `incident_services.healthy_through`, and the fingerprint index. The 14 earlier dev triggers stay unlinked; no incidents were backfilled. `DetectionRule` gains `severity`, and default rules are now version 2.
- The P05-T02 follow-up is done: shuffled arrivals give the same incident as ordered ones (demo dataset, one seed).
- **Known gap:** in the Compose run `bank-gateway`, the faulted dependency, had a clear latency fault but stayed out of the incident, because its windows held 19 samples against `min_samples` 20. This is the sensitivity limit in ADR-015, not a correlation fault. The threshold was not changed. P05-T04 can add such services as `affected`.
- **Not re-measured:** triggers are unchanged, so the 2 in 100 healthy generator runs that fire latency (ADR-015) should each open an incident. The one healthy seed tested opened none.
- Left: automatic investigation requests (after Phase 6); operator status updates and read APIs (P05-T05); a service that stops sending keeps its incident open until then; an incident from a partial window after detector downtime is not withdrawn; the Compose `demo` service still backfills and needs `--realtime` to open an incident; `.env` still lacks two demo ingestion tokens, so the Compose run used temporary tokens in one process, and services were restarted without them.

### P05-T04 Implement the temporal graph projection

- [x] Complete this task.

**Implementation:** Use an independent consumer group with receipts to project services, deployments, and observed dependencies. Preserve provenance and validity intervals. Add bounded recursive queries with cycle prevention and three-hop limits.

**Acceptance:** Historical queries exclude future deployments/dependencies; graph cycles terminate within bounded query limits.

**Notes (2026-11-06):**
- **Decision (ADR-018):** `signalops graph-projector` (group `graph-projector-v1`, new Compose service) projects `accepted` traces and deployments with a resolved service. An observed dependency is one edge per merged interval: each observation covers `SIGNALOPS_GRAPH_DEPENDENCY_TTL_SECONDS` (7 days), so a dependency nothing confirms expires with no background job, and shuffled input gives the same edges. A `DEPLOYED_TO` edge lasts until the service's next deployment, also out of order.
- Migration `0012_graph_projection` adds `ex_graph_edges_validity` and the `btree_gist` extension, which stays on downgrade. An advisory lock per relationship makes concurrent projectors merge; the constraint is the guarantee behind it. `test_edges_of_one_relationship_cannot_overlap` covers the constraint.
- **Reads:** `GraphRepository.dependencies` (3 hops, downstream or upstream, node paths against cycles, `MAX_WALK_PATHS` 1000, `truncated`) and `deployments_as_of`, returning the existing `DependencyEntry` and `DeploymentEntry` contracts. Neither returns `valid_to`, because it would disclose a later deployment or observation.
- **Bound measured:** on a dense graph of 80 services (6320 edges) the walk stopped at 1001 paths in about 10 ms. Without the limit, the same query ran about 15 s before the run failed; that failure was not examined. The result-level test could not see this, so `test_walk_reads_no_more_paths_than_its_bound` asserts on the walk.
- **Rebuild limit, found in the Compose run:** a new group reads only retained history. `signals.normalized.v1` keeps 7 days, so 1036 of 2655 stored signals, including 199 per edge and one `inventory` deployment, were not projected. Earliest offsets sum to exactly 1036, and lag is 0. Edges therefore start at 2026-11-06 16:29 although traces go back to 2026-10-30.
- **Not done, by choice:** no `AFFECTS` edges, incident nodes, or `DOCUMENTED_BY`, which the task does not list. The incident fingerprint stays `env:{environment}` and `bank-gateway` is not added as `affected`, so the P05-T03 gap stands; `payment` depends on it in the graph, which P05-T05 can show as the neighborhood.
- Left: an API over these reads (P05-T05); graph rebuild from PostgreSQL past Kafka retention; a dependency on a service not in the catalog is logged and gets no edge; a degraded deployment has no graph edge, though its row exists; edge and node retention. No real-time fault run was repeated here, because `.env` still lacks the two demo ingestion tokens.

### P05-T05 Expose incident and graph APIs

- [x] Complete this task.

**Implementation:** Implement incident list/detail, evidence timeline, graph neighborhood, cursor pagination, and optimistic status updates. Add API-backed incident views to the React shell.

**Acceptance:** A developer can locate the injected incident, inspect its evidence, and see the affected dependency neighborhood.

**Notes (2026-11-06):**
- **Decision (ADR-019):** `PATCH /v1/incidents/{id}` takes a status and version, locks only the incident row, and writes `incident.changed` with the change. Live scope only, so a replay incident is 404. The UI asks for the operator token and holds it in `sessionStorage`.
- **Additions the task did not name:** `GET /v1/incidents/{id}/evidence` gives the timeline full redacted signals, since `EvidenceRef` has no message. `IncidentDetail` gains `triggers` and `triggers_truncated`, both defaulted, so the v1 change is additive.
- **Found while building:** `IncidentPage` allows 100 items and `SIGNALOPS_QUERY_MAX_LIMIT` is 200, so a valid request of 101 would have failed response validation. `check_limit` now takes a `ceiling`. CORS allowed only GET and POST, so PATCH was added.
- **Neighborhood view:** plain SVG with a data table, no graph library. `as_of` is `last_seen_at`. In a cycle, the edge back to the center shows only under `upstream`, from the existing walk.
- **Test shim:** `src/test/setup.ts` drops the signal from `Request`, because Node's `Request` refuses jsdom's `AbortSignal` on a router navigation. The same navigation was exercised in Chrome.
- Left: a reopened incident whose services already recovered resolves on the next healthy window; status changes record no actor (P11-T02); `bank-gateway` is in the neighborhood but not marked as belonging to the incident, the P05-T03 gap; the list filters by service only through links, because no service list route exists. `.env` has no operator or demo ingestion tokens.
- **Not exercised end to end:** the status buttons ran only against stubbed responses in Vitest, because pressing them would have changed the dev incident. `PATCH` ran against real PostgreSQL in integration tests and, for the stale case, against the dev database; its outbox row was not followed through Kafka. The Compose `api` and `frontend` images were not rebuilt, so they still run the previous code.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: detect incidents and project service knowledge graph`. Split this into task-sized commits as needed.

## Phase 6 Durable jobs and workflow runtime

**Dependencies:** Phases 3 and 5.

**Outcome:** An investigation survives a worker restart and rejects stale-worker writes.

### P06-T01 Implement investigation requests and dispatch

- [x] Complete this task.

**Implementation:** Create a request endpoint with caller/route-scoped Idempotency-Key, canonical request hash, and durable response mapping. Atomically create investigation plus outbox request; dispatcher creates one job before acknowledging. Enable detector-triggered requests using stable request keys.

**Acceptance:** A repeated request returns the same investigation; key reuse with another body returns conflict; duplicate Kafka requests create one job.

**Notes (2026-11-06):**
- **Decision (ADR-020, resolves Q5):** `POST /v1/incidents/{id}/investigations` writes the investigation, an `idempotency_keys` row, and the outbox request together. The key scope is workspace plus route, because tokens carry no identity. The hash covers the incident and body, so a key reused on another incident is 409. The unique `request_key` decides races. `signalops dispatcher` (group `dispatcher-v1`, new Compose service) creates the job.
- **Detector requests:** a new live incident requests one investigation in its own transaction, keyed `detector:{incident_id}`. `SIGNALOPS_INVESTIGATION_AUTO_REQUEST=false` turns it off.
- **Choices the task did not state:** requests are live only, so a replay cannot gain unplanned spend; the body can only lower the configured budget, steps, and deadline; a repeat skips that check, so it still answers after a limit drops; `evidence_cutoff_at` is the request time; the new default limits are 1 USD, 20 steps, and 600 seconds, which are provisional until Phase 7 measures real calls.
- Left: `GET /v1/investigations/{id}` and any UI for requests (P06-T05, Phase 8); a reopened incident gets no new automatic investigation; nothing limits how many investigations an operator starts for one incident; automatic requests ignore severity; nothing yet reports an investigation left without a job; a request's actor is not recorded (P11-T02).
- **Not exercised end to end:** the detector-triggered path ran in integration tests only, because `.env` has no demo ingestion tokens to drive a real fault. The operator path ran through real Compose services.

### P06-T02 Implement job claiming and recovery

- [x] Complete this task.

**Implementation:** Claim queued or expired jobs with short SELECT FOR UPDATE SKIP LOCKED transactions. Increment lease_generation, heartbeat, and reclaim expired work. Bound retries and set available_at for delayed attempts.

**Acceptance:** Two workers cannot own the same current lease; expired workers cannot checkpoint or complete after another worker takes over.

**Notes (2026-11-07):**
- **Decision (ADR-021):** `claim_job` and `JobRepository` use `SKIP LOCKED` and PostgreSQL's clock. `JobRepository.hold` is the fence: a row lock after checking status, owner, and generation, in the writer's own transaction. `JobRunner` adds heartbeats and settles the job. Each claim spends an attempt, so a crash loop stops at `max_attempts`.
- **Choices the task did not state:** an exhausted or permanently failed job fails its investigation and writes `investigation.completed` in that transaction, because otherwise an investigation stays `running` forever; `save_checkpoint` writes running steps only, and P06-T03 adds step completion and output; a stopping worker releases its job and refunds the attempt; an expired lease nobody took stays valid; `last_error` keeps only the type of an unexpected exception, because its text can quote logs.
- **Deliberately not added:** a `signalops worker` command or Compose `investigation-worker`. No workflow exists to run, and a worker now could only fail real jobs or mark them succeeded with no diagnosis. P06-T03 adds both with the first handler.
- Left: `complete_job` finishes only the job, so the investigation stays `running` until P06-T05 writes its terminal state; claiming does not yet check deadlines or cancellation (P06-T04), so a job whose investigation is already terminal would still be claimed; nothing yet reports queued or failed job counts (P11).
- **Not exercised:** a real killed process. Lease expiry was simulated by moving `lease_until` in PostgreSQL or by short real leases in one test process. P06-T03's acceptance kills a real worker.

### P06-T03 Implement the persisted workflow engine

- [x] Complete this task.

**Implementation:** Define typed workflow nodes, versioned state transitions, deterministic step keys, checkpoints, and validated output reuse. Run a fake-provider sequence before any real model calls.

**Acceptance:** Killing the worker after a completed node resumes at the next unfinished node and retains completed output.

**Notes (2026-11-07):**
- **Decision (ADR-022):** `agents/workflow.py` holds typed `Node`s in a versioned `WORKFLOWS` map, and `WorkflowEngine` is the `JobRunner` handler. The node key is the step key. Each step has a fenced open and a fenced end in separate transactions, and the node runs outside any transaction. `signalops worker` and Compose `investigation-worker` are added, as P06-T02 deferred.
- **The fake sequence:** `load_context` reads the incident, `investigate` makes no model call and always abstains, and `conclude` builds an `insufficient_evidence` `Diagnosis`. The `Diagnosis` stays in `agent_steps.output`; nothing writes it to the investigation yet. The gateway is P07-T01, so no provider is called, even a fake one.
- **Choices the task did not state:** `reused_output` raises `PermanentJobError` on unusable output, because a completed step cannot run again; `max_steps` reached also fails the job, and whether it should abstain instead is left to Phase 7; a failed node marks its step `failed` with an `error_record` in `checkpoint.state`, so `save_checkpoint` now reopens failed steps; `IncidentBrief` holds only fields fixed at detection, because severity can change after the evidence cutoff.
- **Test code:** `tests/integration/hanging_worker.py` is a real worker whose `investigate` never returns, so production code needs no fault-injection setting. `add_job` moved to `tests/integration/job_support.py`.
- Left: the investigation stays `running` after its job succeeds, so each one finished by the worker is stranded until P06-T05, and that task must decide how to recover those rows; output is not published as progress (P06-T05); deadlines and cancellation are unchecked between nodes (P06-T04); Phase 7's real investigator needs a new workflow version, since v1 steps hold fake output; nodes run only in sequence (Phase 9).

### P06-T04 Implement cancellation and deadlines

- [x] Complete this task.

**Implementation:** Persist cancellation requests and terminal transitions with version checks. Check cancellation/deadline before calls and between nodes; fence stale completion.

**Acceptance:** Cancellation during a call does not permit new downstream calls; the UI eventually shows the durable terminal state.

**Notes (2026-11-07):**
- **Decision (ADR-023):** migration `0014` adds `investigations.cancel_requested_at`. `POST /v1/investigations/{id}/cancel` ends the investigation at once unless a job is running; then the lease holder ends both. `JobRepository.stop_reason` is the one check, read by the claim, each heartbeat, the transaction that opens a step, and each settling write. `fail_investigation` became `end_investigation`, which also marks steps left `running` as `failed`.
- **Choices the task did not state:** a passed deadline ends the job and investigation `failed`, with `deadline_exceeded` in `last_error`, because `status` has no deadline value; a heartbeat interrupts the running node, because a check only between nodes never fires for a hung call; a cancellation beats any outcome not yet committed, while work finished past the deadline keeps its result; a repeated cancellation is `202`, and an investigation that ended another way is `409`. The API takes no `version`: a cancellation is a one-way request, and heartbeats and claims would make a client's version stale for no benefit. Writes hold the row locks and the ORM's `version_id_col` applies.
- **Deviation:** the acceptance needs a UI, which is P08-T02, so this task adds `GET /v1/investigations/{id}` (from P06/P07) and a minimal `/investigations/:id` page: status, times, cancel, and 3-second polling that stops at a terminal status. Nothing links to the page, because no route lists an incident's investigations; it is reachable by URL only. P08-T02 adds that path, start controls, and the rest of the results.
- `test_exhausting_a_job_keeps_a_terminal_investigation` changed: such a job now ends `cancelled`, not `failed`, closing the gap noted under P06-T02.
- Left: a cancellation of a succeeded job's stranded `running` investigation ends it `cancelled`, which is the only way out of that state until P06-T05, which must also settle how it meets this path; Phase 7 must check `stop_reason` before each model call inside a node, since a node is one call only today; with no worker running, nothing ends a running or overdue job; a cancellation records no actor (P11-T02); `InvestigationView` carries no cancelled-reason or deadline detail, so a deadline failure looks like any other `failed` in the UI.

### P06-T05 Persist workflow progress and completion

- [x] Complete this task.

**Implementation:** Write progress_events with step commits and terminal completion with its outbox event. Store structured findings and operational metadata rather than unrestricted private reasoning.

**Acceptance:** Progress remains queryable after restart; replaying job execution cannot produce contradictory terminal states.

**Notes (2026-11-07):**
- **Decision (ADR-024):** `Workflow` gained a typed `conclusion: Node[Diagnosis]`, and `WorkflowEngine` returns it. `complete_job` takes the diagnosis and ends the job and investigation in one fenced transaction, with `investigation.completed` and an `investigation.ended` progress event. `save_checkpoint`, `complete_step`, `fail_step`, and `abandon_steps` write their progress in the transaction of the step change. `contracts/progress.py` types the events.
- **Stranded rows:** migration `0015` settles investigations left `running` by a `succeeded` job, as P06-T03 required, using the stored `conclude` output and a hardcoded `investigation.completed` envelope. It leaves a `running` job alone, because that job's next attempt settles it. The one dev row, `f8fb3276`, became `insufficient_evidence` and its event was published.
- **Choices the task did not state:** events hold identifiers and categories, never output or error text; reused steps write no event; a step abandoned by an ending gets `step.failed` with `abandoned`, so the history agrees with `agent_steps`; a cancellation request writes no event, because the task names only step commits and completion; a partial unique index backs the one ending.
- **Deviation:** "queryable" needed a read, and SSE is P08-T03, so this task adds `GET /v1/investigations/{id}/progress` with an integer `after` cursor (the same value as a future `Last-Event-ID`). `/events` stays reserved for SSE. No frontend change: the page already shows the status and diagnosis summary, and P08-T02 shows steps.
- **Changed behavior:** a cancellation racing a completion can now lose, as `InvestigationFinishedError`, `409`. Before this task it always won, because the investigation stayed `running` after its job succeeded. `test_a_cancellation_racing_completion_has_one_outcome` covers both orders.
- Left: a crash after `conclude` commits loses that diagnosis if the job is then out of attempts or past its deadline, because the claim ends those jobs without running them, so the investigation ends `failed` with the diagnosis only in `agent_steps`; `max_steps` still fails rather than abstains (Phase 7); `InvestigationView` carries no reason for a `failed` outcome; the detector-triggered path to a job still ran only in integration tests, as noted under P06-T01.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: implement durable investigation execution`. Split this into task-sized commits as needed.

## Phase 7 LLM gateway and first investigator

**Dependencies:** Phase 6.

**Outcome:** A single bounded agent returns a validated diagnosis with verifiable evidence and accounted cost.

### P07-T01 Create a provider abstraction and fake provider

- [x] Complete this task.

**Implementation:** Define structured request/response types, provider errors, usage metadata, prompt versions, and deterministic fixtures. Implement one real provider adapter behind the same interface.

**Acceptance:** Fake mode reproduces success, timeout, throttling, and invalid-output behavior without network access.

**Notes (2026-11-07):**
- **Decision (ADR-025, resolves Q8):** `gateway/types.py` holds provider-neutral requests and responses. `ModelProvider.complete` is one call. `AnthropicProvider` uses the official SDK (`anthropic` 2.6, a new dependency the task requires) with `max_retries=0`, because the gateway must reserve for each retry. `FakeProvider` runs step scripts; `fixtures/gateway/v1` holds `success`, `timeout`, `throttled`, and `invalid_output`.
- **Choices the task did not state:** `ProviderError` keeps a category, status, and request ID, never provider text; 408, 5xx, timeouts, lost connections, and unreadable responses are ambiguous, 429 and 529 are not; the default model is `claude-opus-5-5`; the agent's final answer will be a tool call, because a terminal tool validates and repairs the same way on any provider; `ProviderBlock` carries a provider's thinking back unchanged within a conversation and is never stored.
- **Deviation from the skill default:** no server-side refusal fallback, because a fallback model can cost more than the reservation (ADR-025).
- Compose passes the key to `investigation-worker` only. Startup fails on `anthropic` with no key.
- **Not verified:** `AnthropicProvider` ran against a stub transport with the SDK's real client and response parsing, never against the live API. No paid call was made.

### P07-T02 Enforce budgets and provider limits

- [x] Complete this task.

**Implementation:** Reserve conservative per-call maximum cost atomically in PostgreSQL before dispatch; include all in-flight calls. Use Redis for atomic rate/token limits. Persist usage, pricing version, provider request IDs, and actual/estimated/unknown cost classification.

**Acceptance:** Parallel calls cannot oversubscribe budget; an ambiguous timeout retains a conservative reservation and any retry reserves separately.

**Notes (2026-11-07):**
- **Decision (ADR-026):** `ModelGateway.complete` takes a Redis slot, then one transaction checks the lease and `stop_reason`, closes pending calls of older leases as `unknown`, and reserves the call's maximum cost through the conditional `hold_budget` update. No transaction spans the call. Retries live here, and each one is a new `llm_calls` row with its own reservation.
- **Migration `0016`:** `pricing_version`, cache tokens, `lease_generation`, `error_category`, `completed_at` on `llm_calls`.
- **Choices the task did not state:** the maximum treats each request byte as a token written to the cache, because no tokenizer runs offline; `attempt` numbers every call of a step across job attempts; settling is keyed by the call, not fenced, because a stale worker's answer is still a fact about money, while a closed call stays closed; ending an investigation closes its pending calls as `unknown`, so a cancelled call keeps its reservation as ADR-023 required; fake models have prices and their costs are `estimated`; prices cover Opus 5.5 and Sonnet 5.5, and Haiku 5.5 is left out because its rate changes above 100K tokens; running out of budget ends the job `failed` and the investigation `budget_exceeded`.
- Left: reservations are far above typical cost, so a budget can end an investigation early; prices are copied by hand; the worker does not use the gateway until P07-T04.

### P07-T03 Implement constrained evidence tools

- [x] Complete this task.

**Implementation:** Build query_logs, get_trace_summary, get_deployments, get_service_dependencies, and retrieve_runbooks. Store versioned runbook content with provenance in graph node properties initially. Enforce evidence cutoff, workspace/replay scope, row/byte caps, and timeouts.

**Acceptance:** Tools reject out-of-scope and future evidence; requests cannot turn tool access into unrestricted SQL or shell execution.

**Notes (2026-11-07):**
- **Decision (ADR-027):** `agents/tools.py` holds the five tools, with one typed argument model each and unknown fields forbidden. `ToolScope` comes from the investigation, so input cannot name a workspace, replay run, or cutoff. `ToolExecutor.run` records `tool_calls` under the lease fence, keeping a result hash and never the rows.
- **Choices the task did not state:** the cutoff applies to `ingested_at` as well as `occurred_at`, because a late signal carries an old `occurred_at`; a window past the cutoff is rejected, not clamped; limits are 6 hours, 50 rows, 32 KiB, and a 5-second read; `TextRedactor` masks sensitive `key=value` pairs and bearer tokens in free text; `TraceSummaryEntry` gained optional `downstream_services`, an additive contract change; a repeated call in a step reuses its row; a quarantined trace record is skipped.
- **Runbooks:** one graph node per version, keyed `{key}@{version}`, with `DOCUMENTED_BY` edges from `published_at`. `signalops seed` adds two synthetic runbooks in the live scope.
- Left: free-text masking is a pattern match, and source-specific redaction fields do not reach it; the replay scope has no runbooks until P10; nothing calls the tools until P07-T04; the read cache is P07-T05.

### P07-T04 Implement the first investigation workflow

- [x] Complete this task.

**Implementation:** Load incident context, execute bounded tool calls, synthesize hypotheses, and return the typed diagnosis or insufficient_evidence. Add maximum steps, output-token limits, and bounded structured-output repair.

**Acceptance:** The timeout fixture identifies the affected service with existing evidence IDs; underdetermined input yields abstention rather than an invented cause.

**Notes (2026-11-07):**
- **Decision (ADR-028):** `WORKFLOW_VERSION` is `v2`, with the same three step keys. `agents/investigator.py` is a bounded loop of gateway calls and tools ending in a `submit_findings` tool call. `conclude` names the service from the catalog. `v1` stays for existing rows. `signalops worker` now builds the gateway, rate limiter, and tools.
- **Fake mode:** `gateway/fake_investigator.py` plays the loop by a fixed rule on real evidence: read error traces and logs, follow failing calls downstream, check the last service's durations, cite what it read, and abstain below 5 errors in a service. It proves the plumbing and is no measure of diagnostic quality.
- **Choices the task did not state:** turns are `max_steps` minus the node count, resolving what P06-T03 left open: an agent out of turns abstains, while a `max_steps` below the node count still fails; a refusal abstains; a reply cut off at `max_tokens` is dropped, since it may hold a partial tool call; repairs tell the model field locations only; 5 tool calls per turn and 10 per step; a brief lists the workspace's services and detector-cited signals known by the cutoff; a tool result is capped at 16 KiB, down from 32 KiB, so a full step fits the default budget's reservations.
- **Open:** the conversation is memory only, so a restarted node pays for its turns again; ten full tool results can still reach `budget_exceeded` on reservations alone; citations are not verified yet (P07-T05).

### P07-T05 Validate findings and cache safe reads

- [x] Complete this task.

**Implementation:** Verify cited evidence IDs, scope, cutoff, confidence bounds, and output shape. Cache read-only context in Redis with keys including replay scope, cutoff, and data version.

**Acceptance:** Stale or cross-run cache content cannot alter evidence scope; missing citations fail validation.

**Notes (2026-11-07):**
- **Decision (ADR-029):** `agents/findings.py` checks the named service and every citation against PostgreSQL, on submission, so the model can correct itself, and again in `conclude`, where a failure abstains. `agents/read_cache.py` caches tool results for 60 seconds under keys holding workspace, replay scope, cutoff, data version, tool, and arguments. A stored value repeats its identity and is revalidated as a contract, so a stale or foreign one is a miss.
- **Choices the task did not state:** a citation must be known by the cutoff on both clocks; the data version is the count and latest `ingested_at` of signals of the last hour, because the case that matters at a fixed cutoff is a late commit under lag; citations never use the cache; an unreachable Redis is a miss; `ToolScope` moved to `read_cache.py`.
- Left: citations are checked for existence and scope, not for whether they support the claim (Phase 9, P10); hits are rare, since each investigation has its own cutoff, so the cache mainly serves retries; the version does not see graph or runbook changes, which show within the TTL.

### P07-T06 Verify gateway failure behavior

- [x] Complete this task.

**Implementation:** Check permanent versus retryable errors, capped backoff, conservative unknown-cost accounting, and resume after model timeout. Log sanitized operational metadata.

**Acceptance:** No provider credentials or raw sensitive payloads appear in UI/logs; tests explicitly acknowledge external calls may have executed before timeout.

**Notes (2026-11-07):**
- **Found and fixed:** two leaks. A failed job attempt logged `exc_info`, and a traceback can quote evidence, since pydantic messages include `input_value`; it now logs the exception type. At `SIGNALOPS_LOG_LEVEL=DEBUG`, the SDK and HTTP client loggers would log request bodies; `configure_logging` now keeps them at `WARNING`. Both tests failed before the fixes.
- **Timeouts:** the tests name it directly: a timed-out call reached the provider and may have run and been billed, so the record stays `unknown` with its reservation held. They assert conservative records, not that no call happened.
- **Resume:** a job whose calls all time out requeues, reuses `load_context`, restarts `investigate`, and finishes, with both unknown reservations still held. A job that never succeeds fails after three attempts with every reservation held.
- **Stale workers:** a worker's late answer cannot reopen a call a later lease closed, and it cannot continue past the fence.
- Left: a model could quote text that masking missed into its diagnosis, because diagnosis strings are not redacted again; `AnthropicProvider` still has not run against the live API; no real-browser check of the investigation page was made in this phase, since the UI did not change.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: add budgeted model gateway and evidence tools`. Split this into task-sized commits as needed.

**Phase 7 gate (2026-11-07): passed on the fake provider.** All six tasks are checked. Outcome demonstrated on the rebuilt Compose stack with `SIGNALOPS_MODEL_PROVIDER=fake`: `signalops demo-post downstream-timeout --realtime --duration-s 300` through the API opened a live incident, which requested investigation `bc4cb419-ed57-43cc-9a1b-993b04fdc576` on its own. It ended `completed` in under a second, naming `bank-gateway` as `downstream_timeout`, with 10 cited signals that all exist in scope and were known by the cutoff; 3 settled calls, 3 tool calls, 0 reserved, 78076 microUSD spent (`estimated`), and 1 job attempt. `GET /v1/investigations/{id}` returned the diagnosis and usage, and `/progress` returned seven events. With `redis` stopped, operator investigation `ba68520f-6ec1-4ae0-bfce-fc402f4e252c` made no model call for 12 seconds and logged `rate limiter unavailable`; once Redis started, it finished on the same job attempt. Throwaway tokens were set for the run and removed after it, and `.env` was not changed. No paid call was made, so this shows the workflow, accounting, and bounds, not diagnostic quality: the fake follows a fixed rule. Open items carried forward: `AnthropicProvider` has never run against the live API; reservations are unmeasured against real usage; R1 still needs the UI of Phase 8.

## Phase 8 Incident workspace and live frontend

**Dependencies:** Phases 5 and 7.

**Outcome:** The first portfolio slice is usable from fault injection to diagnosis in the browser.

### P08-T01 Complete incident browsing

- [x] Complete this task.

**Implementation:** Add service/severity/status filters, cursor navigation, timeline, quality flags, affected services, and bounded graph display. Include meaningful loading, empty, and error states.

**Acceptance:** A user can navigate from the incident list to its evidence and dependency graph without manual API calls.

**Notes (2026-11-08):**
- Most of this existed from P05-T05: status and severity filters, cursor paging, the evidence timeline, the bounded graph, and their loading, empty, stale, and error states. This task added what was missing.
- **Additive contract change:** `IncidentPage.items` are now `IncidentListItem`, an `IncidentSummary` with `services` (roles, primary first, default empty). `IncidentRepository.services_of_many` loads them for one page in one query. The detail's services are also primary first now, so the graph centers on a service whose rule fired. The UI treats a missing `services` as none, so it still reads an older server.
- **UI:** the list has a Services column whose names set the `service_id` filter, and a chip naming the filtered service with a clear button. The detail has a Data quality section counting cited evidence by quality status, and the timeline shows each signal's quality flags without opening its details.
- **Environment:** this machine had no `uv`, `pnpm`, installed dependencies, or Docker. `uv` and `pnpm` (through corepack) were installed in user space. Docker needs root, so integration tests were skipped at first. Once Docker was available (P08-T04), the new assertion in `test_list_filters_and_scope` passed with the rest of the integration suite.

### P08-T02 Build investigation controls and results

- [x] Complete this task.

**Implementation:** Add start/cancel controls, workflow progress, hypotheses, verified evidence links, uncertainty, limitations, and token/cost display. Distinguish actual cost from estimates/unknown usage.

**Acceptance:** Repeated button clicks do not create duplicate investigations; insufficient evidence is presented as a valid outcome.

**Notes (2026-11-08):**
- **New read routes (additive):** `GET /v1/incidents/{id}/investigations` lists the newest 20 investigations of an incident, so a detector-started run can be found from the incident. `GET /v1/investigations/{id}/evidence` returns the root cause's cited signals, checked again against workspace, replay scope, and cutoff on both clocks, redacted as `/v1/signals` is; a citation failing the check is listed as unverified and never returned. Schemas `incident-investigations.v1.json` and `investigation-evidence.v1.json`.
- **No duplicate starts:** the start button is disabled while its request is in flight, and one request keeps one `Idempotency-Key` in `sessionStorage` per incident across a lost response or a reload. The key is dropped once the server answers anything other than a `503`, so a later deliberate start is a new investigation. The server's same-key, same-investigation rule was verified in P06-T01.
- **Results:** the investigation page shows workflow steps (each step's latest attempt), the outcome, the root cause hypothesis with confidence labelled as the model's uncalibrated estimate, cited evidence marked verified, other hypotheses, suggested actions (each needing a person), limitations, and a cost panel: budget, spent split into actual and estimated, current reservation, and calls of unknown cost with what they hold. Insufficient evidence is a distinct, non-error outcome that lists the missing evidence.
- **Progress transport:** `useProgress` reads `/progress` after the last event held, every 3 seconds while the investigation is open. P08-T03 replaces the polling with SSE writing to the same query cache.
- **Integration (run in P08-T04):** three new tests in `test_investigation_requests.py` (listing, the 20-row bound, and evidence scope, cutoff, and redaction) pass on Compose.

### P08-T03 Implement durable SSE and recovery

- [x] Complete this task.

**Implementation:** Serve progress_events through SSE using Last-Event-ID. Use Redis pub/sub only for wake-ups, plus periodic database catch-up. Define behavior for an expired progress cursor.

**Acceptance:** Reconnect replays missed events in order; missing a Redis notification does not leave a connected UI permanently stale.

**Notes (2026-11-08):**
- **Decision (ADR-030, resolves Q4):** `GET /v1/investigations/{id}/events` replays `progress_events` after `Last-Event-ID` (or `after`), then reads again on a Redis wake-up or every `SIGNALOPS_SSE_CATCHUP_SECONDS` (5 s), with a keep-alive comment when nothing is new. A cursor naming no event of this investigation, whether foreign or expired, gets `event: reset` and the whole retained history. The stream ends after `investigation.ended` or after `SIGNALOPS_SSE_MAX_STREAM_SECONDS` (300 s), and at most `SIGNALOPS_SSE_MAX_STREAMS` (200) are open per API process. No transaction stays open while a stream waits.
- **Wake-ups after commit:** `ProgressRepository.add` marks the session; `Database.transaction()` hands the marked investigations to an `on_progress` hook after commit and never on rollback. The worker and the API (cancellations) use `RedisProgressNotifier`, which drops a slow or failed publish. Readers never depend on it.
- **Browser:** `api/progressStream.ts` reads the stream with `fetch`, since `EventSource` cannot send the operator token. It parses SSE, reconnects with `Last-Event-ID` and capped backoff, treats 20 s without bytes as a dead connection, and handles `reset`. `useProgressStream` starts after the history read and merges events into the same query cache, ignoring IDs it holds; a step's end or the investigation's end refreshes the investigation. The page shows Live, reconnecting, and stopped states. Investigation polling dropped from 3 s to a 15 s fallback.
- **Verified:** `tests/unit/test_progress_stream.py` covers ordered replay after a cursor, reset, a missed wake-up covered by catch-up, an immediate read on a wake-up, a spurious wake-up, paging a backlog, the lifetime close, disconnects, and cursor validation. Frontend tests cover reconnect after a drop and after a server close with the last ID received, reset, replayed duplicates, and no stream for an ended investigation.
- **Integration (run in P08-T04):** `tests/integration/test_progress_stream.py` (real PostgreSQL and Redis: a wake-up delivering before a 30 s catch-up, catch-up with no Redis at all, reconnect replay, no wake-up on rollback, and the route's headers, cursors, reset, and scope) passes on Compose.

### P08-T04 Verify the full user journey

- [x] Complete this task.

**Implementation:** Add one browser integration check against a deterministic backend/fake provider: ingest fault, find incident, start investigation, inspect evidence, reconnect, and cancel another run.

**Acceptance:** The R1 journey runs from a clean checkout with documented commands and no paid API dependency.

**Notes (2026-11-08):**
- `apps/web/e2e/journey.e2e.ts` (Playwright, `pnpm e2e`) ingests a real-time `downstream-timeout` fault through the demo poster, waits for the detector's incident, opens it from the list, checks the evidence timeline, data quality, and dependency graph, double-clicks Start investigation and checks through the API that exactly one investigation was added, reloads mid-run and checks that a stream request carried `Last-Event-ID`, checks that the page lists every step of the durable `/progress` history once, inspects the diagnosis (verified citations, uncalibrated confidence) or the abstention and the estimated cost, and cancels a second run. Commands and variables are in `docs/development.md`, "Browser journey check".
- `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS` (fake provider only, default 0, passed to `investigation-worker`) delays each fake reply, so with 3 a run stays open long enough to reconnect to and cancel. Unit-tested.
- **Run (2026-11-08):** from a clean working tree at `300342a`, following "Browser journey check": `.env` made from `.env.example` with fresh random tokens and `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3`, `docker compose up -d --build --wait`, `demo-seed`, then `pnpm e2e` with `E2E_BROWSER_CHANNEL=chrome`. It passed on the first run in 2.9 minutes; no code change was needed. Provider `fake`, so no paid call. Two local details, not repository changes: the shell predated the user's `docker` group membership, so Docker commands (including `E2E_INGEST_COMMAND`) ran through `sg docker -c`, and the installed Google Chrome was used instead of downloading Playwright's Chromium.
- **Integration suite:** `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 603 passed, including the tests added in P08-T01 to T03.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Phase 8 gate (2026-11-08): passed on the fake provider.** All four tasks are checked. Outcome demonstrated in a real browser on the Compose stack (`SIGNALOPS_MODEL_PROVIDER=fake`, `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3`) by `pnpm e2e` at `300342a`. A real-time `downstream-timeout` run (262 records accepted, 0 rejected) opened incident `7cb83270-5566-42dd-bcda-7c8a7f2ba1ac` ("High error rate on payment (demo)"), which started investigation `97795cd8-6a63-43a9-bc17-4a5089f8e2ab` on its own. From the UI, a double click on Start investigation added exactly one investigation, `a868bb2d-d010-4522-806a-9604cc632f24`. A reload mid-run resumed the stream with a numeric `Last-Event-ID`, and the page listed each of the history's steps once. Both runs ended `completed`, naming `bank-gateway` as `downstream_timeout` with 10 cited signals, all verified by `/evidence` (0 unverified). Each run had 7 progress events, 3 model calls and 78144 microUSD spent, all `estimated`, with 0 reserved and 0 unknown. A third run, `da6754bc-6333-4646-81c2-3bae42b78b06`, was cancelled from the UI and stored no diagnosis. `SIGNALOPS_INTEGRATION=1 uv run pytest` 1190 passed. No paid call was made, so this shows the workflow and UI, not diagnostic quality. Open items carried forward: CI does not run the browser journey; `AnthropicProvider` has still never run against the live API. Q11 is resolved in the same commit: `CLAUDE.md` now points at the planning files in `docs/`.

**Suggested phase commit:** `feat: build incident investigation workspace`. Split this into task-sized commits as needed.

## Phase 9 Multi-agent investigations

**Dependencies:** Phases 7 and 8.

**Outcome:** Specialist collaboration is bounded, persisted, and demonstrably recoverable.

### P09-T01 Define role contracts and handoffs

- [x] Complete this task.

**Implementation:** Implement coordinator, telemetry investigator, deployment investigator, dependency/runbook investigator, synthesizer, and verifier as logical workflow roles. Version their prompts and structured outputs.

**Acceptance:** Each role receives only its required context and returns typed findings with provenance and explicit uncertainty.

**Notes (2026-11-08):**
- **Decision (ADR-031):** workflow `v3` in `agents/multi_agent.py`: `load_context`, `plan`, `telemetry`, `deployment`, `dependency`, `synthesize`, `verify`, `conclude`, run in sequence for now (P09-T02 makes the branches parallel). Contracts in `contracts/roles.py`, with schemas `investigation-plan`, `specialist-findings`, `synthesis`, and `verification` (`.v1.json`) under `contracts/schemas/workflow/`. Prompts `telemetry-v1`, `deployment-v1`, `dependency-v1`, `synthesizer-v1`, `verifier-v1`.
- **The coordinator is code:** `plan` writes each specialist's brief, turns, and budget share into its output, so what each role was told is stored. Telemetry gets detector-cited signals; deployment and dependency get the incident's services; the synthesizer gets only reports; the verifier gets the leading citations as code read them, redacted.
- **Handoff checks in code:** a specialist cannot cite a signal or runbook its own tool results did not return; the synthesis cannot cite what no specialist cited or credit a role with no findings; the verifier must judge exactly the leading citations; `conclude` keeps only accepted citations and runs ADR-029's check again. Any tool outside a role's two, including another role's, is refused.
- **Refactor:** the v2 loop moved to `agents/agent_loop.py`, shared by every role; v2 behaviour, messages, and tests are unchanged. Engine types moved to `agents/nodes.py` and `load_incident` to `agents/briefs.py`, re-exported where tests import them.
- **Choices the task did not state:** `SIGNALOPS_INVESTIGATION_WORKFLOW_VERSION` (`v3` default; `v2` kept for P10 baselines); defaults rise to 40 steps and 3 USD, since three branches will hold reservations at once; turns go to synthesis (2) and verification (2) before specialists (up to 5 each); with no findings, synthesis and verification make no call (`ended_by: skipped`); `AgentEnding` records how each loop ended.
- **Fake mode:** `gateway/fake_roles.py` answers by prompt version. On `downstream-timeout` the telemetry role names `bank-gateway`, the deployment role reports the unrelated `inventory` deployment at 0.3 confidence, which stays visible as an alternative, and the verifier accepts citations from or naming the service.
- **Left:** budget shares are computed but not enforced until P09-T02; contradictions are the synthesizer's alone until P09-T03; replay scopes have no graph or runbooks, so the dependency role reports missing evidence in replay tests.

### P09-T02 Implement bounded parallel scheduling

- [x] Complete this task.

**Implementation:** Run at most three specialist branches per investigation, enforce a global worker limit, and share a durable total budget. Reserve downstream capacity rather than exhausting the budget before verification.

**Acceptance:** Parallel execution respects per-investigation/global limits; saturated capacity queues work without unrestricted task spawning.

**Notes (2026-11-08):**
- **Decision (ADR-032):** `nodes.Parallel` is a workflow stage of 1–3 nodes (`MAX_PARALLEL_BRANCHES`), each its own fenced step; `v3` runs its specialists as one. Each branch waits on the engine's `branch_slots` before its step opens. The worker runs `SIGNALOPS_WORKER_JOB_CONCURRENCY` (2) runners on one engine with `SIGNALOPS_AGENT_GLOBAL_BRANCHES` (4) slots, so the limit covers the process; extra jobs stay queued in PostgreSQL.
- **Budget:** `ModelGateway.complete` takes a `BudgetLimit`. `held_back` is subtracted inside the conditional `hold_budget` update, so parallel calls still cannot oversubscribe. `step_cap` bounds a step's held and spent money across attempts (`LlmCallRepository.step_usage`, read under the job lock). Specialists hold back the plan's reserve (one synthesis and one verification call) and stay within an equal share; a call that does not fit raises `BudgetShareExhaustedError`, which ends only that branch (`ended_by: budget_share`).
- **Failures:** every branch of a stage runs to its end, then the most decisive failure is raised (cancellation, stop, lost lease, budget, permanent, retryable). A throttled branch is retried alone on the next attempt; completed siblings are reused.
- **Verified:** three branches in flight at once and every branch opened before any finished; two jobs sharing 2 slots peaked at 2 calls in flight and finished all 6 branches; every branch's usage stayed within its share and shares plus reserve within the budget; shares too small for any call stopped all branches with no call made and ended `insufficient_evidence`, not `budget_exceeded`; a step cap counts earlier spend and held unknown reservations; a throttled branch was retried alone.
- **Left:** the global limit is per process (fleet total = processes × slots, plus the provider rate limit); the reserve is an estimate from bounded contracts, not a guarantee.

### P09-T03 Implement synthesis and verification

- [x] Complete this task.

**Implementation:** Merge compatible findings, surface contradictions, inspect citation support, and permit at most one follow-up round within the remaining budget and deadline.

**Acceptance:** Contradictory evidence is visible; a verifier cannot trigger an unbounded investigation loop.

**Notes (2026-11-08):**
- **Decision (ADR-033):** merging and citation support came with P09-T01 (the synthesizer ranks merged hypotheses; the verifier judges each citation and `conclude` keeps only accepted ones). This task adds code-detected contradictions and the follow-up round.
- **Contradictions:** `detect_conflicts` records every pair of roles naming different failing services at confidence ≥ 0.3, stored as `SynthesisOutput.detected_contradictions`. `Diagnosis.contradictions` (additive, default empty; schemas, OpenAPI, and four contract fixtures regenerated) carries the synthesizer's and code's, for diagnosed and abstained results. The verifier sees them, and the investigation page has a Contradictions section; it reads a missing field from an older server as empty.
- **Follow-up:** nodes `follow_up` and `reverify` (10 nodes in `v3`). A round is offered only with at least 2 turns left for it and room in budget and deadline for two calls at their maximum, read from PostgreSQL; code refuses `needs_follow_up` when not offered; `follow_up` rechecks room and holds back one verification call; `reverify` is never offered a round. Verifier prompt `verifier-v2`.
- **Verified:** a synthesizer that drops the disagreement still yields a diagnosis listing `bank-gateway` against the `inventory` deployment; an abstention keeps it; one round runs and the final verdict names `bank-gateway`; a verifier that always asks gets one round, then is refused until it abstains (3 verifier calls in total); no round is offered with 90 s left against a 60 s request timeout; a round granted but out of room by its start does not run and the investigation abstains.
- **Choices the task did not state:** the follow-up step's role is `coordinator`, since code dispatches it; its findings record the specialist that ran. A granted round that cannot run abstains rather than falling back to the first verdict.
- **Left:** code sees service-level disagreement only; the room check covers the first call of each role in the round.

### P09-T04 Verify multi-agent recovery

- [x] Complete this task.

**Implementation:** Inject crashes after one branch completes, during another model call, and before final persistence. Reclaim with a newer lease generation and reuse valid checkpoints.

**Acceptance:** Completed branches are reused; stale writes fail; ambiguous provider calls remain visible in cost accounting.

**Notes (2026-11-08):**
- **Tests only; no code change was needed.** `tests/integration/test_multi_agent_recovery.py` with `tests/integration/multi_agent_victim.py`, a real `v3` worker process with the real gateway, tools, and fake roles that hangs at a chosen point and is sent SIGKILL. `signalops worker` (the real CLI, two runners) then reclaims the job after the 3-second lease.
- **After branches completed, during another model call:** telemetry and dependency completed while the deployment call was reserved and in flight. On resume at generation 2, `load_context`, `plan`, telemetry, and dependency kept their rows unchanged; only deployment (attempt 2) and the later steps ran. The hung call was closed `unknown` with `error_category` `abandoned`, its reservation still held: `reserved_microusd` after completion equals it, and `usage` shows 1 unknown call holding it. The dead lease's `hold` and `complete_job` raise `LeaseLostError`. The progress history shows deployment started at attempts 1 and 2 and every other branch once.
- **Before final persistence:** a worker that stored `conclude` and hung before the terminal write was killed; the resumed attempt reran no step and made no model call, and the investigation ended with the stored diagnosis and one `investigation.ended` event.
- **Stale writer:** in process, the old lease's deployment call was held open, the lease expired, and `worker-b` claimed generation 2. The late answer settled its call (a fact about money); the next fenced write raised `LeaseLostError`, leaving the step at generation 1 for the new lease, which completed it and every later step.
- **Repeated:** the three tests passed 3 times in a row (53 s each run).

### P09-T05 Add prompt-injection and abstention cases

- [x] Complete this task.

**Implementation:** Place adversarial instructions in logs/runbooks and include missing-evidence incidents. Keep all tool authorization in code.

**Acceptance:** Evidence text cannot add tools, widen workspace scope, expose secrets, or trigger remediation.

**Notes (2026-11-08):**
- **Fixture:** `fixtures/adversarial/v1/injections.json` (synthetic, labelled): six cases planted in logs and a runbook, each with the attempts it should provoke and the secrets it carries. Only tests read the labels; P10 evaluation can reuse them.
- **Tests:** `tests/integration/test_prompt_injection.py`. A compromised telemetry model obeys the planted text: `run_shell`, `restart_service`, and the deployment role's `get_deployments` return "There is no such tool."; `workspace_id` and `replay_scope` arguments fail validation; an outside service and a window past the cutoff are `out_of_scope`; a real signal from another run, planted as a citation, is refused as unread. Tool definitions never change on any request. No fixture secret appears in any provider request, step output, diagnosis, tool-call row, or captured log at DEBUG; even the model's echo of the evidence holds only `[REDACTED]`. The incident, the scope's signals, and the outbox are unchanged except for the investigation's completion event. A runbook with instructions arrives as redacted tool data and leaves the honest diagnosis unchanged. A synthesizer's `requires_approval: false` is refused, and every stored action requires approval. An incident with no evidence, and one whose only evidence is logs asserting a cause, abstain.
- **Mutation check:** with redaction disabled, two of these tests fail; with the per-role tool check removed, this file's main test and `test_a_specialist_cannot_call_another_roles_tools` fail. Both mutations were reverted.
- **No code change was needed:** tool authorization, scope, cutoff, validation, and redaction were already in code (ADR-027, ADR-029, ADR-031).
- **Left:** redaction matches sensitive `key=value` pairs and bearer tokens only; a secret in free text of another shape would reach a model.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Phase 9 gate (2026-11-08): passed on the fake provider.** All five tasks are checked. R2 ("durable specialist workflow survives a worker crash") was demonstrated on the rebuilt Compose stack (`SIGNALOPS_MODEL_PROVIDER=fake`, `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3`, defaults otherwise: `v3`, 40 steps, 3 USD, 2 jobs and 4 branch slots per worker).
- **Found and fixed at the gate:** the first live run, detector investigation `26d54db3-db5f-46bb-8e5b-7000a3ce6891` on incident `88b6d26a-a170-4d55-9c97-fa098af82bf3`, ended `failed` after three attempts. The dependency role cited runbooks as `key@version`, which the `Key` pattern rejected. Replay tests had no graph or runbooks, so none caught it. Fixed in `53db2c6` with a `RunbookRef` type, plus a test that projects the graph and seeds runbooks into a replay scope; that test fails without the fix.
- **Crash and resume:** operator investigation `aeba2631-becf-414e-89be-015ab7c96de5` on the same incident, started through the API. `docker compose kill investigation-worker` hit when deployment and dependency had completed and a telemetry call was in flight (generation 1). After `docker compose start`, the worker reclaimed the job when the lease expired: job `succeeded`, 2 attempts, generation 2, `last_error` `lease_expired`. `load_context`, `plan`, deployment, and dependency stayed at attempt 1, generation 1 with their original `completed_at`; telemetry and every later step ran at generation 2. The in-flight call is `unknown`/`abandoned` with its 311140 microUSD still reserved, and `GET /v1/investigations/{id}` shows `unknown_calls` 1, `unknown_reserved_microusd` 311140, and 12 model calls, 167292 microUSD `estimated`. The diagnosis names `bank-gateway` with 10 citations and one contradiction (telemetry against the deployment role's `inventory` lead).
- **Browser journey on `v3`:** `pnpm e2e` passed in 2.7 minutes on incident `69232f49-70c4-4745-8a21-cfe0b32a0c00`: investigations `9c90b031-f186-4b9a-9a05-d35977a26681` (automatic) and `374896e7-0a29-40e2-8e9a-2d6cf3915449` (UI, double click started one) both `completed` on `v3` with 10 steps, naming `bank-gateway` with one contradiction. `a8db9fa2-adf8-4f86-b753-6ac32714d176` was cancelled from the UI.
- **Checks:** `SIGNALOPS_INTEGRATION=1 uv run pytest` 1260 passed; `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (181 files), `uv run signalops export-schemas --check`, `docker compose config --quiet` pass; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 69 passed.
- No paid call was made. This shows the multi-agent workflow, bounds, recovery, and UI, not diagnostic quality: the fake roles follow fixed rules. Open items carried forward: `AnthropicProvider` has never run against the live API, so no prompt has met a real model; the branch limit is per worker process; redaction is pattern-based; CI does not run the browser journey.

**Suggested phase commit:** `feat: orchestrate specialist agents with verification`. Split this into task-sized commits as needed.

## Phase 10 Evaluation infrastructure

**Dependencies:** Phase 9.

**Outcome:** A reproducible held-out benchmark compares deterministic, single-agent, and multi-agent behavior.

### P10-T01 Create evaluation persistence and access roles

- [x] Complete this task.

**Implementation:** Add evaluation_scenarios, evaluation_runs, and evaluation_cases. Store ground truth separately and deny investigation runtime access. Record dataset hash, workflow/prompt versions, model settings, seed, and pricing configuration.

**Acceptance:** The evaluator can score outcomes; agent tools and database credentials cannot read expected outcomes or injection labels.

**Notes (2026-11-08):**
- **Decision (ADR-034):** schema `evaluation` (migration `0017_evaluation`) with `scenarios`, `runs`, `replays`, and `cases`. Group roles `signalops_runtime` (DML on `public`, nothing on `evaluation`) and `signalops_evaluator` (both); default privileges cover later tables; neither may write `alembic_version`. `signalops migrate` now also creates or updates login roles `signalops_app` and `signalops_eval` from `SIGNALOPS_DB_RUNTIME_PASSWORD` and `SIGNALOPS_DB_EVALUATOR_PASSWORD` (`persistence/roles.py`). Compose runs every application process as `signalops_app`; `migrate` stays the owner.
- **Deviation from the plan's logical schema:** a run covers a split and several variants, rather than one scenario and one variant. `replays` gives each scenario one replay scope per run, so every variant and trial investigates identical evidence. Cases keep the expected outcome without its evidence labels.
- **Code:** `contracts/evaluation.py` (families, splits, variants, `ExpectedOutcome`, `ObservedOutcome`, `CaseScores`), `persistence/models/evaluation.py`, `persistence/repositories/evaluation.py` (immutable scenario registration, run claim and lease, replays, cases), `evaluation/scoring.py` (`score_case`, deterministic labels only; P10-T05 extends it). Alembic compares both schemas (`migrations/env.py`).
- **Verified (`tests/integration/test_evaluation_access.py`):** the runtime group has no table privilege of any kind on the four tables, no schema usage, and PUBLIC has none. As a provisioned runtime login, every read of an evaluation table, `SET ROLE signalops_evaluator`, an `alembic_version` update, and `CREATE TABLE` are refused with "permission denied". The same login claims and runs a whole `v3` investigation (tools, budget holds, model calls, ending) to a `bank-gateway` diagnosis. The evaluator login registers a scenario, is refused a changed one, creates a run, replay, and case, and stores a score in which a wrong service counts as a false diagnosis. Mutation: granting the runtime group schema usage and table reads failed two of the tests (reverted).
- **Injection labels:** `fixtures/` is not copied into the backend image, and the adversarial labels are read only by tests. Scenario injection specs live in `evaluation.scenarios`.
- **Choices the task did not state:** in Compose, the role passwords fall back to `POSTGRES_PASSWORD` when unset, so existing local `.env` files keep working; `.env.example` sets separate placeholders. Roles are cluster-wide, so the downgrade revokes grants and keeps the roles.
- **Left:** the API's evaluation routes need `SIGNALOPS_EVALUATION_DATABASE_URL` (P10-T06). The Compose stack has not yet been restarted on the runtime login (this shell has no Docker access); the phase gate does that.

### P10-T02 Build isolated replay orchestration

- [ ] Complete this task.

**Implementation:** Assign a replay scope per run, use stable event IDs within it, simulate time, flush windows, and wait on explicit pipeline completion markers. Include a timeout and reconciliation report.

**Acceptance:** Identical runs do not contaminate each other; completion is based on drained work and final windows rather than arbitrary sleeps.

### P10-T03 Implement scenario families and held-out variants

- [ ] Complete this task.

**Implementation:** Cover downstream timeout, pool exhaustion, deployment regression, memory pressure, and healthy noisy traffic. Add mixed and insufficient-evidence variants. Integrate a small real instrumented demo through an optional profile.

**Acceptance:** Synthetic replay cases are labeled as synthetic; real fault demonstrations expose only signals available to the investigators.

### P10-T04 Implement three comparable baselines

- [ ] Complete this task.

**Implementation:** Create a deterministic diagnosis baseline plus single-agent and multi-agent variants. Freeze evidence/tool access and total token budget per case; run repeated model trials.

**Acceptance:** Comparison results include variant, sample count, variance, and abstentions; a larger agent budget is never hidden as an architectural gain.

### P10-T05 Implement scoring and evidence auditing

- [ ] Complete this task.

**Implementation:** Measure detection precision/recall, service-plus-failure accuracy, evidence support, unsupported diagnoses, abstention, latency, and cost. Define incident matching windows and one-to-one matching to avoid double-counting. Use deterministic labels where possible and human audit samples for semantic support.

**Acceptance:** A known false diagnosis and unsupported citation reduce their respective scores; judge-model scores are not presented as ground truth.

### P10-T06 Build the evaluation results view

- [ ] Complete this task.

**Implementation:** Add API endpoints to start/list/read runs and a UI comparison showing case outcomes, quality, latency, cost, and failure examples.

**Acceptance:** A reviewer can inspect aggregate results and drill into the evidence for a failed case.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: add isolated replay and evaluation harness`. Split this into task-sized commits as needed.

## Phase 11 Operational hardening and scale

**Dependencies:** Phase 10.

**Outcome:** The system has documented recovery behavior, security boundaries, and measured capacity.

### P11-T01 Complete observability

- [ ] Complete this task.

**Implementation:** Expose ingestion throughput, lag, outbox age, data quality, detection latency, queued/running jobs, lease expiry, tool/model duration, tokens, and unknown costs. Add correlation IDs and an optional metrics/dashboard profile.

**Acceptance:** A fault can be followed from ingestion to investigation; slow model calls and growing Kafka lag are distinguishable.

### P11-T02 Verify security and query boundaries

- [ ] Complete this task.

**Implementation:** Add authenticated operator access before public exposure, source credential rotation, CORS configuration, payload limits, secret references, and workspace/replay checks. Bind stateful infrastructure privately and audit redaction paths.

**Acceptance:** Unauthenticated users cannot query evidence/start runs; source credentials cannot access operator APIs.

### P11-T03 Run crash and outage integration checks

- [ ] Complete this task.

**Implementation:** Exercise PostgreSQL outage, Kafka outage, Redis restart, consumer rebalance, outbox crash, worker lease expiry, and model throttling. Reconcile accepted logical records after recovery.

**Acceptance:** Every accepted in-retention record is processed or explicitly quarantined; duplicate physical events cause no duplicate logical effects.

### P11-T04 Benchmark and tune measured bottlenecks

- [ ] Complete this task.

**Implementation:** Replay one million synthetic signals and separately target 100 events/second for 30 minutes. Record hardware, payload size, versions, CPU/memory, storage growth, API p95, and lag. Profile indexes and pools before adding infrastructure.

**Acceptance:** Publish achieved results including failures; do not describe a target as achieved without a retained report.

### P11-T05 Implement retention and recovery procedures

- [ ] Complete this task.

**Implementation:** Add dry-run cleanup for raw/normalized data, aggregates, receipts, caches, and progress history. Preserve cited excerpts/hashes or mark expired evidence. Document database backup/restore, fixture recovery, and single-node durability limits.

**Acceptance:** Restoring a backup recovers durable state; cleanup cannot silently invalidate cited evidence or required deduplication history.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `test: validate reliability and measure platform throughput`. Split this into task-sized commits as needed.

## Phase 12 Portfolio packaging and release

**Dependencies:** Phase 11.

**Outcome:** A reviewer can reproduce the demonstration and inspect honest evaluation results.

### P12-T01 Write operational and architecture documentation

- [ ] Complete this task.

**Implementation:** Finish quick start, environment guide, architecture/event/ER diagrams, API examples, schema notes, runbook, and ADRs for Kafka, PostgreSQL graph, Redis, durable jobs, and gateway budgets.

**Acceptance:** A new developer starts the fake-provider demonstration using the guide without undocumented local setup.

### P12-T02 Prepare the demonstration narrative

- [ ] Complete this task.

**Implementation:** Record fault injection, noisy telemetry, incident detection, diagnosis with evidence, worker crash/resume, and baseline comparison. Label synthetic data, hardware limits, and unsupported hypotheses.

**Acceptance:** Every claimed capability is visible in the demo or linked to retained verification evidence.

### P12-T03 Finalize release checks and roadmap state

- [ ] Complete this task.

**Implementation:** Run required CI/integration checks for the release commit, record benchmark artifacts, review credentials/fixtures, tag the release, and update this ledger with completed tasks and deferred items.

**Acceptance:** Release notes link the exact code revision, validation results, known limits, and next priorities.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `docs: publish reproducible SignalOps portfolio release`. Split this into task-sized commits as needed.

## Cross-cutting implementation rules

1. Keep PostgreSQL transactions short. Do not hold a job-claim transaction open during model calls or Kafka publication.
2. Commit Kafka offsets only after durable handling. If processing in parallel is introduced, commit only the contiguous completed offset frontier per partition.
3. Persist evidence cutoff, replay scope, workflow version, and prompt versions for every investigation. All tool and cache paths enforce them.
4. Use receipts plus business uniqueness for application idempotency. Kafka producer idempotence is not an exactly-once guarantee for database or external model effects.
5. Persist every completed workflow node before advancing. On resume, validate checkpoint schema/version and reuse compatible results. Do not silently resume an old workflow with new incompatible code.
6. Preserve conservative accounting for uncertain model outcomes. Budget reservations include parallel calls; Redis rate limits never replace the durable budget ledger.
7. Treat API authorization, tool permissions, output validation, and database scoping as code responsibilities. Prompts cannot grant permissions.
8. Keep clocks injectable, fixtures deterministic, and evaluator truth unavailable to the runtime. Model seed configuration alone does not guarantee reproducible provider output.
9. Model calls and large replay jobs are explicit actions with configured budgets and resource limits. Everyday CI uses fake-provider fixtures.
10. Record architecture deviations before implementing them. A new dependency should solve an observed problem or a clearly required capability.

## Definition of done for a task

- [ ] Implementation and contracts match the intended behavior.
- [ ] Relevant failure and concurrency cases have meaningful verification.
- [ ] Required lint, type, build, and affected integration checks pass.
- [ ] Logs, errors, metrics, and UI states make the outcome inspectable where relevant.
- [ ] Documentation and configuration examples reflect the final behavior.
- [ ] The task checkbox and progress ledger reference the commit and evidence.

## Deferred backlog

Keep these outside the release critical path unless measurements justify promoting them:

- Metrics as a fourth input adapter; support tickets and additional unstructured sources later.
- Dedicated graph database and external object/telemetry storage.
- Schema registry after contract evolution becomes operationally significant.
- Distributed stream processing after durable detector windows exceed measured capacity.
- Multi-workspace product features beyond initial scope enforcement.
- Cloud deployment, Kubernetes, multi-broker Kafka, and disaster recovery across hosts.
- Additional model providers and a separately deployed gateway.
- Autonomous remediation, which requires a new action-permission and verification design.

## Work log

### 2026-10-29 — P01-T01
- Status: completed
- Commit: `05e81df`
- Implemented: directory layout, root `pyproject.toml` + `uv.lock`, backend package with subpackage stubs and `signalops` CLI, Vite/React/TS app in `apps/web` with `pnpm-lock.yaml`, bootstrap commands in `docs/development.md`.
- Validation commands and results: `uv sync --locked` ok; `uv run signalops --version` → `signalops 0.1.0`; subpackage imports ok; `pnpm install --frozen-lockfile` ok; `pnpm build` ok; `git check-ignore` on `.env`, `.env.local`, `.venv`, `apps/web/node_modules`, `apps/web/dist` all ignored, `.env.example` not ignored.
- Evidence or artifact references: none retained.
- Deviations and reasons: root-level Python project (see task notes).
- Remaining issues: none.
- Next task: P01-T02

### 2026-10-29 — P01-T02
- Status: completed
- Commit: `0bf47b1`
- Implemented: ruff, mypy strict, pytest config and CLI unit tests; ESLint 10, `tsc -b --noEmit`, Vitest + Testing Library with an App smoke test; GitHub Actions workflow with backend and frontend jobs; canonical commands in `docs/development.md` and `CLAUDE.md`.
- Validation commands and results: in a clean copy (`git ls-files -co --exclude-standard`): `uv sync --locked`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (12 files, no issues), `uv run pytest` (2 passed); `pnpm install --frozen-lockfile`, `pnpm lint`, `pnpm typecheck`, `pnpm test` (1 passed), `pnpm build` all pass. Negative check with temporary invalid files: ruff check exit 1, ruff format exit 1, mypy 3 errors, pytest 1 failed, eslint exit 1, tsc 2 errors, vitest 1 failed, build exit 2. Temporary files removed.
- Evidence or artifact references: none retained.
- Deviations and reasons: `pnpm test` instead of `pnpm test --run`; `uv run mypy` (paths configured in pyproject) instead of `uv run mypy signalops`.
- Remaining issues: CI not yet executed on GitHub (no remote). `.claude/settings.json` allowlist not updated for argument-less commands.
- Next task: P01-T03

### 2026-10-29 — P01-T03
- Status: completed
- Commit: `1a84ca7`
- Implemented: `compose.yaml` with PostgreSQL, Redis, single-node KRaft Kafka, health checks, named volumes, a private `signalops` network, loopback-only host ports; shared backend `infra/docker/backend.Dockerfile`; `.dockerignore`; `.env.example`; CI `compose` job; infrastructure section in `docs/development.md`; decisions.md Q2 resolved.
- Validation commands and results: `docker compose config --quiet` ok; `docker compose up -d --wait` → kafka, postgres, redis healthy. From `docker compose run --rm --no-deps` client containers: `kafka-topics.sh --create` on `kafka:9092` ok, console producer (`acks=all`) sent 2 messages, console consumer received both; `psql -h postgres` created and populated a table; `redis-cli -h redis SET/GET` ok. `docker compose up -d --force-recreate --wait` replaced all container IDs; the PostgreSQL row and both Kafka messages were still readable. `docker compose ps` shows host ports bound to 127.0.0.1 only. `docker compose --profile tools build backend` ok; container prints `signalops 0.1.0` and runs as uid 10001. Smoke table, topic, and key removed afterwards.
- Evidence or artifact references: none retained.
- Deviations and reasons: loopback host ports for future host-run integration tests; Compose file at root rather than in `infra/`.
- Remaining issues: CI not yet executed on GitHub (no remote).
- Next task: P01-T04

### 2026-10-29 — P01-T04
- Status: completed
- Commit: `16431b8`
- Implemented: `config.py`, `logs.py`, `dependencies.py` (clients + readiness checks), `contracts/health.py`, `api/app.py`, `api/health.py`, `signalops api` command; unit and opt-in integration tests; React layout, routing, query client, API client, loading/error components, health view and tests; `api` and `frontend` Compose services, web Dockerfile, nginx config; docs for commands, URLs, and environment variables; decisions Q1 and Q3 resolved.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (23 files) pass; `uv run pytest` 11 passed, 2 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 2 passed against Compose (ready; unreachable Redis reported while PostgreSQL and Kafka stay ok). `docker compose up -d --build --wait` → all 5 services healthy. Stopping redis, postgres, kafka in turn: `/health/live` 200 each time, `/health/ready` 503 with `ConnectionError`, `OperationalError`, `timeout` for the stopped one only; 200 after restart. API logs are JSON and do not contain the database password. Frontend `pnpm lint`, `pnpm typecheck`, `pnpm test` (8 passed), `pnpm build` pass. Through nginx: `/status` 200 HTML, `/health/ready` proxied; with the API stopped `/health/ready` returns 502 in about 1 ms and `/status` still 200. `docker compose build api frontend` ok.
- Evidence or artifact references: none retained.
- Deviations and reasons: UI health route is `/status` to keep it out of the proxied `/health/` namespace.
- Remaining issues: no real-browser run of the UI; CI not yet executed on GitHub.
- Next task: P01-T05

### 2026-10-29 — P01-T05
- Status: completed
- Commit: `70ac5d9`
- Implemented: `signalops migrate` and `signalops init-topics` commands; Alembic environment, template, baseline revision, `alembic.ini`; declarative `Base`; topic catalogue with idempotent provisioning and partition mismatch detection; Compose one-shot `migrate` and `init-topics` services gating `api`; shared backend environment anchor; `.env.example` and docs for environment variables, startup order, profiles, and migrations; README quick start.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (28 files) pass; `uv run pytest` 14 passed, 4 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 4 passed (migration to head twice in a throwaway database; topic create, idempotent rerun, partition mismatch error; readiness). `uv run alembic current` → `0001_baseline (head)`, `uv run alembic check` → no new operations. Clean-start check in a separate Compose project from a clean copy with only `.env.example`: `grep` found no API key settings; `up --build --wait` in 27 s; `init-topics` created 6 topics; `kafka-topics --describe` shows 3/3/1/1/1/1 partitions; `retention.ms` 259200000 (raw) and 604800000 (normalized); `alembic_version` = `0001_baseline`; API ready 200, UI `/status` 200, UI-proxied readiness 200; second `up` re-ran both jobs with exit 0 and no topics created. The throwaway project and its volumes were removed; dev volumes untouched. Frontend `pnpm lint`, `pnpm typecheck`, `pnpm test` (8 passed), `pnpm build` pass; `docker compose config --quiet` ok.
- Evidence or artifact references: none retained.
- Deviations and reasons: reserved profiles without services (see task notes); 7-day retention for topics the plan does not specify.
- Remaining issues: first `init-topics` run crashed on a log field named `created` (reserved `LogRecord` attribute) after creating the topics; fixed and covered by a unit test.
- Next task: P02-T01

### 2026-10-29 — P02-T01
- Status: completed
- Commit: `37c0596` (with P02-T02)
- Implemented: `contracts/evidence.py` enumerations; `persistence/models/` package with `Workspace`, `Source`, `Service`, `Signal`, `Deployment`, `SignalAggregate`; Alembic revision `0002_source_evidence` (tables, composite workspace keys, CHECK constraints, plan indexes plus `deployments(service_id, replay_scope, deployed_at DESC)`); `migrations/env.py` imports the models; shared scratch-database fixtures in `tests/integration/conftest.py`; migration test reads the head revision instead of a hardcoded ID; `tests/integration/test_evidence_schema.py`; data-model and development docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (32 files) pass; `uv run pytest` 14 passed, 28 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 28 passed (24 new). The first integration run failed two cases: a truncated 63+-character unique-constraint name and overlapping count checks; both fixed in the model and revision before the passing run. `docker compose run --rm migrate` upgraded the dev database `0001_baseline -> 0002_source_evidence`; `uv run alembic check` → no new upgrade operations; API `/health/ready` 200 afterwards. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (extra scope columns, enumeration values, constraint naming).
- Remaining issues: none for this task.
- Next task: P02-T02

### 2026-10-29 — P02-T02
- Status: completed
- Commit: `37c0596` (with P02-T01)
- Implemented: `contracts/workflow.py` (enumerations, terminal sets, transition maps); `persistence/models/workflow.py` (`Incident`, `IncidentService`, `IncidentSignal`, `Investigation`, `Job`, `AgentStep`, `ToolCall`, `LlmCall`, `ProgressEvent`) and `persistence/models/common.py`; `UNIQUE(workspace_id, replay_scope, id)` on `signals`; Alembic revision `0003_incident_workflow` with status-transition triggers; `tests/integration/test_workflow_schema.py`, shared `schema_support.py`, fixtures moved to `conftest.py`, `tests/__init__.py` and `tests/integration/__init__.py`; data-model and development docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (39 files) pass; `uv run pytest` 14 passed, 100 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 100 passed (72 new, including downgrade to baseline and upgrade through 0003). First runs failed on JSON `null` in nullable JSONB columns (model fix), on a `model` column colliding with a test helper keyword (helpers made positional-only), and on three test cases that tripped a different constraint first (cases corrected). Negative check: removing `failed -> running` from the Python step map made the transition test fail; map restored. `docker compose build migrate` and `docker compose run --rm migrate` upgraded the dev database `0002_source_evidence -> 0003_incident_workflow`; `uv run alembic check` → no new upgrade operations; four `status_transition` triggers present; API `/health/ready` 200. `docker compose config --quiet` ok. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (scope columns, cutoff and cost-basis columns, no budget-ceiling CHECK, initial incident enumerations, triggers for transitions).
- Remaining issues: none for this task.
- Next task: P02-T03

### 2026-10-29 — P02-T03
- Status: completed
- Commit: `45a1b18`
- Implemented: `contracts/graph.py`; `persistence/models/graph.py` (`GraphNode`, `GraphEdge`) and `persistence/models/reliability.py` (`ConsumerReceipt`, `OutboxEvent`); `UNIQUE(workspace_id, replay_scope, id)` on `deployments`; Alembic revision `0004_graph_reliability`; `tests/integration/test_graph_reliability_schema.py`; data-model docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (43 files) pass; `uv run pytest` 14 passed, 147 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 161 passed (47 new; the existing downgrade-to-baseline test now covers 0004). First run failed three cases: two from a test helper creating duplicate service nodes (helper fixed) and one exposing the NULL-passing entity-reference CHECK (schema fixed). Throwaway database: `alembic upgrade head`, `alembic check` (no operations), downgrade to 0003 and upgrade again. Negative index check by hand: without `ix_outbox_events_created_at` the plan is seq scan plus sort. `docker compose run --rm --build migrate` upgraded the dev database `0003_incident_workflow -> 0004_graph_reliability`; `alembic check` against it → no new upgrade operations; API `/health/ready` 200. `docker compose config --quiet` ok. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (scope columns on the graph, typed entity references, extra checks).
- Remaining issues: none for this task.
- Next task: P02-T04

### 2026-10-29 — P02-T04
- Status: completed
- Commit: `7cf9c28`
- Implemented: contract modules `common.py`, `events.py`, `adapters.py`, `incidents.py`, `checkpoints.py`, `tools.py`, `diagnosis.py`, and models added to `evidence.py`; shared length limits moved from persistence to contracts; 20 fixtures in `fixtures/contracts/v1/` (removed `fixtures/.gitkeep`); `tests/contract/test_contract_fixtures.py` and `test_contract_failures.py`; events and diagnosis docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (52 files) pass; `uv run pytest tests/contract` 99 passed; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 260 passed. First fixture run failed six event round-trips because the test re-validated through `EventEnvelope[EventPayload]` instead of `decode_event` (test fixed). Negative check: removing the numeric-timestamp guard and the abstention root-cause check each failed exactly one test; both restored. `uv run alembic current` → `0004_graph_reliability (head)`, `uv run alembic check` → no new upgrade operations. Frontend and Compose unchanged, not re-run.
- Evidence or artifact references: `fixtures/contracts/v1/`.
- Deviations and reasons: see task notes (diagnosis service reference and `missing_evidence`, replay scope derivation, chosen size limits).
- Remaining issues: none for this task.
- Next task: P02-T05

### 2026-10-29 — P02-T05
- Status: completed
- Commit: `d555add`
- Implemented: `contracts/investigations.py` (mapping, `InvestigationView`, `terminal_status_for`), `contracts/compatibility.py`, `contracts/export.py`, `signalops export-schemas [--check] [--allow-breaking]`; mapping enforced in `InvestigationCompletedPayload` and the persistence model; revision `0005_diagnosis_status_mapping`; exported schemas and OpenAPI under `backend/signalops/contracts/schemas/`; generated `apps/web/src/api/generated/investigationStatus.ts` plus `investigationStatus.ts` and its test; investigation fixtures; `tests/contract/test_status_mapping.py`, `test_schema_export.py`; integration mapping test and terminal-state helpers updated; docs (events, api, data-model, decisions Q6, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (57 files) pass; `uv run pytest` 190 passed, 148 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 338 passed. First integration run failed `test_enumerations_reject_unknown_values[Investigation-status]` because the new CHECK rejected unknown statuses before the enumeration check; rule rephrased in model and migration (equality of the two texts checked). `uv run signalops export-schemas --check` exit 0. Negative checks: removing `span_id` from `StructuredLogInput` made `export-schemas` exit 1 ("property removed") and the freshness tests fail; mapping `budget_exceeded` to a diagnosis in Python without a migration failed the database mapping test; both restored. `docker compose run --rm --build migrate` upgraded the dev database `0004_graph_reliability -> 0005_diagnosis_status_mapping`; `alembic check` no new operations; API `/health/ready` 200. Frontend `pnpm lint`, `pnpm typecheck`, `pnpm test` (13 passed), `pnpm build` pass. `docker compose config --quiet` ok.
- Evidence or artifact references: `backend/signalops/contracts/schemas/`, `fixtures/contracts/v1/investigations/`.
- Deviations and reasons: see task notes (early `InvestigationView`, tightened completed payload, health-only OpenAPI).
- Remaining issues: none for this task.
- Next task: P02-T06

### 2026-10-30 — P02-T06
- Status: completed
- Commit: `40284c3`
- Implemented: `persistence/database.py` (`create_engine`, `Database.transaction`); `SIGNALOPS_DB_*` pool and timeout settings; API lifespan uses the shared engine factory; `persistence/repositories/catalog.py`; `persistence/seed.py` and `signalops seed`; revision `0006_baseline_indexes` with matching model indexes; `tests/integration/test_repositories.py`, `tests/unit/test_seed.py`, CLI test; docs (development, data-model).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (63 files) pass; `uv run pytest` 194 passed, 158 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 352 passed. Negative checks: removing the investigations index from model and revision failed `test_every_foreign_key_has_a_supporting_index`; a `transaction()` that commits in `finally` failed the application-error and conflict rollback tests (the integrity-error case still passed because PostgreSQL aborts the transaction itself); both restored. `docker compose run --rm --build migrate` upgraded the dev database `0005_diagnosis_status_mapping -> 0006_baseline_indexes`; `uv run alembic check` no new operations; `uv run signalops seed` twice → 3 sources and 4 services in the demo workspace; API container recreated, `/health/ready` 200. `docker compose config --quiet` ok. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (catalog-only seed, no IntegrityError translation yet).
- Remaining issues: none for this task.
- Next task: P03-T01

### 2026-10-30 — P03-T01
- Status: completed
- Commit: `f438f0f`
- Implemented: topic routing and `message_key` in `streaming/topics.py`; `streaming/producer.py` (`EventProducer`, `OutgoingMessage`, `DeliveryResult`, error categories); `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS`; `tests/unit/test_topics.py` (pinned fixture keys, key stability), `tests/unit/test_producer.py` (fake producer: ordering, categories, fatal), `tests/integration/test_producer_delivery.py` (real broker, scratch topics); docs (events key table and producer settings, development configuration).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (66 files) pass; `uv run pytest` 209 passed, 163 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 372 passed. Negative checks: treating every delivery callback as success failed the unknown-topic and unreachable-broker integration tests and two producer unit tests; keying raw signals by `event_id` failed the pinned-key and key-stability tests; both restored and re-run green. `docker compose config --quiet` ok. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (no headers; key separator and omission rule chosen here).
- Remaining issues: none for this task.
- Next task: P03-T02

### 2026-10-30 — P03-T02
- Status: completed
- Commit: pending (validated against the working tree)
- Implemented: `persistence/repositories/reliability.py` (`ReceiptRepository`, `OutboxRepository`); `streaming/consumer.py` (`consumer_config`, `create_consumer`, `TransactionalProcessor`, `ConsumerRunner`, `RecordProcessingError`); `tests/unit/test_consumer.py` (fake consumer: commit ordering, failure without commit, decode failure, commit failure, record errors, close); `tests/integration/test_consumer_transactions.py` (real broker and scratch database: crash between commits, duplicate message, failed record not skipped); docs/events.md and docs/reliability.md consumer sections.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (70 files) pass; `uv run pytest` 222 passed, 166 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 388 passed; `docker compose config --quiet` ok. Negative checks: ignoring the receipt result failed the crash and duplicate integration tests; committing the offset before processing failed two integration and four unit tests; both restored and re-run green. Frontend unchanged, not re-run.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (failures stop the runner until P03-T04; one record at a time per consumer).
- Remaining issues: none for this task.
- Next task: P03-T03

### 2026-10-30 — P03-T03
- Status: completed
- Commit: pending (validated against the working tree)
- Implemented: `OutboxRepository.pending`, `mark_published`, `record_failed_attempts`, and `clock_timestamp()` for `created_at` in `add`; `streaming/outbox.py` (`OutboxPublisher`, `publish_rounds`, `outgoing_message`, `PublishSummary`); settings `SIGNALOPS_OUTBOX_BATCH_SIZE` and `SIGNALOPS_OUTBOX_IDLE_INTERVAL_MS`; `signalops outbox-publisher` and its Compose service; `tests/unit/test_outbox.py` (round ordering, stored message fields); `tests/integration/test_outbox_publisher.py` (crash after ack, failed delivery, aggregate ordering); the pending-query plan test now uses the repository's order; docs (reliability, events, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (73 files) pass; `uv run pytest` 226 passed, 169 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 395 passed; `docker compose config --quiet` ok; `signalops outbox-publisher` on the host against Compose exited 0 on SIGTERM. Negative checks: marking every row published regardless of delivery failed the failed-delivery and ordering integration tests; ignoring the failed-aggregate block failed the ordering test; both restored and re-run green. Frontend unchanged, not re-run. The Compose `outbox-publisher` container was not built or started.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (no single-publisher enforcement).
- Remaining issues: rejected rows block their aggregate until P03-T04.
- Next task: P03-T04

### 2026-10-30 — P03-T04
- Status: completed
- Commit: `aac21fd`
- Implemented: `streaming/retry.py` (`RetryPolicy`); `streaming/failures.py` (failure envelope, deterministic failure event ID, redrive header); `ConsumerRunner` quarantine and pause/seek/resume retries; `OutgoingMessage.headers`; outbox `schedule_retry`, `quarantine`, `redrive` and the blocking `pending` query; migration `0007_outbox_retry` and model columns; `streaming/redrive.py` and the `redrive-record` and `redrive-outbox` commands; retry settings; tests `tests/unit/test_consumer.py` (rewritten), `test_retry.py`, `test_failures.py`, CLI cases, `tests/integration/test_failure_handling.py`, new outbox cases; docs (events, reliability, data model, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (79 files) pass; `uv run pytest` 255 passed, 177 skipped; `uv run signalops export-schemas --check` ok; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 432 passed; `docker compose config --quiet` ok. On the host against Compose: `signalops migrate` applied 0007 to the dev database; `redrive-outbox` with an unknown ID exited 1; `redrive-record` for an out-of-range offset on an empty topic timed out with an error. Negative checks: committing the original when the failure event was not acknowledged failed 2 unit and 2 integration tests; removing the earlier-version blocker from `pending` failed 2 outbox integration tests; removing the partition pause failed 3 unit tests; all restored and re-run green. Frontend unchanged, not re-run. Containers were not rebuilt.
- Evidence or artifact references: none retained.
- Deviations and reasons: see task notes (exhausted transient failures stop rather than quarantine; nil workspace for unreadable originals; failure events published directly, not through the outbox).
- Remaining issues: retry state survives a partition revoke until P03-T05; no failure metrics yet.
- Next task: P03-T05

### 2026-10-30 — P03-T05
- Status: completed
- Commit: `7d6490f`
- Implemented: `ConsumerStats`, `ConsumerRunner.report_stats`, lag, `subscribe` with rebalance callbacks (retry state dropped and pause lifted on revoke/lost), stop-aware `run` with a final report; `create_consumer` no longer subscribes; `OutboxRepository.backlog` and `OutboxBacklog`; `OutboxPublisher.report_stats`, `PublishSummary.__add__`, stop between rounds; setting `SIGNALOPS_STREAMING_STATS_INTERVAL_MS`; Compose `stop_grace_period` for `outbox-publisher`; tests: new unit cases in `tests/unit/test_consumer.py` and `test_outbox.py`, `tests/integration/test_streaming_lifecycle.py`, two outbox integration cases, call sites updated for `subscribe`; docs (reliability, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (80 files) pass; `uv run pytest` 265 passed, 182 skipped; `uv run signalops export-schemas --check` ok; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 447 passed; `docker compose config --quiet` ok. On the host against Compose, `.venv/bin/signalops outbox-publisher` with a 1 s stats interval logged `outbox stats` each second and, on SIGTERM, a final line and `outbox publisher stopped`, exit 0. Negative checks: removing the resume on revoke failed 2 unit tests and the integration revoke test (the reassigned partition was never fetched); keeping retry state on revoke failed the same 3; removing the stop check between outbox rounds failed the stop integration test; all restored and re-run green. Frontend unchanged, not re-run. Containers were not rebuilt.
- Evidence or artifact references: none retained.
- Deviations and reasons: logs instead of a metrics endpoint (acceptance allows either; metrics are P11-T01); see task notes.
- Remaining issues: no consumer entry command yet (P04-T02); failed offset commits are not retried on revoke.
- Next task: P04-T01

### 2026-10-30 — P04-T01
- Status: completed
- Commit: `05d5df5`
- Implemented: `backend/signalops/demo/generator.py` (`Scenario`, `DemoRecord`, `ScenarioLabels`, `generate`, `serialize_records`); `signalops demo-generate`; `orders-db` service in the demo seed; `fixtures/scenarios/v1/healthy.labels.json` and `downstream-timeout.labels.json`; `tests/unit/test_demo_generator.py`; docs (development, evaluation). Ledger now records `7d6490f` for P03-T05.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (83 files) pass; `uv run pytest` 283 passed, 182 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 465 passed. `uv run signalops seed` twice against the dev database: services now include `orders-db`. Negative checks: an unseeded `random.Random()` failed 5 tests (fixture hashes, reproducibility, CLI); writing the scenario/seed into log attributes failed the no-label tests and fixture hashes; a fault-only attribute on payment logs failed the shared-shape test and the downstream-timeout hash; all restored and re-run green. Frontend and Compose unchanged, not re-run.
- Evidence or artifact references: `fixtures/scenarios/v1/`.
- Deviations and reasons: no publishing or Compose `demo` service until the ingestion endpoint exists (P04-T02); records are not committed, only labels and hashes (see task notes).
- Remaining issues: none for this task.
- Next task: P04-T02

### 2026-10-30 — P04-T02
- Status: completed
- Commit: `fd47da0`
- Implemented: `POST /v1/ingest/{source_id}` (`api/ingest.py`, `adapters/ingestion.py`, `contracts/ingestion.py`); `CatalogRepository.get_source`; ingestion settings (`SIGNALOPS_INGEST_TOKENS`, record/byte limits, timestamp bounds, redaction list); API resources gain a source lookup and an ingest producer; `signalops demo-post` (`demo/poster.py`); Compose `SIGNALOPS_INGEST_TOKENS` passthrough and `demo` profile services; `.env.example` placeholder; exported schemas and OpenAPI; ADR-011; docs (api, development, events, reliability, decisions).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (90 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 329 passed, 185 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 514 passed; `docker compose config --quiet` passes. Live stack (image rebuilt, API restarted with temporary tokens set only in the shell, then restored): demo profile posted 1028/1028 records; broker-outage check as in the ledger. Negative checks: reporting every delivery as accepted failed 7 tests (including the integration outage test); a random `event_id` failed 9; skipping redaction failed the unit and integration redaction tests; all restored and re-run green. Frontend unchanged (it does not consume the OpenAPI file), not re-run.
- Evidence or artifact references: `tests/unit/test_ingestion.py`, `tests/unit/test_demo_poster.py`, `tests/integration/test_ingestion_api.py`.
- Deviations and reasons: no 429 rate limit (not in the task's implementation list; no owning task yet). Integration tests redirect messages to a scratch topic so `signals.raw.v1` stays clean for workers; the live-stack checks used the real topic.
- Remaining issues: rate limiting unowned; message-text redaction not done; token changes need an API restart. The live checks left 1030 demo/outage records on the dev `signals.raw.v1` (3-day retention); the P04-T03 normalizer will consume them.
- Next task: P04-T03

### 2026-10-30 — P04-T03
- Status: completed
- Commit: `5252a13`
- Implemented: `adapters/sources.py`, `adapters/normalizer.py`, `persistence/repositories/evidence.py`, `CatalogRepository.find_service_id`; `signalops normalizer`; Compose `normalizer` service; `tests/unit/test_sources.py`, `tests/integration/test_normalizer.py`; ADR-012; docs (events, data-model, development, decisions). Ledger now records `fd47da0` for P04-T02.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (95 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 353 passed, 196 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 549 passed; `docker compose config --quiet` passes. Integration tests feed records through `ingest_batch` into the normalizer against real PostgreSQL: a 10-minute downstream-timeout dataset (all three kinds) is queryable by service + time window and by trace ID with deployment rows; missing/unknown/contradictory identity rows have NULL service and the matching flag; a redelivery and a second consumer group store one signal, one deployment, one event; an invalid record leaves no receipt or row; through a real Kafka topic, three records plus a duplicate commit offset 4 and identical messages with distinct IDs stay separate. Live stack as in the ledger. Negative checks: resolving despite a contradiction failed 2 integration tests; dropping the business-key `ON CONFLICT` failed the cross-group test; both restored and re-run green. Frontend unchanged, not re-run.
- Evidence or artifact references: `tests/unit/test_sources.py`, `tests/integration/test_normalizer.py`.
- Deviations and reasons: malformed records still go to `processing.failed.v1` until P04-T04 (see task notes).
- Remaining issues: none for this task beyond those assigned to P04-T04/T05. The Compose `normalizer` and `outbox-publisher` were left running against the dev stack.
- Next task: P04-T04

### 2026-10-30 — P04-T04
- Status: completed
- Commit: `c23b1b4`
- Implemented: `adapters/quality.py`; `Normalizer(timing)` with malformed-as-evidence and combined flags (`identity_flags` replaces `classify_identity`); ingestion fallback IDs and `missing_source_event_id`; `QualityFlag.DERIVED_EVENT_ID`; `RawSignalPayload.source_event_id_derived`; optional `IngestRecord.source_event_id`; normalizer timing settings; CLI wiring; compatibility-checker fix; regenerated schemas and OpenAPI; `signal_raw` fixture; tests `tests/unit/test_quality.py`, new cases in `tests/unit/test_ingestion.py`, `tests/contract/test_schema_export.py`, `tests/integration/test_normalizer.py`; ADR-013; docs (api, events, decisions, development, data-model). Ledger now records `5252a13` for P04-T03.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (97 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 381 passed, 202 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 583 passed; `docker compose config --quiet` passes. Acceptance in integration tests against real PostgreSQL: two ID-less log lines with identical text 1 ms apart and an ID-less trace become three signals; the whole batch retried later reproduces the same event IDs and is three duplicates; the earlier P04-T03 Kafka test still shows identical text with distinct IDs as separate rows and a redelivery collapsing. Malformed records get a receipt, one quarantined signal, one event, no deployment, and collapse on redelivery; an envelope fault still raises with no receipt or row; skewed/late/on-time records resolve identity and get `clock_skew`/`late_arrival`/no flags. Live stack as in the ledger. Negative checks: re-raising on malformed records failed 2 tests; hashing only the log message failed 2; disabling timing flags failed 4; all restored and re-run green (78 passed). Frontend unchanged (it does not consume the OpenAPI file), not re-run.
- Evidence or artifact references: `tests/unit/test_quality.py`, `tests/unit/test_ingestion.py`, `tests/integration/test_normalizer.py`.
- Deviations and reasons: generator quality fixtures left to P10-T01 (see task notes).
- Remaining issues: replay timing; unused `truncated_message`. The live check left 6 `live-p04t04-*`/derived records in the dev database; the Compose `normalizer` and `outbox-publisher` run the rebuilt image.
- Next task: P04-T05

### 2026-10-30 — P04-T05
- Status: completed
- Commit: `7baea33`
- Implemented: `api/evidence.py` (three routes, window/limit checks, `SignalCursor`, `OutputRedaction`); `api/auth.py` (`OperatorTokens`, `operator_workspace`); `contracts/queries.py` and exported schemas; `persistence/repositories/scope.py`; `EvidenceRepository` read methods; `CatalogRepository.list_sources`; `Resources.database`; settings `SIGNALOPS_OPERATOR_TOKENS` and `SIGNALOPS_QUERY_*` with validators; migration `0008_evidence_query_indexes` and model indexes; Compose passthrough and `.env.example` placeholder; ADR-014; docs (api, data-model, development, decisions); tests `tests/unit/test_evidence_api.py`, `tests/integration/test_evidence_api.py`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (103 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 434 passed, 223 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 657 passed (evidence integration tests also pass with `-W error::Warning`); `docker compose config --quiet` passes; `alembic check` on the migrated dev database reports no new operations. Acceptance in integration tests against real PostgreSQL, with signals in two workspaces and two replay scopes: each token sees only its workspace, each `replay_scope` only its rows, filters by another workspace's source return nothing, the shared trace ID resolves per scope and is 404 where absent; five signals paged two at a time arrive in order with the ID tiebreaker and no repeats, also with the default window taken from the cursor; cursors replayed with another filter, scope, window, or workspace are 400; configured limits and windows are enforced (422) and the default limit applies; a quarantined record's `password` (process list) and `customer_email` (source list added after storage) are `[REDACTED]` in responses; quality counts match per source, scope, and window, including zero-count sources. Unit tests: 401 for missing, wrong, Basic, and source tokens on all three routes; an operator token cannot ingest; 422 for every out-of-bounds parameter before database access; malformed cursors 400; settings refuse shared tokens and defaults above maximums. Live stack as in the ledger. Frontend unchanged (it does not consume the OpenAPI file), not re-run.
- Evidence or artifact references: `tests/unit/test_evidence_api.py`, `tests/integration/test_evidence_api.py`, `migrations/versions/0008_evidence_query_indexes.py`.
- Deviations and reasons: interim operator tokens added ahead of P11-T02 because scope enforcement needs a principal (ADR-014); the plan's service and source signal indexes replaced (task notes).
- Remaining issues: see task notes. The Compose `api` runs the rebuilt image with no operator tokens configured.
- Next task: P05-T01

### 2026-10-30 — P05-T01
- Status: completed
- Commit: pending (validated against the working tree)
- Implemented: `detection/rules.py` (`DetectionRule`, `DEFAULT_RULES`, `window_for`, `series_name`, `duration_bucket`, `histogram_quantile`, `evaluate`); `detection/detector.py` (`Detector`, `DetectionPolicy`, `sample_for`, `DETECTOR_CONSUMER_GROUP`); `persistence/models/detection.py` (`DetectionSample`, `DetectionTrigger`); `persistence/repositories/detection.py` (`DetectionRepository`); `EvidenceRepository.get_signal`; migration `0009_detection_state`; settings `SIGNALOPS_DETECTION_WINDOW_SECONDS` and `SIGNALOPS_DETECTION_RULES`; `signalops detector` command and the shared `_run_consumer`; Compose `detector` service; ADR-015; docs (events, data-model, reliability, development, decisions); tests `tests/unit/test_detection_rules.py`, `tests/integration/test_detector.py`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (109 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 452 passed, 232 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 684 passed; `docker compose config --quiet` passes; `alembic check` on the migrated dev database reports no new operations; 0009 downgrade/upgrade round trip on a scratch database. Acceptance in integration tests against real PostgreSQL: 20 signals with 4 errors processed, then redelivered with the same event IDs (all `duplicate`) and with new event IDs (applied, no effect), leave one window of 20 samples and 4 errors and one trigger; 14 failing slow traces and 9 error logs in one window (below `min_samples`) give no trigger; a rule fires once per window, is suppressed for windows 60 s and 300 s later, and fires again at 360 s; p95 fires from the histogram (18 fast, 2 slow of 20); windows stay separate per replay scope and service; deployments, degraded, unresolved, and quarantined signals leave no samples; a signal ID from another scope or unknown is a `ContractError` with no receipt; 60 concurrent transactions (30 signals, each twice) count exactly 30 samples, 10 errors, and the exact histogram; the generated demo fault, run through ingestion, the normalizer, and the detector, fires only for payment, checkout, and bank-gateway, inside the fault, once per rule and service. Unit tests: window alignment, buckets, quantiles, exact threshold behaviour, low-sample evaluation, rule validation, settings JSON override and duplicate IDs. Live stack as in the ledger. Frontend unchanged, not re-run.
- Evidence or artifact references: `tests/unit/test_detection_rules.py`, `tests/integration/test_detector.py`, `migrations/versions/0009_detection_state.py`.
- Deviations and reasons: `detection_samples` and `detection_triggers` tables beyond the plan; aggregates per metric series rather than per rule (task notes, ADR-015).
- Remaining issues: see task notes. Generator calibration shows rare latency triggers in healthy runs; P05-T02/T03 decide how triggers become incidents.
- Next task: P05-T02

### 2026-11-06 — P05-T02
- Status: completed
- Commit: `5886d2e`
- Implemented: closure-time rule evaluation (`detection/detector.py`); `WindowCloser` and `flush_replay` (`detection/closure.py`); `detector-closer` and `detector-flush` commands and Compose `detector-closer`; repository closure and clock operations; migration `0010_window_closure`; settings `SIGNALOPS_DETECTION_ALLOWED_LATENESS_SECONDS` and `SIGNALOPS_DETECTION_CLOSER_INTERVAL_MS`; `demo-post --realtime`; ADR-016; docs (architecture, data-model, decisions, development, events, reliability).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (110 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 458 passed, 248 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 706 passed, including the downgrade/upgrade schema test; race, concurrency, and shuffle tests repeated 5 times, 6 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations. Dev migration closed 80 earlier windows (1031 samples) as `backfill` and left the 7 existing triggers unchanged. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_detector.py`, `tests/unit/test_detection_rules.py`, `tests/unit/test_demo_poster.py`, `tests/unit/test_cli.py`. Compose run on 2026-11-06, seed 781637803: 7 triggers, `checkout` and `payment` error rates at 16:31, all three latency rules at 16:32, closed by wall clock with 0 late samples. Closer outage: 6 windows open while stopped, 0 after restart.
- Deviations and reasons: rules moved from per-sample to closure evaluation (ADR-016). Acceptance was verified on triggers, since incidents arrive in P05-T03. The offline calibration used a scratch script over the generator and is not a repository check.
- Remaining issues: see task notes. The Compose `demo` service still backfills, so it needs `--realtime` or replay ingestion before it can show an incident.
- Next task: P05-T03.

### 2026-11-06 — P05-T03
- Status: completed
- Commit: `4077875`
- Implemented: incident correlation, recovery, and events (`detection/incidents.py`, `persistence/repositories/incidents.py`); breach and recovery calls in `detection/detector.py`; migration `0011_incident_correlation`; settings `SIGNALOPS_INCIDENT_CORRELATION_GAP_SECONDS` and `SIGNALOPS_INCIDENT_RECOVERY_SECONDS`; `DetectionRule.severity`; `replay_run_id_of`; ADR-017; docs (architecture, data-model, decisions, development, events, reliability). Test fixtures `seeded`, `database`, and `run_id` moved to `tests/integration/conftest.py`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (113 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 463 passed, 267 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 730 passed, including the downgrade/upgrade schema test; the concurrency test repeated 5 times, 3 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations. Three deliberate mutations (no cooldown extension, no all-services recovery check, no scope lock) each failed the tests. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_incidents.py`, `tests/unit/test_detection_rules.py`. Compose run on 2026-11-06, seeds 781637804 (fault, 300 s) and 781637805 (healthy, 420 s), both `--realtime`: incident `5a5b37da-6ac4-49f6-a8d5-cd75426975d5`, first seen 17:01, last seen 17:03, detected 17:02:34, resolved 17:08, version 6.
- Deviations and reasons: the acceptance's "correlation window" is implemented as a session, because time buckets split a fault crossing a boundary (ADR-017). Automatic investigation requests stay out of scope, as the task states.
- Remaining issues: see task notes, chiefly the `bank-gateway` sample-count miss.
- Next task: P05-T04.

### 2026-11-06 — P05-T04
- Status: completed
- Commit: `651ece8`
- Implemented: `detection/graph.py` (`GraphProjector`, `GraphPolicy`); `persistence/repositories/graph.py` (node upserts, interval merge, deployment intervals, bounded as-of reads); `signalops graph-projector` and Compose `graph-projector`; migration `0012_graph_projection`; setting `SIGNALOPS_GRAPH_DEPENDENCY_TTL_SECONDS`; ADR-018; docs (architecture, data-model, decisions, development, events, reliability). Test helper `_store` gains `downstream`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (117 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 474 passed, 301 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 775 passed, including the downgrade/upgrade schema test; concurrency and node-ordering tests repeated 5 times, 2 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations. Eight deliberate mutations each failed the tests: no cycle guard, no hop limit, no as-of filter, no path bound, no relationship lock, no merge across touching edges, no end on a deployment, and both quality guards removed. The path-bound and quality mutations survived at first: the first exposed a real test gap, now closed; the second was one guard covering for the other. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_graph.py`, `tests/unit/test_graph_projection.py`, `tests/integration/test_graph_reliability_schema.py`. Compose on 2026-11-06: 1619 receipts, lag 0, 0 warnings or errors; `checkout` to `inventory`, `orders-db`, and `payment`, and `payment` to `bank-gateway`, 331 observations each, matching `signals` for the retained range; `dependencies` as of 17:02 returned the four edges with `bank-gateway` at 2 hops, and as of 16:00 none.
- Deviations and reasons: the plan's open-ended `valid_to` applies only to deployments; observed dependencies always end, so stale ones expire (ADR-018). `btree_gist` is a new extension, needed for the exclusion constraint.
- Remaining issues: see task notes, chiefly the rebuild limit of Kafka retention.
- Next task: P05-T05.

### 2026-11-06 — P05-T05
- Status: completed
- Commit: `8a27f66`
- Implemented: `api/incidents.py` (list, detail, status change, evidence timeline, service graph); `change_status` in `detection/incidents.py`; incident reads in `persistence/repositories/incidents.py`; `CatalogRepository.get_services`; contracts `IncidentTrigger`, `IncidentStatusUpdate`, `IncidentEvidencePage`, `ServiceGraph`, and schemas; PATCH in CORS; ADR-019; docs (api, decisions, development, events). Web: `/incidents` and `/incidents/:incidentId`, `OperatorGate`, `DependencyGraph`, `StaleNotice`, `api/incidents.ts`, `api/operatorToken.ts`, and no retry on 4xx.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (120 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 488 passed, 327 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 815 passed; the concurrency test repeated 5 times, 1 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 35 passed. Twelve deliberate mutations each failed the tests: no version check, no row lock, no event, detail and list without scope, PATCH on any scope, no contract ceiling, unbound cursor, keyset repeating a row, unredacted evidence, ignored `as_of`, and services from any workspace.
- Evidence or artifact references: `tests/integration/test_incident_api.py`, `tests/unit/test_incident_api.py`, `apps/web/src/pages/Incident*.test.tsx`. Host API on 8011 against Compose infrastructure with a throwaway operator token, Vite in front, Chrome: signed in, opened incident `5a5b37da-6ac4-49f6-a8d5-cd75426975d5` from the list by a client-side navigation, saw checkout and payment, 4 rule firings, evidence including the `bank-gateway` timeout log, and `payment` to `bank-gateway` at 2 hops as of 17:03; no console errors. As-of 16:00 gave no edges. A stale `PATCH` gave 409 and left version 6.
- Deviations and reasons: `GET /v1/incidents/{id}/evidence` and `PATCH` are new routes; the plan's detail route holds rule firings and references, with the timeline paged separately, because 200 full signals do not belong in every detail poll.
- Remaining issues: see task notes.
- Next task: P06-T01.

### 2026-11-06 — P06-T01
- Status: completed
- Commit: `1fa2d42`
- Implemented: `agents/investigations.py` (`request_by_operator`, `request_by_detector`, `Dispatcher`, `InvestigationPolicy`); `api/investigations.py`; `persistence/repositories/investigations.py`; `IdempotencyKey` model and migration `0013_idempotency_keys`; contracts `InvestigationRequest`, `InvestigationAccepted`, and schemas; `signalops dispatcher` and Compose `dispatcher`; four `SIGNALOPS_INVESTIGATION_*` settings; ADR-020; docs (api, data-model, decisions, development, events).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (125 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 499 passed, 349 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 848 passed, including the downgrade/upgrade schema test; the two concurrency tests repeated 5 times, 2 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations. Fourteen deliberate mutations each failed the tests: no hash check, hash without the incident, request key without the workspace, no repeat lookup, no read after a lost race, no limit check, no incident check, no outbox row for either trigger, a job for a finished investigation, dispatcher ignoring scope, non-idempotent job insert, replay requesting investigations, and the disabled policy ignored. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_investigation_requests.py`, `tests/unit/test_investigation_requests.py`. Rebuilt Compose image, migrated to `0013_idempotency_keys`, host API on 8012 with a throwaway operator token: two identical `POST`s on incident `5a5b37da-6ac4-49f6-a8d5-cd75426975d5` returned 202 with investigation `f8fb3276-6d3e-4369-bcdb-f87a46077018`; the same key with another body returned 409. The outbox row was published, `dispatcher-v1` recorded its receipt, and one queued job exists with 0 attempts. That queued investigation stays in the dev database; no worker exists to run it.
- Deviations and reasons: the plan requests an investigation when a rule crosses its threshold; here it is once per new live incident, because later crossings extend the same incident. Replay incidents request nothing (ADR-020).
- Remaining issues: see task notes.
- Next task: P06-T02.

### 2026-11-07 — P06-T02
- Status: completed
- Commit: `e4a3a36`.
- Implemented: `persistence/repositories/jobs.py` (`JobRepository`, `JobLease`, `LeaseLostError`); `agents/jobs.py` (`claim_job`, `complete_job`, `retry_job`, `fail_job`, `release_job`, `JobRunner`, `JobPolicy`); `fail_investigation` in `agents/investigations.py`; `SIGNALOPS_JOB_LEASE_SECONDS` and `SIGNALOPS_JOB_POLL_INTERVAL_MS`; ADR-021; docs (data-model, decisions, development, events, reliability). No migration: the columns and indexes date from `0003_incident_workflow`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (129 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 506 passed, 394 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 900 passed; `tests/integration/test_job_leases.py` repeated 5 times, 45 passed each; `docker compose config --quiet` passes; `alembic check` reports no new operations. Of 29 deliberate mutations, 28 fail the tests, three of them only after adding the same-owner and wrong-owner cases. The survivor, a lost lease attempting to settle, is rejected by the fence with the same outcome. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_job_leases.py`, `tests/unit/test_jobs.py`. The tests ran against Compose PostgreSQL with up to eight concurrent claims on separate connections.
- Deviations and reasons: none from the task. No worker command yet; see task notes.
- Remaining issues: see task notes.
- Next task: P06-T03.

### 2026-11-07 — P06-T03
- Status: completed
- Commit: `8887386`.
- Implemented: `agents/workflow.py` (`Node`, `Workflow`, `WORKFLOWS`, `NodeContext`, `WorkflowEngine`, `reused_output`); `contracts/steps.py`; `steps_of`, `complete_step`, `fail_step`, and failed-step reopening in `JobRepository`; `signalops worker`; Compose `investigation-worker`; ADR-022; docs (agents-and-workflow, decisions, development, reliability). No migration.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (136 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 514 passed, 412 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 926 passed; `docker compose config --quiet` passes; `tests/integration/test_workflow_recovery.py` repeated 5 times, all passed. Of 16 deliberate mutations, all fail the tests, two only after adding repository-level tests for completed-output protection and the ending lease's generation. `alembic check` was not rerun beyond `test_migrations_match_models`: no model changed. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_workflow_recovery.py`, `tests/integration/test_workflow_engine.py`, `tests/unit/test_workflow.py`. `investigation-worker` built and ran in Compose and completed the three steps of the one queued dev job, whose investigation stays `running` as noted under the task.
- Deviations and reasons: `investigate` is a plain function rather than a fake provider, because the gateway is P07-T01.
- Remaining issues: see task notes.
- Next task: P06-T04.

### 2026-11-07 — P06-T04
- Status: completed
- Commit: `5d89874`.
- Implemented: migration `0014_investigation_cancellation`; `cancel_investigation` and `end_investigation` in `agents/investigations.py`; `JobStop`, `stop_reason`, `locked_for`, `abandon_steps` in `JobRepository`; `JobStoppedError`, `stop_job`, and stop handling in `agents/jobs.py`; the pre-step check in `agents/workflow.py`; `GET /v1/investigations/{id}` and `POST /v1/investigations/{id}/cancel`; `InvestigationView.cancel_requested_at` with regenerated schemas and fixtures; `apps/web/src/pages/InvestigationPage.tsx`, `api/investigations.ts`, `postJson`; ADR-023; docs (agents-and-workflow, api, data-model, decisions, development, events, reliability).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (137 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 514 passed, 464 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 978 passed; `tests/integration/test_cancellation.py` with `test_workflow_recovery.py` repeated 5 times, 48 passed each; `docker compose config --quiet` passes; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 42 passed. Of 17 deliberate mutations, all fail the tests. `test_migrations_match_models` and `test_downgrade_to_baseline_and_upgrade_again` pass with `0014`.
- Evidence or artifact references: `tests/integration/test_cancellation.py`, `tests/integration/test_workflow_recovery.py`, `tests/integration/test_investigation_requests.py`, `apps/web/src/pages/InvestigationPage.test.tsx`. Compose run, rebuilt and migrated to `0014`, with `investigation-worker` stopped: a request through the API was dispatched, the cancel answered `202 cancelled`, a repeat `202`, the job ended `cancelled` with 0 attempts, `investigations.completed.v1` was published at aggregate version 3, and the restarted worker ran no step. Headless Chrome against the built frontend on port 8090, with `tests/integration/hanging_worker.py` on the host (6-second lease) hung in `investigate`: the page showed Running, then the requested notice, then Cancelled 3.3 s later without a reload. The job ended `cancelled` with 1 attempt, `load_context` stayed `completed`, `investigate` became `failed`, `conclude` had no row, and the worker process stayed alive. A throwaway operator token was set on the `api` container for these runs and removed after them. They left two cancelled investigations in the dev database.
- Deviations and reasons: see task notes (read route and minimal page added ahead of P08-T02, because the acceptance needs them).
- Remaining issues: see task notes.
- Next task: P06-T05.

### 2026-11-07 — P06-T05
- Status: completed
- Commit: `988f7e0`.
- Implemented: `contracts/progress.py` and `investigations/progress-page.v1.json`; `persistence/repositories/progress.py`; migration `0015_investigation_outcomes`; `Workflow.conclusion`, `JobHandler` returning `Diagnosis`, `complete_job(database, lease, diagnosis)`, `end_investigation(..., diagnosis)`; progress writes in `JobRepository`; `GET /v1/investigations/{id}/progress`; `fixtures/contracts/v1/progress/page.json`; ADR-024; docs (agents-and-workflow, api, data-model, decisions, development, events, reliability, README).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (141 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 530 passed, 491 skipped; `SIGNALOPS_INTEGRATION=1 uv run pytest` 1021 passed; `tests/integration/test_investigation_outcomes.py`, `test_cancellation.py`, and `test_workflow_recovery.py` repeated 5 times, 73 passed each; `docker compose config --quiet` passes. Of 20 deliberate mutations, all fail the tests, the migration's job-status check only after the fixture gained a `running` job with a completed `conclude`. `test_migrations_match_models` and `test_downgrade_to_baseline_and_upgrade_again` pass with `0015`. Frontend checks not run: no frontend change.
- Evidence or artifact references: `tests/integration/test_investigation_outcomes.py`, `tests/integration/test_workflow_recovery.py`, `tests/integration/test_investigation_requests.py`, `tests/contract/test_progress_contract.py`. Compose run, rebuilt and migrated to `0015`: the stranded dev investigation `f8fb3276` became `insufficient_evidence`, version 3, with `investigations.completed.v1` published. With `investigation-worker` stopped, a request through the API (investigation `02cd6bfd-24aa-42fa-90fc-49b350d918ab`) was dispatched; `tests/integration/hanging_worker.py` on the host (6-second lease) took it, hung in `investigate`, and received SIGKILL. The Compose worker resumed after the lease expired. With the `api` and worker containers then restarted, `GET /v1/investigations/{id}` returned `insufficient_evidence` with its diagnosis, and `/progress` returned eight events: `load_context` attempt 1, `investigate` attempt 1 then 2, `conclude` attempt 2, and the ending. The job ended `succeeded`, 2 attempts, generation 2, `last_error` `lease_expired`; `investigations.completed.v1` was published with `diagnosis_status`. A throwaway operator token was set for this run and the configured one restored after it. The run left two more ended investigations in the dev database. No model was called.
- Deviations and reasons: see task notes (a JSON progress read ahead of the SSE route, because the acceptance needs a query).
- Remaining issues: see task notes.
- Next task: P07-T01.

### 2026-11-07 — P07-T01
- Status: completed
- Commit: `7dc0fdb`
- Implemented: `gateway/types.py`, `gateway/errors.py`, `gateway/provider.py`, `gateway/fake.py`, `gateway/anthropic_provider.py`; `fixtures/gateway/v1/*.json`; model settings and `resolved_model`; `anthropic` dependency; Compose key passing; `.env.example`; ADR-025 (resolves Q8); docs (decisions, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (147 files) pass; `uv run pytest` 548 passed, 491 skipped; `docker compose config --quiet` passes.
- Evidence or artifact references: `tests/unit/test_gateway_providers.py` covers fake success, timeout, throttling, and invalid output with no network, and the adapter's request shape, thinking round trip, status categories, and refusal over a stub transport.
- Deviations and reasons: see task notes (no refusal fallback).
- Remaining issues: `AnthropicProvider` has not run against the live API, and `build_provider` returns an empty fake script until P07-T04 adds the investigator's fake.
- Next task: P07-T02.

### 2026-11-07 — P07-T02
- Status: completed
- Commit: `b3c7a42`
- Implemented: `gateway/pricing.py`, `gateway/limits.py`, `gateway/gateway.py`, `BudgetExceededError`; `persistence/repositories/llm_calls.py`; migration `0016_llm_call_accounting`; `exhaust_budget` in `agents/jobs.py`; pending calls closed in `end_investigation`; model limit settings; ADR-026; docs (agents-and-workflow, data-model, decisions, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (153 files) pass; `SIGNALOPS_INTEGRATION=1 uv run pytest` passes after the schema fixture fix (the full run had 1 fixture failure, fixed, then `tests/integration/test_workflow_schema.py` passed). `test_migrations_match_models` and `test_downgrade_to_baseline_and_upgrade_again` pass with `0016`.
- Evidence or artifact references: `tests/integration/test_model_gateway.py` on real PostgreSQL and Redis: 12 parallel calls on a 5-call budget make exactly 5 provider calls; 8 concurrent holds stop at the budget with no job lock; an ambiguous timeout keeps its reservation while the retry reserves separately; a retry that no longer fits ends with the reservation held; a cancellation or stale lease prevents the call. `tests/unit/test_gateway_pricing.py`.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P07-T03.

### 2026-11-07 — P07-T03
- Status: completed
- Commit: `242d7bb`
- Implemented: `agents/tools.py`; `persistence/repositories/tool_calls.py`; `SignalFilter` severity, trace, and `known_by` filters; `EvidenceRepository.has_service` and `list_deployments`; `GraphRepository.ensure_runbook` and `runbooks`, `RunbookVersion`; demo runbooks in the seed; `TraceSummaryEntry.downstream_services` with regenerated schema; ADR-027; docs (agents-and-workflow, decisions, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` pass; `tests/integration/test_agent_tools.py` 29 passed on real PostgreSQL. The commit below went in with one contract test failing, `test_fixture_round_trips[tool_results/get_trace_summary.json]`, because the fixture lacked the new field. A follow-up commit fixed the fixture; after it, the graph, evidence API, repository, contract, and unit tests pass.
- Evidence or artifact references: `tests/integration/test_agent_tools.py` covers future and late signals, out-of-scope services and replay runs, nine widening inputs including SQL and a shell command, row and byte caps, the timeout, masking, as-of graph reads, runbook versions, and fencing.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P07-T04.

### 2026-11-07 — P07-T04
- Status: completed
- Commit: `16e12da`
- Implemented: `agents/investigator.py`; `gateway/fake_investigator.py`; responders in `FakeProvider`; `ProposedDiagnosis`, `ServiceBrief`, and the brief's new fields in `contracts/steps.py`; `load_incident`, `investigate_with_agent`, `conclude_findings`, and workflow `v2`; `NodeContext.at`; `build_gateway`; worker wiring; `CatalogRepository.list_services`; `IncidentRepository.evidence_known_by`; agent settings; ADR-028; docs (agents-and-workflow, decisions, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (159 files), `uv run signalops export-schemas --check` pass; `SIGNALOPS_INTEGRATION=1 uv run pytest` 1112 passed.
- Evidence or artifact references: `tests/integration/test_investigator.py`. A generated `downstream-timeout` dataset passed through the real normalizer and detector into a replay scope; the workflow on the fake investigator names `bank-gateway` with cited signals that exist in scope, 3 settled calls, and zero reserved. An incident with no evidence and a healthy dataset both abstain. Repair, its bound, cut-off replies, refusals, step and tool limits, a cancellation between turns, budget exhaustion between turns, and a node retry reusing `load_context` are covered. No model was called.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P07-T05.

### 2026-11-07 — P07-T05
- Status: completed
- Commit: `1b35914`
- Implemented: `agents/findings.py`, `agents/read_cache.py`; `EvidenceRepository.missing_or_future` and `data_version`; the cache in `ToolExecutor`; verification in `investigate_with_agent` and `conclude_findings`; `SIGNALOPS_TOOL_CACHE_SECONDS`; ADR-029; docs (agents-and-workflow, decisions, development).
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (161 files), `uv run signalops export-schemas --check` pass; `tests/integration/test_investigator.py` and `tests/integration/test_agent_tools.py` 65 passed on real PostgreSQL and Redis; `uv run pytest` passes.
- Evidence or artifact references: absent, other-run, after-cutoff, and late-arriving citations are refused, sent back, and end in abstention; empty citations and out-of-range confidence fail validation; a forged stored proposal abstains at `conclude`. Planted cache values for another run, with a future entry, for another tool, and as garbage do not reach the agent, while a valid entry for the exact read does.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P07-T06.

### 2026-11-07 — P07-T06
- Status: completed
- Commit: `7f77717`
- Implemented: exception type only in the failed-attempt log; `PROVIDER_CLIENT_LOGGERS` held at `WARNING`; `tests/integration/test_gateway_failures.py`, `tests/unit/test_gateway_policy.py`; `docs/reliability.md` model call section, ADR-021 note.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (163 files), `uv run signalops export-schemas --check` pass; `SIGNALOPS_INTEGRATION=1 uv run pytest` 1151 passed; `tests/integration/test_gateway_failures.py` with `test_model_gateway.py` repeated 3 times, 32 passed each.
- Evidence or artifact references: with a sensitive log line stored and every logger at DEBUG, timeouts, a rejection, and a success leave no evidence, secret, or prompt in logs or in the stored diagnosis, job error, steps, progress, and call rows. `AnthropicProvider` over a stub transport returning a 500 that quotes evidence leaves no key or payload. Permanent and retryable categories, capped backoff, a capped `retry-after`, unknown-cost accounting, resume after a model timeout, a cancellation during a hung call, and a stale worker's late answer are covered.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: Phase 7 gate.

### 2026-11-07 — Phase 7 gate
- Status: completed on the fake provider
- Commit: `7671838`
- Implemented: `InvestigationUsage` gained model calls, tokens, spend by basis, and unknown calls, as an additive contract change, because `docs/api.md` promised labelled cost; `LlmCallRepository.usage_of`; regenerated schemas and fixtures; two test gaps closed; status docs.
- Validation commands and results: `SIGNALOPS_INTEGRATION=1 uv run pytest` 1153 passed, after fixing one exact `usage` comparison; `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (163 files), `uv run signalops export-schemas --check`, `docker compose config --quiet` pass; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 42 passed. Of 28 deliberate mutations, 26 failed the tests at first. The 2 survivors, an unfenced first tool write and an unchecked cached tool and cutoff, exposed test gaps, which were closed, and both then failed the tests, as did each half of the cache check.
- Evidence or artifact references: see the Phase 7 gate note.
- Deviations and reasons: none.
- Remaining issues: see the gate note. The run left two more investigations and one demo dataset in the dev database.
- Next task: P08-T01.

### 2026-11-08 — P08-T01
- Status: completed (component level; one integration assertion not yet run)
- Commit: `4e5eaef`
- Implemented: `IncidentListItem` with `services`; `IncidentRepository.services_of_many`; primary-first service order; list Services column, service filter chip, detail Data quality section, inline quality flags; tests in `IncidentsPage.test.tsx` and `IncidentDetailPage.test.tsx`; `docs/api.md`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (163 files), `uv run signalops export-schemas` (page and OpenAPI regenerated), `uv run pytest` 560 passed and 593 skipped (integration, no Docker); `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 48 passed, including a list-to-evidence-and-graph navigation test against a stubbed API.
- Evidence or artifact references: none beyond the tests.
- Deviations and reasons: the integration test for list services was written but not run, because Docker is unavailable.
- Remaining issues: run the incident API integration tests on Compose.
- Next task: P08-T02.

### 2026-11-08 — P08-T02
- Status: completed (component level; three integration tests not yet run)
- Commit: `7b3a938`
- Implemented: `IncidentInvestigations`, `InvestigationSummary`, `InvestigationEvidence`; `InvestigationRepository.for_incident`, `EvidenceRepository.known_signals`; the two routes; `api/investigations.ts`, `api/progress.ts`, `api/requestKey.ts`, `api/checks.ts`, `hooks/useProgress.ts`, `InvestigationStatusBadge`; the incident Investigations section and start control; the rewritten investigation page; `docs/api.md`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (163 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 562 passed and 596 skipped (integration, no Docker); `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 59 passed, including repeated and double clicks sending one request, a lost response retried with the same key, a later start with a new key, insufficient evidence as an outcome, unverified citations, and actual versus estimated versus unknown cost.
- Evidence or artifact references: none beyond the tests.
- Deviations and reasons: two read routes not in the plan's endpoint table, because the UI cannot otherwise find an incident's investigations or show cited evidence.
- Remaining issues: run the new integration tests on Compose.
- Next task: P08-T03.

### 2026-11-08 — P08-T03
- Status: completed (unit and component level; integration tests not yet run)
- Commit: `e4318f1`
- Implemented: `api/progress_stream.py`, the `/events` route, `wakeups.py`, `Database.on_progress` and `mark_progress`, `ProgressRepository.exists`, three `SIGNALOPS_SSE_*` settings; `api/progressStream.ts`, `useProgressHistory`, `useProgressStream`, stream state notices; ADR-030; `docs/api.md`, `docs/development.md`, `docs/reliability.md`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (167 files), `uv run signalops export-schemas --check` pass; `uv run pytest` 586 passed and 603 skipped (integration, no Docker); `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 67 passed.
- Evidence or artifact references: none beyond the tests.
- Deviations and reasons: the browser uses `fetch` instead of `EventSource`, which cannot send the operator token.
- Remaining issues: run `tests/integration/test_progress_stream.py` on Compose; no real-browser check yet (P08-T04).
- Next task: P08-T04.

### 2026-11-08 — P08-T04
- Status: completed
- Commit: `300342a` (journey and fake delay), `49ffcb0` (run recorded)
- Implemented: `apps/web/e2e/journey.e2e.ts`, `playwright.config.ts`, `tsconfig.e2e.json`, `pnpm e2e`, `@playwright/test` dev dependency; `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS` in settings, `build_provider`, Compose, and `.env.example`; "Browser journey check" in `docs/development.md`.
- Validation commands and results: `docker compose config --quiet` passes; `docker compose up -d --build --wait` brought every service up healthy; `pnpm e2e` passed on the first run (1 test, 2.9 min); `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 603 passed; `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy` (167 files) pass; `pnpm lint`, `pnpm typecheck`, and `pnpm build` pass; `pnpm test` 67 passed.
- Evidence or artifact references: Playwright report in `apps/web/e2e-report/` (ignored by git); IDs in the Phase 8 gate note.
- Deviations and reasons: none in the repository. Locally, Docker ran through `sg docker -c` because the shell predated the user's group membership, and the installed Chrome replaced Playwright's Chromium (`E2E_BROWSER_CHANNEL=chrome`).
- Remaining issues: CI does not run the journey.
- Next task: Phase 8 gate.

### 2026-11-08 — Phase 8 gate
- Status: passed on the fake provider
- Commit: this gate commit (see git log for `docs(P08)`)
- Implemented: no code; roadmap evidence, and `CLAUDE.md` now names the planning files in `docs/` (resolves Q11).
- Validation commands and results: `SIGNALOPS_INTEGRATION=1 uv run pytest` 1190 passed; the browser journey on Compose (see the P08-T04 entry).
- Evidence or artifact references: incident `7cb83270-5566-42dd-bcda-7c8a7f2ba1ac`; investigations `97795cd8-6a63-43a9-bc17-4a5089f8e2ab` (automatic), `a868bb2d-d010-4522-806a-9604cc632f24` (UI), and `da6754bc-6333-4646-81c2-3bae42b78b06` (cancelled).
- Deviations and reasons: none.
- Remaining issues: see the gate note's open items.
- Next task: P09-T01.

### 2026-11-08 — P09-T01
- Status: completed
- Commit: `e91588f`
- Implemented: `contracts/roles.py`; `agents/agent_loop.py`, `agents/multi_agent.py`, `agents/nodes.py`, `agents/briefs.py`; `gateway/fake_roles.py`; `SIGNALOPS_INVESTIGATION_WORKFLOW_VERSION`; new budget and step defaults; four workflow schemas; `tests/integration/test_multi_agent.py`, `tests/integration/multi_agent_support.py`, `tests/unit/test_roles.py`; ADR-031; `docs/agents-and-workflow.md`, `docs/development.md`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (176 files), `uv run signalops export-schemas` (4 new schemas) pass; `uv run pytest` 606 passed; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 612 passed on Compose.
- Evidence or artifact references: the tests check each role's offered tools and brief fields from the requests the fake received, typed findings with provenance and uncertainty, and refusals of an unread citation, another role's tool, an uncited hypothesis, and an incomplete verification.
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P09-T02.

### 2026-11-08 — P09-T02
- Status: completed
- Commit: `11d34ba`
- Implemented: `nodes.Parallel` and the engine's parallel stage with `branch_slots`; `BudgetLimit`, `BudgetShareExhaustedError`, `LlmCallRepository.step_usage`, `hold_budget(held_back=)`; enforced branch shares and reserve in `v3`; worker runners and slots; two settings; `tests/integration/test_parallel_branches.py`, unit tests for stages, failure precedence, and settings; ADR-032; docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (177 files) pass; `uv run pytest` 614 passed; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 620 passed on Compose.
- Evidence or artifact references: see task notes, "Verified".
- Deviations and reasons: the global limit is per worker process, not fleet-wide (ADR-032).
- Remaining issues: see task notes.
- Next task: P09-T03.

### 2026-11-08 — P09-T03
- Status: completed
- Commit: `14485ad`
- Implemented: `Diagnosis.contradictions` and `Contradiction`; `FollowUpRequest`, `FollowUpOutput`, `Verdict.NEEDS_FOLLOW_UP`, `detected_contradictions`; `detect_conflicts`, `room_left`, `follow_up_fits`, the `follow_up` and `reverify` nodes, `verifier-v2`; fake verifier rounds; regenerated schemas and fixtures; the UI Contradictions section; `tests/integration/test_synthesis_verification.py`, unit and component tests; ADR-033; `docs/api.md`, `docs/agents-and-workflow.md`.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (178 files), `uv run signalops export-schemas` pass; `uv run pytest` (unit and contract) 624 passed; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration` 627 passed on Compose; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass; `pnpm test` 69 passed.
- Evidence or artifact references: see task notes, "Verified".
- Deviations and reasons: none.
- Remaining issues: see task notes.
- Next task: P09-T04.

### 2026-11-08 — P09-T04
- Status: completed
- Commit: `443e844`
- Implemented: `tests/integration/test_multi_agent_recovery.py`, `tests/integration/multi_agent_victim.py`; `docs/reliability.md` recovery section, failure table, and checklist.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (180 files) pass; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration/test_multi_agent_recovery.py` 3 passed, repeated 3 times.
- Evidence or artifact references: see task notes.
- Deviations and reasons: none.
- Remaining issues: none specific; the recovery covers fake roles, not a live provider.
- Next task: P09-T05.

### 2026-11-08 — P09-T05
- Status: completed
- Commit: `21ba99f`
- Implemented: `fixtures/adversarial/v1/injections.json`; `tests/integration/test_prompt_injection.py`; `docs/agents-and-workflow.md` injection section.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (181 files) pass; `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration/test_prompt_injection.py` 5 passed; with redaction disabled 2 failed, and with the role tool check removed 2 failed (reverted).
- Evidence or artifact references: see task notes.
- Deviations and reasons: none.
- Remaining issues: pattern-based redaction (see task notes).
- Next task: Phase 9 gate.

### 2026-11-08 — Phase 9 gate
- Status: passed on the fake provider
- Commit: `53db2c6` (fix), then this gate commit (see git log for `docs(P09)`)
- Implemented: the `RunbookRef` fix and its graph-backed test; roadmap evidence; `docs/agents-and-workflow.md` status.
- Validation commands and results: `SIGNALOPS_INTEGRATION=1 uv run pytest` 1260 passed; ruff, format, mypy, schema check, compose config pass; `pnpm lint`, `pnpm typecheck`, `pnpm build` pass, `pnpm test` 69 passed; `pnpm e2e` passed (2.7 min) on Compose.
- Evidence or artifact references: incidents `88b6d26a-a170-4d55-9c97-fa098af82bf3` and `69232f49-70c4-4745-8a21-cfe0b32a0c00`; investigations `26d54db3-db5f-46bb-8e5b-7000a3ce6891` (failed, the bug), `aeba2631-becf-414e-89be-015ab7c96de5` (crash and resume), `9c90b031-f186-4b9a-9a05-d35977a26681`, `374896e7-0a29-40e2-8e9a-2d6cf3915449`, `a8db9fa2-adf8-4f86-b753-6ac32714d176` (journey).
- Deviations and reasons: none.
- Remaining issues: see the gate note's open items. The runs left investigations and demo data in the dev database.
- Next task: P10-T01.

### 2026-11-08 — P10-T01
- Status: completed
- Commit: see git log for `feat(P10-T01)`
- Implemented: migration `0017_evaluation` (schema, tables, group roles, grants); `persistence/roles.py` and login-role provisioning in `signalops migrate`; evaluation contracts, models, repository, `score_case`; Compose runtime login and `.env.example` placeholders; settings `SIGNALOPS_EVALUATION_DATABASE_URL`, `SIGNALOPS_DB_RUNTIME_*`, `SIGNALOPS_DB_EVALUATOR_*`; ADR-034; data-model and development docs.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (187 files) pass; `uv run pytest` 624 passed (unit and contract); `SIGNALOPS_INTEGRATION=1 uv run pytest tests/integration/test_evaluation_access.py tests/integration/test_repositories.py tests/integration/test_evidence_schema.py tests/integration/test_one_shot_commands.py` passed (includes the downgrade round trip and the model/migration comparison over both schemas); `docker compose --env-file .env.example config --quiet` ok.
- Evidence or artifact references: see task notes.
- Deviations and reasons: run, replay, and case tables instead of the plan's single-scenario runs (see task notes).
- Remaining issues: the Compose stack still runs as the owner until it is rebuilt and restarted.
- Next task: P10-T02.

## Work log template

Copy this entry for each completed task. Leave task checkboxes unchanged until the acceptance condition passes.

```markdown
### YYYY-MM-DD — PXX-TXX
- Status: in progress | completed | blocked
- Commit:
- Implemented:
- Validation commands and results:
- Evidence or artifact references:
- Deviations and reasons:
- Remaining issues:
- Next task:
```

## Source and interpretation

Source: `SignalOps_Implementation_Plan.md`, version 1, 16 October 2026. This roadmap preserves its architecture and decomposes its seven broad milestones into twelve delivery phases. API idempotency storage, window closure metadata, and provider pricing provenance are explicit implementation refinements; add migrations or fields when their phases require them. The data model in the source remains a logical design until migrations and integration checks validate it.
