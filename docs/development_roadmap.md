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
| Active phase | Phase 4 Ingestion and data quality; Phase 3 gate passed |
| Active task | None |
| Last completed task | P04-T01 |
| Latest verified commit | `7d6490f` (P03-T05); P04-T01 pending commit, validated against the working tree |
| Validation evidence | P04-T01: `generate` with seed 42 reproduces the committed dataset hashes (healthy 931 records `8163ba51…`, downstream-timeout 991 records `92f1ca3f…`); the same seed gives identical bytes and another seed a different hash. All records validate against their adapter contracts and seeded services; serialized signals contain no label words or scenario names; fault records share their shapes with healthy ones. Downstream-timeout has 49 payment timeout logs inside the window and at most 5 outside. `SIGNALOPS_INTEGRATION=1 uv run pytest` 465 passed. Earlier evidence is in the work log |
| Blockers | None recorded |
| Next task | P04-T02 |

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

- [ ] Complete this task.

**Implementation:** Build POST /v1/ingest/{source_id}; validate source credentials, batch size, timestamps, payload bounds, and redaction. Acknowledge each accepted item only after Kafka acknowledgement. Define partial-success and ambiguous-timeout behavior.

**Acceptance:** Broker outage does not produce a false acceptance; client retry uses the same source event ID.

### P04-T03 Implement three source adapters

- [ ] Complete this task.

**Implementation:** Normalize structured logs, trace summaries, and deployments into the common evidence model. Resolve service plus environment; preserve source attributes and provenance.

**Acceptance:** All three fixture formats produce queryable evidence; contradictory identity is flagged rather than guessed.

### P04-T04 Implement normalization quality policies

- [ ] Complete this task.

**Implementation:** Classify accepted, degraded, and quarantined records. Detect malformed payloads, missing IDs, clock skew, late arrival, and unknown services. Define adapter-specific stable identity fallback when needed.

**Acceptance:** Identical message text with distinct source event IDs remains separate evidence; duplicates with stable IDs collapse correctly.

### P04-T05 Expose evidence queries and quality summaries

- [ ] Complete this task.

**Implementation:** Add bounded source/service/time filters, cursor pagination, trace lookup, and source quality counts. Use repository scope helpers for workspace and replay boundaries.

**Acceptance:** Evidence queries cannot read another scope, exceed configured limits, or leak redacted fields.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: ingest and normalize telemetry sources`. Split this into task-sized commits as needed.

## Phase 5 Detection and knowledge graph

**Dependencies:** Phase 4.

**Outcome:** An injected fault produces one correlated incident with temporal dependencies and inspectable evidence.

### P05-T01 Implement durable detection windows

- [ ] Complete this task.

**Implementation:** Persist sample/error counts and latency histograms per service, replay scope, rule, and time window. Deduplicate before aggregate updates. Configure minimum samples, thresholds, cooldowns, and rule versions.

**Acceptance:** Duplicate delivery does not inflate error rate; low-sample windows do not trigger misleading incidents.

### P05-T02 Handle late data and window closure

- [ ] Complete this task.

**Implementation:** Implement configurable 30-second allowed lateness, durable closure/recovery state, and explicit late flags. Use wall-clock closure live and a simulated clock plus final flush during replay.

**Acceptance:** Shuffled events within the lateness bound yield the expected incident; older arrivals follow the documented historical policy.

### P05-T03 Create correlated incidents atomically

- [ ] Complete this task.

**Implementation:** Define fingerprint and correlation-window keys, attach evidence and services, track recovery, and write incident events through the outbox. Initially allow manual investigation requests; add automatic requests after Phase 6.

**Acceptance:** Repeated triggers in one correlation window produce one logical incident; genuine later incidents remain distinguishable.

### P05-T04 Implement the temporal graph projection

- [ ] Complete this task.

**Implementation:** Use an independent consumer group with receipts to project services, deployments, and observed dependencies. Preserve provenance and validity intervals. Add bounded recursive queries with cycle prevention and three-hop limits.

**Acceptance:** Historical queries exclude future deployments/dependencies; graph cycles terminate within bounded query limits.

### P05-T05 Expose incident and graph APIs

- [ ] Complete this task.

**Implementation:** Implement incident list/detail, evidence timeline, graph neighborhood, cursor pagination, and optimistic status updates. Add API-backed incident views to the React shell.

**Acceptance:** A developer can locate the injected incident, inspect its evidence, and see the affected dependency neighborhood.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: detect incidents and project service knowledge graph`. Split this into task-sized commits as needed.

## Phase 6 Durable jobs and workflow runtime

**Dependencies:** Phases 3 and 5.

**Outcome:** An investigation survives a worker restart and rejects stale-worker writes.

### P06-T01 Implement investigation requests and dispatch

- [ ] Complete this task.

**Implementation:** Create a request endpoint with caller/route-scoped Idempotency-Key, canonical request hash, and durable response mapping. Atomically create investigation plus outbox request; dispatcher creates one job before acknowledging. Enable detector-triggered requests using stable request keys.

**Acceptance:** A repeated request returns the same investigation; key reuse with another body returns conflict; duplicate Kafka requests create one job.

### P06-T02 Implement job claiming and recovery

- [ ] Complete this task.

**Implementation:** Claim queued or expired jobs with short SELECT FOR UPDATE SKIP LOCKED transactions. Increment lease_generation, heartbeat, and reclaim expired work. Bound retries and set available_at for delayed attempts.

**Acceptance:** Two workers cannot own the same current lease; expired workers cannot checkpoint or complete after another worker takes over.

### P06-T03 Implement the persisted workflow engine

- [ ] Complete this task.

**Implementation:** Define typed workflow nodes, versioned state transitions, deterministic step keys, checkpoints, and validated output reuse. Run a fake-provider sequence before any real model calls.

**Acceptance:** Killing the worker after a completed node resumes at the next unfinished node and retains completed output.

### P06-T04 Implement cancellation and deadlines

- [ ] Complete this task.

**Implementation:** Persist cancellation requests and terminal transitions with version checks. Check cancellation/deadline before calls and between nodes; fence stale completion.

**Acceptance:** Cancellation during a call does not permit new downstream calls; the UI eventually shows the durable terminal state.

### P06-T05 Persist workflow progress and completion

- [ ] Complete this task.

**Implementation:** Write progress_events with step commits and terminal completion with its outbox event. Store structured findings and operational metadata rather than unrestricted private reasoning.

**Acceptance:** Progress remains queryable after restart; replaying job execution cannot produce contradictory terminal states.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: implement durable investigation execution`. Split this into task-sized commits as needed.

## Phase 7 LLM gateway and first investigator

**Dependencies:** Phase 6.

**Outcome:** A single bounded agent returns a validated diagnosis with verifiable evidence and accounted cost.

### P07-T01 Create a provider abstraction and fake provider

- [ ] Complete this task.

**Implementation:** Define structured request/response types, provider errors, usage metadata, prompt versions, and deterministic fixtures. Implement one real provider adapter behind the same interface.

**Acceptance:** Fake mode reproduces success, timeout, throttling, and invalid-output behavior without network access.

### P07-T02 Enforce budgets and provider limits

- [ ] Complete this task.

**Implementation:** Reserve conservative per-call maximum cost atomically in PostgreSQL before dispatch; include all in-flight calls. Use Redis for atomic rate/token limits. Persist usage, pricing version, provider request IDs, and actual/estimated/unknown cost classification.

**Acceptance:** Parallel calls cannot oversubscribe budget; an ambiguous timeout retains a conservative reservation and any retry reserves separately.

### P07-T03 Implement constrained evidence tools

- [ ] Complete this task.

**Implementation:** Build query_logs, get_trace_summary, get_deployments, get_service_dependencies, and retrieve_runbooks. Store versioned runbook content with provenance in graph node properties initially. Enforce evidence cutoff, workspace/replay scope, row/byte caps, and timeouts.

**Acceptance:** Tools reject out-of-scope and future evidence; requests cannot turn tool access into unrestricted SQL or shell execution.

### P07-T04 Implement the first investigation workflow

- [ ] Complete this task.

**Implementation:** Load incident context, execute bounded tool calls, synthesize hypotheses, and return the typed diagnosis or insufficient_evidence. Add maximum steps, output-token limits, and bounded structured-output repair.

**Acceptance:** The timeout fixture identifies the affected service with existing evidence IDs; underdetermined input yields abstention rather than an invented cause.

### P07-T05 Validate findings and cache safe reads

- [ ] Complete this task.

**Implementation:** Verify cited evidence IDs, scope, cutoff, confidence bounds, and output shape. Cache read-only context in Redis with keys including replay scope, cutoff, and data version.

**Acceptance:** Stale or cross-run cache content cannot alter evidence scope; missing citations fail validation.

### P07-T06 Verify gateway failure behavior

- [ ] Complete this task.

**Implementation:** Check permanent versus retryable errors, capped backoff, conservative unknown-cost accounting, and resume after model timeout. Log sanitized operational metadata.

**Acceptance:** No provider credentials or raw sensitive payloads appear in UI/logs; tests explicitly acknowledge external calls may have executed before timeout.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: add budgeted model gateway and evidence tools`. Split this into task-sized commits as needed.

## Phase 8 Incident workspace and live frontend

**Dependencies:** Phases 5 and 7.

**Outcome:** The first portfolio slice is usable from fault injection to diagnosis in the browser.

### P08-T01 Complete incident browsing

- [ ] Complete this task.

**Implementation:** Add service/severity/status filters, cursor navigation, timeline, quality flags, affected services, and bounded graph display. Include meaningful loading, empty, and error states.

**Acceptance:** A user can navigate from the incident list to its evidence and dependency graph without manual API calls.

### P08-T02 Build investigation controls and results

- [ ] Complete this task.

**Implementation:** Add start/cancel controls, workflow progress, hypotheses, verified evidence links, uncertainty, limitations, and token/cost display. Distinguish actual cost from estimates/unknown usage.

**Acceptance:** Repeated button clicks do not create duplicate investigations; insufficient evidence is presented as a valid outcome.

### P08-T03 Implement durable SSE and recovery

- [ ] Complete this task.

**Implementation:** Serve progress_events through SSE using Last-Event-ID. Use Redis pub/sub only for wake-ups, plus periodic database catch-up. Define behavior for an expired progress cursor.

**Acceptance:** Reconnect replays missed events in order; missing a Redis notification does not leave a connected UI permanently stale.

### P08-T04 Verify the full user journey

- [ ] Complete this task.

**Implementation:** Add one browser integration check against a deterministic backend/fake provider: ingest fault, find incident, start investigation, inspect evidence, reconnect, and cancel another run.

**Acceptance:** The R1 journey runs from a clean checkout with documented commands and no paid API dependency.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: build incident investigation workspace`. Split this into task-sized commits as needed.

## Phase 9 Multi-agent investigations

**Dependencies:** Phases 7 and 8.

**Outcome:** Specialist collaboration is bounded, persisted, and demonstrably recoverable.

### P09-T01 Define role contracts and handoffs

- [ ] Complete this task.

**Implementation:** Implement coordinator, telemetry investigator, deployment investigator, dependency/runbook investigator, synthesizer, and verifier as logical workflow roles. Version their prompts and structured outputs.

**Acceptance:** Each role receives only its required context and returns typed findings with provenance and explicit uncertainty.

### P09-T02 Implement bounded parallel scheduling

- [ ] Complete this task.

**Implementation:** Run at most three specialist branches per investigation, enforce a global worker limit, and share a durable total budget. Reserve downstream capacity rather than exhausting the budget before verification.

**Acceptance:** Parallel execution respects per-investigation/global limits; saturated capacity queues work without unrestricted task spawning.

### P09-T03 Implement synthesis and verification

- [ ] Complete this task.

**Implementation:** Merge compatible findings, surface contradictions, inspect citation support, and permit at most one follow-up round within the remaining budget and deadline.

**Acceptance:** Contradictory evidence is visible; a verifier cannot trigger an unbounded investigation loop.

### P09-T04 Verify multi-agent recovery

- [ ] Complete this task.

**Implementation:** Inject crashes after one branch completes, during another model call, and before final persistence. Reclaim with a newer lease generation and reuse valid checkpoints.

**Acceptance:** Completed branches are reused; stale writes fail; ambiguous provider calls remain visible in cost accounting.

### P09-T05 Add prompt-injection and abstention cases

- [ ] Complete this task.

**Implementation:** Place adversarial instructions in logs/runbooks and include missing-evidence incidents. Keep all tool authorization in code.

**Acceptance:** Evidence text cannot add tools, widen workspace scope, expose secrets, or trigger remediation.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: orchestrate specialist agents with verification`. Split this into task-sized commits as needed.

## Phase 10 Evaluation infrastructure

**Dependencies:** Phase 9.

**Outcome:** A reproducible held-out benchmark compares deterministic, single-agent, and multi-agent behavior.

### P10-T01 Create evaluation persistence and access roles

- [ ] Complete this task.

**Implementation:** Add evaluation_scenarios, evaluation_runs, and evaluation_cases. Store ground truth separately and deny investigation runtime access. Record dataset hash, workflow/prompt versions, model settings, seed, and pricing configuration.

**Acceptance:** The evaluator can score outcomes; agent tools and database credentials cannot read expected outcomes or injection labels.

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
- Commit: pending (validated against the working tree)
- Implemented: `backend/signalops/demo/generator.py` (`Scenario`, `DemoRecord`, `ScenarioLabels`, `generate`, `serialize_records`); `signalops demo-generate`; `orders-db` service in the demo seed; `fixtures/scenarios/v1/healthy.labels.json` and `downstream-timeout.labels.json`; `tests/unit/test_demo_generator.py`; docs (development, evaluation). Ledger now records `7d6490f` for P03-T05.
- Validation commands and results: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy` (83 files) pass; `uv run pytest` 283 passed, 182 skipped; with `.env` sourced, `SIGNALOPS_INTEGRATION=1 uv run pytest` 465 passed. `uv run signalops seed` twice against the dev database: services now include `orders-db`. Negative checks: an unseeded `random.Random()` failed 5 tests (fixture hashes, reproducibility, CLI); writing the scenario/seed into log attributes failed the no-label tests and fixture hashes; a fault-only attribute on payment logs failed the shared-shape test and the downstream-timeout hash; all restored and re-run green. Frontend and Compose unchanged, not re-run.
- Evidence or artifact references: `fixtures/scenarios/v1/`.
- Deviations and reasons: no publishing or Compose `demo` service until the ingestion endpoint exists (P04-T02); records are not committed, only labels and hashes (see task notes).
- Remaining issues: none for this task.
- Next task: P04-T02

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
