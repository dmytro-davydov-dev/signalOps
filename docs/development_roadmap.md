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
| Active phase | Phase 1 Project foundation |
| Active task | P01-T02 (next) |
| Last completed task | P01-T01 |
| Latest verified commit | None |
| Validation evidence | P01-T01: `uv sync --locked`, `uv run signalops --version`, subpackage imports, `pnpm install --frozen-lockfile`, `pnpm build` pass; `git check-ignore` confirms env files, `.venv`, `node_modules`, `dist` excluded |
| Blockers | None recorded |
| Next task | P01-T02 |

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

- [ ] Complete this task.

**Implementation:** Configure Python formatting/linting and type checks, TypeScript checking, frontend linting, pytest, and a frontend test runner. Add CI for these checks and the frontend production build.

**Acceptance:** A clean checkout passes CI; intentionally invalid code is rejected by the relevant check.

### P01-T03 Create Docker Compose infrastructure

- [ ] Complete this task.

**Implementation:** Add PostgreSQL, Redis, single-node Kafka in KRaft mode, named volumes, private networking, health checks, and one shared backend image. Pin compatible versions after a startup spike; document selected versions.

**Acceptance:** Kafka works from container clients; database and Redis are reachable internally; recreating containers retains database records.

### P01-T04 Bootstrap FastAPI and React

- [ ] Complete this task.

**Implementation:** Implement API configuration, structured logging, liveness/readiness endpoints, and lifespan-managed clients. Add React routing, layout, API client configuration, loading/error components, and a health view.

**Acceptance:** Readiness reflects missing required dependencies; UI renders an API error usefully rather than crashing.

### P01-T05 Define development profiles and commands

- [ ] Complete this task.

**Implementation:** Add one-shot migration and topic-init commands plus optional demo, evaluation, and observability profiles. Document environment variables and use fake model mode by default.

**Acceptance:** The default startup does not require the full OpenTelemetry Demo or a paid model key.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `chore: bootstrap SignalOps workspace`. Split this into task-sized commits as needed.

## Phase 2 Persistence and contracts

**Dependencies:** Phase 1.

**Outcome:** Migrations and contract fixtures establish a durable, versioned domain model.

### P02-T01 Implement source and evidence tables

- [ ] Complete this task.

**Implementation:** Create workspaces, sources, services, signals, deployments, and signal_aggregates. Add UTC timestamps, JSONB attributes, kind/status checks, foreign keys, and uniqueness on source_id, replay_scope, source_event_id.

**Acceptance:** Duplicate source identities in one replay scope are rejected; the same fixture can exist in separate replay runs.

### P02-T02 Implement incident and workflow tables

- [ ] Complete this task.

**Implementation:** Create incidents, incident_services, incident_signals, investigations, jobs, agent_steps, tool_calls, llm_calls, and progress_events. Add request uniqueness, optimistic versions, leases, and nonnegative budget constraints.

**Acceptance:** Invalid states and duplicate logical steps fail; one investigation cannot create two logical jobs.

### P02-T03 Implement graph and reliability tables

- [ ] Complete this task.

**Implementation:** Create graph_nodes, graph_edges, consumer_receipts, and outbox_events with the indexes from the source plan. Add temporal interval checks and workspace-safe relationships.

**Acceptance:** Dangling graph edges are rejected; duplicate receipts are rejected; pending outbox lookup uses its intended index.

### P02-T04 Define typed application contracts

- [ ] Complete this task.

**Implementation:** Create Pydantic models for envelopes, raw adapter inputs, normalized evidence, incident DTOs, workflow checkpoints, tool results, and diagnoses. Define optional UUID source_id for non-source domain events and nullable UUID causation_id; replace illustrative placeholders with valid fixtures.

**Acceptance:** Valid examples round-trip; unsupported versions, malformed IDs, oversized fields, and invalid timestamps fail predictably.

### P02-T05 Define API and event compatibility rules

- [ ] Complete this task.

**Implementation:** Export JSON Schemas and OpenAPI into versioned files. Define additive optional changes, unknown-field preservation in source attributes, and new-version handling for breaking changes. Document diagnosis status versus investigation terminal status mapping.

**Acceptance:** A completed investigation may contain diagnosed output; insufficient_evidence maps consistently across API, persistence, and UI.

### P02-T06 Add migrations and repository boundaries

- [ ] Complete this task.

**Implementation:** Create Alembic revisions, deterministic seed data, transaction helpers, and bounded database pools. Add baseline indexes and a schema migration test against real PostgreSQL.

**Acceptance:** An empty database upgrades to head; repository transactions roll back failed writes without leaving partial state.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: define persistence models and event contracts`. Split this into task-sized commits as needed.

## Phase 3 Streaming reliability

**Dependencies:** Phase 2.

**Outcome:** Database effects and Kafka offsets remain consistent under redelivery and publisher crashes.

### P03-T01 Provision Kafka topics and producer settings

- [ ] Complete this task.

**Implementation:** Create signals.raw.v1, signals.normalized.v1, incidents.changed.v1, investigations.requested.v1, investigations.completed.v1, and processing.failed.v1. Configure keys, retention, producer idempotence, acknowledgements, and delivery callbacks.

**Acceptance:** Producer acknowledgement failures reach the caller; partition keys are stable and documented.

### P03-T02 Build the consumer transaction pattern

- [ ] Complete this task.

**Implementation:** Disable automatic commits. Process one record at a time per partition initially; insert receipt, domain effect, and outbox records in one database transaction, then commit the next Kafka offset.

**Acceptance:** A crash after DB commit but before offset commit causes redelivery with no duplicate logical effect; offsets never skip unfinished earlier records.

### P03-T03 Implement the outbox publisher

- [ ] Complete this task.

**Implementation:** Poll committed pending rows, publish using the stored event ID/key, and mark delivery after acknowledgement. Start with one publisher and preserve aggregate version order. Avoid holding a database transaction open during broker calls.

**Acceptance:** A crash after publication but before marking delivery republishes safely; failed delivery leaves the row pending.

### P03-T04 Implement retry and failure handling

- [ ] Complete this task.

**Implementation:** Separate transient infrastructure failures from permanent validation failures. Add bounded retry with jitter, structured attempt metadata, redacted failure events, and a controlled redrive command. Pause/retry partitions without violating poll deadlines.

**Acceptance:** Permanent failures are explicitly quarantined; failure-event publication errors do not silently acknowledge the original.

### P03-T05 Add streaming observability and lifecycle

- [ ] Complete this task.

**Implementation:** Measure lag, consumed/produced counts, receipt deduplications, processing latency, retry counts, and outbox age. Implement graceful shutdown and safe rebalance behavior.

**Acceptance:** Stopping a worker mid-batch resumes from a safe offset; backlog growth is visible in logs or metrics.

**Phase gate:** All tasks above are checked, acceptance evidence is recorded in the ledger, and the phase outcome is demonstrated against real infrastructure where relevant.

**Suggested phase commit:** `feat: implement Kafka consumers and transactional outbox`. Split this into task-sized commits as needed.

## Phase 4 Ingestion and data quality

**Dependencies:** Phase 3.

**Outcome:** Three heterogeneous adapters produce normalized, scoped evidence with auditable quality outcomes.

### P04-T01 Build a deterministic demo producer

- [ ] Complete this task.

**Implementation:** Generate checkout, payment, and database signals with realistic noise, stable event IDs, trace relationships, and versioned deployment events. Start with healthy and downstream-timeout fixtures.

**Acceptance:** A fixed seed produces reproducible data; the runtime sees signals without evaluator labels.

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
- Commit: `chore: create repository layout` (hash recorded in the next ledger update)
- Implemented: directory layout, root `pyproject.toml` + `uv.lock`, backend package with subpackage stubs and `signalops` CLI, Vite/React/TS app in `apps/web` with `pnpm-lock.yaml`, bootstrap commands in `docs/development.md`.
- Validation commands and results: `uv sync --locked` ok; `uv run signalops --version` → `signalops 0.1.0`; subpackage imports ok; `pnpm install --frozen-lockfile` ok; `pnpm build` ok; `git check-ignore` on `.env`, `.env.local`, `.venv`, `apps/web/node_modules`, `apps/web/dist` all ignored, `.env.example` not ignored.
- Evidence or artifact references: none retained.
- Deviations and reasons: root-level Python project (see task notes).
- Remaining issues: none.
- Next task: P01-T02

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
