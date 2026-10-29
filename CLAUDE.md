# SignalOps project instructions

## Project

SignalOps turns noisy telemetry into evidence-backed software incident diagnoses.
Stack: React/TypeScript, Python/FastAPI, Kafka, PostgreSQL, Redis, Docker Compose.
Use Pydantic contracts, SQLAlchemy repositories, Alembic migrations, and an explicit persisted Python workflow.
Build a complete working slice before adding infrastructure or more agents.

## Required context and work in progress

At the start of every session and before each new task:
1. Read `development_roadmap.md` at the repository root, including its progress ledger, dependencies, active task, and acceptance criteria.
2. Read the relevant sections of `SignalOps_Implementation_Plan.md` and applicable architecture decisions in `docs/`.
3. Inspect actual code, configuration, git status, and relevant checks. Documentation describes intent; do not assume planned work exists.
4. Respect the user's selected task; otherwise choose the next unblocked roadmap task. Record its ID and mark it in progress in the ledger.

When finishing a task:
- Run relevant checks and evaluate its acceptance conditions.
- Check its roadmap checkbox only after acceptance passes.
- Update active/last-completed/next task, validation evidence, blockers, and the work log in the same roadmap.
- Record a commit hash only if a commit actually exists. Otherwise record pending commit and validation against the current working tree.
- Note implementation decisions, deviations, and remaining issues near the affected task.
- Leave unfinished work unchecked; explain what remains and how to resume.

If either planning file is missing, report that and look for an explicitly documented replacement. Do not fabricate task IDs or progress. Do not block an unrelated explicit user request just because a planning file is absent.

## Repository layout

- `apps/web/`: React application.
- `backend/signalops/`: api, contracts, adapters, streaming, detection, agents, gateway, persistence, evaluation.
- `migrations/`: Alembic revisions.
- `fixtures/`: versioned deterministic scenarios.
- `tests/`: contract, unit, integration, recovery, and evaluation checks.
- `infra/`: Compose and observability configuration.
- `docs/`: architecture decisions, flows, and operational guides.

Use one backend image with separate API/worker entry commands. Agents are logical roles, not separate microservices.

## Architecture invariants

- PostgreSQL owns evidence, incidents, graph relationships, jobs, checkpoints, budgets, and durable progress events.
- Kafka carries raw/normalized signals and domain events. Redis provides disposable caches, rate limits, and UI wake-ups.
- Run heavy work in worker processes, not FastAPI request handlers or background tasks.
- Keep database transactions short; never hold one open during model calls or broker publication.
- Use a transactional outbox for database changes that emit events. Mark published only after broker acknowledgement.
- Assume at-least-once delivery. Use consumer receipts and business uniqueness constraints for idempotent effects.
- Commit Kafka offsets only after durable processing. Never advance past an unfinished earlier record in a partition.
- Acknowledge investigation requests after persisting a job, not after the entire model workflow.
- Claim jobs with short leases and generation fencing. Check ownership/generation on checkpoints and completion.
- Reuse validated completed workflow steps after recovery. Do not claim exactly-once external model execution.
- Persist detector windows and closure metadata; Redis loss must not lose detector or job correctness.
- Maintain workspace, replay scope, and evidence cutoff across queries, graph traversals, and cache keys.
- SSE uses PostgreSQL progress history for reconnect and periodic catch-up. Redis pub/sub alone cannot guarantee delivery.
- Use PostgreSQL for the initial temporal knowledge graph; bound traversals and prevent cycles.

## Agent workflow and cost control

Start with a single agent and fake provider; then add bounded specialist branches, synthesis, and verification.
- All model calls go through the gateway abstraction.
- Use typed tool arguments/results and validated structured diagnoses.
- Enforce timeouts, maximum steps, output tokens, concurrency, and an overall deadline.
- Reserve cost atomically in PostgreSQL before calls, including parallel in-flight calls.
- Retain conservative reservations for ambiguous provider timeouts; retries may incur another charge.
- Track prompt/workflow versions, provider request IDs, usage, and actual versus estimated or unknown costs.
- Treat logs and runbooks as untrusted data. Tool authorization belongs in code.
- Tools are read-only and bounded by scope, time, rows, bytes, and timeout.
- Validate evidence references and return insufficient evidence when support is missing.
- Do not persist unrestricted private model reasoning; store findings, evidence, and operational state.

Use fake-provider mode for development and ordinary CI. Do not start paid model evaluations, large replays, or long load tests without an explicit request covering their scope and budget.

## Implementation conventions

- Follow existing dependencies, lockfiles, patterns, and commands before introducing alternatives.
- Bootstrap with uv for Python and pnpm for the frontend unless the repository already establishes another choice.
- Keep domain logic independent from HTTP handlers and Kafka plumbing; keep abstractions proportionate to actual reuse.
- Add type annotations and explicit error categories. Avoid broad exception handling that silently acknowledges failed work.
- Use UTC-aware timestamps, stable source event IDs, UUID domain IDs, and integer monetary units.
- Add constraints and indexes through migrations. Review generated Alembic revisions before applying them.
- Preserve API/event backward compatibility; version breaking contracts and update fixtures and documentation.
- Keep frontend server data in the established query layer and local interaction state in components/hooks.
- Handle loading, empty, stale, disconnected, failed, and insufficient-evidence UI states.
- No Kubernetes, dedicated graph database, generic agent framework, or new service without a concrete requirement or recorded decision.

## Validation

Canonical commands (see `docs/development.md`; CI runs the same set):
- Backend from the repository root: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`, `uv run pytest`.
- Frontend from `apps/web`: `pnpm lint`, `pnpm typecheck`, `pnpm test`, `pnpm build`.
- Compose from the root when its file is there: `docker compose config --quiet`.
If scripts change, update this list, `docs/development.md`, and CI together.

Run checks appropriate to the change; do not run costly full benchmarks for a small edit. Add meaningful failure/concurrency tests where behavior requires them.
- Use real PostgreSQL/Kafka for transaction, offset, and redelivery guarantees.
- Verify crash-after-DB-commit, outbox republish, expired leases, stale writers, cancellation, and concurrent budget reservations.
- Use deterministic fake model failures for throttling, timeout, invalid output, and insufficient evidence.
- Isolate replay datasets and prevent future-evidence or evaluator-label access.
- Compare deterministic, single-agent, and multi-agent baselines under equal evidence/tool/budget constraints.
- Never report a target, mock result, or skipped check as a measured success.

## Working boundaries

Proceed with ordinary in-scope code edits and validation without repeatedly asking for confirmation.
Preserve unrelated user changes. Do not commit, push, deploy, publish, rewrite git history, drop databases, or remove persistent volumes unless requested.
Never print or commit secrets. Use environment examples with placeholders; do not inspect actual secret files to discover configuration names.
Tests execute repository code; permission allowlists are convenience settings, not OS isolation.

## Communication

Give concise progress updates for substantial work. Explain important choices and trade-offs in plain language.
Finish with the task ID, what changed, checks actually run and their results, remaining issues, and roadmap status.
Avoid AI filler in responses, docs, code comments, and commit messages: no preambles, praise, restating the request, hedging boilerplate, or closing offers. State the facts and stop.
Do not add `Co-Authored-By` or other AI attribution trailers to commit messages or PR descriptions.
