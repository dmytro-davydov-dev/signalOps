# SignalOps

SignalOps turns noisy telemetry into evidence-backed software incident diagnoses.

It ingests logs, trace summaries, and deployment events, detects incidents with deterministic time-window rules, and runs bounded, budgeted agent investigations. Each investigation returns a validated diagnosis that cites evidence, or an explicit *insufficient evidence* result. A replay-based evaluation lab scores diagnoses against injected faults.

> **Status: planned, not implemented.** This repository holds the design and delivery plan. No application code, Compose stack, or measured results exist yet. Everything below describes intended behavior. See the [progress ledger](docs/development_roadmap.md#progress-ledger) for actual progress.

## What it does

```mermaid
flowchart LR
    S["Sources<br/>logs · traces · deployments"] --> I["FastAPI ingestion"]
    I --> K[("Kafka")]
    K --> N["Normalizer"] --> P[("PostgreSQL<br/>+ outbox")]
    P --> K
    K --> D["Detector"] --> P
    P --> W["Investigation workers<br/>+ LLM gateway"] --> P
    P --> API["Query API + SSE"] --> UI["React incident workspace"]
```

1. **Ingest:** source adapters submit batches with stable event IDs. The API returns *accepted* only after Kafka acknowledges.
2. **Normalize:** a worker validates and scores data quality, resolves service identity, and writes evidence to PostgreSQL. Idempotency comes from consumer receipts.
3. **Detect:** durable error and latency windows group signals into one correlated incident.
4. **Investigate:** a durable job runs an explicit, checkpointed workflow. Agents use read-only, scope-bounded tools through a budgeted model gateway.
5. **Explain:** the UI shows the timeline, the service graph, live agent progress, the diagnosis with clickable evidence, uncertainty, and cost.
6. **Evaluate:** isolated replays compare deterministic, single-agent, and multi-agent baselines under equal constraints.

## Stack

| Layer | Technology |
| --- | --- |
| Frontend | React, TypeScript (`apps/web`, pnpm) |
| Backend | Python, FastAPI, Pydantic, SQLAlchemy, Alembic (`backend/signalops`, uv) |
| Event backbone | Apache Kafka (single-node KRaft for development) |
| Durable state | PostgreSQL: evidence, incidents, jobs, checkpoints, budgets, graph |
| Disposable state | Redis: caches, rate limits, UI wake-ups |
| Runtime | Docker Compose, one backend image with separate process commands |

## Documentation

| Document | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Components, process boundaries, ownership of state, end-to-end flow |
| [Events](docs/events.md) | Kafka topics, keys, the common envelope, delivery and failure rules |
| [Data model](docs/data-model.md) | PostgreSQL tables, constraints, temporal graph, indexes, retention |
| [Agents and workflow](docs/agents-and-workflow.md) | Workflow nodes, agent roles, tools, gateway, budgets, diagnosis contract |
| [API](docs/api.md) | HTTP endpoints, idempotency, SSE progress stream, error categories |
| [Reliability](docs/reliability.md) | Outbox, receipts, offsets, leases, recovery guarantees, failure tests |
| [Evaluation](docs/evaluation.md) | Fault scenarios, baselines, metrics, leakage prevention |
| [Development](docs/development.md) | Repository layout, proposed commands, conventions, workflow |
| [Decisions](docs/decisions.md) | Proposed architecture decision records and open questions |

Planning sources:

- [Implementation plan](docs/SignalOps_Implementation_Plan.md): the original design (v1, 16 Oct 2026).
- [Development roadmap](docs/development_roadmap.md): 12 phases, task IDs, acceptance criteria, and the progress ledger.
- [CLAUDE.md](CLAUDE.md): project instructions for AI-assisted development.

## Quick start

None yet. Bootstrapping is roadmap task **P01-T01**. The intended developer experience:

- A clean checkout starts with `docker compose up`.
- It needs **no paid model key**, because the fake model provider is the default.
- Migrations and topic creation run as one-shot services.
- Optional profiles (`demo`, `evaluation`, `observability`) keep the default startup light.

[Development](docs/development.md) lists the proposed commands. They will be confirmed when Phase 1 lands.

## Scope

**In scope for the first releases:** one workspace, three adapters (logs, traces, deployments), read-only investigations, and remediation that only a human applies.

**Explicitly not claimed:**

- Production high availability from single-node Compose.
- Exactly-once model execution.
- Throughput or accuracy numbers before a benchmark report is retained.

**Deferred:** a metrics adapter (after R1), a dedicated graph database, Kubernetes, multi-broker Kafka, and autonomous remediation. See the [deferred backlog](docs/development_roadmap.md#deferred-backlog).

| Release | Phases | Gate |
| --- | --- | --- |
| R0 Data platform | 1–5 | Fault signals produce a durable incident with inspectable evidence |
| R1 First portfolio slice | 1–8 | One agent diagnoses a fault with evidence in the UI |
| R2 Multi-agent platform | 9 | Specialist workflow survives a worker crash |
| R3 Evaluated release | 10–12 | Held-out comparisons, load results, reproducible demo |
