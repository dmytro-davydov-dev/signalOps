# Architecture Decision Records and Open Questions

These are **proposed ADRs** drawn from [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) and the [development roadmap](development_roadmap.md). Implementation has not validated them yet. Revisit each one when its phase lands, and record any deviation here **before** implementing it.

Format: **Decision**, **Reason**, **Trade-off**, **Revisit when**.

## ADR-001: Python/FastAPI backend, React/TypeScript frontend, Docker Compose runtime

**Decision:** One Python codebase (FastAPI, Pydantic, SQLAlchemy, Alembic) built into one backend image with separate entry commands for the API and each worker. React + TypeScript frontend. Docker Compose for local and demo environments.
**Reason:** One language for the pipeline, workers, and agent runtime. Typed contracts shared across processes. Low operational overhead for a single developer.
**Trade-off:** Compose offers no production HA. The process split is logical and has to be enforced by discipline.
**Revisit when:** A process needs an independent release cadence or a different runtime.

## ADR-002: Kafka for transport, PostgreSQL for durable state

**Decision:** Kafka carries raw signals, normalized signal references, and domain events. PostgreSQL owns evidence, incidents, jobs, checkpoints, budgets, the graph, and progress history.
**Reason:** Kafka gives partitioned ordering, independent consumer groups, and replay. PostgreSQL gives transactions, constraints, and queryable state, which correctness depends on.
**Trade-off:** Two systems to keep consistent, solved with the transactional outbox (ADR-005). A single local broker gives no durability across host loss.
**Revisit when:** Measured throughput or retention outgrows a single broker, or a schema registry becomes necessary.

## ADR-003: Redis is disposable

**Decision:** Redis holds only caches, rate limits, and pub/sub wake-ups. Losing it must not affect correctness.
**Reason:** Redis pub/sub is at-most-once, and TTL-based state disappears silently. Durable budgets, detector windows, and progress events belong in PostgreSQL.
**Trade-off:** More database load. SSE needs periodic database catch-up in addition to pub/sub.
**Revisit when:** Profiling shows PostgreSQL contention on a path Redis could safely cache.

## ADR-004: Temporal knowledge graph in PostgreSQL

**Decision:** `graph_nodes` and `graph_edges` with validity intervals and provenance, traversed by bounded recursive CTEs (≤ 3 hops, cycle prevention, row limits).
**Reason:** Graphs are small at MVP scale, and keeping them next to evidence avoids a second store and a sync pipeline.
**Trade-off:** Deep or analytical traversals will get expensive.
**Revisit when:** Measured traversal complexity or latency justifies a dedicated graph projection.

## ADR-005: At-least-once delivery with receipts and a transactional outbox

**Decision:**
- Consumers insert a receipt, the domain effect, and outbox rows in one transaction, and commit offsets afterwards.
- The outbox publisher marks rows published only after broker acknowledgement.
**Reason:** Exactly-once across Kafka, PostgreSQL, and model providers is unachievable in practice. Idempotent effects are achievable and testable.
**Trade-off:** Receipt storage and retention. Duplicate publication is possible and must be harmless.
**Revisit when:** Receipt volume becomes a storage problem. Retention must still cover the replay horizon.

## ADR-006: Durable jobs with leases and generation fencing

**Decision:** Investigations run as PostgreSQL jobs claimed with `SKIP LOCKED`, with short leases, heartbeats, and a `lease_generation` that every checkpoint and completion write checks.
**Reason:** Requests can be acknowledged once persisted. Crashed workers' jobs are recovered, and stale writers are rejected.
**Trade-off:** Polling overhead. An in-flight model call can still overlap after lease loss, which deadlines and budget reservations bound.
**Revisit when:** Job volume needs a dedicated queue. That would need measurements first.

## ADR-007: Explicit persisted Python workflow, not an agent framework

**Decision:** A hand-written state machine with typed nodes, deterministic step keys, and persisted checkpoints. Agents are logical roles. Start with one agent, then add at most 3 parallel specialists, a synthesizer, and a verifier.
**Reason:** Checkpoint reuse, budget control, and recovery tests need full control over execution and persistence.
**Trade-off:** More code to write. Framework conveniences are lost.
**Revisit when:** Workflow complexity clearly exceeds what the state machine expresses cleanly.

## ADR-008: In-process LLM gateway with PostgreSQL budget reservations

**Decision:** All model calls go through a gateway module. It handles routing, timeouts, retries, structured output validation, prompt versions, and usage tracking. Cost is reserved atomically in PostgreSQL before each call, including parallel in-flight calls. Ambiguous timeouts keep their reservation.
**Reason:** It prevents budget oversubscription and makes every call attributable. A fake provider enables deterministic, offline CI.
**Trade-off:** Conservative reservations can end investigations early. Unknown costs are reported as unknown, never guessed.
**Revisit when:** Several runtimes need model access, which would justify extracting a gateway service.

## ADR-009: Read-only, bounded tools; evidence is untrusted

**Decision:** Tools are typed, read-only, and enforce workspace, replay scope, evidence cutoff, time, row, byte, and timeout limits. Authorization lives in code. Log and runbook text cannot change permissions.
**Reason:** Prompt-injection resistance and reproducible evaluation. Agents cannot see future evidence or ground truth.
**Trade-off:** Agents may hit caps and abstain more often.
**Revisit when:** Evaluation shows caps causing avoidable abstentions.

## ADR-010: Evaluate against simpler baselines under equal constraints

**Decision:** Compare deterministic, single-agent, and multi-agent variants with identical evidence, tools, and total token budget on held-out scenarios. Keep multi-agent only if it earns its cost.
**Reason:** It avoids presenting a bigger budget as an architectural gain.
**Trade-off:** More evaluation work, and repeated paid runs need explicit budget approval.

---

## Open questions

| # | Question | Decide by |
| --- | --- | --- |
| Q1 | Which Python Kafka client (for example `confluent-kafka` or `aiokafka`)? It must support manual commits, idempotent producers, and delivery callbacks. | P01-T03 / P03-T01 |
| Q2 | Exact Kafka, PostgreSQL, and Redis image versions after the startup spike | P01-T03 |
| Q3 | Frontend query layer and routing libraries | P01-T04 |
| Q4 | Expired or unknown SSE cursor behavior (full resync vs error) | P08-T03 |
| Q5 | Idempotency-key storage table and retention window | P06-T01 |
| Q6 | Diagnosis status ↔ investigation terminal status mapping | P02-T05 |
| Q7 | Detector window closure metadata schema | P05-T02 |
| Q8 | Which real model provider is implemented first, and its pricing provenance format | P07-T01 / P07-T02 |
| Q9 | Operator authentication mechanism before public exposure | P11-T02 |
| Q10 | Whether the OpenTelemetry Demo fits local resources, or a smaller synthetic app suffices for the `demo` profile | P10-T03 |
| Q11 | Planning files live in `docs/`, but `CLAUDE.md` expects them at the repository root. Move them, or update `CLAUDE.md`? | Before P01-T01 |
