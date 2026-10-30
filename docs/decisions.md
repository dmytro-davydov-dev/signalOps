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

## ADR-011: Ingestion acknowledges per record after Kafka, with derived event IDs

**Decision:**
- `POST /v1/ingest/{source_id}` reports each record as `accepted` only after Kafka acknowledged it, `rejected` when the record itself is invalid, or `failed` when it was not acknowledged. Any failed record makes the response 503 with the same per-record body.
- The raw envelope's `event_id` is UUIDv5 of source, replay scope, and `source_event_id`, so every retry of a record, including after an ambiguous timeout, carries the same event ID and consumer receipts deduplicate it.
- Source tokens come from `SIGNALOPS_INGEST_TOKENS` (source ID to token), compared as SHA-256 digests in constant time. `sources.config` holds only non-secret settings such as `redact_fields`.
- Ingestion checks shape, size, and coarse timestamp bounds only; payloads go to the normalizer unvalidated, so malformed records become auditable quarantined evidence.
**Reason:** No false acceptance during a broker outage, and safe blind retries of a whole batch. Keeping tokens out of the database follows the rule that source configuration stores no credentials.
**Trade-off:** Adding or rotating a token requires restarting the API; one token per source. A client that resends a `source_event_id` with a different payload gets the first version kept downstream.
**Revisit when:** Sources are created at runtime, or rotation without a restart is needed (P11-T02 covers source credential rotation): move digests to a table or secret store.

---

## ADR-012: Normalizer resolves identity exactly and flags everything else

**Decision:**
- A signal's service is the catalog row matching its claimed `(service, environment)` exactly. Claims come from the record's fields, or from well-known attributes (`service.name`, `deployment.environment`, …) when a field is absent.
- Missing or uncatalogued identity leaves `service_id` NULL with `degraded` status and `missing_service` or `unknown_service`. Disagreeing claims leave it NULL with `quarantined` status and `contradictory_identity`; the disagreeing keys go in `attributes.provenance.identity_conflicts`.
- The reported identity is always kept in `attributes.provenance`, and source attributes stay apart from normalizer fields (`source`, `record`, `provenance` sections).
**Reason:** The plan forbids assigning an arbitrary service. A self-contradicting record should not feed per-service detection, but it stays queryable evidence. Separate sections stop a source key from overwriting a derived field.
**Trade-off:** No fuzzy or case-insensitive matching and no default environment per source, so a source with inconsistent spelling produces degraded evidence until the catalog or the source is fixed. Unknown services are not auto-registered.
**Revisit when:** A source cannot send an environment per record (add an explicit per-source binding in `sources.config`), or a detector rule needs a status the flag effects of ADR-013 do not give.

## ADR-013: Quality flags map to status by fixed effect; derived IDs at ingestion

**Decision:**
- Each quality flag has one effect: quarantine (`malformed_payload`, `contradictory_identity`), degrade (`missing_service`, `unknown_service`, `clock_skew`), or note (`late_arrival`, `derived_event_id`, `truncated_message`). The most severe effect sets the status, so status follows from the flags.
- Malformed records become quarantined evidence with their contract errors and raw record (when it fits), not `processing.failed.v1` entries. Only envelope faults still fail.
- Timing is judged on the envelope's `occurred_at` versus `ingested_at`, for the live scope only. Skew beyond 5 minutes degrades; arrival over 15 minutes after the event is noted.
- A record without `source_event_id` gets one derived at ingestion, before the raw event ID, from per-kind identity fields of the redacted payload (see api.md). The signal is flagged `derived_event_id`.
**Reason:** A per-detector status would couple the normalizer to rules that do not exist yet; flags keep the detail, and the status is a coarse filter. Malformed input is a data-quality outcome the plan wants counted per source, and a failure topic is not queryable evidence. Late evidence is sound evidence; lateness matters to detection windows, which the detector judges itself. Deriving the ID at ingestion keeps the raw event ID, receipts, and business key aligned for retries. Hashing redacted values keeps secrets out of IDs.
**Trade-off:** Two log lines identical in every field and timestamp collapse into one; traces re-sent with changed measurements collapse too. The late threshold is wider than the demo poster's 10-minute backfill, so backfills are not flagged. Changing the redaction list changes derived log IDs, so a retry spanning that change can duplicate. Severity of a malformed record is not inferred.
**Revisit when:** A detector rule needs a stricter status than the flag effects give, a source sends logs without IDs at a rate where same-millisecond duplicates matter, or replay ingestion (P10) needs timing checks against its simulated clock.

---

## Open questions

| # | Question | Decide by |
| --- | --- | --- |
| Q1 | ~~Which Python Kafka client?~~ **Resolved (P01-T04):** `confluent-kafka` (librdkafka): manual offset commits, idempotent producer, per-message delivery callbacks. Blocking calls run in worker threads from async code. | P01-T03 / P03-T01 |
| Q2 | ~~Exact Kafka, PostgreSQL, and Redis image versions after the startup spike~~ **Resolved (P01-T03):** `apache/kafka:4.3.0`, `postgres:17.11-alpine`, `redis:7.4.12-alpine`; see development.md | P01-T03 |
| Q3 | ~~Frontend query layer and routing libraries~~ **Resolved (P01-T04):** TanStack Query 5 for server data, React Router 7 (data router) for routing | P01-T04 |
| Q4 | Expired or unknown SSE cursor behavior (full resync vs error) | P08-T03 |
| Q5 | Idempotency-key storage table and retention window | P06-T01 |
| Q6 | ~~Diagnosis status ↔ investigation terminal status mapping~~ **Resolved (P02-T05):** one-to-one. `completed` ⇔ `diagnosed`, `insufficient_evidence` ⇔ `insufficient_evidence`, no diagnosis otherwise; enforced in contracts, a database CHECK, and the generated UI module. See data-model.md | P02-T05 |
| Q7 | Detector window closure metadata schema | P05-T02 |
| Q8 | Which real model provider is implemented first, and its pricing provenance format | P07-T01 / P07-T02 |
| Q9 | Operator authentication mechanism before public exposure | P11-T02 |
| Q10 | Whether the OpenTelemetry Demo fits local resources, or a smaller synthetic app suffices for the `demo` profile | P10-T03 |
| Q11 | Planning files live in `docs/`, but `CLAUDE.md` expects them at the repository root. Move them, or update `CLAUDE.md`? | Before P01-T01 |
