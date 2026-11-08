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

## ADR-014: Interim operator tokens scoped to one workspace

**Decision:**
- Evidence query routes require a bearer token from `SIGNALOPS_OPERATOR_TOKENS` (workspace ID to token), compared as SHA-256 digests in constant time against every configured token. The matching token's workspace is the query's workspace; no route takes a workspace parameter.
- Settings refuse an operator token equal to any ingestion token, so a source credential cannot open an operator route, and operator tokens are not in the ingestion token set.
- Replay scope is a validated query parameter (`live` by default) inside the token's workspace. Every read filters through `EvidenceScope` (`persistence/repositories/scope.py`).
- Responses re-apply redaction to `attributes` with the current process and source field lists.
**Reason:** P04-T05 must show that queries cannot cross scope, which needs an authenticated principal with a workspace. Q9 (the real operator mechanism) is due at P11-T02; a static token set is the smallest thing that gives the API a principal now and fails closed when unset. Re-redacting on output covers fields added to a list after the evidence was stored.
**Trade-off:** One shared secret per workspace, no users, roles, or audit identity; rotating a token needs an API restart. Replay datasets are readable by any operator of the workspace. Free-text messages are returned as stored, since ingestion does not scrub them either.
**Revisit when:** P11-T02 resolves Q9, the UI needs per-user identity, or replay datasets need separate permissions (for example evaluator isolation in P10).

---

## ADR-015: Detection windows count accepted samples once and record rule triggers

**Decision:**
- The detector counts `log` and `trace` signals that are `accepted` and have a resolved service. Degraded signals with a service carry `clock_skew`, so their `occurred_at` cannot place them in a window.
- Each counted signal inserts a `detection_samples` row keyed by (signal, series) before its aggregate changes; a conflict means it is already counted. Aggregates are updated by additive upserts.
- Windows are epoch-aligned tumbling windows; the series name includes the window length.
- Rules (`DetectionRule`) are versioned settings: metric, kind, threshold, `min_samples`, cooldown. A rule fires only with at least `min_samples` samples. Latency thresholds must be histogram bucket bounds, and a latency rule fires when fewer than ceil(q × n) samples are at or below the threshold, which is exact for such thresholds.
- A firing inserts a `detection_triggers` row, unique per (service, scope, rule, window). The rule does not fire again for that service for windows starting within `cooldown_seconds`, compared in event time.
- ~~Rules are evaluated after every sample against the updated window.~~ Replaced by closure-time evaluation in ADR-016.

**Reason:** The consumer receipt covers redelivery of one event, but a contribution's identity is the signal; the sample key holds even if the same signal arrives under another event ID, and keeps window membership for recomputation under late data. Reading a quantile as its bucket's upper bound would count a 625 ms request as "at least 1000 ms"; testing counts against a bound avoids that. Triggers separate "a rule crossed its threshold" from "an incident exists", which P05-T03 decides with fingerprints and correlation windows. Event-time cooldowns make replay deterministic.
**Trade-off:** One extra row per counted signal. Evaluating partial windows detects a fault sooner, but the first `min_samples` samples of a window decide alone. Measured offline with the demo generator (100 healthy and 100 fault runs of 10 minutes, default rules, 60 s windows): 2 healthy runs gave latency triggers where two isolated gateway timeouts shared one 20-sample window, and 1 fault run had no latency trigger for `bank-gateway`. A latency `min_samples` of 30 removed the false triggers but missed every latency fault at demo traffic (about 20 traces per service per minute). These are generator measurements, not production rates.
**Revisit when:** P05-T02 adds closure (evaluate at closure, or require persistence across windows), traffic per window grows enough to raise `min_samples`, a metric needs a non-bucket latency threshold, or a fourth adapter adds a metric kind.

---

## ADR-016: Rules evaluate at window closure, with lateness judged from durable clocks

**Decision:**
- Rules evaluate when a window closes, not per sample. A window closes once its clock is past `window_end` plus the allowed lateness (`SIGNALOPS_DETECTION_ALLOWED_LATENESS_SECONDS`, 30); the deadline itself is still on time.
- **Live clock:** a sample is judged by its stored `ingested_at`. Windows are closed by `signalops detector-closer` (Compose `detector-closer`) from the wall clock, every `SIGNALOPS_DETECTION_CLOSER_INTERVAL_MS` (5000).
- **Replay clock:** `detection_clocks` holds one simulated clock per (service, replay scope), the latest counted `occurred_at`, advanced and applied in the detector's consumer transaction. `signalops detector-flush` closes what remains at the end of a replay. The closer never touches replay scopes, and a flush never touches live.
- A sample past its deadline is recorded with `detection_samples.late` and added to `signal_aggregates.late_count` only. It is outside the counts and reaches no rule.
- An on-time sample that finds its window closed joins the counts and re-evaluates that window. Triggers are unique per window, so this is idempotent; a trigger is never withdrawn.
- Closure metadata lives on the window row: `closed_at`, `closed_by` (`wall_clock`, `simulated_clock`, `final_flush`, `backfill`), `late_count`. Open windows are rows, indexed by a partial index, so the closer needs no in-memory state. Migration `0010` closes earlier windows as `backfill` without evaluating them, because their rules already ran.
- `signalops demo-post` posts a whole dataset at once by default, which the policy treats as history. `--realtime` posts records as their timestamps arrive, so a demo can fire rules.

**Reason:** Per-sample evaluation lets the first `min_samples` arrivals decide, so shuffled input can change the outcome; a closed window has one final set of counts. Judging lateness from `ingested_at` makes it a fact about the signal: redelivery gives the same answer, and detector downtime does not turn on-time data into late data, as comparing with the detector's own arrival time would. A scope-wide replay clock would let the partition that is furthest ahead close another service's windows, so the clock follows the unit Kafka keeps in order. The closer is a separate process because a quiet service has no next sample to close its window, and it keeps `ConsumerRunner` free of timers. Closure and sample upserts both hold the window row, so a sample either lands in the counts closure reads or sees the window closed and evaluates it.
**Trade-off:** Detection waits for closure: up to window plus lateness, 90 s by default, instead of as soon as `min_samples` is reached. After detector downtime the closer can close a partly consumed window; a false trigger from those partial counts is not withdrawn, and a re-evaluation of an older window can be suppressed by a newer window's cooldown. Replay does not validate timestamps, so one far-future `occurred_at` moves that service's clock and makes its later samples late. A single demo-post backfill into live is counted as late rather than evaluated. The sample-count calibration in ADR-015 is unchanged by closure: over the same 100 healthy and 100 fault generator runs, per-sample and closure evaluation both fire on 2 healthy runs and both miss latency in 1 fault run (generator measurements, ordered input). Closure gives order independence here, not fewer false positives; the demo false positives remain for P05-T03 or a persistence requirement.
**Revisit when:** Detection latency needs an early-warning path, triggers need withdrawal, closure should wait on consumer lag, or replay orchestration (P10-T02) replaces the manual flush.

---

## ADR-017: Incidents correlate by environment within a session, and recovery needs evidence

**Decision:**
- The fingerprint is `env:{environment}` of the breaching service.
- A correlation window is a session: a breach joins the fingerprint's latest incident when its window falls within `SIGNALOPS_INCIDENT_CORRELATION_GAP_SECONDS` (600) of the incident's seen range. `correlation_window_start` is the start of the window that opened the incident.
- Every breached window reaches the incident, including those a rule's cooldown suppresses. Cooldown limits triggers, not incident lifetime. Only triggers cite evidence: up to 20 breaching samples per trigger, worst first, and at most the contract's 200 per incident.
- A trigger, its incident change, and an `incident.changed` outbox row share one transaction. An advisory lock per replay scope serializes correlation decisions; it is taken before the per-rule lock.
- A window shows its service healthy when a rule had `min_samples` and none breached. An incident resolves when every service has such a window ending at least `SIGNALOPS_INCIDENT_RECOVERY_SECONDS` (300) after `last_seen_at`. `resolved_at` is that window's end.
- A breach after resolution and within the gap reopens the incident. A breach in a window older than the resolution extends it without reopening.
- Severity is the highest among breached rules (`DetectionRule.severity`; rule versions bumped to 2). Every service with a breach is `primary`, matching the role's definition; `affected` waits for the graph.
- `detected_at` is the wall clock live and the window's closure deadline in replay.

**Reason:** One downstream fault fires rules on several dependent services, and dependencies are not known until P05-T04, so a per-service fingerprint would split the fault. Time buckets would split a fault crossing a boundary, and the unique key takes either. A suppressed breach must count, or a fault outlasting the gap would split into incidents and look recovered under cooldown. Absence of breaches also describes a dead service or a stopped detector, so recovery needs windows that could have fired. Event time gives replay and live the same answers; only the live `detected_at` reports when detection really happened, for latency measurement.
**Trade-off:** Two unrelated faults in one environment inside the gap merge into one incident. Sessions depend on processing order when breaches are processed out of order by more than the gap, which can split what ordered input keeps together; shuffling inside the lateness bound does not (`test_demo_fault_is_one_incident_that_recovers_with_or_without_shuffling`, one seed). The title describes the first breach processed. A service that stops sending keeps its incident open until an operator resolves it (P05-T05). A healthy generator run that fires a trigger opens an incident: triggers were not made stricter here, so the 2 in 100 healthy runs measured in ADR-015/016 carry over.
**Revisit when:** P05-T04 gives dependencies to split a fingerprint by connected component, unrelated concurrent faults merge in evaluation, or false positives call for requiring a breach to persist across windows.

---

## ADR-018: Observed edges are merged intervals that expire

**Decision:** `signalops graph-projector` (group `graph-projector-v1`) projects `signals.normalized.v1`. An observed dependency is one edge per interval: an observation at `t` covers `[t, t + SIGNALOPS_GRAPH_DEPENDENCY_TTL_SECONDS)` (default 7 days), and every edge of the pair that interval touches merges into one row. `valid_to` is always set. `provenance_signal_id` is the earliest signal; `properties` holds the observation count and the last observation. A deployment's `DEPLOYED_TO` edge runs from `deployed_at` to the service's next deployment, or stays open. Migration `0012_graph_projection` adds `ex_graph_edges_validity`, which rejects overlapping intervals for one pair and relation, and the `btree_gist` extension it needs.

Reads take the evidence cutoff as `as_of`. `GraphRepository.dependencies` walks `DEPENDS_ON` up to 3 hops downstream or upstream, with node paths preventing cycles and `MAX_WALK_PATHS` (1000) bounding the walk; it reports `truncated`. `deployments_as_of` gives what each service ran. Neither returns an edge's `valid_to`, and confidence is a constant 1.0, so a historical answer holds nothing from after `as_of`.

**Reason:** Unions of intervals do not depend on arrival order, so shuffled and replayed input give the same edges. Every instant in an edge is within the TTL after an observation at or before it, so an as-of read cannot be supported only by a later signal, and a removed dependency expires with no background job. An advisory lock per relationship makes concurrent projectors merge; the exclusion constraint is the guarantee if a writer does not. Observation counts in `confidence` would leak future evidence into historical answers.

**Trade-off:** A removed dependency stays valid for up to the TTL, and one called less often than the TTL splits into several edges. Each trace rewrites its edge rows, which is a hot row per dependency. `observations` assumes the consumer receipt: event IDs derive from the signal, but the same signal under a new event ID would count twice, while the interval and provenance would not change. A service not in the catalog has no node, so a dependency on it is invisible. A rebuild from a new consumer group reaches only as far back as the topic's 7-day retention.

**Revisit when:** Traces name uncatalogued dependencies often enough to matter, edge write load shows up in profiling, or the graph must be rebuilt from PostgreSQL evidence past Kafka retention.

## ADR-019: Operators change incident status at a version, and evidence still wins

**Decision:** `PATCH /v1/incidents/{id}` takes a status and the version the operator read. `detection.incidents.change_status` locks the incident row, compares versions, and writes the change with an `incident.changed` outbox row in one transaction; `resolved_at` is the wall clock. A stale version is `409`. The same status at the current version changes nothing. Only the live scope can change. The route takes the row lock alone, not `lock_scope`.

The UI asks for the operator token and keeps it in `sessionStorage`. A `401` discards it, and changing it clears cached queries.

**Reason:** A request body version gives the operator a check against the detector and other operators without a held lock. The scope lock is shared by detector batches, so taking it would queue a request behind them; one row and one outbox insert cannot deadlock with them. Replay incidents are evaluation data, and an operator write would change a run's result. A token built into the bundle would be readable by anyone who can load the page, and nginx injecting one would authenticate every visitor.

**Trade-off:** Evidence overrides an operator in both directions: a breach after `resolved_at` within the gap reopens the incident, and an incident reopened after its services recovered resolves again on the next healthy window. No hold exists to keep it open. Operator tokens carry no identity, so a change records no actor. Live `resolved_at` is wall clock while recovery uses event time, so a breach in a window ending before the click extends the incident without reopening it. A page script can read `sessionStorage`; the UI has no third-party scripts and React escapes evidence text.

**Revisit when:** P11-T02 gives operators identities, or operators need an incident to stay open against recovery.

## ADR-020: Investigation requests are keyed durably and dispatched once

**Decision:** `POST /v1/incidents/{id}/investigations` writes the investigation, an `idempotency_keys` row, and an `investigation.requested` outbox row in one transaction, and returns `202`. A key is scoped to `(workspace_id, route, key)`; operator tokens carry no identity (ADR-014), so the workspace is the caller. `request_hash` is the SHA-256 of the canonical incident ID and body. A repeat with the same hash returns the stored investigation, and another hash is `409`. `investigations.request_key` is `operator:{sha256(workspace, route, key)}` or `detector:{incident_id}`, and its unique constraint decides a race: the loser's insert waits for the winner's commit and then reads the winner's key row.

`signalops dispatcher` (group `dispatcher-v1`) creates the job. The receipt catches a redelivered event, and `UNIQUE(jobs.investigation_id)` catches a second event for one request. An investigation already terminal gets no job.

A new live incident requests one investigation in the transaction that creates it. Settings hold the limits (`SIGNALOPS_INVESTIGATION_BUDGET_MICROUSD`, `_MAX_STEPS`, `_DEADLINE_SECONDS`); a request may lower them, not raise them, and a repeat skips that check. `evidence_cutoff_at` is the request time.

Key rows have no expiry: there is one per operator investigation, and it lives as long as the investigation does. This resolves Q5.

**Reason:** The key row and the unique constraint give a retried or concurrent request the same answer without a held lock. The hash covers the incident because the route template does not, so a client bug reusing a key across incidents fails loudly. Creating the job in the dispatcher, not the API, keeps one path for both triggers and lets the outbox carry the request through a broker outage. Requests are live only because a replay's investigations are evaluation runs with their own budget (P10): an automatic request there would add spend to a benchmark and change what it measures.

**Trade-off:** A request with an unused key always creates a new investigation, so an operator can start several for one incident, each with its own budget. Nothing limits that yet. A reopened incident gets no new automatic investigation, because its key is stable, and an operator must ask. Until the outbox publisher and dispatcher catch up, an investigation has no job, and nothing yet reports one stuck in that state. Automatic requests run for every new live incident regardless of severity. The detector's request is a write in its window-closure transaction, so it adds an insert and an outbox row there.

**Revisit when:** P11-T02 gives operators identities, which adds a caller to the key; or spend from automatic requests justifies a severity threshold.

## ADR-021: Job leases use PostgreSQL's clock and a row-lock fence

**Decision:** `claim_job` selects a queued, due job or a running job past `lease_until`, with `FOR UPDATE SKIP LOCKED`, and commits the lease before any work. A fenced write calls `JobRepository.hold`, which locks the job row after checking status, owner, and generation, and commits in the same transaction. Lease times come from `clock_timestamp()`.

Every claim spends an attempt, including a takeover, and a job out of attempts fails with its investigation and writes `investigation.completed`. A handler's `PermanentJobError` fails the job. Any other exception retries it after the shared capped, jittered backoff, stored in `available_at`. A stopping worker requeues its job and refunds the attempt. An expired lease nobody took can still heartbeat and write.

**Reason:** PostgreSQL's clock is the one all workers share, so skew cannot extend or cut a lease. A conditional `UPDATE` alone would leave a gap between checking the lease and writing a step row in another table. The row lock closes it, because a claim skips the locked row until the write commits. Counting claims, not reported failures, is what stops a job that kills its worker, since a killed worker reports nothing. Refunding on shutdown keeps a rolling restart from failing a job. Letting an unclaimed expired lease continue loses nothing: its generation is still current, and it saves repeating paid work.

The log line for a failed attempt holds the exception type only (P07-T06), because a traceback can quote evidence.

**Trade-off:** A deployment with crashing workers can fail a healthy job after three claims. Fenced writes and heartbeats on one job serialize on its row, which is short because they commit at once. The job's attempt count and the shared `SIGNALOPS_RETRY_MAX_ATTEMPTS` are unrelated: jobs use their `max_attempts` column. `JobRunner` runs one job at a time per process, so concurrency means more processes.

**Revisit when:** Phase 7 measures real call lengths against the 30-second lease; or workers need more than one job per process.

## ADR-022: The workflow is a fixed node sequence with a fenced write per step

**Decision:** `agents/workflow.py` defines a workflow as a versioned tuple of typed nodes in `WORKFLOWS`. `WorkflowEngine` is the job handler. It reloads the steps, reuses completed ones after validating them, and runs the rest. Each step gets a fenced open and a fenced end, in separate short transactions, and the node runs between them outside any transaction. The node key is the step key. An investigation runs the version it was requested with.

A failed node marks its step `failed` and a retry reopens it; a step left `running` by a dead worker is taken over. Completed output that does not validate, or that another workflow version wrote, raises `PermanentJobError` and fails the job. Reaching `max_steps` does the same.

**Reason:** A step row is the only checkpoint, so resuming needs no replay log and a crash at any point leaves a state the same code can continue from. Separate transactions keep model calls out of database transactions. Failing on unusable output is the only honest choice: a completed step cannot change state, so it can neither run again nor be trusted.

**Trade-off:** A node can run twice if its worker dies inside it, so execution is at least once. Nodes run strictly in sequence, so parallel specialists (Phase 9) must extend the engine. Any change to a node's behavior or output that resuming investigations would notice needs a new workflow version. `StepCheckpoint.completed_tool_calls` stays empty until tools exist.

**Revisit when:** Phase 9 adds parallel branches or the verifier's follow-up round, which need more than one step per node.

## ADR-023: Cancellation is a durable request; workers end the job

**Decision:** `POST /v1/investigations/{id}/cancel` sets `investigations.cancel_requested_at`. With no job or a queued job, it ends the investigation in that transaction. With a running job it only records the request, and the job's lease holder ends both, because only that worker knows what is in flight. A repeat changes nothing; an investigation that ended another way is `409`.

`JobRepository.stop_reason` is the one check, for cancellation and for `deadline_at` by PostgreSQL's clock. Four places read it: the claim, which also finishes the job of a dead worker; each heartbeat, which interrupts the handler; the transaction that opens a step; and each settling write. A passed deadline ends the job and its investigation `failed`, with `deadline_exceeded` in `last_error`. Writers lock the job row before the investigation row.

**Reason:** A request needs a durable row because no live connection to a worker exists. A job row lock serializes the request against settling, so a cancellation lands fully before or fully after a settlement. A check only between nodes would never fire for a hung call, since heartbeats keep the lease alive, hence the check in the heartbeat. `status` has no deadline value, and adding one for a case `failed` plus a category describes would change the API, the event, and the UI mapping.

**Trade-off:** Stopping can take up to a third of a lease (10 s by default). An interrupted call may still be charged, so Phase 7 keeps its reservation. A cancellation beats any outcome that has not committed, including finished work. Work finished past the deadline keeps its result, since no new call was made. With no worker running, nothing ends a running or overdue job until one starts.

**Revisit when:** Phase 7 adds calls inside a node, which must check `stop_reason` first.

## ADR-024: One transaction ends the job and the investigation

**Decision:** A workflow ends in a `Diagnosis` node, `Workflow.conclusion`. `WorkflowEngine` returns that diagnosis, and `complete_job` ends the job and the investigation in one transaction under the lease fence: status from `terminal_status_for`, the diagnosis, `investigation.completed` with its `diagnosis_status`, and an `investigation.ended` progress event. A step's progress event commits with the step's transition. Events hold identifiers and categories, never output or error text. `GET /v1/investigations/{id}/progress` reads them after an integer cursor.

Revision `0015_investigation_outcomes` adds a partial unique index allowing one `investigation.ended` per investigation. It also settles investigations left `running` by a `succeeded` job, using their `conclude` output.

**Reason:** Writing both statuses together removes the state where a job has succeeded and its investigation has not ended, which every reader would otherwise need to handle. A replay of a finished workflow reuses the stored `conclude` step, so it reaches the stored outcome even if a model would now answer differently. Sharing the transaction means progress cannot claim a change that rolled back. Reused steps write nothing, so a replay does not repeat history. A cancellation that finds a succeeded job now finds an ended investigation, which is `409`. A migration fits the repair because no code path can create those rows again, so no permanent sweep is needed.

**Trade-off:** The history has no event for a reused step or for a cancellation request, and one attempt's `step.started` can follow another's with no ending between them when a worker died. Readers take the latest attempt of a step. The migration hardcodes the event envelope, because it must not change with the code. It settles live rows only, since requests are live only, and its downgrade keeps the outcomes. A crash after `conclude` commits still loses that diagnosis if the job is then out of attempts or past its deadline, because the claim ends such jobs without running them.

**Revisit when:** Phase 7 wants a job with a finished conclusion to settle at the claim. (P08-T03 now serves this history over SSE; see ADR-030.)

## ADR-025: Provider calls go through one neutral interface, and the Anthropic SDK never retries

**Decision:** `gateway/types.py` defines provider-neutral requests and responses, and `ModelProvider.complete` is one call. `FakeProvider` runs scripts from code or `fixtures/gateway/v1`, and `AnthropicProvider` adapts the Messages API through the official SDK with `max_retries=0`. `ProviderError` keeps a category, status, and request ID, never provider text. Timeouts, lost connections, 5xx, 408, and unreadable responses are ambiguous; 429 and 529 are throttles with no work done; other 4xx are permanent. The default model is `claude-opus-5-5`. The final answer is a tool call rather than a structured output format, and no server-side refusal fallback is set. Compose passes the key to `investigation-worker` only.

**Reason:** Each retry is a possibly billable call, so the gateway has to see and reserve for every one, which SDK retries would hide. An error body can quote a request, and a request carries evidence. A terminal tool keeps validation and repair in one place, on any provider. A fallback model may cost more than the reserved model, and a refused call can bill both, so it would break the rule that a reservation bounds a call's cost.

**Trade-off:** A refusal ends the turn with no answer, so the agent abstains on it. Claude thinking blocks must return unchanged within a conversation, so they stay in memory as `ProviderBlock`s and a restarted node starts its conversation again. Provider behavior is verified against recorded response shapes over a stub transport, not against the live API, because no paid call was authorized.

**Revisit when:** A paid evaluation is authorized (P10), or refusals turn out to matter on incident evidence; then reserve for primary plus fallback and enable fallbacks.

## ADR-026: The gateway reserves each call's maximum, and Redis only meters the provider

**Decision:** `ModelGateway.complete` takes a Redis rate-limit slot, then one transaction checks the lease and `stop_reason`, closes pending calls of older leases as `unknown`, and reserves the call's maximum cost. `hold_budget` is a conditional `UPDATE` on `reserved_microusd + spent_microusd + amount <= budget_microusd`. The maximum assumes every request byte is a token written to the cache and all of `max_output_tokens` is used. A success replaces the reservation with its cost, a definite failure releases it, and an ambiguous failure keeps it and records no cost. Each retry is a new `llm_calls` row. A call that no longer fits raises `BudgetExceededError`, which ends the job `failed` and the investigation `budget_exceeded`. Ending an investigation closes its pending calls as `unknown`. Prices are integer microUSD per million tokens in `gateway/pricing.py`, stored by `PRICING_VERSION`, and a model with no price cannot be called. Revision `0016` adds the version, cache tokens, the lease generation, an error category, and `completed_at`.

**Reason:** A conditional update needs no read and no lock across the call, so in-flight parallel calls count and none can pass the limit. Bytes bound tokens without a tokenizer or a paid count call. A call's reservation is the only record of money a dead worker may have spent, so it stays until something proves otherwise. Reserving after the rate-limit wait means a stop committed during a pause still prevents the call. Rate limits protect the provider's quota, so Redis can lose them without touching money.

**Trade-off:** Reservations run far above typical cost, so a budget can end an investigation while real spend is well under it. Unknown reservations never return, so `reserved_microusd` after an ending is exposure, not spend. Prices are copied by hand and can go stale; `cost_basis` `actual` means provider-reported tokens at those prices, not an invoice. Fake costs are `estimated`. Rate-limit tokens are an estimate made before the call. Settling is not fenced by the lease, because a stale worker's answer is still a fact about money, but a call already closed stays closed.

**Revisit when:** Measured reservation error ends investigations early (use `count_tokens`, which needs an authorized call), a second worker region shares the limits, or Phase 9 reserves downstream capacity.

## ADR-027: Tools take typed arguments, and code alone supplies scope and cutoff

**Decision:** `agents/tools.py` defines one argument model per tool with unknown fields forbidden. `ToolScope` carries the workspace, replay scope, and cutoff, and only the workflow builds it, from the investigation row. A signal is visible when both `occurred_at` and `ingested_at` are at or before the cutoff. A window past the cutoff is rejected, never clamped. Windows are at most 6 hours, a call returns at most 50 rows and 16 KiB, and a read has a timeout. Failures return a `ToolFailure` to the agent and record a failed `tool_calls` row, whose result is only a hash. Free text passes through `TextRedactor`, which masks `key=value` pairs of the configured sensitive fields and bearer tokens. Runbooks are graph nodes keyed `{key}@{version}`, with their content in `properties` and `DOCUMENTED_BY` edges from `published_at`.

**Reason:** A model cannot name what it has no field for, so authorization needs no prompt to hold. `ingested_at` matters because a late signal carries an old `occurred_at`, which would let a replay see evidence that had not arrived. A rejection teaches the agent its limit, while a silent clamp would let it believe it had seen a window it had not. Keeping results out of `tool_calls` leaves one copy of evidence, under retention rules.

**Trade-off:** Masking free text is a pattern match, so a secret in an unusual form still reaches the provider. Source-specific redaction fields do not apply to text. Runbooks are copied per replay scope, so a replay has none until P10 seeds them. Resolving the latest version happens in Python over every runbook node, which is fine for a handful.

**Revisit when:** Evaluation shows the caps causing abstentions, or runbook counts grow past a few dozen.

## ADR-028: Workflow `v2` is one bounded agent whose answer is a tool call

**Decision:** `WORKFLOW_VERSION` becomes `v2`, keeping the step keys `load_context`, `investigate`, and `conclude`. `v1` stays in `WORKFLOWS` for existing rows. `investigate` runs `agents/investigator.py`: a loop of gateway calls and evidence tools that ends when the model calls `submit_findings` with a valid `ProposedDiagnosis`. An unusable reply goes back with field locations only, up to `SIGNALOPS_AGENT_MAX_REPAIRS` times. Past that, on a refusal, or out of turns, the agent abstains. A reply cut off at `max_tokens` is dropped, because it may hold a partial tool call. Turns are `max_steps` minus the workflow's nodes, so a request's `max_steps` bounds model use and a limit reached by the agent abstains rather than fails. Each step allows 10 tool calls and each turn 5. `conclude` names the service from the catalog, so the model never supplies a name that reaches the diagnosis. Running out of budget still ends `budget_exceeded`.

`IncidentBrief` gained the first window, the workspace's services, and detector-cited signals known by the cutoff. In fake mode `gateway/fake_investigator.py` plays the loop by a fixed rule on real evidence.

**Reason:** A fixed workflow, one agent, and a code-checked answer are the slice the plan asks for before specialists. Abstaining on an exhausted limit keeps a cost or shape problem from reading as a system failure, and it is the honest answer when the model produced no support. Keeping step keys means the progress history and UI need no changes.

**Trade-off:** The conversation is memory only, so a node restarted after a crash or a failed retry pays for its turns again; only a completed step is reused, and `completed_tool_calls` stays empty. Reservations grow with the conversation, so ten full tool results can approach the default 1 USD budget and end `budget_exceeded` although real spend is lower. A `max_steps` below the node count still fails the job, because the conclusion cannot run. The fake's result shows the plumbing works and says nothing about diagnostic quality.

**Revisit when:** Phase 9 splits the agent into specialists, or a paid run measures reservations against real usage.

## ADR-029: Citations are verified in PostgreSQL, and cached reads prove their identity

**Decision:** `verify_findings` checks that a diagnosed result's service is in the workspace and that every cited signal is in the investigation's scope and known by its cutoff. It runs on submission, so the model gets a chance to correct itself, and again in `conclude`, where a failure abstains. It always reads PostgreSQL. Tool results may come from Redis under `cache:query:{format}:{workspace}:{replay scope}:{cutoff}:{data version}:{tool}:{argument hash}` for 60 seconds. The value repeats its identity, and a read compares that and revalidates the result as a contract, so a mismatch is a miss. The data version is the count and latest `ingested_at` of signals of the last hour known by the cutoff. An unreachable Redis is a miss.

**Reason:** Code, not the model, decides whether a citation holds. Checking twice covers a stored proposal being reused after a restart, and abstaining beats storing an unverifiable cause. A key alone protects against collisions by construction, and the stored identity plus revalidation also handle a value written by a bug or by another format. The data version covers the case that matters at a fixed cutoff: a signal ingested before the cutoff that commits after it because the normalizer lagged.

**Trade-off:** The version is a query per tool call, so a hit saves little work on a small read, and hits are rare because each investigation has its own cutoff; the cache mainly serves retries and repeats within a minute. The version does not see evidence older than an hour committing late, graph edges, or runbook changes, so such a change can take up to the TTL to show. It reports only that citations exist in scope, not that they support the claim, which is the verifier's job in Phase 9 and evaluation's in P10.

**Revisit when:** Phase 9 adds claim-level verification, or measurements show the cache does not earn its query.

## ADR-030: Progress streams replay PostgreSQL, and an unknown cursor resets

**Decision:** `GET /v1/investigations/{id}/events` serves `progress_events` as SSE with each row's ID as the event `id`. It replays after `Last-Event-ID` (or `after`) in ID order, then reads again on a Redis wake-up on `ui:investigation:{id}` or every `SIGNALOPS_SSE_CATCHUP_SECONDS`, sending a keep-alive comment when nothing is new. A cursor that names no event of this investigation (from another investigation, or removed by future retention) gets `event: reset` and the whole retained history, and the client discards what it held. The stream ends after `investigation.ended` and after `SIGNALOPS_SSE_MAX_STREAM_SECONDS`; the client reconnects with its cursor. Wake-ups publish after commit: `ProgressRepository.add` marks the session, and `Database.transaction()` passes the marked investigations to an `on_progress` hook, `RedisProgressNotifier` in the worker and the API. The browser reads the stream with `fetch`, because `EventSource` cannot send the operator token.

**Reason:** IDs are ascending in commit order within an investigation (ADR-024), so "after the cursor" never skips a committed event. A reset is safer than an error: the client recovers on its own, and the history is small. Publishing after commit means a woken reader always finds the row; a rollback publishes nothing. Catch-up bounds the delay of a lost wake-up, so Redis loss slows the stream and never stalls it. A bounded lifetime keeps no connection, and no subscription, open indefinitely.

**Trade-off:** Each open stream holds a Redis pub/sub connection and reads PostgreSQL every catch-up interval, capped by `SIGNALOPS_SSE_MAX_STREAMS` per API process. A wake-up may be published for a commit a reader has already read, which costs one empty read. The reset cannot tell an expired cursor from a foreign one; both resync. Detecting expiry precisely needs retention metadata, which P11-T05 adds if it trims history.

**Revisit when:** many viewers watch the same investigations (share one subscription per process), or P11-T05 trims progress history.

## ADR-031: Workflow `v3` splits the agent into roles with code-built briefs and code-checked handoffs

**Decision:** Workflow `v3` (`agents/multi_agent.py`) runs `load_context`, `plan`, the specialists `telemetry`, `deployment`, and `dependency`, then `synthesize`, `verify`, and `conclude`. The coordinator is code, not a model: `plan` reads the incident's services and the investigation's limits and stores an `InvestigationPlan` holding each specialist's brief, turns, and budget share, and the reserve held back for synthesis and verification. Each specialist runs the shared `AgentLoop` (`agents/agent_loop.py`) with its own prompt (`telemetry-v1`, `deployment-v1`, `dependency-v1`), two tools, and a `SpecialistReport` of findings, each with a confidence and a required uncertainty. The synthesizer (`synthesizer-v1`) and verifier (`verifier-v1`) get no tools. The synthesizer reads only the specialists' reports and how each branch ended. The verifier reads the synthesis and the leading hypothesis's citations, which code reads again from PostgreSQL within scope and cutoff and redacts. Code checks every handoff and returns a refusal to the model as a repair: a specialist may cite only signal IDs and runbook versions its own tool results returned; the synthesis may cite only evidence a specialist cited and credit only roles that found something; the verifier must judge exactly the leading citations, and cannot accept one code could not read. `conclude` keeps only the citations the verifier accepted, verifies them once more as ADR-029 does, and names the service from the catalog. Stored outputs carry `Provenance`: role, prompt version, model, turns, tool call keys, signals read, and how the loop ended. A role out of turns, repairs, or budget reports what is missing instead. With no findings at all, synthesis and verification make no call. Turns are `max_steps` minus 8 nodes, allotted to synthesis (2) and verification (2) first, then up to 5 per specialist. New investigations run `SIGNALOPS_INVESTIGATION_WORKFLOW_VERSION` (`v3` by default; `v2` stays available for comparison in P10). Defaults rise to 40 steps and 3 USD, because three branches hold reservations at once. In fake mode, `gateway/fake_roles.py` plays each role by a fixed rule, chosen by prompt version.

**Reason:** Planning which three specialists to run needs no model, so a code coordinator saves a call and makes each role's context inspectable in `agent_steps.output`. Narrow tools and briefs per role keep a role from wandering into another's evidence, and a code check at each handoff means no role can introduce a citation that nothing read. Giving downstream roles turns and money first keeps the specialists from leaving nothing to finish with.

**Trade-off:** Eight nodes and five prompts cost more calls than `v2`; P10 must show whether that buys quality. A specialist cites only what its own loop read, so a branch restarted after a crash rereads before it can cite. The verifier judges support from a short redacted text per signal, which can miss context. The fake roles prove the handoffs and checks, not diagnostic quality. P09-T02 and T03 add parallel branches, budget shares, contradictions, and the follow-up round, so `v3` was not final until the Phase 9 gate.

**Revisit when:** P10 compares `v2` and `v3` under equal budgets, or a real model shows a role needs other tools or context.

## ADR-032: Specialists run as one parallel stage under process-wide slots and durable budget shares

**Decision:** A workflow stage may be `Parallel`: one to three nodes that run at once, each its own step, none reading another's output (`MAX_PARALLEL_BRANCHES` = 3 is the per-investigation limit, checked when the workflow is built). `v3` runs its three specialists this way. Each branch waits on the engine's `branch_slots` semaphore before its step opens. One engine serves every `JobRunner` in a worker process, and the worker runs `SIGNALOPS_WORKER_JOB_CONCURRENCY` runners, so `SIGNALOPS_AGENT_GLOBAL_BRANCHES` bounds the process's branches; a branch over the limit waits, and a job over the runner count stays queued in PostgreSQL. The engine lets every branch of a stage finish, so a failure loses no sibling's work, then raises the most decisive failure: a cancellation, a stop, a lost lease, an exhausted budget, a permanent error, then a retryable one. Money stays one durable budget in PostgreSQL. `ModelGateway.complete` takes an optional `BudgetLimit`: `held_back` must stay free (the conditional `hold_budget` update subtracts it from the budget), and `step_cap` bounds what the step's calls hold and spend together across attempts, read under the job row lock. The plan holds back one synthesis and one verification call at the most a 48 KiB request with the configured output tokens can cost, and splits the rest into equal branch shares. A call that does not fit raises `BudgetShareExhaustedError`, which ends that branch with `ended_by: budget_share`; synthesis and verification draw on the whole budget, and only their failure to fit ends the investigation `budget_exceeded`.

**Reason:** A fixed stage keeps branching inside the explicit state machine, with no spawned tasks beyond its nodes. A semaphore is enough to bound one process. The fleet-wide bound on model use already exists in Redis (ADR-026), and a lease-based PostgreSQL semaphore would add renewals for little gain at this scale. Holding money back for later roles means a greedy specialist cannot leave nothing to verify with, and a share keeps one branch from starving the others. Letting siblings finish turns a transient failure into a retry of one branch.

**Trade-off:** The global limit is per process, so the fleet runs up to processes × `SIGNALOPS_AGENT_GLOBAL_BRANCHES` branches, bounded in total only by the provider rate limit. A branch waiting for a slot holds its job lease and its turn in the deadline. The reserve is an estimate from bounded contracts: a larger downstream request still reserves its real maximum and may not fit. A share counts every attempt of its step, so a branch retried after an ambiguous timeout has less left, which is the conservative side. Letting siblings finish delays the retry by the slowest branch.

**Revisit when:** several worker processes share a provider quota tightly enough to need a fleet-wide branch limit, or measured reservations show the reserve or shares are badly sized.

## ADR-033: Code records contradictions, and the verifier gets at most one follow-up round

**Decision:** `synthesize` compares the specialists' findings in code and records every pair of roles naming different failing services at confidence 0.3 or more (`SynthesisOutput.detected_contradictions`). The diagnosis carries the synthesizer's contradictions, then code's that it did not already list, in a new `Diagnosis.contradictions` field (additive, default empty), whether it names a cause or abstains; the verifier sees them too, and the UI shows them. The workflow gains two nodes, `follow_up` and `reverify`, between `verify` and `conclude`. The first verification is offered a follow-up (`follow_up_allowed`) only when the plan left it at least 2 turns and the remaining budget and deadline, read from PostgreSQL's clock, fit one more specialist call and one more verification call at their maximum (twice the downstream call reserve, twice the request timeout). The verifier may then answer `needs_follow_up` with a specialist role and a question; code refuses that verdict when it was not offered. `follow_up` checks the room again, then runs that specialist once with its original brief plus the question, its calls holding back one verification call. `reverify` judges the leading citations plus the follow-up's citations for the leading service, and is never offered a follow-up. `conclude` uses the second verification if a round ran, and abstains if a round was granted but could not run. Turns go to synthesis, both verifications, the branches, then up to 3 for the follow-up. The verifier prompt became `verifier-v2`.

**Reason:** A synthesizer that smooths a disagreement away should not be able to hide it, so code finds the simplest kind itself: two roles blaming different services. One fixed `follow_up` node makes the bound structural: there is no edge back, and no output can add one. Checking room before offering and again before running keeps a follow-up from spending what the final verdict needs.

**Trade-off:** Code finds only service-level disagreement; contradictions inside one role's findings, or about failure type, depend on the synthesizer. The room check covers the first call of each, so a long follow-up may stop on its share and leave the second verification to judge less. Two more nodes mean `max_steps` must be at least 10 for `v3` to finish. A round granted and then refused for room ends in abstention rather than the first verdict, because the verifier said the evidence was not yet enough.

**Revisit when:** P10 shows whether follow-up rounds change outcomes enough to pay for themselves, or a real model surfaces contradictions code cannot see.

## ADR-034: Ground truth lives in its own schema, which the runtime role cannot read

**Decision:** Evaluation tables are in PostgreSQL schema `evaluation`: `scenarios` (immutable per name and version, with injection spec, expected outcome, and dataset hash), `runs` (a split, variants, trials, seed, workflow and prompt versions, model, pricing, and limits), `replays` (one replay scope per scenario per run), and `cases` (one per scenario, variant, and trial). Migration 0017 creates group roles `signalops_runtime`, with read and write on `public` and nothing on `evaluation`, and `signalops_evaluator`, with both. Default privileges extend each to later tables. `signalops migrate` provisions the login roles from configured passwords. Compose runs every application process as the runtime login, and only evaluation commands use `SIGNALOPS_EVALUATION_DATABASE_URL`.

**Reason:** Tools already cannot name a table, but a guarantee that rests on PostgreSQL's privileges holds even against a bug or a crafted query on the runtime connection. One schema makes the boundary a single grant to check, and default privileges mean a new table never quietly lands on the wrong side. A run covering a whole split and every variant, with one replay per scenario, gives the variants identical evidence, which is what a comparison under equal constraints needs.

**Trade-off:** Every application process now needs a provisioned login. Locally, a missing password falls back to the owner's, which suits only a development machine. Roles are cluster-wide, so the downgrade revokes grants but leaves the roles. The plan's one-scenario, one-variant `evaluation_runs` became `runs` plus `replays` plus `cases`.

**Revisit when:** P11-T02 adds operator authentication and secret references, or evaluation needs per-run isolation beyond replay scopes.

## ADR-035: A replay is complete when PostgreSQL shows its work drained, on simulated time

**Decision:** The evaluation replay publishes a dataset through `ingest_batch` with a replay run ID, so event IDs derive from the scope. It ingests in event-time ticks, received at the tick's end plus a fixed delay. It then polls PostgreSQL until normalizer receipts cover every accepted raw event, every normalized event of the scope is published, and detector and graph-projector receipts cover every normalized event. Only then does it flush the scope's windows. Otherwise it stops at a timeout and reports the raw events never normalized, without flushing. The same markers judge `KafkaTransport`, which feeds the running services, and `LocalTransport`, which runs the processors in process.

**Reason:** Receipts and outbox state are the durable record of processed work, so they are exact where a sleep or lag reading is a guess. Kafka lag says nothing about which scope a record belongs to. Simulated ingestion time keeps the evidence cutoff meaningful: an investigation at the incident's event-time detection cannot see records that arrived later. Deriving IDs from the scope makes reruns safe and keeps parallel replays apart.

**Trade-off:** A record the normalizer quarantines has no receipt, so its replay times out rather than completing with a footnote. That is deliberate, but a slow quarantine path costs the whole timeout. Each poll counts rows by scope, which is fine for evaluation-sized datasets but not for the million-signal benchmark (P11-T04). Ticks give `ingested_at` up to 3 seconds of artificial delay.

**Revisit when:** P11-T04 replays a million signals, or quarantined records need to count as reconciled.

## Open questions

| # | Question | Decide by |
| --- | --- | --- |
| Q1 | ~~Which Python Kafka client?~~ **Resolved (P01-T04):** `confluent-kafka` (librdkafka): manual offset commits, idempotent producer, per-message delivery callbacks. Blocking calls run in worker threads from async code. | P01-T03 / P03-T01 |
| Q2 | ~~Exact Kafka, PostgreSQL, and Redis image versions after the startup spike~~ **Resolved (P01-T03):** `apache/kafka:4.3.0`, `postgres:17.11-alpine`, `redis:7.4.12-alpine`; see development.md | P01-T03 |
| Q3 | ~~Frontend query layer and routing libraries~~ **Resolved (P01-T04):** TanStack Query 5 for server data, React Router 7 (data router) for routing | P01-T04 |
| Q4 | ~~Expired or unknown SSE cursor behavior (full resync vs error)~~ **Resolved (P08-T03):** full resync: `event: reset`, then the whole retained history; see ADR-030 | P08-T03 |
| Q5 | ~~Idempotency-key storage table and retention window~~ **Resolved (P06-T01):** `idempotency_keys`, retained with the investigation, no expiry; see ADR-020 | P06-T01 |
| Q6 | ~~Diagnosis status ↔ investigation terminal status mapping~~ **Resolved (P02-T05):** one-to-one. `completed` ⇔ `diagnosed`, `insufficient_evidence` ⇔ `insufficient_evidence`, no diagnosis otherwise; enforced in contracts, a database CHECK, and the generated UI module. See data-model.md | P02-T05 |
| Q7 | ~~Detector window closure metadata schema~~ **Resolved (P05-T02):** columns on `signal_aggregates` plus `detection_clocks`; see ADR-016 | P05-T02 |
| Q8 | ~~Which real model provider is implemented first, and its pricing provenance format~~ **Resolved:** Anthropic (ADR-025); pricing in ADR-026 | P07-T01 / P07-T02 |
| Q9 | Operator authentication mechanism before public exposure. Interim: static per-workspace tokens (ADR-014) | P11-T02 |
| Q10 | Whether the OpenTelemetry Demo fits local resources, or a smaller synthetic app suffices for the `demo` profile | P10-T03 |
| Q11 | ~~Planning files live in `docs/`, but `CLAUDE.md` expects them at the repository root. Move them, or update `CLAUDE.md`?~~ **Resolved (Phase 8 gate):** the files stay in `docs/`; `CLAUDE.md` points at `docs/development_roadmap.md` and `docs/SignalOps_Implementation_Plan.md` | Before P01-T01 |
