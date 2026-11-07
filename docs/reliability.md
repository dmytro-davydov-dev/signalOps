# Reliability and recovery

> Status: planned guarantees, not yet verified. Sources: [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §4, §8, §13, and roadmap Phases 3, 6, 9, and 11. A guarantee counts as met only after its test runs against real PostgreSQL and Kafka.

## Delivery model

SignalOps assumes **at-least-once** delivery everywhere and makes its effects idempotent. It never claims exactly-once processing of database writes or external model calls.

| Mechanism | Purpose |
| --- | --- |
| Stable `source_event_id` and `event_id` | Retries and republishes carry the same identity |
| `consumer_receipts(consumer_group, event_id)` | Inserted in the same transaction as the effect, so redelivery is a no-op |
| Business uniqueness constraints | One logical signal, incident, job, or step regardless of duplicates |
| Transactional outbox | Database changes and the events they emit cannot diverge |
| Manual offset commits | An offset advances only after durable processing |
| Leases with generation fencing | A stale worker cannot overwrite newer progress |

## Consumer transaction pattern

```text
poll record
  BEGIN
    INSERT consumer_receipt         -- conflict ⇒ already processed, skip effect
    apply domain effect
    INSERT outbox_event(s)
  COMMIT
commit Kafka offset (next)
```

- Implemented in `streaming/consumer.py` (`ConsumerRunner`, `TransactionalProcessor`) and `persistence/repositories/reliability.py` (`ReceiptRepository`, `OutboxRepository`). Verified against Compose PostgreSQL and Kafka by `tests/integration/test_consumer_transactions.py`.
- Records are processed one at a time per consumer, so the next poll waits for the previous offset commit.
- A receipt insert that races a concurrent uncommitted insert of the same receipt waits for it, then reports a duplicate.
- A handler failure rolls back the transaction, including the receipt.
- A permanent failure (`ContractError`) is published to `processing.failed.v1`; the offset is committed only after that event is acknowledged. A transient failure pauses and rewinds the partition, retries with backoff while the runner keeps polling, and after the last attempt stops the runner without committing. See [events.md](events.md#failure-handling).
- Lifecycle: a stop (SIGTERM/SIGINT once an entry command wires it) is checked between records, so the record in hand finishes through its offset commit before the consumer closes and leaves the group. A kill at any point is covered by the redelivery rule above. Verified by `tests/integration/test_streaming_lifecycle.py`.
- Rebalance: callbacks run inside `poll`, when no record is in progress, so there is nothing to finish or commit on revoke. A revoked or lost partition's pending retry is dropped and its pause lifted (librdkafka keeps a pause across a revoke, so a partition reassigned to the same member would otherwise never be fetched); the next owner starts at the committed offset.
- If parallelism is added later, only the contiguous completed offset frontier is committed.
- No transaction stays open during a broker call or a model call.

## Outbox publisher

```text
SELECT pending rows (bounded, ordered by aggregate version)
publish each with stored event_id and key   -- outside any transaction
on ack: UPDATE published_at
```

- Implemented in `streaming/outbox.py` (`OutboxPublisher`, run by `signalops outbox-publisher`) with `OutboxRepository.pending`, `mark_published`, `schedule_retry`, `quarantine`, and `redrive`. Verified against Compose PostgreSQL and Kafka by `tests/integration/test_outbox_publisher.py`.
- A single publisher in the MVP keeps each aggregate's events in order. Compose runs one `outbox-publisher`; nothing yet prevents a second instance, which would publish duplicates and could reorder.
- Within a batch, rows go out in rounds: each aggregate's lowest pending version first, all its topics together. An aggregate whose row fails sits out the rest of the batch, so a later version is never acknowledged before an earlier one.
- `OutboxRepository.add` sets `created_at` with `clock_timestamp()`, so when versions are allocated under the aggregate's row lock, a later version also sorts later across batches.
- A crash after the ack but before `published_at` is set causes a republish with the same event ID and key. Consumers deduplicate.
- A stop is checked between publish rounds; rows not yet sent stay pending for the next process. Compose gives the publisher a stop grace period longer than the delivery timeout, so an in-flight round normally finishes; a kill mid-round republishes acknowledged rows.
- A failed delivery leaves the row pending and increments `attempts` (also incremented when the row is marked published). A transient failure sets `next_attempt_at` by capped, jittered backoff; a broker rejection sets `quarantined_at`. `OutboxRepository.pending` skips a row while it or an earlier pending version of its aggregate is deferred or quarantined, so order holds across batches. `signalops redrive-outbox` clears a quarantine.

## Job leases

- The claim is a short transaction: `SELECT … FOR UPDATE SKIP LOCKED`. It sets `lease_owner` and `lease_until`, increments `lease_generation` and `attempts`, and commits. Lease times come from PostgreSQL's clock.
- The worker heartbeats three times per lease (`SIGNALOPS_JOB_LEASE_SECONDS`, default 30). Expired leases can be reclaimed by another worker, which records `lease_expired` in `last_error`.
- Every checkpoint and completion write goes through `JobRepository.hold`, which locks the job row after checking owner and generation. The write commits in that transaction, and a claim skips the locked row, so a takeover cannot fall between the check and the write. An expired lease nobody took stays valid.
- A worker that cannot reach PostgreSQL keeps trying until its last renewal runs out, then cancels the handler.
- Each claim spends an attempt, so a crash loop stops at `max_attempts` (default 3). The next claim fails that job and its investigation, with the `investigation.completed` outbox row in one transaction. A retry waits out a capped, jittered delay in `available_at`. A stopping worker releases its job and refunds the attempt.
- `last_error` holds a category and the exception type. Only `TransientJobError` and `PermanentJobError` store a message, because other exception text can quote logs or evidence.
- After losing its lease, a worker may still have a model call in flight. Provider deadlines and conservative budget reservations bound the damage.

## Workflow recovery

- `WorkflowEngine` (`agents/workflow.py`) opens a step, runs its node outside any transaction, and ends the step. Both writes go through `JobRepository.hold`.
- A new attempt reloads the steps, reuses completed output after validating it, and runs the rest. A step left `running` or `failed` is reopened under the new lease.
- A node may run twice if its worker dies inside it. Only completed output is guaranteed to be reused.
- Completed output that fails validation, or that another workflow version wrote, fails the job. A completed step never runs again.
- Verified by `tests/integration/test_workflow_recovery.py`: a real process is sent SIGKILL inside the second node, and `signalops worker` resumes it after the lease expires, keeping the first step's row unchanged.

## Detector state

- The receipt insert and the aggregate update share one transaction, so a Redis-only window would not survive a restart.
- Two checks run before an aggregate changes: the consumer receipt per event, then the `detection_samples` key per (signal, series). A signal redelivered under another event ID adds nothing (P05-T01).
- Aggregates change by one upsert that adds to the stored counts and histogram, so concurrent writers to one window serialize on its row. A transaction-scoped advisory lock per (service, scope, rule) serializes the cooldown check and the trigger insert; the trigger unique key backs it.
- Cooldowns compare window starts (event time), so a replay reaches the same triggers as the live run. Windows of a service close oldest first, which keeps the cooldown decision the same whatever the arrival order.
- Events up to **30 seconds late** update their window. Rules run at closure, and an on-time sample that finds its window closed re-evaluates it; the trigger unique key makes that idempotent (ADR-016).
- Older events are kept as evidence with a late flag (`detection_samples.late`, `late_count`). They never reach a rule, so they cannot silently create historical incidents.
- Live mode closes windows by wall clock (`detector-closer`). Replay uses a durable per-service simulated clock plus a final flush (`detector-flush`).
- Lateness comes from stored timestamps, so redelivery gives the same answer and consumer lag does not make a signal late.
- Closure state is the window row: a restart finds open windows there, and a crash mid-closure rolls the batch back, leaving them open. Closers lock rows with `FOR UPDATE SKIP LOCKED`; a sample and a closure on one window serialize on its row, so neither misses the other.
- A trigger, its incident change, and the `incident.changed` outbox row share one transaction, so a crash leaves all or none. A transaction-scoped advisory lock per replay scope makes each correlation decision alone, and `uq_incidents_correlation` backs it (ADR-017).
- The graph projector has its own consumer group and receipts. Edges for one relationship take an advisory lock, and `ex_graph_edges_validity` rejects overlap, so concurrent projectors merge. Edges come from event time, so redelivery and reordering give the same intervals. A new group rebuilds only what `signals.normalized.v1` still retains (7 days).
- Recovery comes from closed windows, in event time. A service with no traffic shows no recovery, so detector or source downtime cannot resolve an incident.
- After detector downtime the closer can close a window before its backlog arrives. The backlog re-evaluates it, but a trigger raised from partial counts is not withdrawn.

## Streaming stats

Consumers and the outbox publisher log one structured line every `SIGNALOPS_STREAMING_STATS_INTERVAL_MS` (default 30 s) and once on shutdown. Counts cover the interval since the previous line; redeliveries are counted again.

| Message | Fields |
| --- | --- |
| `consumer stats` | `consumer_group`, `interval_seconds`, `consumed`, `applied`, `duplicates` (receipt hits), `quarantined`, `retries_scheduled`, `processing_ms_avg`/`processing_ms_max` (poll return to offset commit, committed outcomes only), `paused_partitions`, `lag` per assigned partition and `lag_total` |
| `outbox stats` | `interval_seconds`, `outbox_published`, `outbox_failed`, `outbox_quarantined`, `outbox_deferred`, `outbox_pending` (all unpublished rows), `outbox_pending_quarantined`, `outbox_oldest_age_seconds` |

- Lag is the high watermark minus the group's committed offset, queried from the broker at report time; without a committed offset the low watermark is used. It is `null` when the broker does not answer in time. A rising `lag_total` or `outbox_oldest_age_seconds` across lines is backlog growth.
- These are logs, not a metrics endpoint. Exported metrics and dashboards are P11-T01.

## What survives what

| Failure | Expected outcome |
| --- | --- |
| Crash after DB commit, before offset commit | Redelivery, receipt hit, one logical effect |
| Outbox crash after broker ack | Duplicate publish, deduplicated downstream |
| Worker killed after a completed step | Lease expires, another worker resumes at the next unfinished step, stale writes rejected |
| Redis restart or empty volume | Cache misses and briefly paused model calls. Investigations, windows, and SSE history intact |
| Provider ambiguous timeout | Unknown cost recorded, reservation retained, retry reserved separately |
| Kafka outage during ingest | No record reported accepted; 503 with per-record `failed` (`ambiguous_timeout` when the broker may have written it). Client retries with the same `source_event_id`s and gets the same event IDs. Verified against a stopped Compose broker (P04-T02) |
| Permanent validation failure | Quarantined to `processing.failed.v1`, original not lost; redrivable while retained |
| Failure event cannot be published | Original offset not committed; retried, then the consumer stops |
| Consumer stopped or rebalanced mid-stream | Record in hand committed or redelivered; pending retry state dropped with the partition |
| Database down during consumption | Partition paused and retried with backoff, then the consumer stops at the same offset; nothing quarantined |

## Verification checklist

These come from the plan's §13. Each needs a meaningful automated or scripted check before it can be claimed.

- [ ] Contract fixtures: missing fields, unsupported versions, malformed timestamps, oversized payloads, contradictory service identity.
- [x] Duplicate Kafka message plus a crash between DB commit and offset commit leaves one logical effect (P03-T02).
- [x] Outbox crash after broker ack causes a harmless duplicate (P03-T03).
- [x] A consumer stopped mid-stream resumes after its last committed record with nothing skipped or repeated; a revoke drops a pending retry and the reassigned partition is fetched again (P03-T05).
- [x] Permanent failures are quarantined to `processing.failed.v1`; an unacknowledged failure event never commits the original; transient failures retry in place; a redriven record is applied once (P03-T04).
- [x] Worker killed after one step: lease expiry, checkpoint reuse, stale-writer rejection (P06-T03).
- [ ] Provider throttling, ambiguous timeout, malformed output, parallel budget exhaustion.
- [ ] Redis restart mid-investigation: durable work survives and SSE catches up.
- [ ] Late and out-of-order signals, plus replay isolation. No access to future evidence or ground truth.
- [ ] Reconciliation: every accepted in-retention record ends processed or explicitly quarantined.

## Known limits

- Single-node Kafka (replication factor 1) and local volumes: no protection against host or disk loss.
- Docker Compose is a development environment, not production high availability.
- Backup and restore procedures are documented in P11-T05.
