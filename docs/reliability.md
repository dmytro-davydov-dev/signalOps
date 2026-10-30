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
- A failed delivery leaves the row pending and increments `attempts` (also incremented when the row is marked published). A transient failure sets `next_attempt_at` by capped, jittered backoff; a broker rejection sets `quarantined_at`. `OutboxRepository.pending` skips a row while it or an earlier pending version of its aggregate is deferred or quarantined, so order holds across batches. `signalops redrive-outbox` clears a quarantine.

## Job leases

- The claim is a short transaction: `SELECT … FOR UPDATE SKIP LOCKED`. It sets `lease_owner` and `lease_until`, increments `lease_generation`, and commits.
- The worker heartbeats while running. Expired leases can be reclaimed by another worker.
- Every checkpoint and completion write is conditional on owner and generation.
- After losing its lease, a worker may still have a model call in flight. Provider deadlines and conservative budget reservations bound the damage.

## Detector state

- The receipt insert and the aggregate update share one transaction, so a Redis-only window would not survive a restart.
- Events up to **30 seconds late** update their window, and the rule is recomputed idempotently.
- Older events are kept as evidence with a late flag. They never silently create historical incidents.
- Live mode closes windows by wall clock. Replay uses a simulated clock plus a final flush.

## What survives what

| Failure | Expected outcome |
| --- | --- |
| Crash after DB commit, before offset commit | Redelivery, receipt hit, one logical effect |
| Outbox crash after broker ack | Duplicate publish, deduplicated downstream |
| Worker killed after a completed step | Lease expires, another worker resumes at the next unfinished step, stale writes rejected |
| Redis restart or empty volume | Cache misses and briefly paused model calls. Investigations, windows, and SSE history intact |
| Provider ambiguous timeout | Unknown cost recorded, reservation retained, retry reserved separately |
| Kafka outage during ingest | No false acceptance. Client retries with the same IDs |
| Permanent validation failure | Quarantined to `processing.failed.v1`, original not lost; redrivable while retained |
| Failure event cannot be published | Original offset not committed; retried, then the consumer stops |
| Database down during consumption | Partition paused and retried with backoff, then the consumer stops at the same offset; nothing quarantined |

## Verification checklist

These come from the plan's §13. Each needs a meaningful automated or scripted check before it can be claimed.

- [ ] Contract fixtures: missing fields, unsupported versions, malformed timestamps, oversized payloads, contradictory service identity.
- [x] Duplicate Kafka message plus a crash between DB commit and offset commit leaves one logical effect (P03-T02).
- [x] Outbox crash after broker ack causes a harmless duplicate (P03-T03).
- [x] Permanent failures are quarantined to `processing.failed.v1`; an unacknowledged failure event never commits the original; transient failures retry in place; a redriven record is applied once (P03-T04).
- [ ] Worker killed after one step: lease expiry, checkpoint reuse, stale-writer rejection.
- [ ] Provider throttling, ambiguous timeout, malformed output, parallel budget exhaustion.
- [ ] Redis restart mid-investigation: durable work survives and SSE catches up.
- [ ] Late and out-of-order signals, plus replay isolation. No access to future evidence or ground truth.
- [ ] Reconciliation: every accepted in-retention record ends processed or explicitly quarantined.

## Known limits

- Single-node Kafka (replication factor 1) and local volumes: no protection against host or disk loss.
- Docker Compose is a development environment, not production high availability.
- Backup and restore procedures are documented in P11-T05.
