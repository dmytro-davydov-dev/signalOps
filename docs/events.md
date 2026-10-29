# Kafka event contracts

> Status: planned design. The source is [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §4. Topic provisioning is roadmap task P03-T01, and typed contracts are P02-T04 and P02-T05.

## Topics

| Topic | Message key | Consumer groups | Initial partitions |
| --- | --- | --- | --- |
| `signals.raw.v1` | `source_id` + service hint | `normalizer-v1` | 3 |
| `signals.normalized.v1` | `workspace_id` + `service_id` | `detector-v1`, later `graph-projector-v1` | 3 |
| `incidents.changed.v1` | `incident_id` | `notification-projector-v1` | 1 |
| `investigations.requested.v1` | `investigation_id` | `dispatcher-v1` | 1 |
| `investigations.completed.v1` | `investigation_id` | `evaluation-projector-v1` | 1 |
| `processing.failed.v1` | original `event_id` | failure triage, controlled redrive | 1 |

- Ordering is guaranteed **only within a partition**. Nothing assumes a global order across sources.
- Each consumer group receives every event independently. Members of one group split the partitions between them.
- Changing a key or a partition count requires a review of ordering assumptions. Measure before you resize.

## Common envelope

```json
{
  "event_id": "e7cacb54-6312-45e0-945b-96dd98dd3aec",
  "event_type": "signal.normalized",
  "schema_version": 1,
  "workspace_id": "82631aa1-86f5-4d37-b39f-fac74ebbdac8",
  "source_id": "b6c72d79-7ec9-4df7-88d0-1d4332c88808",
  "occurred_at": "2026-10-16T10:40:00Z",
  "ingested_at": "2026-10-16T10:40:01Z",
  "correlation_id": "checkout-latency-demo-001",
  "causation_id": null,
  "trace_id": null,
  "replay_run_id": null,
  "payload": {
    "signal_id": "37575f37-98af-4cb8-a875-26c3dde75ec7",
    "service_id": "dc2bf08d-6c3c-44ea-855a-d7d42eeefb36",
    "kind": "log",
    "quality_status": "accepted"
  }
}
```

| Field | Notes |
| --- | --- |
| `event_id` | UUID that stays stable across republish and redrive. Consumers deduplicate on it. |
| `event_type`, `schema_version` | Select the Pydantic payload model. Unsupported versions are quarantined. |
| `workspace_id` | Required on every event. |
| `source_id` | UUID for signal events. Optional for non-source domain events (P02-T04). |
| `occurred_at`, `ingested_at` | UTC, RFC 3339. |
| `correlation_id`, `causation_id` | Tracing across the pipeline. `causation_id` is a nullable UUID. |
| `replay_run_id` | `null` for live traffic. Otherwise it identifies the isolated evaluation run. |
| `payload` | Raw payloads are adapter-specific. Normalized events carry evidence **IDs**, not large payload copies. |

## Versioning rules

- Each event type has a Pydantic model. JSON Schemas are exported and fixtures are checked in CI.
- Within `v1`, only **additive, optional** fields are allowed.
- Breaking changes get a new versioned topic or schema version plus an explicit adapter.
- Unknown fields in a supported envelope are kept in `attributes`, never dropped silently.

## Delivery rules

- **Producers.** Producer idempotence is on, `acks=all`, and delivery callbacks report failures to the caller.
- **Consumers.** Auto-commit is off. Consumers assume at-least-once delivery. A consumer commits the next offset only after its database transaction commits, and it never commits past an unfinished earlier record in the partition.
- **Idempotent effects.** Consumer receipts (`consumer_group`, `event_id`) plus business uniqueness constraints make redelivery harmless. Kafka producer idempotence does **not** make database writes or model calls exactly-once.
- **Outbox.** Events derived from database changes go through the transactional outbox. A row is marked published only after broker acknowledgement, so a crash in between causes a harmless republish. See [reliability.md](reliability.md).

## Failure handling

| Failure | Handling |
| --- | --- |
| Transient (broker, database, network) | Capped exponential backoff with jitter: 5 attempts, 30 s maximum delay. Long delays are persisted in job state, never slept through a poll deadline. |
| Permanent (validation, unsupported version) | Publish to `processing.failed.v1` with redacted error details and the original identifiers. |
| Failure event cannot be published | The original is **not** acknowledged. |
| Redrive | Keeps the original `event_id` and records a separate redrive attempt ID. |

Local Kafka is a single broker with replication factor 1. Persistent volumes survive container recreation, not machine failure.
