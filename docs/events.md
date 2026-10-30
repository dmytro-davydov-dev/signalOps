# Kafka event contracts

> Status: envelope and payload contracts implemented in `backend/signalops/contracts/events.py` (P02-T04); valid examples for every event type are in [`fixtures/contracts/v1/events/`](../fixtures/contracts/v1/events/). The source is [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §4. JSON Schema export and compatibility rules are P02-T05. Topic routing, message keys, and producer settings are implemented in `backend/signalops/streaming/topics.py` and `producer.py` (P03-T01).

## Topics

| Topic | Message key | Consumer groups | Initial partitions |
| --- | --- | --- | --- |
| `signals.raw.v1` | `source_id` + service hint | `normalizer-v1` | 3 |
| `signals.normalized.v1` | `workspace_id` + `service_id` | `detector-v1`, later `graph-projector-v1` | 3 |
| `incidents.changed.v1` | `incident_id` | `notification-projector-v1` | 1 |
| `investigations.requested.v1` | `investigation_id` | `dispatcher-v1` | 1 |
| `investigations.completed.v1` | `investigation_id` | `evaluation-projector-v1` | 1 |
| `processing.failed.v1` | original `event_id` | failure triage, controlled redrive | 1 |

Exact key strings (`message_key` in `streaming/topics.py`). Parts are joined with `:`; an absent optional part is omitted, not replaced by a placeholder. Keys use only identifiers that stay the same across republish, redrive, and replay, never `event_id` (except as the original's ID for failures), timestamps, or correlation IDs. Unit tests pin the key of every event fixture.

| Topic | Key | Example |
| --- | --- | --- |
| `signals.raw.v1` | `{source_id}:{service_hint}`, or `{source_id}` without a hint | `b6c72d79-…-1d4332c88808:payment` |
| `signals.normalized.v1` | `{workspace_id}:{service_id}`, or `{workspace_id}` when the service is unresolved | `82631aa1-…:5f3e2d1c-…` |
| `incidents.changed.v1` | `{incident_id}` | |
| `investigations.requested.v1`, `investigations.completed.v1` | `{investigation_id}` | |
| `processing.failed.v1` | `{original_event_id}`, or `{original_topic}:{original_partition}:{original_offset}` when the original could not be parsed | `signals.raw.v1:2:1187` |

The `service_hint` is the source's own string, used as given: two spellings of one service land on different raw partitions until the normalizer resolves identity. Unresolved normalized signals of a workspace share one partition. Producers use the `murmur2_random` partitioner, the Java client's default mapping, so the same key reaches the same partition from any client while the partition count is unchanged.

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
| `event_id` | UUID that stays stable across republish and redrive. Consumers deduplicate on it. For `signal.raw`, ingestion derives it as UUIDv5 of source, replay scope, and `source_event_id`, so client retries share one ID ([ADR-011](decisions.md#adr-011-ingestion-acknowledges-per-record-after-kafka-with-derived-event-ids)). |
| `event_type`, `schema_version` | Select the Pydantic payload model. Unsupported versions are quarantined. |
| `workspace_id` | Required on every event. |
| `source_id` | UUID, required for `signal.raw` and `signal.normalized`, optional for other domain events. |
| `occurred_at`, `ingested_at` | UTC, RFC 3339. |
| `correlation_id`, `causation_id` | Tracing across the pipeline. `causation_id` is a nullable UUID. |
| `replay_run_id` | `null` for live traffic (replay scope `live`). Otherwise the evaluation run UUID; evidence is stored under replay scope `replay:<uuid>`. |
| `payload` | Raw payloads are adapter-specific. Normalized events carry evidence **IDs**, not large payload copies. |

## Event types

| `event_type` | Topic | Payload model (v1) |
| --- | --- | --- |
| `signal.raw` | `signals.raw.v1` | `RawSignalPayload`: source kind, `source_event_id`, optional service hint, adapter record (≤ 64 KiB) |
| `signal.normalized` | `signals.normalized.v1` | `NormalizedSignalPayload`: signal and service IDs, kind, quality status |
| `incident.changed` | `incidents.changed.v1` | `IncidentChangedPayload`: incident ID and version, change kind, status, severity, replay scope |
| `investigation.requested` | `investigations.requested.v1` | `InvestigationRequestedPayload`: investigation and incident IDs, request key, trigger, workflow version |
| `investigation.completed` | `investigations.completed.v1` | `InvestigationCompletedPayload`: terminal status, optional diagnosis status |
| `processing.failed` | `processing.failed.v1` | `ProcessingFailedPayload`: consumer group, original topic/partition/offset/event ID, error category and redacted detail |

Raw adapter records are validated by the normalizer against `contracts/adapters.py` (`StructuredLogInput`, `TraceSummaryInput`, `DeploymentInput`). Fields those models do not define are moved into `attributes`; a name present both as an unknown field and inside `attributes` is rejected.

### Normalizer (`normalizer-v1`)

`signalops normalizer` consumes `signals.raw.v1` with `TransactionalProcessor` and `adapters.normalizer.Normalizer`. In one transaction per record it writes the receipt, the `signals` row, a `deployments` row for a deployment with a resolved service, and a `signal.normalized` outbox row (aggregate = signal ID, version 1). Mapping per source kind (`adapters/sources.py`):

| Source kind | Signal kind | Severity | Message | `attributes.record` |
| --- | --- | --- | --- | --- |
| `structured_log` | `log` | `level` (aliases `warn`, `err`, `fatal`, `trace`) | `message` | `span_id` |
| `trace_summary` | `trace` | `error` when `status` is `error`, else `info` | `{operation} {status} in {duration_ms} ms` plus failed-span count | operation, duration, status, span counts, downstream services |
| `deployment` | `deployment` | `info` | `deployed {revision}` plus previous revision and summary | revision, previous revision, change summary |

`signals.attributes` has three sections: `source` (the record's attributes and unknown fields, after ingestion redaction), `record` (above), and `provenance` (`raw_event_id`, `source_kind`, `adapter_version`, `reported_service`, `reported_environment`, `correlation_id`, and `identity_conflicts` when present).

`signal.raw` payloads carry `source_event_id_derived` (default `false`; additive in v1), set when ingestion derived the ID.

**Identity** ([ADR-012](decisions.md#adr-012-normalizer-resolves-identity-exactly-and-flags-everything-else)): the service and environment come from the record's fields, or, when a field is absent, from a well-known attribute (`service.name`, `service_name`, `service`; `deployment.environment`, `deployment.environment.name`, `environment`, `env`). The pair is looked up exactly in `services`:

| Outcome | `service_id` | Status | Flag |
| --- | --- | --- | --- |
| Pair found | resolved | `accepted` | none |
| Service or environment absent | NULL | `degraded` | `missing_service` |
| Pair not in the catalog | NULL | `degraded` | `unknown_service` |
| Claims disagree (field vs attribute, or two attributes) | NULL | `quarantined` | `contradictory_identity` |

**Quality policy** (`adapters/quality.py`, [ADR-013](decisions.md#adr-013-quality-flags-map-to-status-by-fixed-effect-derived-ids-at-ingestion)). Every flag has a fixed effect; the most severe one sets `quality_status`:

| Flag | Raised when | Effect |
| --- | --- | --- |
| `malformed_payload` | The record fails its adapter contract, or the normalized signal fails validation (for example attributes over 16 KiB) | `quarantined` |
| `contradictory_identity` | Identity claims disagree (above) | `quarantined` |
| `missing_service`, `unknown_service` | Identity absent or not in the catalog (above) | `degraded` |
| `clock_skew` | Live scope: `occurred_at` more than `SIGNALOPS_NORMALIZER_MAX_CLOCK_SKEW_SECONDS` (300) after `ingested_at` | `degraded` |
| `late_arrival` | Live scope: `ingested_at` more than `SIGNALOPS_NORMALIZER_LATE_ARRIVAL_SECONDS` (900) after `occurred_at` | none (`accepted`) |
| `derived_event_id` | `source_event_id_derived` is true | none (`accepted`) |
| `truncated_message` | Reserved; nothing truncates yet | none |

Timing compares the envelope's own timestamps, so reprocessing gives the same flags; `occurred_at` is stored as reported, never corrected. Replay scopes skip timing flags. The detector's 30-second lateness rule is separate and uses its window clock (P05).

A **malformed** record is stored, not dropped: kind from the source, severity `info`, no `service_id` or `trace_id`, message `malformed {source_kind} record: {errors}`, `attributes.record.contract_errors` with field locations and messages, and the raw (redacted) record under `attributes.source`. If keeping the record would exceed 16 KiB, `source` is empty and `provenance.record_omitted_bytes` holds its size; the full record stays on `signals.raw.v1` for its retention. It gets a receipt and a `signal.normalized` event like any other signal, and never a `deployments` row.

IDs are derived, so reprocessing yields the same rows: signal ID = UUIDv5(source, replay scope, `source_event_id`); deployment and `signal.normalized` event IDs derive from the signal ID. `UNIQUE(source_id, replay_scope, source_event_id)` backs the receipt: a record already stored (for example by another consumer group) stages no second deployment or event. Only envelope faults raise `ContractError` and go to `processing.failed.v1`: a source outside the envelope's workspace, or a `source_kind` that disagrees with the source's kind.

## Decoding

`decode_event(bytes)` validates a message value and raises `ContractError` with one of these categories. Error details list field locations and messages only, never input values, so they can go on `processing.failed.v1`.

| Category | Cause |
| --- | --- |
| `oversized` | Message value above 256 KiB (checked before parsing; `encode()` applies the same limit) |
| `malformed_json` | Not UTF-8 JSON |
| `unknown_event_type` | Missing or unrecognized `event_type` |
| `unsupported_version` | Known type, but no payload model for that `schema_version` |
| `invalid_envelope` | Header field invalid: malformed UUID, naive or numeric timestamp, missing `source_id` on a signal event, non-integer version |
| `invalid_payload` | Payload fails its model |

Timestamps must be RFC 3339 strings with an offset; they are converted to UTC and serialized with `Z`.

## Versioning rules

These rules cover every exported contract: event envelopes and payloads, adapter records, API bodies, checkpoints, tool results, and diagnoses.

- **Exported files.** `uv run signalops export-schemas` writes JSON Schemas to `backend/signalops/contracts/schemas/` (one file per contract and version, for example `events/signal.raw.v1.json`), the OpenAPI document to `schemas/openapi.v1.json`, and `apps/web/src/api/generated/investigationStatus.ts`. `uv run pytest` fails while committed files differ from the models (`test_committed_artifacts_match_the_contracts`); `export-schemas --check` reports the same without writing.
- **Allowed within a version:** new optional fields, new optional nullable alternatives, and documentation changes. Consumers ignore unknown event fields, so producers may add them before consumers upgrade.
- **Breaking:** removing or renaming a field, making a field required, changing a type, removing an enumeration value, or tightening a bound. `export-schemas` refuses to rewrite an existing schema with a structural break (`contracts/compatibility.py`) and writes nothing; tightened bounds are not detected and need review. `--allow-breaking` exists only for contracts that have never been deployed.
- **Adding an enumeration value** is structurally additive, but a v1 consumer validating that field rejects the new value and quarantines the message. Deploy consumers first, or treat the change as breaking. Values backed by database CHECK constraints also need a migration.
- **New versions.** A breaking event change registers a new `(event_type, schema_version)` payload model in `EVENT_PAYLOADS` and a new schema file; the old version stays registered until no producer or retained message uses it. Consumers that do not know a version reject it as `unsupported_version` and quarantine the message on `processing.failed.v1`, never guess. A new topic (`*.v2`) is needed only when the key or partitioning changes. Breaking API changes use a new path prefix (`/v2`). Checkpoints bump `CHECKPOINT_SCHEMA_VERSION`; older checkpoints fail to decode and their step is re-run.
- **Unknown source fields.** Fields a raw adapter record model does not define are moved into `attributes` (and stored in `signals.attributes`), never dropped. A name that appears both as an unknown field and inside `attributes` is rejected rather than merged.

## Delivery rules

- **Producers.** `EventProducer` (`streaming/producer.py`) sets `enable.idempotence=true`, `acks=all`, at most 5 in-flight requests, `message.timeout.ms` from `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS`, and `linger.ms=5`. `publish(messages)` blocks until each message is acknowledged or failed and returns one result per message, in order, with the partition and offset or an error category:

  | Category | Meaning | Caller action |
  | --- | --- | --- |
  | `timeout` | No acknowledgement within the delivery timeout. The broker may have written it. An unknown topic also ends here, because librdkafka waits for the topic to appear. | Treat as not sent; retrying may duplicate, which consumers absorb by `event_id` |
  | `transient` | Retriable broker or network error that outlasted librdkafka's internal retries | Retry later |
  | `rejected` | Permanent for this message (size, authorization) | Do not retry unchanged |
  | `queue_full` | Local queue full; never handed to librdkafka | Retry after backoff |
  | `fatal` | The idempotent producer lost its guarantees; later `publish` calls raise `ProducerUnusableError` | Replace the producer (restart the process) |

  A message is never reported as delivered without a broker acknowledgement. Async callers run `publish` in a worker thread.
- **Consumers.** Auto-commit is off. Consumers assume at-least-once delivery. A consumer commits the next offset only after its database transaction commits, and it never commits past an unfinished earlier record in the partition. `ConsumerRunner` (`streaming/consumer.py`) handles one record at a time: decode, one transaction through `TransactionalProcessor` (receipt, the handler's effect, outbox rows via `OutboxRepository`), then a synchronous commit of offset + 1. A new group starts at the earliest retained offset. A failed offset commit is logged, not raised: the effect is durable and a later commit or a redelivery that hits the receipt covers it. Failures follow [Failure handling](#failure-handling).
- **Idempotent effects.** Consumer receipts (`consumer_group`, `event_id`) plus business uniqueness constraints make redelivery harmless. Kafka producer idempotence does **not** make database writes or model calls exactly-once.
- **Outbox.** Events derived from database changes go through the transactional outbox. The publisher sends the stored topic, key, and envelope (re-serialized from JSONB, so key order may differ from the original encoding) with the row ID as event ID. A row is marked published only after broker acknowledgement, so a crash in between causes a harmless republish. A transient delivery failure defers the row with backoff; a `rejected` result quarantines it. See [reliability.md](reliability.md).

## Failure handling

| Failure | Handling |
| --- | --- |
| Permanent: `ContractError` from `decode_event` (malformed JSON, unknown type, unsupported version, invalid envelope or payload, oversized) or raised by the handler for content it cannot accept | `ConsumerRunner` publishes a `processing.failed.v1` event, then commits the record's offset. No receipt is written, so a redrive is processed. |
| Transient: any other handler exception (database, network, bugs) | The partition is paused and rewound to the record while the runner keeps polling; it resumes after a full-jitter backoff (`SIGNALOPS_RETRY_*`: 5 attempts, 1 s base, 30 s cap). After the last attempt the runner raises `RecordProcessingError` without committing; the process exits and its restart policy brings it back at the same offset. |
| Failure event not acknowledged | Treated as transient: the original is **not** committed. The retry republishes the same failure event ID. |
| Outbox: `timeout`, `transient`, `queue_full` | Row deferred (`next_attempt_at`) with the same backoff, never abandoned. Its aggregate's later versions wait. |
| Outbox: `rejected` | Row quarantined (`quarantined_at`, `last_error`); later versions of its aggregate wait until `signalops redrive-outbox EVENT_ID`. Nothing discards an outbox row. |
| Redrive of a record | `signalops redrive-record --topic --partition --offset` (from the failure event) reads the original from Kafka and republishes key, value, and headers unchanged to the same topic, with a new `signalops-redrive-attempt-id` header. The envelope, and so `event_id`, is the original's. A failure of the redriven copy records that attempt ID in `redrive_attempt_id`. `processing.failed.v1` itself is not redrivable. |

Failure event fields:

- `event_id`: UUIDv5 of consumer group, topic, partition, and offset, so a republished failure event deduplicates.
- `workspace_id` and `replay_run_id`: from the original envelope, else from those fields of the raw document when they parse. A failure event whose original names no readable workspace uses the nil UUID `00000000-0000-0000-0000-000000000000`.
- `causation_id`, `original_event_id`: the original event ID when known. `correlation_id` and `trace_id` are copied when the original decoded.
- `error_detail`: the `ContractError` detail (field locations and messages, never values).

Local Kafka is a single broker with replication factor 1. Persistent volumes survive container recreation, not machine failure.
