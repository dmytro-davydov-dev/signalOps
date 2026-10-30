# HTTP API

> Status: planned design. The source is [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §9. The generated OpenAPI document, [`openapi.v1.json`](../backend/signalops/contracts/schemas/openapi.v1.json), is authoritative for implemented routes (health and ingestion); body schemas for planned routes are exported alongside it (for example `investigations/investigation.v1.json`). Compatibility rules are in [events.md](events.md#versioning-rules).

All endpoints are versioned under `/v1`. Request and response bodies are Pydantic models. Timestamps are UTC (RFC 3339) and IDs are UUIDs.

## Endpoints

| Method and path | Behavior | Roadmap |
| --- | --- | --- |
| `POST /v1/ingest/{source_id}` | Bounded batch ingestion with a per-record acceptance result | P04-T02 |
| `GET /v1/incidents` | Cursor pagination, filters for service, severity, status | P05-T05 |
| `GET /v1/incidents/{id}` | Timeline, affected services, evidence references | P05-T05 |
| `POST /v1/incidents/{id}/investigations` | Requires `Idempotency-Key`. Returns **202** with the investigation ID | P06-T01 |
| `GET /v1/investigations/{id}` | State, finding, evidence, usage, limitations | P06/P07 |
| `POST /v1/investigations/{id}/cancel` | Persists a cancellation request | P06-T04 |
| `GET /v1/investigations/{id}/events` | SSE progress stream with a durable reconnect cursor | P08-T03 |
| `GET /v1/services/{id}/graph` | Bounded neighborhood as of a timestamp | P05-T05 |
| `POST /v1/evaluation-runs` | Starts an isolated scenario run | P10-T06 |
| `GET /v1/evaluation-runs/{id}` | Case results and aggregate metrics | P10-T06 |

Liveness and readiness endpoints arrive in P01-T04. Readiness reports missing required dependencies.

## Ingestion

Implemented in P04-T02 (`api/ingest.py`, `adapters/ingestion.py`; bodies in `contracts/ingestion.py`, schemas under `contracts/schemas/ingestion/`).

```http
POST /v1/ingest/{source_id}
Authorization: Bearer <source token>
Content-Type: application/json

{ "records": [ { "source_event_id": "…", "occurred_at": "2026-10-30T10:00:00Z", "payload": { … } } ] }
```

Checks, in order:

1. **Credential.** The bearer token must match the token configured for this `source_id` in `SIGNALOPS_INGEST_TOKENS`. Missing, wrong, or another source's token → `401`. Tokens never live in `sources.config`.
2. **Source.** Looked up in PostgreSQL: unknown → `404`, disabled → `403`, invalid `config` → `500` (fails closed rather than skipping redaction), database unavailable → `503`.
3. **Size.** Body over `SIGNALOPS_INGEST_MAX_REQUEST_BYTES` (Content-Length or streamed) or more than `SIGNALOPS_INGEST_MAX_RECORDS` records → `413`. A body that is not `{"records": [ … ]}` with at least one record → `422`.
4. **Each record, independently.** Rejected with a reason when: the record does not match `IngestRecord` (unknown fields, naive or numeric timestamp, missing payload) → `invalid_record`; the payload exceeds 64 KiB serialized → `oversized`; `occurred_at` is more than `SIGNALOPS_INGEST_MAX_FUTURE_SECONDS` ahead or `SIGNALOPS_INGEST_MAX_AGE_SECONDS` behind receipt → `timestamp_out_of_range`; the `source_event_id` already appeared earlier in the batch → `duplicate_in_batch`. Smaller skew and late arrival are flagged by the normalizer (P04-T04), not rejected. Payloads are not validated against the adapter contracts here; the normalizer quarantines malformed ones so the outcome stays auditable. Rejection details list field locations, never values.
5. **Redaction.** Values of sensitive keys are replaced with `"[REDACTED]"` at any depth before publishing: `SIGNALOPS_INGEST_REDACT_FIELDS` plus the source's `config.redact_fields`. Keys match case-insensitively, with `-` and spaces read as `_`. Field names only; free text inside messages is not scrubbed.
6. **Publish.** Each remaining record becomes a `signal.raw` envelope on `signals.raw.v1`. `event_id` is UUIDv5 of source, replay scope, and `source_event_id` (`raw_event_id`), so a retry carries the same ID. `correlation_id` is the response's `batch_id`. `service_hint` is the payload's `service` when it is a valid name.

Per-record outcomes (`results`, in request order, with counts):

| `status` | Meaning | Client action |
| --- | --- | --- |
| `accepted` | Kafka acknowledged the record | None |
| `rejected` | The record is invalid (`reason` above) | Fix it; resending unchanged fails again |
| `failed` | Not acknowledged. `ambiguous_timeout`: no answer within the delivery timeout, so the broker may have written it. `broker_unavailable`: another broker or client error | Retry with the same `source_event_id` |

The response is `200` when every record is accepted or rejected, and `503` with `Retry-After` and the same body when any record failed. Resending the whole batch is safe: accepted records reproduce their event IDs and are deduplicated downstream. If the HTTP call itself times out, the outcome is unknown; retry the batch with the same IDs. Worst-case latency is about `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS` (30 s by default) when the broker is down, so client timeouts should exceed it.

Not yet implemented: the per-source `429` rate limit (Redis). No roadmap task owns it yet. Token rotation is P11-T02.

## Investigation requests and idempotency

```http
POST /v1/incidents/{id}/investigations
Idempotency-Key: 6f1c…
```

- The key is scoped to caller plus route. The server stores a canonical hash of the request body with it.
- The same key and body return the **same** investigation. The same key with a different body returns `409 Conflict`.
- The response is `202 Accepted` once the investigation and its outbox request are persisted. The API does not wait for the workflow.
- Detector-triggered requests use stable internal request keys, so duplicates create one job.

## Investigation result

`GET /v1/investigations/{id}` returns:

- The investigation state (see [data-model.md](data-model.md#state-machines)).
- The validated diagnosis or insufficient-evidence result (see [agents-and-workflow.md](agents-and-workflow.md#diagnosis-contract)).
- The evidence cutoff and replay scope.
- Token usage and cost, labelled *actual*, *estimated*, or *unknown*.
- Limitations.

The body is `InvestigationView` (`contracts/investigations.py`). Its `diagnosis` follows one mapping, shared with the `investigation.completed` event, the database, and the UI:

| Investigation `status` | `diagnosis` | UI label |
| --- | --- | --- |
| `completed` | required, `status: "diagnosed"` | Diagnosed |
| `insufficient_evidence` | required, `status: "insufficient_evidence"`, null `root_cause`, non-empty `missing_evidence` | Insufficient evidence (an abstention, not a failure) |
| `queued`, `running`, `failed`, `cancelled`, `budget_exceeded` | `null` | Queued, Running, Failed, Cancelled, Budget exceeded |

## Progress stream (SSE)

```http
GET /v1/investigations/{id}/events
Accept: text/event-stream
Last-Event-ID: 1042
```

- Each SSE `id` is the `progress_events.id` from PostgreSQL.
- On reconnect, the server replays events after `Last-Event-ID` in order.
- Redis pub/sub only wakes the stream. A periodic database catch-up covers missed notifications, so a connected client cannot stay stale.
- Behavior for an expired or unknown cursor is defined in P08-T03.

## Error categories

| Status | Meaning |
| --- | --- |
| `400` / `422` | Malformed request or contract validation failure |
| `401` / `403` | Missing or invalid credential. Source credentials cannot call operator APIs. |
| `404` | Not found within the caller's workspace or replay scope |
| `409` | Idempotency key reuse with a different body, or an optimistic version conflict |
| `413` | Batch or payload exceeds configured limits |
| `429` | Rate limit exceeded |
| `503` | A required dependency (Kafka, PostgreSQL) is unavailable. Not accepted. |

## Security

- Operator APIs require authentication before the demo is exposed publicly.
- Ingestion uses separate source-specific credentials.
- Request-size and query limits, plus workspace and replay scope checks, apply to every route.
- CORS is configured explicitly.
