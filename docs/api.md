# HTTP API

> Status: planned design. The source is [SignalOps_Implementation_Plan.md](SignalOps_Implementation_Plan.md) §9. Once implemented, the generated OpenAPI document (P02-T05) becomes authoritative, and this page should link to it.

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

```http
POST /v1/ingest/{source_id}
Authorization: Bearer <source credential>
Content-Type: application/json

{ "records": [ { "source_event_id": "…", "occurred_at": "…", "payload": { … } } ] }
```

- The API validates the source credential, batch size, payload bounds, and timestamps, then redacts configured sensitive fields.
- A record is reported `accepted` **only after Kafka acknowledges it**. A broker outage never produces a false acceptance.
- Responses report outcomes per record (accepted, rejected with reason, or failed). Clients retry with the **same** `source_event_id`, and the pipeline deduplicates downstream.
- When a per-source rate limit is exceeded, the API returns `429`.

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
