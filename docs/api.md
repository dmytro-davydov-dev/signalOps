# HTTP API

> Status: every route below is implemented. The generated OpenAPI document, [`openapi.v1.json`](../backend/signalops/contracts/schemas/openapi.v1.json), is authoritative; the running API serves it at `/docs`. Body schemas are exported alongside it (for example `investigations/investigation.v1.json`). Compatibility rules are in [events.md](events.md#versioning-rules). Runnable examples are in [Examples](#examples).

All endpoints are versioned under `/v1`. Request and response bodies are Pydantic models. Timestamps are UTC (RFC 3339) and IDs are UUIDs.

## Endpoints

| Method and path | Behavior | Roadmap |
| --- | --- | --- |
| `POST /v1/ingest/{source_id}` | Bounded batch ingestion with a per-record acceptance result | P04-T02 |
| `GET /v1/signals` | Evidence in a time window, filters for source, service, kind, quality status; cursor pagination | P04-T05 |
| `GET /v1/traces/{trace_id}` | Signals carrying one trace ID | P04-T05 |
| `GET /v1/sources/quality` | Per-source counts by quality status and flag | P04-T05 |
| `GET /v1/incidents` | Cursor pagination, filters for service, severity, status | P05-T05 |
| `GET /v1/incidents/{id}` | Rule firings, affected services, evidence references | P05-T05 |
| `PATCH /v1/incidents/{id}` | Resolve or reopen a live incident at a version | P05-T05 |
| `GET /v1/incidents/{id}/evidence` | Evidence timeline: cited signals oldest first, cursor pagination | P05-T05 |
| `POST /v1/incidents/{id}/investigations` | Requires `Idempotency-Key`. Returns **202** with the investigation ID | P06-T01 |
| `GET /v1/incidents/{id}/investigations` | The incident's newest investigations (at most 20) | P08-T02 |
| `GET /v1/investigations/{id}` | State, finding, evidence, usage, limitations | P06-T04, P07 |
| `GET /v1/investigations/{id}/evidence` | The diagnosis's cited signals, checked again against scope and cutoff | P08-T02 |
| `GET /v1/investigations/{id}/trace` | Ingestion batches, incident, job, steps, and stage durations of one investigation | P11-T01 |
| `POST /v1/investigations/{id}/cancel` | Persists a cancellation request | P06-T04 |
| `GET /v1/investigations/{id}/progress` | Durable progress history after an integer cursor | P06-T05 |
| `GET /v1/investigations/{id}/events` | SSE progress stream with a durable reconnect cursor | P08-T03 |
| `GET /v1/services/{id}/graph` | Bounded neighborhood as of a timestamp | P05-T05 |
| `POST /v1/evaluation-runs` | Starts an isolated scenario run | P10-T06 |
| `GET /v1/evaluation-runs/{id}` | Case results and aggregate metrics | P10-T06 |

`GET /health/live` and `GET /health/ready` need no credential. Readiness is 503 with per-dependency error categories when PostgreSQL, Redis, or Kafka does not answer.

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
4. **Each record, independently.** Rejected with a reason when: the record does not match `IngestRecord` (unknown fields, naive or numeric timestamp, missing payload) → `invalid_record`; the payload exceeds 64 KiB serialized → `oversized`; `occurred_at` is more than `SIGNALOPS_INGEST_MAX_FUTURE_SECONDS` ahead or `SIGNALOPS_INGEST_MAX_AGE_SECONDS` behind receipt → `timestamp_out_of_range`; the `source_event_id` already appeared earlier in the batch → `duplicate_in_batch`; no `source_event_id` and the payload lacks the fields to derive one (below) → `missing_source_event_id`. Smaller skew and late arrival are flagged by the normalizer, not rejected. Payloads are not validated against the adapter contracts here; the normalizer quarantines malformed ones so the outcome stays auditable. Rejection details list field locations, never values.
5. **Redaction.** Values of sensitive keys are replaced with `"[REDACTED]"` at any depth before publishing: `SIGNALOPS_INGEST_REDACT_FIELDS` plus the source's `config.redact_fields`. Keys match case-insensitively, with `-` and spaces read as `_`. Field names only; free text inside messages is not scrubbed.
6. **Missing IDs.** A record may omit `source_event_id`. Ingestion then derives a stable one from the redacted payload ([ADR-013](decisions.md#adr-013-quality-flags-map-to-status-by-fixed-effect-derived-ids-at-ingestion)) and returns it in the result, so a retry of the same record collapses:
   - `trace_summary`: `trace_id`, `service`, `operation` (all required);
   - `deployment`: `service`, `environment`, `revision` (all required), and `occurred_at`;
   - `structured_log`: `occurred_at` and the whole payload. Two lines identical in every field and timestamp are one event; identical text at different times or with any differing field stays separate.

   Derived IDs look like `derived:{kind}:{40 hex}`, and the signal is flagged `derived_event_id`. Send real IDs whenever the source has them.
7. **Publish.** Each remaining record becomes a `signal.raw` envelope on `signals.raw.v1`. `event_id` is UUIDv5 of source, replay scope, and `source_event_id` (`raw_event_id`), so a retry carries the same ID. `correlation_id` is the response's `batch_id`. `service_hint` is the payload's `service` when it is a valid name.

Per-record outcomes (`results`, in request order, with counts):

| `status` | Meaning | Client action |
| --- | --- | --- |
| `accepted` | Kafka acknowledged the record | None |
| `rejected` | The record is invalid (`reason` above) | Fix it; resending unchanged fails again |
| `failed` | Not acknowledged. `ambiguous_timeout`: no answer within the delivery timeout, so the broker may have written it. `broker_unavailable`: another broker or client error | Retry with the same `source_event_id` |

The response is `200` when every record is accepted or rejected, and `503` with `Retry-After` and the same body when any record failed. Resending the whole batch is safe: accepted records reproduce their event IDs and are deduplicated downstream. If the HTTP call itself times out, the outcome is unknown; retry the batch with the same IDs. Worst-case latency is about `SIGNALOPS_KAFKA_DELIVERY_TIMEOUT_MS` (30 s by default) when the broker is down, so client timeouts should exceed it.

Not implemented: a per-source `429` rate limit. Token rotation: docs/security.md.

## Evidence queries

Implemented in P04-T05 (`api/evidence.py`, `api/auth.py`; queries in `persistence/repositories/evidence.py`; bodies in `contracts/queries.py`, schemas under `contracts/schemas/evidence/`).

```http
GET /v1/signals?service_id=…&since=2026-10-30T09:00:00Z&until=2026-10-30T10:00:00Z&limit=50
Authorization: Bearer <operator token>
```

**Credential and scope.** Every route requires a token from `SIGNALOPS_OPERATOR_TOKENS` ([ADR-014](decisions.md#adr-014-interim-operator-tokens-scoped-to-one-workspace)). Its workspace bounds the query; there is no workspace parameter. Missing, wrong, or a source ingestion token → `401`. `replay_scope` (default `live`, same pattern as stored scopes) selects the dataset; signals of other workspaces and scopes are never returned, and a trace absent from the scope is `404`.

**Bounds.** `limit` defaults to `SIGNALOPS_QUERY_DEFAULT_LIMIT` (50); above `SIGNALOPS_QUERY_MAX_LIMIT` (200) → `422`. Time windows are half-open (`since` inclusive, `until` exclusive) with RFC 3339 offsets (naive or numeric → `422`). `until` defaults to now and `since` to `until` minus `SIGNALOPS_QUERY_DEFAULT_WINDOW_SECONDS` (1 h); a span over `SIGNALOPS_QUERY_MAX_WINDOW_SECONDS` (7 days) or `since >= until` → `422`. Limits are rejected, never clamped. Each query runs in one short read transaction under the session `statement_timeout`. Database unavailable → `503`.

**Redaction.** `attributes` are redacted again on output with `SIGNALOPS_INGEST_REDACT_FIELDS` plus the signal's source `config.redact_fields` as they are now, so a field added after ingestion is hidden too. Invalid source config → `500` (fails closed). `message` text is returned as stored; ingestion does not scrub free text.

| Route | Returns |
| --- | --- |
| `GET /v1/signals` | `SignalPage`: signals with `since <= occurred_at < until`, newest first (ties by ID descending), optional `source_id`, `service_id`, `kind`, `quality_status`. `window` echoes the resolved window; `next_cursor` is null on the last page |
| `GET /v1/traces/{trace_id}` | `TraceEvidence`: the trace's signals oldest first, up to `limit`; `truncated` when more exist |
| `GET /v1/sources/quality` | `QualitySummary`: every source of the workspace with `total`, `by_status` (all three statuses), and `by_flag` (flags present) for signals with `since <= ingested_at < until` |

**Cursors.** Opaque, versioned, and bound to the query: workspace, replay scope, filters, and the resolved window. Pass the cursor with the same filters; `since`/`until` may be left out (the cursor's window is used) or must be equal. A cursor from another query → `400 cursor belongs to a different query`; an unreadable one → `400 invalid cursor`. Pages use keyset position `(occurred_at, id)`, so rows inserted later inside the window and older than the position appear on later pages, and none repeat.

## Incidents and service graphs

Implemented in P05-T05 (`api/incidents.py`; reads in `persistence/repositories/incidents.py` and `graph.py`; status changes in `detection/incidents.py`; bodies in `contracts/incidents.py` and `contracts/queries.py`).

Credential, `replay_scope`, RFC 3339 rules, `503`, and redaction are those of [evidence queries](#evidence-queries). Absent from the workspace or scope → `404`.

| Route | Returns |
| --- | --- |
| `GET /v1/incidents` | `IncidentPage`: newest `detected_at` first (ties by ID descending), optional `service_id`, `severity`, `status`. Each row (`IncidentListItem`) carries its `services` with roles, primary first (added in P08-T01; absent from older servers). `limit` is at most 100, the contract's page size, even when `SIGNALOPS_QUERY_MAX_LIMIT` is higher. A cursor from another query → `400` |
| `GET /v1/incidents/{id}` | `IncidentDetail`: services with roles (primary first since P08-T01), up to 200 evidence references, and rule firings oldest first (up to 200; `triggers_truncated` when more exist) |
| `GET /v1/incidents/{id}/evidence` | `IncidentEvidencePage`: cited signals with `relevance`, oldest first. A cursor from another incident → `400` |
| `GET /v1/services/{id}/graph` | `ServiceGraph`: `downstream` and `upstream` `DEPENDS_ON` edges, up to `hops` (1–3, default 3), the services they name, and what each ran at `as_of`. `as_of` defaults to now; pass the incident's time for the neighborhood it had. `truncated` means a walk reached its bound. A service never observed has empty edge lists. A path never re-enters its start, so in a cycle the edge back to the service shows only under `upstream` |

**Status changes.** `PATCH /v1/incidents/{id}` with `{"status": "resolved" | "open", "version": N}` ([ADR-019](decisions.md#adr-019-operators-change-incident-status-at-a-version-and-evidence-still-wins)):

- `N` must be the current version, or the response is `409` and nothing changes. Read again and decide again.
- The status the incident already has at version `N` is a `200` with no change and no event, so retrying after a lost response is safe.
- A change bumps the version and writes `incident.changed` in one transaction. The response is the new `IncidentDetail`.
- Live incidents only; there is no `replay_scope` parameter, and a replay incident is `404`.

## Investigation requests and idempotency

```http
POST /v1/incidents/{id}/investigations
Idempotency-Key: 6f1c…
```

The body is optional. `InvestigationRequest` holds `budget_microusd`, `max_steps`, and `deadline_seconds` (at least 60), each of which may lower the configured limit and not raise it; above it, the response is `422`. The response is `InvestigationAccepted`: `id`, `incident_id`, `status`, `created_at`.

- The key is 1–256 characters and scoped to caller plus route ([ADR-020](decisions.md#adr-020-investigation-requests-are-keyed-durably-and-dispatched-once)). Operator tokens carry no identity, so the workspace is the caller.
- The server stores a canonical hash of the incident and the body, so field order, spacing, and an explicit `null` change nothing.
- The same key and request return the **same** investigation, with its current status. The same key with a different body or a different incident returns `409 Conflict`.
- The response is `202 Accepted` once the investigation and its outbox request are persisted. The API does not wait for the workflow.
- Live incidents only; a replay incident is `404`.
- Detector-triggered requests use the stable key `detector:{incident_id}`, so duplicates create one job.

## Investigation result

`GET /v1/investigations/{id}` returns:

- The investigation state (see [data-model.md](data-model.md#state-machines)).
- The validated diagnosis or insufficient-evidence result (see [agents-and-workflow.md](agents-and-workflow.md#diagnosis-contract)).
- The evidence cutoff and replay scope.
- `usage`: budget, reserved, and spent amounts, model calls and tokens, spend split into `actual_microusd` and `estimated_microusd`, and `unknown_calls` with the `unknown_reserved_microusd` they keep held. An unknown call may have been billed.
- Limitations.

The body is `InvestigationView` (`contracts/investigations.py`). Its `diagnosis` follows one mapping, shared with the `investigation.completed` event, the database, and the UI:

| Investigation `status` | `diagnosis` | UI label |
| --- | --- | --- |
| `completed` | required, `status: "diagnosed"` | Diagnosed |
| `insufficient_evidence` | required, `status: "insufficient_evidence"`, null `root_cause`, non-empty `missing_evidence` | Insufficient evidence (an abstention, not a failure) |
| `queued`, `running`, `failed`, `cancelled`, `budget_exceeded` | `null` | Queued, Running, Failed, Cancelled, Budget exceeded |

`replay_scope` selects a replay run, as on the incident routes.

`diagnosis.contradictions` (added in P09-T03, additive, default empty) lists specialists whose findings point to different causes, with the roles and up to 20 signal IDs each, for diagnosed and abstained results alike. Workflow `v3` fills it; `v1` and `v2` leave it empty. A client that predates it can ignore it, and the UI reads a missing field as empty.

## Incident investigations and cited evidence

Added in P08-T02 for the investigation workspace. Credential, `replay_scope`, and `503` are those of the incident routes; outside the workspace or scope → `404`.

| Route | Returns |
| --- | --- |
| `GET /v1/incidents/{id}/investigations` | `IncidentInvestigations`: up to 20 `InvestigationSummary` rows (status, times, `diagnosis_status`), newest `created_at` first, operator and detector requests alike. `truncated` when more exist |
| `GET /v1/investigations/{id}/evidence` | `InvestigationEvidence`: the root cause's cited signals that exist in the investigation's scope and were known by its `evidence_cutoff_at` (both `occurred_at` and `ingested_at`), in citation order and redacted as `/v1/signals` is. Citations failing that check are listed in `unverified_signal_ids` and never returned. Empty without a root cause |

A stored diagnosis passed the same check before it was saved (ADR-029), so `unverified_signal_ids` should be empty; the UI warns when it is not.

## Investigation trace (P11-T01)

`GET /v1/investigations/{id}/trace` returns `InvestigationTrace`, with the same credential, `replay_scope`, `404`, and `503` rules as the other investigation reads:

- `evidence`: count of the incident's cited signals, their first `occurred_at`, first and last `ingested_at`, and up to 20 ingestion batch IDs (the envelopes' `correlation_id`, which is the `batch_id` an ingestion response returns).
- `job`: status, attempts, lease generation, and the last error's category only.
- `steps`: each workflow step's attempt, generation, start and end, and its model and tool call counts and summed durations.
- `stages`: wall-clock seconds for ingestion → detection → request (live only; replays detect in event time), request → first step, and the run, plus summed model and tool time. Parallel specialists can make `model_s` exceed `running_s`.
- `log_fields`: the structured log fields (`correlation_id`, `incident_id`, `investigation_id`, `job_id`) that select this path's log lines. See docs/observability.md.

Every response carries `X-Request-ID`: the client's value when it is 1–64 characters of `A-Za-z0-9._:-`, otherwise a generated one. The same ID is on every log line the request wrote.

## Cancellation

```http
POST /v1/investigations/{id}/cancel
```

There is no body and no `Idempotency-Key`, because repeating the request changes nothing. The response is `202` with `InvestigationView`.

- With no worker holding the job, the response already shows `cancelled`.
- With a running job, it shows `running` and `cancel_requested_at`. The worker stops within about a third of a lease; read the investigation to see the terminal state.
- An investigation that ended another way is `409`. Live scope only; a replay investigation is `404`.

## Progress history

```http
GET /v1/investigations/{id}/progress?after=1042&limit=50
```

- The body is `ProgressPage` ([`progress-page.v1.json`](../backend/signalops/contracts/schemas/investigations/progress-page.v1.json)), oldest event first. `after` is the last event ID already read, default 0. `next_after` is set when more events exist.
- Event types: `step.started`, `step.completed`, `step.failed` (with `error_category`; `abandoned` means the investigation ended around a running step), and `investigation.ended` (terminal status and diagnosis status). There is exactly one `investigation.ended`, and it comes last.
- A retried or taken-over step has one set of events per attempt. Take a step's latest attempt. A reused step adds nothing.
- Events hold identifiers and categories. Read the outcome from `GET /v1/investigations/{id}`.
- Scoped like the investigation read: `replay_scope` defaults to live, and an investigation outside it is `404`.

## Progress stream (SSE)

Implemented in P08-T03 (`api/progress_stream.py`, route in `api/investigations.py`, wake-ups in `wakeups.py`; [ADR-030](decisions.md#adr-030-progress-streams-replay-postgresql-and-an-unknown-cursor-resets)).

```http
GET /v1/investigations/{id}/events
Accept: text/event-stream
Authorization: Bearer <operator token>
Last-Event-ID: 1042
```

- Scoped like the investigation read: `replay_scope` defaults to live; outside it → `404`, before the stream starts. `Last-Event-ID` that is not a non-negative integer → `400`; `after` (query) works the same way and loses to the header.
- The first frame is `retry: 3000`. Each event is `id: <progress_events.id>`, `event: progress`, and `data:` holding the same `ProgressEventView` JSON as `/progress`.
- **Cursor.** Absent or `0`: the whole retained history. A cursor naming an event of this investigation: the events after it, in order. Any other cursor (another investigation's event, or one removed by retention): `event: reset` with `data: {"reason":"unknown_cursor"}`, then the whole history; discard held events and read the investigation again.
- **Liveness.** The stream reads PostgreSQL on a Redis wake-up and at least every `SIGNALOPS_SSE_CATCHUP_SECONDS` (5 s), and sends `: keepalive` when nothing is new. A lost wake-up delays an event by at most that interval; without Redis the stream still works on catch-up alone.
- **End.** The stream closes after `investigation.ended`, the last event, and after `SIGNALOPS_SSE_MAX_STREAM_SECONDS` (300 s). Reconnect with `Last-Event-ID` after a close without an ending.
- More than `SIGNALOPS_SSE_MAX_STREAMS` (200) open streams in one API process → `503` with `Retry-After`.
- Browsers: `EventSource` cannot send `Authorization`, so the UI reads the stream with `fetch` (`apps/web/src/api/progressStream.ts`). It treats 20 s without bytes as a dead connection.

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

- Operator routes require an operator credential; ingestion requires a source credential for that source; only the health probes are open. Credentials, rotation, limits, CORS, and redaction: docs/security.md (ADR-038).
- Bodies over 64 KiB on non-ingestion routes are `413` before authentication (`SIGNALOPS_API_MAX_BODY_BYTES`).
- Workspace comes from the credential; replay scope is a validated parameter inside it.
- Evidence messages are masked for sensitive `key=value` pairs and bearer tokens, as model inputs are.
- Every response carries `X-Request-ID`, and each request is logged once with its credential ID.

## Evaluation runs (P10-T06)

Operator token required; a run belongs to the token's workspace (another workspace's run is 404). The routes use the evaluator's connection (`SIGNALOPS_EVALUATION_DATABASE_URL`, ADR-034) and answer 503 without it. Starting a run only queues it; `signalops evaluation-worker` executes it.

| Route | Result |
| --- | --- |
| `POST /v1/evaluation-runs` | `202` with the queued run. Body: `label`, `split` (`held_out` default), `variants` (all three default), `trials` (1–20), optional `budget_microusd`, `max_steps`, `deadline_seconds`, `scenarios`. An unknown scenario is 422 |
| `GET /v1/evaluation-runs?limit=` | Newest runs with case counts by status |
| `GET /v1/evaluation-runs/{id}` | Configuration (split hash, workflow and prompt versions, `model_settings`, pricing, equal `limits`), `metrics` (see `docs/evaluation.md`), and every case: scenario, family, split, `synthetic`, variant, trial, expected outcome (without evidence labels), observed outcome, scores |
| `GET /v1/evaluation-runs/{id}/cases/{case_id}` | One case with its replay report (including detection) and each cited signal, with `fault_evidence` from the labels, plus its audits |
| `POST /v1/evaluation-runs/{id}/cases/{case_id}/audits` | `201`. Body: `signal_id`, `supported`, `auditor`, optional `note`. The signal must be cited by the case (422 otherwise). An auditor's later judgement of the same citation replaces the earlier one |

The UI's Evaluation page lists runs and can queue one. A run page compares the variants (samples, accuracy and its spread across trials, root-cause accuracy, evidence precision, unsupported diagnoses, abstentions, latency, cost, harness failures), shows detection results, and filters cases to incorrect and failed ones. A case page shows expected against observed, each citation's label, links to the investigation in its replay scope, and records audits. Runs on the fake provider carry a notice that they show the harness, not diagnostic quality.

## Examples

Recorded on 2026-11-09 against the Compose stack after the [quick start](quickstart.md) fault run, on the fake provider. IDs come from that run; responses are trimmed (`…`). The tokens are those in `.env`: `$OPERATOR_TOKEN` from `SIGNALOPS_OPERATOR_TOKENS`, `$LOGS_TOKEN` from `SIGNALOPS_INGEST_TOKENS` for the logs source.

```bash
API=http://127.0.0.1:8010
auth=(-H "Authorization: Bearer $OPERATOR_TOKEN")
```

**Ingest one log record** (source token, per-record result):

```bash
curl -s -X POST -H "Authorization: Bearer $LOGS_TOKEN" -H 'Content-Type: application/json' \
  $API/v1/ingest/b6c72d79-7ec9-4df7-88d0-1d4332c88808 -d '{
  "records": [{
    "source_event_id": "example-1",
    "occurred_at": "2026-11-09T12:45:00Z",
    "payload": {"service": "payment", "environment": "demo", "level": "error",
                "message": "upstream bank-gateway call timed out after 2000 ms"}
  }]}'
```

```json
{"batch_id":"ba18f643-…","accepted":1,"rejected":0,"failed":0,
 "results":[{"index":0,"source_event_id":"example-1","status":"accepted","event_id":"8dbabbc7-…","reason":null,"detail":null}]}
```

`accepted` means Kafka has the record. Whether its payload fits the adapter contract is decided by the normalizer: a payload with the wrong fields (for example `severity` instead of `level`) is stored as a `quarantined` signal with the flag `malformed_payload`, visible in `GET /v1/sources/quality`.

**List incidents:**

```bash
curl -s "${auth[@]}" $API/v1/incidents
```

```json
{"items":[{"id":"2d5348d8-7450-4d59-9026-1dc46876e492","replay_scope":"live",
  "title":"High error rate on checkout (demo)","severity":"high","status":"open",
  "detected_at":"2026-11-09T12:44:32.758552Z","first_seen_at":"2026-11-09T12:43:00Z","last_seen_at":"2026-11-09T12:44:00Z",
  "resolved_at":null,"version":4,
  "services":[{"name":"checkout","environment":"demo","role":"primary",…},{"name":"payment","environment":"demo","role":"primary",…}]}],
 "next_cursor":null}
```

**The incident's investigations** (the detector requested the first one):

```bash
curl -s "${auth[@]}" $API/v1/incidents/2d5348d8-7450-4d59-9026-1dc46876e492/investigations
```

```json
{"incident_id":"2d5348d8-…","replay_scope":"live",
 "items":[{"id":"2f20afcb-b0a2-4358-909d-3400e4837e0f","status":"completed","created_at":"2026-11-09T12:44:32.771116Z",
           "completed_at":"2026-11-09T12:44:34.253568Z","cancel_requested_at":null,"diagnosis_status":"diagnosed"}],
 "truncated":false}
```

**Read the result:**

```bash
curl -s "${auth[@]}" $API/v1/investigations/2f20afcb-b0a2-4358-909d-3400e4837e0f
```

```json
{"id":"2f20afcb-…","status":"completed","workflow_version":"v3","evidence_cutoff_at":"2026-11-09T12:44:32.771116Z",
 "usage":{"budget_microusd":3000000,"reserved_microusd":0,"spent_microusd":134780,"model_calls":9,
          "input_tokens":26130,"output_tokens":1513,"actual_microusd":0,"estimated_microusd":134780,
          "unknown_calls":0,"unknown_reserved_microusd":0,…},
 "diagnosis":{"status":"diagnosed",
   "summary":"Failing calls end at this service, and its evidence shows the fault.",
   "root_cause":{"service_name":"bank-gateway","failure_type":"downstream_timeout","confidence":0.7,
                 "evidence_signal_ids":["b985656f-…","4203ca9f-…",…]},
   "alternatives":[{"hypothesis":"inventory: deployment_regression",
                    "assessment":"Revision inventory-2026.10.16-2 of this service was deployed shortly before the incident window."}],
   "limitations":["Produced by a deterministic fake role."],
   "missing_evidence":[],
   "contradictions":[{"roles":["telemetry_investigator","deployment_investigator"],…}],
   "workflow_version":"v3"}}
```

Spend is `estimated_microusd`: the fake provider has a price table but no bill. The fake roles answer by fixed rules over the evidence they are given, so a correct root cause here shows that the pipeline carries evidence through, not that a model reasoned. The unrelated deployment the demo includes appears as an alternative and a recorded contradiction.

**Cited evidence**, checked again against scope and cutoff:

```bash
curl -s "${auth[@]}" $API/v1/investigations/2f20afcb-b0a2-4358-909d-3400e4837e0f/evidence
```

```json
{"investigation_id":"2f20afcb-…","evidence_cutoff_at":"2026-11-09T12:44:32.771116Z",
 "items":[{"id":"b985656f-…","kind":"trace","source_event_id":"bank-gateway-span-779790df0638a904edf9",
           "occurred_at":"2026-11-09T12:44:25.228Z","message":"POST /authorize ok in 1353 ms",
           "attributes":{"record":{"operation":"POST /authorize","duration_ms":1353,…},…},…},…],
 "unverified_signal_ids":[]}
```

**Request another investigation.** The same key and body return the same investigation; the same key with another body is `409`:

```bash
curl -s -i -X POST "${auth[@]}" -H 'Idempotency-Key: docs-example-1' -H 'Content-Type: application/json' \
  -d '{"max_steps": 20}' $API/v1/incidents/2d5348d8-7450-4d59-9026-1dc46876e492/investigations
```

```text
HTTP/1.1 202 Accepted
{"id":"1b65a28f-ec7d-4927-93f7-0326303a090c","incident_id":"2d5348d8-…","status":"queued","created_at":"2026-11-09T12:45:08.841984Z"}
```

**Progress history and live stream:**

```bash
curl -s "${auth[@]}" "$API/v1/investigations/2f20afcb-b0a2-4358-909d-3400e4837e0f/progress?limit=2"
curl -sN "${auth[@]}" -H 'Last-Event-ID: 2' $API/v1/investigations/2f20afcb-b0a2-4358-909d-3400e4837e0f/events
```

```json
{"investigation_id":"2f20afcb-…","replay_scope":"live",
 "items":[{"id":1,"event_type":"step.started","payload":{"step_key":"load_context","agent_role":"coordinator","attempt":1},…},
          {"id":2,"event_type":"step.completed","payload":{"step_key":"load_context","agent_role":"coordinator","attempt":1},…}],
 "next_after":2}
```

**Trace after a worker crash.** An investigation whose worker was killed after two steps (quick start step 5) finished on the second attempt, reusing them:

```bash
curl -s "${auth[@]}" $API/v1/investigations/d174acbc-8085-4a3e-9281-4db2a7bc1b02/trace
```

```json
{"status":"completed",
 "job":{"status":"succeeded","attempts":2,"lease_generation":2,"last_error_category":"lease_expired"},
 "steps":[{"step_key":"load_context","attempt":1,"lease_generation":1,…},
          {"step_key":"plan","attempt":1,"lease_generation":1,…},
          {"step_key":"telemetry","attempt":2,"lease_generation":2,…},
          …,
          {"step_key":"conclude","attempt":2,"lease_generation":2,…}],
 …}
```

Its `usage` shows `"unknown_calls":3,"unknown_reserved_microusd":564645`: the calls in flight when the worker died keep their reservation, because a real provider may have billed them.

**Without a credential**, every route except ingestion and the health probes is `401`:

```bash
curl -s -o /dev/null -w '%{http_code}\n' $API/v1/incidents   # 401
```
