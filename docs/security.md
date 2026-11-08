# Security boundaries

Scope: one local workspace on a single host (roadmap scope). This page states what each boundary enforces, how it is checked, and what it does not cover. Decisions: ADR-014 (operator principal and scope), ADR-034 (ground truth isolation), ADR-038 (credentials, limits, and exposure).

## Principals and routes

| Caller | Credential | Opens |
| --- | --- | --- |
| Anyone | none | `GET /health/live`, `GET /health/ready` |
| A source | source credential for that `source_id` | `POST /v1/ingest/{source_id}` only |
| An operator | operator credential for one workspace | every other `/v1` route, bounded to that workspace |

- Operator and source credentials are disjoint sets; a token present in both is refused at startup and on reload.
- No route takes a workspace parameter. Replay scope is a validated query parameter inside the credential's workspace, and every read filters through `EvidenceScope`.
- Evaluation routes additionally use the evaluator database login; the runtime login cannot read ground truth (ADR-034).
- `tests/unit/test_route_security.py` walks every path in the OpenAPI document and requires 401 for no token, a source token, an expired operator token, and a wrong token on each operator route, and 401 for operator tokens on ingestion. A route added without the operator dependency fails it. Every route must stay in the schema (`export-schemas --check` keeps it committed).

## Credentials

Two inputs, used together:

1. `SIGNALOPS_CREDENTIALS_FILE`: named credentials stored as SHA-256 digests.

   ```json
   {
     "operators": [
       {"id": "ops-2026-11", "workspace_id": "<uuid>", "sha256": "<64 hex>", "not_after": "2027-02-01T00:00:00Z"}
     ],
     "sources": [
       {"id": "logs-key-2", "source_id": "<uuid>", "sha256": "<64 hex>"}
     ]
   }
   ```

   `printf '%s\n' "$TOKEN" | signalops hash-token` prints a digest (tokens of at least 16 characters; the token is read from stdin so it stays out of shell history and process lists). IDs are unique; `not_after` is optional.
2. `SIGNALOPS_OPERATOR_TOKENS` and `SIGNALOPS_INGEST_TOKENS`: plaintext JSON maps for development. Their credential IDs are `env:operator:<workspace>` and `env:source:<source>`.

Every comparison hashes the presented token and compares it in constant time with every candidate of its kind, so timing reveals neither which credential matched nor whether an expired one did.

**Rotation without restart:** the API re-reads the file when its modification time or size changes, at most every `SIGNALOPS_CREDENTIALS_RELOAD_SECONDS` (5). To rotate: add the new credential, wait for the reload, move clients to the new token, then delete the old entry. A file that becomes unreadable or invalid keeps the last valid set and logs `credentials file rejected`, so a bad edit cannot lock everyone out or bring back a removed credential. A missing or invalid file at startup stops the API.

**Audit:** each request writes one `api request` log line with method, path, status, duration, `request_id`, and the matching `credential_id` (never the token).

## Settings and secrets

- `SIGNALOPS_SECRETS_DIR` (for example `/run/secrets`): a file named after a variable, such as `SIGNALOPS_DATABASE_URL` or `SIGNALOPS_MODEL_API_KEY`, supplies its value. Environment variables win over files.
- `SIGNALOPS_ENVIRONMENT=production` refuses plaintext operator and ingest tokens, requires `SIGNALOPS_CREDENTIALS_FILE`, and requires `https` CORS origins.
- CORS: `SIGNALOPS_CORS_ORIGINS` must list exact `scheme://host[:port]` origins; wildcards, paths, and `null` are refused. Empty (the Compose default) sends no CORS headers, so only the same-origin UI behind nginx can call the API from a browser.
- `.env.example` holds placeholders only. Never commit `.env` or a credentials file.

## Request limits

- Ingestion: `SIGNALOPS_INGEST_MAX_REQUEST_BYTES` (4 MiB) and `SIGNALOPS_INGEST_MAX_RECORDS` (500), checked by the route.
- Every other route: `SIGNALOPS_API_MAX_BODY_BYTES` (64 KiB), checked by middleware on the declared length and on the bytes actually received, before authentication or route code runs (413).
- Queries: page size and time window bounds (`SIGNALOPS_QUERY_*`), progress stream count and length (`SIGNALOPS_SSE_*`), evidence tools bounded by rows, bytes, window, and timeout.

## Network exposure

- Compose publishes host ports on `127.0.0.1` only: PostgreSQL, Redis, Kafka, API, frontend, Prometheus, Grafana. `test_compose_publishes_ports_on_loopback_only` checks `compose.yaml`.
- Containers talk on the private `signalops` network. Metrics listeners (port 9100) are never published and have no authentication; anyone on that network can read operational counts, which carry no evidence content or IDs.
- nginx proxies only `/health/` and `/v1/` to the API.
- PostgreSQL, Redis, and Kafka run without TLS and, for Redis and Kafka, without authentication. That is acceptable only on a single host's private network.

## Redaction paths

Evidence can carry secrets. Each path that moves evidence text, and what masks it:

| Path | Masking | Check |
| --- | --- | --- |
| Ingestion → Kafka → PostgreSQL | Structured payload fields by name: process list `SIGNALOPS_INGEST_REDACT_FIELDS` plus each source's `redact_fields`, before publication. Free-text messages are stored as sent | `tests/unit/test_ingestion.py` |
| Normalizer quality notes and quarantine events | Field locations and messages only, never values | `tests/unit/test_quality.py`, `tests/unit/test_failures.py` |
| Evidence API (`/v1/signals`, traces, incident and investigation evidence) | Attributes re-redacted by name with the current lists; messages masked for sensitive `key=value` pairs (both lists) and bearer tokens (P11-T02) | `tests/unit/test_evidence_api.py` |
| Agent tools and the verifier's cited signals → model | The same text masking, with each signal's source list (P11-T02); excerpts truncated | `tests/integration/test_agent_tools.py` |
| Model output → diagnosis, findings, progress events | Built from masked inputs; citations are IDs checked in PostgreSQL. Provider client debug logging is forced to WARNING | `tests/unit/test_logs.py` |
| Logs | Counts, IDs, categories, and exception types; no record values, messages, prompts, or tokens | review; `api request` lines carry the credential ID only |
| Metrics | Bounded enum labels; no IDs or content | `observability/metrics.py` |
| Evaluation labels | Evaluator schema only; the runtime login has no privilege | `tests/integration/test_evaluation_access.py` |

**Limits:** text masking matches sensitive `key=value` (or `key: value`) pairs and bearer tokens. A secret in free text of another shape, such as a bare password in a sentence, reaches PostgreSQL, operators, and models unmasked.

## Not covered

There are no user accounts, roles, sessions, or single sign-on. Before exposing the API beyond one host, put it behind an authenticating reverse proxy or identity provider and TLS, enable TLS and authentication on PostgreSQL, Redis, and Kafka, and move credentials into a secret store (ADR-038).
