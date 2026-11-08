# Observability

Three views of one system: Prometheus metrics, structured JSON logs with correlation fields, and the investigation trace route. Decision: ADR-037.

## Running it

```bash
docker compose --profile observability up -d     # metrics-exporter, Prometheus, Grafana
```

- Prometheus: <http://127.0.0.1:59090> (`PROMETHEUS_HOST_PORT`), 3 days of local retention.
- Grafana: <http://127.0.0.1:53000> (`GRAFANA_HOST_PORT`). Anonymous viewers open the provisioned *SignalOps operations* dashboard (`infra/observability/grafana/dashboards/signalops.json`); the admin password is `GRAFANA_ADMIN_PASSWORD`.
- Without the profile, every process still serves `/metrics` on port 9100 inside the Compose network, and `docker compose run --rm --no-deps backend operations-snapshot` prints the durable state as JSON.
- On the host, set `SIGNALOPS_METRICS_PORT` to give a process a listener; unset (the default) means none.

Prometheus discovers each service through DNS, so `docker compose up --scale normalizer=3` is scraped per replica with a `service` label.

## Metrics

Process metrics (each process, default registry). Counters that follow a database write count after the commit; a redelivery that hits its receipt is counted by consumer outcome only.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `signalops_ingest_records_total` | `source_kind`, `status` | Ingestion results; `accepted` means the broker acknowledged the record |
| `signalops_consumer_records_total` | `consumer_group`, `outcome` | `applied`, `duplicate`, `quarantined`, `retry_scheduled` |
| `signalops_consumer_processing_seconds` | `consumer_group` | Poll return to offset commit, committed outcomes only |
| `signalops_outbox_events_total` | `outcome` | `published`, `failed`, `quarantined`, `deferred` |
| `signalops_normalized_signals_total` | `source_kind`, `quality_status` | Signals stored by the normalizer |
| `signalops_quality_flags_total` | `source_kind`, `flag` | Quality flags on stored signals |
| `signalops_incidents_opened_total` | `scope` (`live`, `replay`) | Incidents created |
| `signalops_detection_latency_seconds` | | Live incidents: first cited signal's ingestion to the incident commit |
| `signalops_job_events_total` | `event` | `claimed`, `lease_expired` (a claim that took over an unrenewed lease), `completed`, `retried`, `failed`, `budget_exceeded`, `stopped`, `released`, `lease_lost` |
| `signalops_tool_call_seconds` | `tool`, `outcome` | Evidence tool reads |
| `signalops_model_call_seconds` | `provider`, `outcome` | Each provider attempt through the gateway; `unknown` is an ambiguous outcome |
| `signalops_model_tokens_total` | `provider`, `kind` | `input`, `output`, `cache_read`, `cache_write` |
| `signalops_model_cost_microusd_total` | `provider`, `basis` | Settled cost; `estimated` on the fake provider |

Durable state (`metrics-exporter`, refreshed every `SIGNALOPS_METRICS_REFRESH_SECONDS`, default 15):

| Metric | Meaning |
| --- | --- |
| `signalops_kafka_consumer_lag{consumer_group,topic,partition}` | Log end offset minus the group's committed offset, from the broker. Omitted, not zero, when the broker does not answer |
| `signalops_outbox_pending`, `signalops_outbox_quarantined`, `signalops_outbox_oldest_age_seconds` | Unpublished outbox rows and the oldest one's age |
| `signalops_jobs{status}`, `signalops_jobs_ready`, `signalops_jobs_ready_oldest_age_seconds` | Jobs by status; claimable jobs and how long the oldest has waited |
| `signalops_jobs_expired_leases` | Running jobs whose lease ran out and no worker has reclaimed yet |
| `signalops_investigations{status}` | Investigations by status |
| `signalops_model_calls_in_flight`, `signalops_model_calls_unknown`, `signalops_model_unknown_reserved_microusd` | Reserved calls not yet settled; ambiguous calls and the reservation they keep |
| `signalops_exporter_last_success_timestamp_seconds`, `signalops_exporter_refresh_failures_total` | Snapshot freshness; a failed refresh keeps the previous values |

Label values are bounded enums and names; IDs never become labels.

## Telling problems apart

| Symptom | Moves | Stays flat |
| --- | --- | --- |
| Slow model calls | `signalops_model_call_seconds` quantiles, `signalops_model_calls_in_flight`, trace `stages.model_s` | `signalops_kafka_consumer_lag`, consumer processing time |
| Pipeline backlog (a consumer slow or stopped) | `signalops_kafka_consumer_lag` for that group, trace `stages.ingestion_to_detection_s` | Model call duration |
| Workers saturated or down | `signalops_jobs_ready`, `signalops_jobs_ready_oldest_age_seconds`, trace `stages.request_to_first_step_s` | Lag, model duration |
| A worker died mid-job | `signalops_jobs_expired_leases`, then `signalops_job_events_total{event="lease_expired"}` on the reclaiming worker | |
| Outbox publisher stuck | `signalops_outbox_oldest_age_seconds`, `signalops_outbox_pending` | |
| Possible unbilled charges | `signalops_model_calls_unknown`, `signalops_model_unknown_reserved_microusd` | |

Consumer lag comes from the broker, not from the consumer, so a stuck or stopped consumer still shows rising lag. The consumers' own `consumer stats` log line (docs/reliability.md#streaming-stats) reports the same lag from inside the process.

## Correlation fields in logs

Every JSON log line written inside a bound block carries these fields (`observability/context.py`); a field passed with `extra=` wins:

| Where | Fields |
| --- | --- |
| API request | `request_id` (from `X-Request-ID` when well formed, else generated; echoed in the response) |
| Consumer record | `event_id`, `event_type`, `correlation_id` (the ingestion `batch_id` for signals), `causation_id`, `replay_run_id` |
| Job attempt | `job_id`, `investigation_id`, `lease_generation`, `job_attempt` (inherited by parallel branches) |

Other lines name their own IDs: `ingest batch` (`batch_id`), `incident opened` (`incident_id`), `investigation job queued` (`investigation_id`), `model call settled` (`call_id`, `provider_request_id`).

## Following a fault

1. Ingestion returns a `batch_id`; the API's `ingest batch` line has it with the `request_id`.
2. The normalizer stores it as `attributes.provenance.correlation_id` on every signal, with the `raw_event_id`.
3. `incident opened` (detector or detector-closer) names the incident; `investigation job queued` (dispatcher) names the investigation, with the triggering `event_id`.
4. `GET /v1/investigations/{id}/trace` joins them: the batches behind the incident's evidence, the job, each step's model and tool time, and stage durations. Its `log_fields` select the matching log lines, for example `docker compose logs investigation-worker | grep <investigation_id>`.

Per-record normalizer and detector work is not logged at INFO; quarantines and retries are, with the record's correlation fields.
