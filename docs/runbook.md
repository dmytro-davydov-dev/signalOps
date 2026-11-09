# Operations runbook

Procedures for the local Compose deployment. Each entry gives the symptom, how to confirm it, and what to do. Details live in the linked documents; this page is the index. Commands run from the repository root with `.env` present.

Single-node Compose is a development environment. Nothing here gives high availability ([retention.md](retention.md#single-node-durability-limits)).

## First look

```bash
docker compose ps                                        # which processes run, restart, or exited
curl -s http://127.0.0.1:8010/health/ready               # PostgreSQL, Redis, Kafka as the API sees them
docker compose --profile tools run --rm backend operations-snapshot   # queues, leases, outbox age, unknown calls, lag (JSON)
docker compose logs --since 10m <service>                # JSON lines; grep an ID from below
```

With the observability profile (`docker compose --profile observability up -d`), Grafana at `http://127.0.0.1:53000` shows the same state over time. [observability.md](observability.md#telling-problems-apart) maps symptoms to the metric that moves.

To follow one investigation end to end: `GET /v1/investigations/{id}/trace` returns its ingestion batches, job, steps, stage durations, and the log fields to grep ([api.md](api.md#investigation-trace-p11-t01)).

## Start, stop, upgrade

| Action | Command | Notes |
| --- | --- | --- |
| Start or apply code changes | `docker compose up -d --build --wait` | Reruns the idempotent `migrate` and `init-topics` jobs first |
| Stop, keep data | `docker compose down` | Named volumes stay |
| Delete all data | `docker compose down -v` | Irreversible: evidence, incidents, evaluation results, Kafka records |
| Restart one process | `docker compose restart <service>` | Consumers resume at their committed offset; a worker's job is reclaimed after its lease (30 s) |
| Scale a consumer | `docker compose up -d --scale normalizer=3` | Useful up to the topic's partition count (3 for signal topics). Never scale `outbox-publisher` past 1 |
| Migrations only | `docker compose run --rm migrate` | Also recreates the database login roles |

A topic whose partition count differs from `streaming/topics.py` makes `init-topics` fail; it never alters partitions, because that would reorder keys ([development.md](development.md#infrastructure-repository-root-verified)).

## Symptoms

### Readiness is 503

The body names the failing dependency and an error category. Start it (`docker compose start postgres|redis|kafka`). During a PostgreSQL or Kafka outage, ingestion answers 503 and reports nothing as accepted; clients retry with the same `source_event_id`s. Consumers pause and retry, then exit; `restart: unless-stopped` brings them back at the same offset ([reliability.md](reliability.md#outage-checks-p11-t03)). A Redis outage only empties caches and pauses model calls up to `SIGNALOPS_MODEL_LIMIT_WAIT_SECONDS`.

### Signals were accepted but no evidence appears

1. Consumer lag: `operations-snapshot` → `kafka_lag`, or `signalops_kafka_consumer_lag{consumer_group="normalizer-v1"}`. Rising lag with a running normalizer means it is slow; rising lag with none running means it exited (`docker compose ps`).
2. Data quality per source: `GET /v1/sources/quality` counts `accepted`, `degraded`, and `quarantined` signals and their flags. A payload that fails its adapter contract (`malformed_payload`) or names contradictory services (`contradictory_identity`) is stored as a `quarantined` signal, kept as evidence but not counted by detection. List them with `GET /v1/signals?quality_status=quarantined`; the fix is at the source.
3. Undecodable records (malformed envelope, unknown type or version) never become signals. The consumer publishes a failure event to `processing.failed.v1` and moves on. Read them with `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic processing.failed.v1 --from-beginning --timeout-ms 10000`; each names the original topic, partition, and offset.

After fixing the cause, replay one quarantined record with `docker compose --profile tools run --rm backend redrive-record --topic T --partition P --offset O`. It is applied once; receipts absorb repeats ([events.md](events.md#failure-handling)).

### A fault produced no incident

- Rules fire when a window closes: window length (60 s) plus allowed lateness (30 s), checked by `detector-closer` every 5 s. Expect about 90–120 s after the fault starts.
- Backfilled data (a `demo-post` run without `--realtime`) arrives past the lateness bound, so its windows fire nothing. This is by design ([events.md](events.md#closure-and-lateness)).
- Check `detector-closer` is running and `detector-v1` lag is draining.
- Rules and thresholds: `SIGNALOPS_DETECTION_RULES` ([development.md](development.md#configuration)), set through a Compose override ([quickstart.md](quickstart.md#changing-other-settings)).

### An investigation stays queued

- `signalops_jobs_ready` and `jobs_ready_oldest_age_seconds` rising: no worker is claiming. Check `investigation-worker` is running.
- A request never became a job: check `dispatcher` and `dispatcher-v1` lag, then the outbox (below).
- A job with `attempts` above zero and `available_at` in the future is waiting out a retry backoff after a transient failure; the trace route shows its attempts and last error category.

### A worker died mid-investigation

Nothing to do. `signalops_jobs_expired_leases` rises for up to one lease (`SIGNALOPS_JOB_LEASE_SECONDS`, 30 s). Then a worker reclaims the job with a new generation, reuses every completed step, and rejects writes from the old one. Model calls that were in flight stay `unknown` with their reservations held, because the provider may have billed them ([reliability.md](reliability.md#what-survives-what)).

### An investigation ended `budget_exceeded`, `failed`, or `insufficient_evidence`

- `insufficient_evidence` is an abstention, not a fault: the diagnosis lists `missing_evidence`.
- `budget_exceeded`: the reserved maximum of the next call did not fit. `GET /v1/investigations/{id}` → `usage` shows reserved, spent, and unknown amounts. Raise `SIGNALOPS_INVESTIGATION_BUDGET_MICROUSD` only after reading where it went.
- `failed`: the job ran out of attempts or hit a permanent error; `GET .../trace` → `job.last_error_category`. Request a new investigation with a new `Idempotency-Key`.

### Unknown model costs

`signalops_model_calls_unknown` and `signalops_model_unknown_reserved_microusd` count calls whose outcome is ambiguous (timeout, worker death). They keep their reservation. Reconcile them against the provider's usage console using `provider_request_id` when one was recorded. On the fake provider they cost nothing.

### Outbox backlog

`signalops_outbox_oldest_age_seconds` rising: the publisher is stopped or Kafka is rejecting. Check `outbox-publisher` logs for `outbox stats`. A row the broker rejected permanently is quarantined and blocks later events of the same aggregate, to keep their order. Fix the cause, then `docker compose --profile tools run --rm backend redrive-outbox EVENT_ID` ([reliability.md](reliability.md#outbox-publisher)).

### Live progress in the UI stops updating

The stream falls back to PostgreSQL catch-up every 5 s without Redis, and the browser reconnects with `Last-Event-ID` after a drop. A stream also closes after 300 s by design and the UI reopens it. An `event: reset` means the cursor was removed by retention; the UI reloads the history ([api.md](api.md#progress-stream-sse)).

## Credentials

- Development tokens: `SIGNALOPS_INGEST_TOKENS` and `SIGNALOPS_OPERATOR_TOKENS` in `.env` (`scripts/init-env.sh` generates them). Changing them needs `docker compose up -d` to recreate the containers.
- Rotation without restart: hashed credentials in `SIGNALOPS_CREDENTIALS_FILE`; add the new one, move clients, remove the old one ([security.md](security.md#credentials)).
- A token that leaks: remove it from the file (effective within 5 s) or from `.env` and recreate the API.

## Data lifecycle

| Task | Command | Reference |
| --- | --- | --- |
| Retention report | `docker compose --profile tools run --rm backend retention` | Dry run; prints what would go |
| Apply retention | `... backend retention --apply` | Keeps cited evidence and live investigations |
| Back up | `docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" -Fc -d "$POSTGRES_DB" > signalops-<time>.dump` | [retention.md](retention.md#backup-and-restore) |
| Restore | Stop workers, `pg_restore` into an empty database, `migrate`, rewind consumer groups | [retention.md](retention.md#backup-and-restore) |

Kafka keeps raw signals 3 days and other topics 7 days, so take backups at least daily if you need to replay after a restore.

## Model provider

The default is the deterministic fake provider: no key, no network, no cost, and no diagnostic value. To use Claude, set `SIGNALOPS_MODEL_PROVIDER=anthropic` and `SIGNALOPS_MODEL_API_KEY` in `.env` and recreate `investigation-worker`. Every call is then paid and bounded by the investigation budget (default 3 USD) and the provider rate limits. To run only chosen investigations, first set `SIGNALOPS_INVESTIGATION_AUTO_REQUEST: "false"` for `api`, `detector`, and `detector-closer` in a Compose override ([quickstart.md](quickstart.md#changing-other-settings)).
