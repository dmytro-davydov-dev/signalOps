# Quick start

Run the whole pipeline on your machine with the deterministic fake model: inject a fault into synthetic telemetry, watch SignalOps detect an incident, and read the investigation's diagnosis with its cited evidence. No model key, no paid calls, and no tools besides Docker.

The fake provider proves the plumbing: durable jobs, budgets, checkpoints, evidence validation, and the UI. Its answers are scripted, so they say nothing about diagnostic quality ([evaluation.md](evaluation.md)).

## Requirements

- Linux or macOS with Docker Engine and Compose v2 (`docker compose version`). Tested with Docker 29.9 on Linux x86-64.
- About 4 GB of free memory and 5 GB of disk for images and volumes.
- Free loopback ports 8010, 8090, 55432, 56379, 19094 (change them in `.env` if they clash).
- `curl` for the API examples. `python3` only to pretty-print JSON (optional).

## 1. Configure

```bash
git clone <repository-url> signalops && cd signalops
./scripts/init-env.sh
```

`init-env.sh` copies `.env.example` to `.env` with random local passwords and tokens and prints the **operator token**. Keep it: the UI and the query API ask for it. Every value in `.env` is local-only; `.env` is git-ignored.

To see the token again later:

```bash
grep SIGNALOPS_OPERATOR_TOKENS .env    # the value after the workspace ID
```

You can also run `cp .env.example .env` and replace each placeholder by hand; the comments in the file say what each one is.

## 2. Start the stack

```bash
docker compose up -d --build --wait
docker compose --profile demo run --rm demo-seed
```

The first command builds the backend and web images (a few minutes the first time), starts PostgreSQL, Redis, and Kafka, runs the one-shot `migrate` and `init-topics` jobs, then starts the API, the UI, and the workers: `normalizer`, `detector`, `detector-closer`, `graph-projector`, `dispatcher`, `investigation-worker`, `outbox-publisher`. The second creates the demo workspace, its three sources (logs, traces, deployments), and five services. Both are idempotent.

Check:

```bash
curl -s http://127.0.0.1:8010/health/ready
# {"status":"ready","checks":{"postgres":{"status":"ok",...},"redis":{...},"kafka":{...}}}
```

- UI: <http://127.0.0.1:8090/> (system health at `/status`)
- API reference: <http://127.0.0.1:8010/docs>

## 3. Inject a fault

```bash
docker compose --profile demo run --rm demo \
  demo-post downstream-timeout --realtime --duration-s 300 --api-url http://api:8000
```

This posts five minutes of synthetic traffic for a checkout system (`checkout`, `payment`, `inventory`, `bank-gateway`, `orders-db`) through the ingestion API, in real time, with background noise and one deployment. Between 40% and 80% of the run, `bank-gateway` slows down and `payment` calls start timing out. Nothing in the records names the scenario or the fault. The command prints accepted and rejected counts per source and exits 0 when every record was accepted.

`--realtime` matters: detection rules fire when a one-minute window closes, and a backfilled run (without the flag) arrives past the 30-second lateness bound, so it becomes evidence but opens no incident.

Alternatively, `docker compose --profile demo up -d demo-shop` runs a small real instrumented application for 15 minutes that exports its own telemetry; its downstream really slows after four minutes.

## 4. Watch the incident and the investigation

About two minutes after the fault starts, the detector opens an incident and requests one investigation automatically. With the fake provider the investigation finishes within seconds.

**In the UI:** open <http://127.0.0.1:8090/incidents>, paste the operator token, and open the newest incident. It shows the rule firings, the affected services, the evidence timeline, the data quality of each source, and the dependency graph at incident time. The investigation panel shows live progress per workflow step, then the diagnosis (or an *insufficient evidence* abstention), each claim's cited signals, and the estimated cost. *Start investigation* requests another run; a double click starts only one.

**With the API:**

```bash
export OPERATOR_TOKEN='<operator token from step 1>'
auth=(-H "Authorization: Bearer $OPERATOR_TOKEN")

curl -s "${auth[@]}" http://127.0.0.1:8010/v1/incidents | python3 -m json.tool
INCIDENT=<id from the list>
curl -s "${auth[@]}" http://127.0.0.1:8010/v1/incidents/$INCIDENT | python3 -m json.tool
curl -s "${auth[@]}" http://127.0.0.1:8010/v1/incidents/$INCIDENT/investigations | python3 -m json.tool
INVESTIGATION=<id from that list>
curl -s "${auth[@]}" http://127.0.0.1:8010/v1/investigations/$INVESTIGATION | python3 -m json.tool
```

More requests, with example responses, are in [api.md](api.md#examples).

## 5. Optional: crash a worker mid-run

Make the fake model slow enough to interrupt, then kill the worker while an investigation runs:

```bash
echo 'SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3' >> .env
docker compose up -d investigation-worker            # recreate it with the delay
curl -s "${auth[@]}" -X POST -H "Idempotency-Key: crash-demo-1" \
  http://127.0.0.1:8010/v1/incidents/$INCIDENT/investigations
INVESTIGATION=<id from the response>
# wait until the UI shows a step completed, then:
docker compose kill investigation-worker && docker compose start investigation-worker
```

After the lease expires (30 s), the restarted worker reclaims the job, reuses every completed step, and finishes. The progress history shows each step once per attempt, and model calls that were in flight when the worker died are recorded as `unknown` with their budget reservation kept ([reliability.md](reliability.md#what-survives-what)). `GET /v1/investigations/$INVESTIGATION/trace` shows the job's two attempts and which steps ran in each ([example](api.md#examples)). Remove the delay line from `.env` and run `docker compose up -d investigation-worker` again when done.

## 6. Stop

```bash
docker compose down        # stop; data volumes are kept
docker compose down -v     # stop and delete all data
```

## Environment

`.env` is read by Compose and holds only what the stack needs to start. `.env.example` documents each entry.

| Group | Variables | Notes |
| --- | --- | --- |
| Database | `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `SIGNALOPS_DB_RUNTIME_PASSWORD`, `SIGNALOPS_DB_EVALUATOR_PASSWORD` | The owner applies migrations; application processes connect as a runtime role that cannot read evaluation ground truth |
| Host ports | `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`, `KAFKA_HOST_PORT`, `API_HOST_PORT`, `WEB_HOST_PORT`, `PROMETHEUS_HOST_PORT`, `GRAFANA_HOST_PORT` | Bound to `127.0.0.1` only |
| Credentials | `SIGNALOPS_INGEST_TOKENS` (source ID → token), `SIGNALOPS_OPERATOR_TOKENS` (workspace ID → token) | Must all differ; the IDs are the seeded demo sources and workspace. Hashed, rotatable credentials: [security.md](security.md#credentials) |
| Model | `SIGNALOPS_MODEL_PROVIDER` (`fake`), `SIGNALOPS_MODEL_NAME`, `SIGNALOPS_MODEL_API_KEY`, `SIGNALOPS_FAKE_MODEL_DELAY_SECONDS` | Passed to `investigation-worker` only. `anthropic` makes paid calls |
| Logs | `SIGNALOPS_LOG_LEVEL` | JSON logs on stdout |

After editing `.env`, run `docker compose up -d` to recreate the affected containers.

### Changing other settings

Every backend setting is a `SIGNALOPS_*` variable ([development.md](development.md#configuration) lists them with defaults and bounds). Compose passes only the ones above into containers, so set any other in a `compose.override.yaml` next to `compose.yaml` (git-ignored, merged automatically):

```yaml
# Investigation settings are read where investigations are created:
# the API (operator requests) and the detectors (automatic requests).
x-investigation-settings: &investigation-settings
  SIGNALOPS_INVESTIGATION_AUTO_REQUEST: "false"    # only operator-requested runs
  SIGNALOPS_INVESTIGATION_WORKFLOW_VERSION: v2     # single agent instead of multi-agent v3

services:
  api: { environment: *investigation-settings }
  detector: { environment: *investigation-settings }
  detector-closer: { environment: *investigation-settings }
```

Check the merged result with `docker compose config`, then `docker compose up -d`. A setting given to a process that does not use it is ignored.

### Running on the host

To run tests, checks, or a single process outside containers you also need [uv](https://docs.astral.sh/uv/) and Node.js 24 with pnpm. See [development.md](development.md) for commands, the integration test setup, and the browser journey check.

## Next

- [Architecture](architecture.md) and the [event flow](architecture.md#event-flow)
- [Runbook](runbook.md): what to check when something stops moving
- [Evaluation](evaluation.md): replaying held-out scenarios against three baselines (`docker compose --profile evaluation up -d evaluation-worker`, then the UI's Evaluation page)
- [Observability](observability.md): `docker compose --profile observability up -d` for Prometheus and Grafana
