"""Process entry points for the shared backend image."""

import argparse
import sys
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from signalops import __version__

if TYPE_CHECKING:
    from signalops.config import Settings
    from signalops.contracts.events import EventType
    from signalops.evaluation.runner import RunnerContext
    from signalops.observability.snapshot import OperationsSnapshot
    from signalops.persistence.database import Database
    from signalops.streaming.consumer import EventHandler


# Scenario names from `signalops.demo.generator.Scenario`, kept here so the parser
# does not import the generator; a unit test checks they match.
SCENARIOS = (
    "healthy",
    "downstream-timeout",
    "pool-exhaustion",
    "deployment-regression",
    "memory-pressure",
    "mixed-timeout-deploy",
    "unexplained-errors",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalops")
    parser.add_argument("--version", action="version", version=f"signalops {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="command")
    commands.add_parser("api", help="serve the HTTP API")
    commands.add_parser(
        "migrate",
        help="apply database migrations up to head and provision login roles, then exit",
    )
    commands.add_parser("init-topics", help="create or verify Kafka topics, then exit")
    commands.add_parser(
        "outbox-publisher", help="publish committed outbox rows to Kafka until stopped"
    )
    commands.add_parser(
        "normalizer", help="normalize signals.raw.v1 records into evidence until stopped"
    )
    commands.add_parser(
        "detector", help="update detection windows from signals.normalized.v1 until stopped"
    )
    commands.add_parser(
        "graph-projector",
        help="project signals.normalized.v1 into the temporal knowledge graph until stopped",
    )
    commands.add_parser(
        "dispatcher",
        help="turn investigations.requested.v1 into durable jobs until stopped",
    )
    commands.add_parser(
        "worker", help="claim investigation jobs and run their workflow until stopped"
    )
    commands.add_parser(
        "detector-closer", help="close live detection windows by wall clock until stopped"
    )
    commands.add_parser(
        "metrics-exporter",
        help="serve PostgreSQL and Kafka operational state as Prometheus gauges until stopped",
    )
    commands.add_parser(
        "operations-snapshot", help="print queues, leases, outbox age, and Kafka lag as JSON"
    )
    flush = commands.add_parser(
        "detector-flush",
        help="close a replay's remaining windows and evaluate them, then exit",
    )
    flush.add_argument("--workspace-id", required=True, type=uuid.UUID, help="replay's workspace")
    flush.add_argument("--replay-run-id", required=True, type=uuid.UUID, help="replay run to flush")
    commands.add_parser(
        "seed", help="create or verify the deterministic demo workspace catalog, then exit"
    )
    demo = commands.add_parser(
        "demo-generate",
        help="write a deterministic synthetic telemetry dataset as JSON Lines, then exit",
    )
    demo.add_argument("scenario", choices=SCENARIOS)
    demo.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    demo.add_argument("--start", help="UTC start time, RFC 3339 (default 2026-10-16T10:30:00Z)")
    demo.add_argument(
        "--duration-s", type=int, default=600, help="length of the dataset in seconds"
    )
    demo.add_argument(
        "--out", required=True, help="signals file (JSON Lines of ingestion records); - for stdout"
    )
    demo.add_argument(
        "--labels", help="separate file for evaluator labels; never mixed into the signals"
    )
    post = commands.add_parser(
        "demo-post",
        help="generate a demo dataset and post it to the ingestion API, then exit",
    )
    post.add_argument("scenario", choices=SCENARIOS)
    post.add_argument(
        "--api-url", default="http://localhost:8010", help="API base URL (default %(default)s)"
    )
    post.add_argument(
        "--seed",
        type=int,
        help="random seed (default: a new one per run, so reruns are distinct evidence)",
    )
    post.add_argument("--start", help="UTC start time, RFC 3339 (default: now minus the duration)")
    post.add_argument(
        "--duration-s", type=int, default=600, help="length of the dataset in seconds"
    )
    post.add_argument("--batch-size", type=int, default=200, help="records per request")
    post.add_argument(
        "--realtime",
        action="store_true",
        help="start now and post records as their timestamps arrive, so detection windows fire",
    )
    shop = commands.add_parser(
        "demo-shop",
        help="run a small instrumented checkout application that exports real telemetry "
        "to the ingestion API, with an optional real bank-gateway slowdown",
    )
    shop.add_argument(
        "--api-url", default="http://localhost:8010", help="API base URL (default %(default)s)"
    )
    shop.add_argument("--rate", type=float, default=1.0, help="orders per second (default 1)")
    shop.add_argument("--run-s", type=int, default=600, help="seconds to run (default 600)")
    shop.add_argument(
        "--slow-after-s",
        type=int,
        help="seconds until bank-gateway slows past payment's timeout (default: never)",
    )
    shop.add_argument(
        "--slow-for-s", type=int, default=240, help="how long the slowdown lasts (default 240)"
    )
    replay = commands.add_parser(
        "evaluation-replay",
        help="replay a generated dataset into its own replay scope, wait for the pipeline "
        "to drain, flush its windows, and print the reconciliation report",
    )
    replay.add_argument("scenario", choices=SCENARIOS)
    replay.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    replay.add_argument(
        "--replay-run-id", type=uuid.UUID, help="reuse a run ID to republish (default: new)"
    )
    replay.add_argument(
        "--timeout-s", type=int, default=300, help="longest wait for the pipeline to drain"
    )
    replay.add_argument(
        "--in-process",
        action="store_true",
        help="run the normalizer, detector, and projector here instead of through Kafka",
    )
    evaluate = commands.add_parser(
        "evaluation-run",
        help="queue an evaluation run of a scenario split with each variant (evaluator role)",
    )
    evaluate.add_argument("--label", required=True, help="a name for the run")
    evaluate.add_argument(
        "--split", choices=["development", "held_out"], default="held_out", help="scenario split"
    )
    evaluate.add_argument(
        "--variant",
        action="append",
        choices=["deterministic", "single_agent", "multi_agent"],
        help="variant to compare (repeatable; default all three)",
    )
    evaluate.add_argument("--trials", type=int, default=3, help="trials per agent variant")
    evaluate.add_argument("--scenario", action="append", help="limit to these scenario names")
    evaluate.add_argument("--budget-microusd", type=int, help="equal budget per case")
    evaluate.add_argument(
        "--execute", action="store_true", help="execute the run here instead of queueing it"
    )
    evaluate.add_argument(
        "--in-process", action="store_true", help="with --execute: replay without Kafka"
    )
    worker = commands.add_parser(
        "evaluation-worker", help="claim and execute queued evaluation runs until stopped"
    )
    worker.add_argument("--in-process", action="store_true", help="replay without Kafka")
    redrive = commands.add_parser(
        "redrive-record",
        help="republish one quarantined record to its topic with a new redrive attempt ID",
    )
    redrive.add_argument("--topic", required=True, help="original_topic from the failure event")
    redrive.add_argument("--partition", required=True, type=int, help="original_partition")
    redrive.add_argument("--offset", required=True, type=int, help="original_offset")
    redrive_outbox = commands.add_parser(
        "redrive-outbox", help="make one quarantined outbox row due for publication again"
    )
    redrive_outbox.add_argument("event_id", type=uuid.UUID, help="outbox row id (event_id)")
    export = commands.add_parser(
        "export-schemas", help="write JSON Schemas, OpenAPI, and generated frontend contracts"
    )
    export.add_argument(
        "--check", action="store_true", help="exit 1 if committed files are out of date"
    )
    export.add_argument(
        "--allow-breaking",
        action="store_true",
        help="rewrite schemas even when a change is incompatible (unreleased contracts only)",
    )
    return parser


def run_api() -> int:
    import uvicorn

    from signalops.api.app import create_app
    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,
        proxy_headers=True,
    )
    return 0


def run_migrate() -> int:
    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.persistence.migrations import upgrade_to_head
    from signalops.persistence.roles import provision_login_roles

    settings = get_settings()
    configure_logging(settings.log_level)
    upgrade_to_head(settings)
    provision_login_roles(settings)
    return 0


def run_init_topics() -> int:
    import logging

    from confluent_kafka.admin import AdminClient

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.streaming.topics import TOPICS, ensure_topics

    settings = get_settings()
    configure_logging(settings.log_level)
    admin = AdminClient(
        {"bootstrap.servers": settings.kafka_bootstrap_servers},
        logger=logging.getLogger("signalops.kafka"),
    )
    created = ensure_topics(admin)
    logging.getLogger(__name__).info(
        "kafka topics ready",
        extra={"topics_created": created, "topics_verified": [spec.name for spec in TOPICS]},
    )
    return 0


def run_seed() -> int:
    import asyncio
    import logging

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine
    from signalops.persistence.seed import DEMO_WORKSPACE_ID, seed_demo

    settings = get_settings()
    configure_logging(settings.log_level)

    async def seed() -> None:
        engine = create_engine(settings)
        try:
            await seed_demo(Database.from_engine(engine))
        finally:
            await engine.dispose()

    asyncio.run(seed())
    logging.getLogger(__name__).info(
        "demo catalog ready", extra={"workspace_id": str(DEMO_WORKSPACE_ID)}
    )
    return 0


def run_outbox_publisher() -> int:
    import asyncio
    import signal

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server
    from signalops.persistence.database import Database, create_engine
    from signalops.streaming.outbox import OutboxPublisher
    from signalops.streaming.producer import EventProducer
    from signalops.streaming.retry import RetryPolicy

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)

    async def publish() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        engine = create_engine(settings)
        producer = EventProducer(settings, client_id="signalops-outbox-publisher")
        publisher = OutboxPublisher(
            Database.from_engine(engine),
            producer,
            batch_size=settings.outbox_batch_size,
            idle_interval_seconds=settings.outbox_idle_interval_ms / 1000,
            retry_policy=RetryPolicy.from_settings(settings),
            stats_interval_seconds=settings.streaming_stats_interval_ms / 1000,
        )
        try:
            await publisher.run(stop)
        finally:
            producer.close()
            await engine.dispose()

    asyncio.run(publish())
    return 0


def _run_consumer(
    consumer_group: str,
    event_type: "EventType",
    handler: Callable[["Settings"], "EventHandler"],
) -> int:
    """Consume one event type with a transactional handler until SIGINT or SIGTERM."""
    import asyncio
    import signal

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server
    from signalops.persistence.database import Database, create_engine
    from signalops.streaming.consumer import ConsumerRunner, TransactionalProcessor, create_consumer
    from signalops.streaming.producer import EventProducer
    from signalops.streaming.retry import RetryPolicy
    from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)
    client_id = f"signalops-{consumer_group.removesuffix('-v1')}"

    async def consume() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        engine = create_engine(settings)
        failures = EventProducer(settings, client_id=f"{client_id}-failures")
        runner = ConsumerRunner(
            create_consumer(settings, group_id=consumer_group, client_id=client_id),
            TransactionalProcessor(Database.from_engine(engine), consumer_group, handler(settings)),
            consumer_group=consumer_group,
            failure_publisher=failures,
            retry_policy=RetryPolicy.from_settings(settings),
            stats_interval_seconds=settings.streaming_stats_interval_ms / 1000,
        )
        runner.subscribe([TOPIC_BY_EVENT_TYPE[event_type]])
        try:
            await runner.run(stop)
        finally:
            failures.close()
            await engine.dispose()

    asyncio.run(consume())
    return 0


def run_normalizer() -> int:
    from signalops.adapters.normalizer import NORMALIZER_CONSUMER_GROUP, Normalizer
    from signalops.adapters.quality import TimingPolicy
    from signalops.contracts.events import EventType

    return _run_consumer(
        NORMALIZER_CONSUMER_GROUP,
        EventType.SIGNAL_RAW,
        lambda settings: Normalizer(TimingPolicy.from_settings(settings)),
    )


def run_detector() -> int:
    from signalops.contracts.events import EventType
    from signalops.detection.detector import DETECTOR_CONSUMER_GROUP, DetectionPolicy, Detector

    return _run_consumer(
        DETECTOR_CONSUMER_GROUP,
        EventType.SIGNAL_NORMALIZED,
        lambda settings: Detector(DetectionPolicy.from_settings(settings)),
    )


def run_graph_projector() -> int:
    from signalops.contracts.events import EventType
    from signalops.detection.graph import GRAPH_CONSUMER_GROUP, GraphPolicy, GraphProjector

    return _run_consumer(
        GRAPH_CONSUMER_GROUP,
        EventType.SIGNAL_NORMALIZED,
        lambda settings: GraphProjector(GraphPolicy.from_settings(settings)),
    )


def run_dispatcher() -> int:
    from signalops.agents.investigations import DISPATCHER_CONSUMER_GROUP, Dispatcher
    from signalops.contracts.events import EventType

    return _run_consumer(
        DISPATCHER_CONSUMER_GROUP,
        EventType.INVESTIGATION_REQUESTED,
        lambda _settings: Dispatcher(),
    )


def run_worker() -> int:
    import asyncio
    import signal

    from redis.asyncio import Redis

    from signalops.agents.investigator import AgentRuntime
    from signalops.agents.jobs import JobPolicy, JobRunner
    from signalops.agents.workflow import WorkflowEngine
    from signalops.config import get_settings
    from signalops.gateway.gateway import build_gateway
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server
    from signalops.persistence.database import Database, create_engine
    from signalops.wakeups import RedisProgressNotifier

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)

    async def work() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        engine = create_engine(settings)
        redis = Redis.from_url(str(settings.redis_url))
        # Each committed step or ending wakes the progress streams watching it.
        database = Database.from_engine(engine, on_progress=RedisProgressNotifier(redis))
        gateway = build_gateway(database, redis, settings)
        # One engine for every runner, so its branch slots bound the whole process.
        workflow = WorkflowEngine(
            database,
            runtime=AgentRuntime.from_settings(database, gateway, redis, settings),
            branch_slots=asyncio.Semaphore(settings.agent_global_branches),
        )
        policy = JobPolicy.from_settings(settings)
        runners = [
            JobRunner(database, workflow, policy) for _ in range(settings.worker_job_concurrency)
        ]
        try:
            await asyncio.gather(*(runner.run(stop) for runner in runners))
        finally:
            await redis.aclose()
            await engine.dispose()

    asyncio.run(work())
    return 0


def run_detector_closer() -> int:
    import asyncio
    import signal

    from signalops.config import get_settings
    from signalops.detection.closure import WindowCloser
    from signalops.detection.detector import DetectionPolicy
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server
    from signalops.persistence.database import Database, create_engine

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)

    async def close() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        engine = create_engine(settings)
        closer = WindowCloser(
            Database.from_engine(engine),
            DetectionPolicy.from_settings(settings),
            interval_seconds=settings.detection_closer_interval_ms / 1000,
        )
        try:
            await closer.run(stop)
        finally:
            await engine.dispose()

    asyncio.run(close())
    return 0


def run_detector_flush(args: argparse.Namespace) -> int:
    import asyncio

    from signalops.config import get_settings
    from signalops.contracts.common import replay_scope_for
    from signalops.detection.closure import flush_replay
    from signalops.detection.detector import DetectionPolicy
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine
    from signalops.persistence.repositories.scope import EvidenceScope

    settings = get_settings()
    configure_logging(settings.log_level)
    scope = EvidenceScope(args.workspace_id, replay_scope_for(args.replay_run_id))

    async def flush() -> int:
        engine = create_engine(settings)
        try:
            return await flush_replay(
                Database.from_engine(engine), scope, DetectionPolicy.from_settings(settings)
            )
        finally:
            await engine.dispose()

    print(f"closed {asyncio.run(flush())} windows in {scope.replay_scope}")
    return 0


def run_demo_shop(args: argparse.Namespace) -> int:
    import asyncio
    import os
    from datetime import timedelta

    from pydantic import TypeAdapter, ValidationError

    from signalops.demo.shop import FaultSchedule, ingestion_exporter, run_shop
    from signalops.logs import configure_logging

    configure_logging(os.environ.get("SIGNALOPS_LOG_LEVEL", "INFO"))
    if not args.api_url.startswith(("http://", "https://")):
        print("--api-url must use http or https", file=sys.stderr)
        return 2
    if not 0 < args.rate <= 50:
        print("--rate must be in (0, 50]", file=sys.stderr)
        return 2
    try:
        tokens = TypeAdapter(dict[uuid.UUID, str]).validate_json(
            os.environ.get("SIGNALOPS_INGEST_TOKENS", "{}")
        )
    except ValidationError:
        print(
            "SIGNALOPS_INGEST_TOKENS must be a JSON object of source ID to token", file=sys.stderr
        )
        return 2
    schedule = None
    if args.slow_after_s is not None:
        schedule = FaultSchedule(
            after=timedelta(seconds=args.slow_after_s), duration=timedelta(seconds=args.slow_for_s)
        )
    asyncio.run(
        run_shop(
            rate_per_second=args.rate,
            run_for=timedelta(seconds=args.run_s),
            schedule=schedule,
            export=ingestion_exporter(args.api_url, tokens),
        )
    )
    return 0


def run_evaluation_replay(args: argparse.Namespace) -> int:
    import asyncio
    from datetime import timedelta

    from signalops.adapters.ingestion import DatabaseSourceLookup, IngestPolicy, IngestSource
    from signalops.adapters.quality import TimingPolicy
    from signalops.config import get_settings
    from signalops.contracts.common import replay_scope_for
    from signalops.contracts.evaluation import ReplayStatus
    from signalops.demo.generator import Scenario, generate
    from signalops.detection.detector import DetectionPolicy
    from signalops.detection.graph import GraphPolicy
    from signalops.evaluation.replay import (
        KafkaTransport,
        LocalTransport,
        ReplayReport,
        ReplayTiming,
        ReplayTransport,
        replay,
    )
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine
    from signalops.persistence.repositories.scope import EvidenceScope
    from signalops.persistence.seed import DEMO_SOURCES, DEMO_WORKSPACE_ID
    from signalops.streaming.producer import EventProducer

    settings = get_settings()
    configure_logging(settings.log_level)
    dataset = generate(Scenario(args.scenario), seed=args.seed)
    run_id = args.replay_run_id or uuid.uuid4()
    detection = DetectionPolicy.from_settings(settings)

    async def run() -> ReplayReport:
        engine = create_engine(settings)
        database = Database.from_engine(engine)
        transport: ReplayTransport
        if args.in_process:
            transport = LocalTransport(
                database,
                EvidenceScope(DEMO_WORKSPACE_ID, replay_scope_for(run_id)),
                timing_policy=TimingPolicy.from_settings(settings),
                detection_policy=detection,
                graph_policy=GraphPolicy.from_settings(settings),
            )
        else:
            transport = KafkaTransport(EventProducer(settings, client_id="signalops-replay"))
        try:
            lookup = DatabaseSourceLookup(database)
            sources: dict[uuid.UUID, IngestSource] = {}
            for demo in DEMO_SOURCES:
                source = await lookup.get(demo.id)
                if source is None or not source.enabled:
                    raise SystemExit(f"source {demo.id} is missing or disabled; run seed first")
                sources[source.id] = source
            return await replay(
                database,
                dataset.records,
                replay_run_id=run_id,
                workspace_id=DEMO_WORKSPACE_ID,
                sources=sources,
                transport=transport,
                ingest_policy=IngestPolicy.from_settings(settings),
                detection_policy=detection,
                timing=ReplayTiming(timeout=timedelta(seconds=args.timeout_s)),
            )
        finally:
            transport.close()
            await engine.dispose()

    report = asyncio.run(run())
    print(report.model_dump_json(indent=2))
    return 0 if report.status is ReplayStatus.COMPLETED else 1


def _runner_context(
    settings: "Settings", database: "Database", in_process: bool
) -> "RunnerContext":
    from signalops.adapters.ingestion import IngestPolicy, IngestSource
    from signalops.adapters.quality import TimingPolicy
    from signalops.detection.detector import DetectionPolicy
    from signalops.detection.graph import GraphPolicy
    from signalops.evaluation.replay import KafkaTransport, LocalTransport, ReplayTransport
    from signalops.evaluation.runner import RunnerContext
    from signalops.persistence.repositories.scope import EvidenceScope
    from signalops.persistence.seed import DEMO_SOURCES, DEMO_WORKSPACE_ID
    from signalops.streaming.producer import EventProducer

    detection = DetectionPolicy.from_settings(settings)

    def transport_for(scope: EvidenceScope) -> ReplayTransport:
        if in_process:
            return LocalTransport(
                database,
                scope,
                timing_policy=TimingPolicy.from_settings(settings),
                detection_policy=detection,
                graph_policy=GraphPolicy.from_settings(settings),
            )
        return KafkaTransport(EventProducer(settings, client_id="signalops-evaluation"))

    return RunnerContext(
        database=database,
        sources={
            source.id: IngestSource(source.id, DEMO_WORKSPACE_ID, source.kind, enabled=True)
            for source in DEMO_SOURCES
        },
        ingest_policy=IngestPolicy.from_settings(settings),
        detection_policy=detection,
        transport_for=transport_for,
    )


def run_evaluation_run(args: argparse.Namespace) -> int:
    import asyncio
    import socket

    from signalops.config import get_settings
    from signalops.contracts.evaluation import RunStatus, ScenarioSplit, SystemVariant
    from signalops.evaluation.runner import RunExecutor, RunRequest, queue_run
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine
    from signalops.persistence.seed import DEMO_WORKSPACE_ID

    settings = get_settings()
    configure_logging(settings.log_level)
    request = RunRequest(
        label=args.label,
        split=ScenarioSplit(args.split),
        variants=tuple(SystemVariant(value) for value in args.variant or SystemVariant),
        trials=args.trials,
        scenarios=tuple(args.scenario or ()),
        budget_microusd=args.budget_microusd,
    )
    evaluator = settings.evaluator_settings()

    async def run() -> int:
        engine = create_engine(evaluator)
        database = Database.from_engine(engine)
        try:
            queued = await queue_run(
                database, request, workspace_id=DEMO_WORKSPACE_ID, settings=settings
            )
            print(f"queued evaluation run {queued.id}")
            if not args.execute:
                return 0
            executor = RunExecutor(
                _runner_context(evaluator, database, args.in_process),
                owner=f"cli-{socket.gethostname()}",
            )
            while (claimed := await executor.claim_and_execute()) is not None:
                if claimed == queued.id:
                    break
            async with database.transaction() as session:
                from signalops.persistence.repositories.evaluation import EvaluationRepository

                stored = await EvaluationRepository(session).run(queued.id)
            assert stored is not None  # noqa: S101 - just created
            detail = stored.error or f"see GET /v1/evaluation-runs/{stored.id}"
            print(f"{stored.status}: {detail}")
            return 0 if stored.status == RunStatus.COMPLETED else 1
        finally:
            await engine.dispose()

    return asyncio.run(run())


def run_evaluation_worker(args: argparse.Namespace) -> int:
    import asyncio
    import contextlib
    import signal
    import socket

    from signalops.config import get_settings
    from signalops.evaluation.runner import RunExecutor
    from signalops.logs import configure_logging
    from signalops.observability.metrics import start_metrics_server
    from signalops.persistence.database import Database, create_engine

    settings = get_settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)
    evaluator = settings.evaluator_settings()

    async def work() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        engine = create_engine(evaluator)
        database = Database.from_engine(engine)
        executor = RunExecutor(
            _runner_context(evaluator, database, args.in_process),
            owner=f"evaluation-{socket.gethostname()}",
        )
        try:
            while not stop.is_set():
                if await executor.claim_and_execute() is None:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), 5)
        finally:
            await engine.dispose()

    asyncio.run(work())
    return 0


def run_redrive_record(args: argparse.Namespace) -> int:
    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.streaming.failures import RecordPosition
    from signalops.streaming.redrive import RedriveError, redrive_record

    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        result = redrive_record(settings, RecordPosition(args.topic, args.partition, args.offset))
    except RedriveError as error:
        print(error, file=sys.stderr)
        return 1
    location = f"{result.topic}[{result.partition}]@{result.offset}"
    print(f"redrive attempt {result.attempt_id} published at {location}")
    return 0


def run_redrive_outbox(args: argparse.Namespace) -> int:
    import asyncio

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine
    from signalops.persistence.repositories.reliability import OutboxRepository

    settings = get_settings()
    configure_logging(settings.log_level)

    async def redrive() -> bool:
        engine = create_engine(settings)
        try:
            async with Database.from_engine(engine).transaction() as session:
                return await OutboxRepository(session).redrive(args.event_id)
        finally:
            await engine.dispose()

    if not asyncio.run(redrive()):
        print(f"no quarantined pending outbox row {args.event_id}", file=sys.stderr)
        return 1
    print(f"outbox row {args.event_id} is due for publication")
    return 0


def run_demo_generate(args: argparse.Namespace) -> int:
    from datetime import datetime, timedelta
    from pathlib import Path

    from signalops.demo.generator import DEFAULT_START, Scenario, generate, serialize_records

    start = DEFAULT_START
    if args.start is not None:
        start = datetime.fromisoformat(args.start)
        if start.tzinfo is None:
            print("--start must include a UTC offset", file=sys.stderr)
            return 2
    try:
        dataset = generate(
            Scenario(args.scenario),
            seed=args.seed,
            start=start,
            duration=timedelta(seconds=args.duration_s),
        )
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    signals = serialize_records(dataset.records)
    if args.out == "-":
        sys.stdout.buffer.write(signals)
        sys.stdout.flush()
    else:
        Path(args.out).write_bytes(signals)
    if args.labels is not None:
        Path(args.labels).write_text(dataset.labels.model_dump_json(indent=2) + "\n")
    print(
        f"{dataset.labels.record_count} records, sha256 {dataset.labels.dataset_hash}",
        file=sys.stderr,
    )
    return 0


def run_demo_post(args: argparse.Namespace) -> int:
    import logging
    import os
    import secrets
    from datetime import UTC, datetime, timedelta

    from pydantic import TypeAdapter, ValidationError

    from signalops.demo.generator import Scenario, generate
    from signalops.demo.poster import (
        PostError,
        post_records,
        post_records_realtime,
        urllib_transport,
    )
    from signalops.logs import configure_logging

    configure_logging(os.environ.get("SIGNALOPS_LOG_LEVEL", "INFO"))
    if not args.api_url.startswith(("http://", "https://")):
        print("--api-url must use http or https", file=sys.stderr)
        return 2
    try:
        tokens = TypeAdapter(dict[uuid.UUID, str]).validate_json(
            os.environ.get("SIGNALOPS_INGEST_TOKENS", "{}")
        )
    except ValidationError:
        print(
            "SIGNALOPS_INGEST_TOKENS must be a JSON object of source ID to token", file=sys.stderr
        )
        return 2
    duration = timedelta(seconds=args.duration_s)
    if args.realtime and args.start is not None:
        print("--realtime starts now; it does not take --start", file=sys.stderr)
        return 2
    if args.realtime:
        start = datetime.now(UTC).replace(microsecond=0)
    elif args.start is None:
        start = datetime.now(UTC).replace(microsecond=0) - duration
    else:
        start = datetime.fromisoformat(args.start)
        if start.tzinfo is None:
            print("--start must include a UTC offset", file=sys.stderr)
            return 2
    # Source event IDs derive from scenario and seed; a fresh seed keeps reruns distinct.
    seed = args.seed if args.seed is not None else secrets.randbelow(2**31)
    try:
        dataset = generate(Scenario(args.scenario), seed=seed, start=start, duration=duration)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    try:
        post = post_records_realtime if args.realtime else post_records
        summary = post(
            dataset.records,
            api_url=args.api_url,
            tokens=tokens,
            transport=urllib_transport(timeout_seconds=60),
            batch_size=args.batch_size,
        )
    except PostError as error:
        print(error, file=sys.stderr)
        return 1
    logging.getLogger(__name__).info(
        "demo dataset posted",
        extra={
            "scenario": args.scenario,
            "seed": seed,
            "start": start.isoformat(),
            "records": len(dataset.records),
            "accepted": summary.accepted,
            "rejected": summary.rejected,
            "rejected_reasons": summary.rejected_reasons,
            "retries": summary.retries,
        },
    )
    return 1 if summary.rejected else 0


def run_export_schemas(args: argparse.Namespace) -> int:
    from signalops.contracts.export import BreakingChangeError, stale_paths, write_all

    if args.check:
        stale = stale_paths()
        for path in stale:
            print(f"out of date: {path}", file=sys.stderr)
        return 1 if stale else 0
    try:
        written = write_all(allow_breaking=args.allow_breaking)
    except BreakingChangeError as error:
        print(error, file=sys.stderr)
        return 1
    for path in written:
        print(f"wrote {path}")
    return 0


def _snapshot_reader(
    settings: "Settings",
) -> tuple[Callable[[], "Awaitable[OperationsSnapshot]"], Callable[[], "Awaitable[None]"]]:
    """A snapshot reader on its own small engine, and its cleanup."""
    import asyncio
    from datetime import UTC, datetime

    from confluent_kafka.admin import AdminClient

    from signalops.observability.snapshot import OperationsSnapshot, database_state, kafka_lag
    from signalops.persistence.database import Database, create_engine

    engine = create_engine(settings)
    database = Database.from_engine(engine)
    admin = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "signalops-metrics-exporter",
        }
    )

    async def read() -> OperationsSnapshot:
        async with database.transaction() as session:
            state = await database_state(session)
        lag = await asyncio.to_thread(kafka_lag, admin)
        return OperationsSnapshot(taken_at=datetime.now(UTC), database=state, kafka_lag=lag)

    return read, engine.dispose


def run_metrics_exporter() -> int:
    import asyncio
    import signal

    from prometheus_client import CollectorRegistry, start_http_server

    from signalops.config import get_settings
    from signalops.logs import configure_logging
    from signalops.observability.exporter import Exporter

    settings = get_settings()
    configure_logging(settings.log_level)
    if settings.metrics_port is None:
        print("metrics-exporter needs SIGNALOPS_METRICS_PORT", file=sys.stderr)
        return 2

    async def export() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        read, close = _snapshot_reader(settings)
        registry = CollectorRegistry()
        exporter = Exporter(read, registry, interval_seconds=settings.metrics_refresh_seconds)
        assert settings.metrics_port is not None  # noqa: S101 - checked above
        start_http_server(settings.metrics_port, addr=settings.metrics_host, registry=registry)
        try:
            await exporter.run(stop)
        finally:
            await close()

    asyncio.run(export())
    return 0


def run_operations_snapshot() -> int:
    import asyncio

    from signalops.config import get_settings

    settings = get_settings()

    async def snapshot() -> str:
        read, close = _snapshot_reader(settings)
        try:
            return (await read()).model_dump_json(indent=2)
        finally:
            await close()

    print(asyncio.run(snapshot()))
    return 0


COMMANDS = {
    "api": run_api,
    "migrate": run_migrate,
    "init-topics": run_init_topics,
    "seed": run_seed,
    "outbox-publisher": run_outbox_publisher,
    "normalizer": run_normalizer,
    "detector": run_detector,
    "graph-projector": run_graph_projector,
    "dispatcher": run_dispatcher,
    "worker": run_worker,
    "detector-closer": run_detector_closer,
    "metrics-exporter": run_metrics_exporter,
    "operations-snapshot": run_operations_snapshot,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "export-schemas":
        return run_export_schemas(args)
    if args.command == "demo-generate":
        return run_demo_generate(args)
    if args.command == "demo-post":
        return run_demo_post(args)
    if args.command == "detector-flush":
        return run_detector_flush(args)
    if args.command == "evaluation-run":
        return run_evaluation_run(args)
    if args.command == "evaluation-worker":
        return run_evaluation_worker(args)
    if args.command == "demo-shop":
        return run_demo_shop(args)
    if args.command == "evaluation-replay":
        return run_evaluation_replay(args)
    if args.command == "redrive-record":
        return run_redrive_record(args)
    if args.command == "redrive-outbox":
        return run_redrive_outbox(args)
    command = COMMANDS.get(args.command)
    if command is None:
        parser.print_help()
        return 1
    return command()


if __name__ == "__main__":
    sys.exit(main())
