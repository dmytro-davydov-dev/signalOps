"""Process entry points for the shared backend image."""

import argparse
import sys
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING

from signalops import __version__

if TYPE_CHECKING:
    from signalops.config import Settings
    from signalops.contracts.events import EventType
    from signalops.streaming.consumer import EventHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalops")
    parser.add_argument("--version", action="version", version=f"signalops {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="command")
    commands.add_parser("api", help="serve the HTTP API")
    commands.add_parser("migrate", help="apply database migrations up to head, then exit")
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
        "detector-closer", help="close live detection windows by wall clock until stopped"
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
    demo.add_argument("scenario", choices=["healthy", "downstream-timeout"])
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
    post.add_argument("scenario", choices=["healthy", "downstream-timeout"])
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

    settings = get_settings()
    configure_logging(settings.log_level)
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

    settings = get_settings()
    configure_logging(settings.log_level)
    upgrade_to_head(settings)
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
    from signalops.persistence.database import Database, create_engine
    from signalops.streaming.outbox import OutboxPublisher
    from signalops.streaming.producer import EventProducer
    from signalops.streaming.retry import RetryPolicy

    settings = get_settings()
    configure_logging(settings.log_level)

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
    from signalops.persistence.database import Database, create_engine
    from signalops.streaming.consumer import ConsumerRunner, TransactionalProcessor, create_consumer
    from signalops.streaming.producer import EventProducer
    from signalops.streaming.retry import RetryPolicy
    from signalops.streaming.topics import TOPIC_BY_EVENT_TYPE

    settings = get_settings()
    configure_logging(settings.log_level)
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


def run_detector_closer() -> int:
    import asyncio
    import signal

    from signalops.config import get_settings
    from signalops.detection.closure import WindowCloser
    from signalops.detection.detector import DetectionPolicy
    from signalops.logs import configure_logging
    from signalops.persistence.database import Database, create_engine

    settings = get_settings()
    configure_logging(settings.log_level)

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


COMMANDS = {
    "api": run_api,
    "migrate": run_migrate,
    "init-topics": run_init_topics,
    "seed": run_seed,
    "outbox-publisher": run_outbox_publisher,
    "normalizer": run_normalizer,
    "detector": run_detector,
    "detector-closer": run_detector_closer,
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
