"""Process entry points for the shared backend image."""

import argparse
import sys

from signalops import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalops")
    parser.add_argument("--version", action="version", version=f"signalops {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="command")
    commands.add_parser("api", help="serve the HTTP API")
    commands.add_parser("migrate", help="apply database migrations up to head, then exit")
    commands.add_parser("init-topics", help="create or verify Kafka topics, then exit")
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


COMMANDS = {"api": run_api, "migrate": run_migrate, "init-topics": run_init_topics}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "export-schemas":
        return run_export_schemas(args)
    command = COMMANDS.get(args.command)
    if command is None:
        parser.print_help()
        return 1
    return command()


if __name__ == "__main__":
    sys.exit(main())
