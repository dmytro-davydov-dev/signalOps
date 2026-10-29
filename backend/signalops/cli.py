"""Process entry points for the shared backend image."""

import argparse
import sys

from signalops import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalops")
    parser.add_argument("--version", action="version", version=f"signalops {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="command")
    commands.add_parser("api", help="serve the HTTP API")
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "api":
        return run_api()
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
