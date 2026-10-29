"""Process entry points for the shared backend image."""

import argparse
import sys

from signalops import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalops")
    parser.add_argument("--version", action="version", version=f"signalops {__version__}")
    parser.add_subparsers(dest="command", metavar="command")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
