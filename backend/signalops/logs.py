"""Structured JSON logging for all processes."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from signalops.observability import context

# Attributes present on every LogRecord; anything else came from `extra=`.
# `color_message` is uvicorn's ANSI-colored duplicate of the message.
_RESERVED = frozenset(vars(logging.makeLogRecord({}))) | {
    "message",
    "asctime",
    "taskName",
    "color_message",
}


PROVIDER_CLIENT_LOGGERS = ("anthropic", "httpx2", "httpcore", "httpcore2")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(context.current())
        for key, value in vars(record).items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # At DEBUG these log request bodies, which hold evidence sent to the provider.
    for name in PROVIDER_CLIENT_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    # Route uvicorn through the same handler instead of its own formatters.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
