"""Correlation fields carried by every log line written inside a bound block.

`bind` adds fields for the current task and everything it awaits or spawns
(asyncio tasks copy the context), so a consumer's event ID or a job's
investigation ID reaches log lines written deep in repositories and the
gateway without passing them down. Fields given to a log call with `extra=`
take precedence.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_FIELDS: ContextVar[dict[str, Any]] = ContextVar("signalops_log_fields", default={})  # noqa: B039 - never mutated


@contextmanager
def bind(**fields: Any) -> Iterator[None]:
    """Add non-null fields to log lines written until the block ends."""
    present = {key: str(value) for key, value in fields.items() if value is not None}
    token = _FIELDS.set({**_FIELDS.get(), **present})
    try:
        yield
    finally:
        _FIELDS.reset(token)


def current() -> dict[str, Any]:
    return _FIELDS.get()
