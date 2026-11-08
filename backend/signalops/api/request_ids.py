"""Request IDs, one audit line per request, and the request body limit.

The ID is taken from `X-Request-ID` when well formed, otherwise generated. It is
bound to every log line written while the request runs and returned in the
response's `X-Request-ID` header. When the request ends, an `api request` line
records method, path, status, duration, and the ID of the credential that
authenticated it (never the token).

Bodies on every route but ingestion are capped at `limit` bytes: a larger
declared or streamed body is answered 413 before any route code runs.
Ingestion enforces its own, larger limit.
"""

import json
import logging
import re
import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from signalops.observability.context import bind

logger = logging.getLogger(__name__)

HEADER = b"x-request-id"
_ACCEPTED = re.compile(rb"^[A-Za-z0-9._:-]{1,64}$")
INGEST_PREFIX = "/v1/ingest/"


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int = 64 * 1024) -> None:
        self._app = app
        self._limit = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        headers = dict(scope["headers"])
        presented = headers.get(HEADER)
        request_id = (
            presented.decode()
            if presented is not None and _ACCEPTED.match(presented)
            else uuid.uuid4().hex
        )
        status_code = 500

        async def send_with_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                message.setdefault("headers", []).append((HEADER, request_id.encode()))
            await send(message)

        began = time.monotonic()
        with bind(request_id=request_id):
            try:
                if scope["path"].startswith(INGEST_PREFIX):
                    await self._app(scope, receive, send_with_id)
                else:
                    body = await _bounded_body(headers, receive, self._limit)
                    if body is None:
                        await _too_large(send_with_id, self._limit)
                    else:
                        await self._app(scope, _replay(body, receive), send_with_id)
            finally:
                state = scope.get("state") or {}
                logger.info(
                    "api request",
                    extra={
                        "method": scope["method"],
                        "path": scope["path"],
                        "status": status_code,
                        "duration_ms": round((time.monotonic() - began) * 1000, 1),
                        "credential_id": state.get("credential_id"),
                    },
                )


async def _bounded_body(headers: dict[bytes, bytes], receive: Receive, limit: int) -> bytes | None:
    """The whole body, or None when it is over `limit` bytes."""
    declared = headers.get(b"content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        return None
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        body += message.get("body", b"")
        if len(body) > limit:
            return None
        if not message.get("more_body", False):
            break
    return bytes(body)


def _replay(body: bytes, receive: Receive) -> Receive:
    sent = False

    async def replayed() -> Message:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        # Later reads wait for the disconnect, as Starlette expects.
        return await receive()

    return replayed


async def _too_large(send: Send, limit: int) -> None:
    content = json.dumps({"detail": f"request exceeds {limit} bytes"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(content)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": content})
