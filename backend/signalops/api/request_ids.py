"""Request IDs: accepted from `X-Request-ID` when well formed, otherwise generated.

The ID is bound to every log line written while the request runs and returned in
the response's `X-Request-ID` header, so a client report can be matched to logs.
"""

import re
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from signalops.observability.context import bind

HEADER = b"x-request-id"
_ACCEPTED = re.compile(rb"^[A-Za-z0-9._:-]{1,64}$")


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        presented = dict(scope["headers"]).get(HEADER)
        request_id = (
            presented.decode()
            if presented is not None and _ACCEPTED.match(presented)
            else uuid.uuid4().hex
        )

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((HEADER, request_id.encode()))
            await send(message)

        with bind(request_id=request_id):
            await self._app(scope, receive, send_with_id)
