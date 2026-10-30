"""Authenticated batch ingestion: `POST /v1/ingest/{source_id}`.

Status codes: 200 when every record has a final outcome (accepted or
rejected); 503 with the same body when any record failed and should be
retried. Resending the whole batch is safe: accepted records get the same
event IDs and are deduplicated downstream. Whole-request errors are 401
(credential), 403 (source disabled), 404 (no such source), 413 (size), 422
(not a batch), and 503 (database or publisher unavailable).
"""

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.exc import SQLAlchemyError

from signalops.adapters.ingestion import (
    IngestPolicy,
    IngestSource,
    SourceConfigError,
    SourceTokens,
    ingest_batch,
)
from signalops.config import Settings
from signalops.contracts.ingestion import IngestRequest, IngestResponse
from signalops.dependencies import Resources

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/ingest", tags=["ingestion"])

bearer = HTTPBearer(auto_error=False, description="Source-specific ingestion token")

RETRY_AFTER_SECONDS = "5"


def _inline_defs(schema: dict[str, Any]) -> dict[str, Any]:
    """Resolve local `$defs` references, which OpenAPI would read against its own root."""
    defs = schema.pop("$defs", {})

    def resolve(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                return resolve(defs[ref.removeprefix("#/$defs/")])
            return {key: resolve(value) for key, value in node.items()}
        if isinstance(node, list):
            return [resolve(item) for item in node]
        return node

    resolved: dict[str, Any] = resolve(schema)
    return resolved


REQUEST_SCHEMA = _inline_defs(IngestRequest.model_json_schema())
REQUEST_BODY_OPENAPI = {
    "requestBody": {"required": True, "content": {"application/json": {"schema": REQUEST_SCHEMA}}}
}


def _unauthorized() -> HTTPException:
    return HTTPException(
        status.HTTP_401_UNAUTHORIZED,
        "missing or invalid source credential",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _unavailable(detail: str) -> HTTPException:
    return HTTPException(
        status.HTTP_503_SERVICE_UNAVAILABLE, detail, headers={"Retry-After": RETRY_AFTER_SECONDS}
    )


async def _read_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, f"request exceeds {limit} bytes")
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > limit:
            raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, f"request exceeds {limit} bytes")
    return bytes(body)


def _records(body: bytes, max_records: int) -> list[object]:
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "body is not valid JSON"
        ) from None
    records = document.get("records") if isinstance(document, dict) else None
    if not isinstance(records, list) or not records:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "body must be an object with non-empty records"
        )
    if len(records) > max_records:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE, f"batch exceeds {max_records} records"
        )
    return records


async def _source(resources: Resources, source_id: uuid.UUID) -> IngestSource:
    if resources.sources is None:
        raise _unavailable("ingestion is not configured")
    try:
        source = await resources.sources.get(source_id)
    except SQLAlchemyError as error:
        logger.warning(
            "ingest source lookup failed",
            extra={"source_id": str(source_id), "error": type(error).__name__},
        )
        raise _unavailable("source catalog unavailable") from None
    except SourceConfigError as error:
        logger.error(
            "ingest source misconfigured", extra={"source_id": str(source_id), "error": str(error)}
        )
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "source configuration is invalid"
        ) from None
    if source is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "source not found")
    if not source.enabled:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "source is disabled")
    return source


@router.post(
    "/{source_id}",
    openapi_extra=REQUEST_BODY_OPENAPI,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": IngestResponse,
            "description": "At least one record failed; retry those with the same IDs",
        },
    },
)
async def ingest(
    source_id: uuid.UUID,
    request: Request,
    response: Response,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> IngestResponse:
    """Publish a batch of source records; each is accepted only after Kafka acknowledges it."""
    settings: Settings = request.app.state.settings
    resources: Resources = request.app.state.resources
    tokens: SourceTokens = request.app.state.source_tokens
    policy: IngestPolicy = request.app.state.ingest_policy
    if credentials is None or not tokens.verify(source_id, credentials.credentials):
        raise _unauthorized()
    source = await _source(resources, source_id)
    if resources.ingest_publisher is None:
        raise _unavailable("ingestion is not configured")

    body = await _read_body(request, settings.ingest_max_request_bytes)
    items = _records(body, settings.ingest_max_records)
    result = await ingest_batch(
        items,
        source=source,
        publisher=resources.ingest_publisher,
        policy=policy,
        now=datetime.now(UTC),
        batch_id=uuid.uuid4(),
    )
    if result.failed:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        response.headers["Retry-After"] = RETRY_AFTER_SECONDS
    return result
