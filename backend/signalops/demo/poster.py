"""Post a generated demo dataset to the ingestion API.

Records are grouped by source and sent in batches with that source's token.
Records that fail (503) are resent with the same `source_event_id` after a
backoff; rejected records are counted and not resent, because resending an
invalid record unchanged fails again.
"""

import json
import logging
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from pydantic import JsonValue

from signalops.demo.generator import DemoRecord

logger = logging.getLogger(__name__)

# (url, body, headers) -> (status, body). Raises OSError when no response arrived.
Transport = Callable[[str, bytes, Mapping[str, str]], tuple[int, bytes]]


class PostError(RuntimeError):
    """The API refused a whole batch, or failures outlasted the retries."""


def urllib_transport(timeout_seconds: float) -> Transport:
    def send(url: str, body: bytes, headers: Mapping[str, str]) -> tuple[int, bytes]:
        if not url.startswith(("http://", "https://")):
            raise ValueError("API URL must use http or https")
        # S310: the scheme is checked above.
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")  # noqa: S310
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    return send


@dataclass
class PostSummary:
    accepted: int = 0
    rejected: int = 0
    # Reasons for rejected records, by count.
    rejected_reasons: dict[str, int] = field(default_factory=dict)
    batches: int = 0
    retries: int = 0


def _item(record: DemoRecord) -> dict[str, JsonValue]:
    item = record.to_json()
    del item["source_id"]
    return item


def post_records(
    records: Sequence[DemoRecord],
    *,
    api_url: str,
    tokens: Mapping[uuid.UUID, str],
    transport: Transport,
    batch_size: int = 200,
    max_attempts: int = 5,
    backoff_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> PostSummary:
    by_source: dict[uuid.UUID, list[DemoRecord]] = {}
    for record in records:
        by_source.setdefault(record.source_id, []).append(record)
    missing = sorted(str(source_id) for source_id in by_source.keys() - tokens.keys())
    if missing:
        raise PostError(f"no ingestion token for sources: {', '.join(missing)}")

    summary = PostSummary()
    for source_id, source_records in by_source.items():
        url = f"{api_url.rstrip('/')}/v1/ingest/{source_id}"
        headers = {
            "Authorization": f"Bearer {tokens[source_id]}",
            "Content-Type": "application/json",
        }
        for start in range(0, len(source_records), batch_size):
            pending = [_item(r) for r in source_records[start : start + batch_size]]
            summary.batches += 1
            for attempt in range(1, max_attempts + 1):
                pending = _send_batch(url, headers, pending, transport, summary)
                if not pending:
                    break
                if attempt == max_attempts:
                    raise PostError(
                        f"{len(pending)} records still failing after {attempt} attempts"
                    )
                summary.retries += 1
                sleep(backoff_seconds * 2 ** (attempt - 1))
    return summary


def _send_batch(
    url: str,
    headers: Mapping[str, str],
    items: list[dict[str, JsonValue]],
    transport: Transport,
    summary: PostSummary,
) -> list[dict[str, JsonValue]]:
    """Send once; return the items to resend."""
    try:
        status, body = transport(url, json.dumps({"records": items}).encode(), headers)
    except OSError as error:
        # No response: the outcome is unknown, so resend everything with the same IDs.
        logger.warning("ingest request failed", extra={"error": type(error).__name__})
        return items
    if status not in (200, 503):
        raise PostError(f"ingest returned {status}: {body[:500].decode(errors='replace')}")
    try:
        results = json.loads(body)["results"]
    except (ValueError, KeyError, TypeError):
        if status == 503:
            # A whole-request 503 (database or publisher unavailable).
            return items
        raise PostError("ingest returned 200 without per-record results") from None
    retry = []
    for result in results:
        match result["status"]:
            case "accepted":
                summary.accepted += 1
            case "rejected":
                summary.rejected += 1
                reason = str(result.get("reason"))
                summary.rejected_reasons[reason] = summary.rejected_reasons.get(reason, 0) + 1
            case _:
                retry.append(items[result["index"]])
    return retry
