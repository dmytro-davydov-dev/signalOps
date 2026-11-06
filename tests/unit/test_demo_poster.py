import json
import uuid
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from signalops.demo.generator import Scenario, generate
from signalops.demo.poster import PostError, post_records, post_records_realtime
from signalops.persistence.seed import DEMO_SOURCES

TOKENS = {source.id: f"token-{source.name}" for source in DEMO_SOURCES}
START = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)


class FakeApi:
    """Accepts records; fails or rejects source event IDs on request."""

    def __init__(
        self,
        *,
        fail_once: set[str] | None = None,
        reject: set[str] | None = None,
        status: int | None = None,
    ) -> None:
        self.fail_once = set(fail_once or ())
        self.reject = reject or set()
        self.status = status
        self.requests: list[tuple[str, dict[str, Any], Mapping[str, str]]] = []

    def __call__(self, url: str, body: bytes, headers: Mapping[str, str]) -> tuple[int, bytes]:
        document = json.loads(body)
        self.requests.append((url, document, headers))
        if self.status is not None:
            return self.status, b'{"detail": "nope"}'
        results = []
        for index, item in enumerate(document["records"]):
            sid = item["source_event_id"]
            if sid in self.fail_once:
                self.fail_once.discard(sid)
                results.append({"index": index, "status": "failed", "reason": "ambiguous_timeout"})
            elif sid in self.reject:
                results.append({"index": index, "status": "rejected", "reason": "invalid_record"})
            else:
                results.append({"index": index, "status": "accepted"})
        failed = any(r["status"] == "failed" for r in results)
        return (503 if failed else 200), json.dumps({"results": results}).encode()


def dataset() -> Any:
    return generate(Scenario.HEALTHY, seed=7, start=START, duration=timedelta(seconds=120))


def test_posts_every_record_per_source_with_its_token() -> None:
    records = dataset().records
    api = FakeApi()
    summary = post_records(
        records, api_url="http://api:8000/", tokens=TOKENS, transport=api, batch_size=25
    )

    assert summary.accepted == len(records)
    assert summary.rejected == 0
    sent: list[str] = []
    for url, document, headers in api.requests:
        source_id = uuid.UUID(url.removeprefix("http://api:8000/v1/ingest/"))
        assert headers["Authorization"] == f"Bearer {TOKENS[source_id]}"
        assert 1 <= len(document["records"]) <= 25
        for item in document["records"]:
            assert set(item) == {"source_event_id", "occurred_at", "payload"}
            sent.append(item["source_event_id"])
    assert sorted(sent) == sorted(r.source_event_id for r in records)


def test_failed_records_are_resent_with_the_same_ids() -> None:
    records = dataset().records
    flaky = {records[0].source_event_id, records[3].source_event_id}
    api = FakeApi(fail_once=flaky)
    sleeps: list[float] = []
    summary = post_records(
        records,
        api_url="http://api",
        tokens=TOKENS,
        transport=api,
        batch_size=1000,
        sleep=sleeps.append,
    )

    assert summary.accepted == len(records)
    assert summary.retries >= 1
    assert sleeps
    sent = Counter(item["source_event_id"] for _, doc, _ in api.requests for item in doc["records"])
    assert {sid for sid, count in sent.items() if count == 2} == flaky
    assert set(sent.values()) == {1, 2}


def test_no_response_resends_the_whole_batch() -> None:
    records = dataset().records[:3]
    calls = 0

    def drop_first(url: str, body: bytes, headers: Mapping[str, str]) -> tuple[int, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("read timed out")
        return FakeApi()(url, body, headers)

    summary = post_records(
        records, api_url="http://api", tokens=TOKENS, transport=drop_first, sleep=lambda _: None
    )
    assert summary.accepted == 3


def test_persistent_failures_stop_after_max_attempts() -> None:
    records = dataset().records[:2]

    def always_failing(url: str, body: bytes, headers: Mapping[str, str]) -> tuple[int, bytes]:
        count = len(json.loads(body)["records"])
        results = [{"index": i, "status": "failed"} for i in range(count)]
        return 503, json.dumps({"results": results}).encode()

    with pytest.raises(PostError, match="after 3 attempts"):
        post_records(
            records,
            api_url="http://api",
            tokens=TOKENS,
            transport=always_failing,
            max_attempts=3,
            sleep=lambda _: None,
        )


def test_rejections_are_counted_not_resent() -> None:
    records = dataset().records
    bad = records[1].source_event_id
    api = FakeApi(reject={bad})
    summary = post_records(records, api_url="http://api", tokens=TOKENS, transport=api)
    assert summary.rejected == 1
    assert summary.rejected_reasons == {"invalid_record": 1}
    assert summary.retries == 0


@pytest.mark.parametrize("status", [401, 403, 413, 422])
def test_whole_request_errors_stop_posting(status: int) -> None:
    with pytest.raises(PostError, match=str(status)):
        post_records(
            dataset().records,
            api_url="http://api",
            tokens=TOKENS,
            transport=FakeApi(status=status),
        )


def test_missing_token_is_reported_before_sending() -> None:
    api = FakeApi()
    tokens = dict(list(TOKENS.items())[:1])
    with pytest.raises(PostError, match="no ingestion token"):
        post_records(dataset().records, api_url="http://api", tokens=tokens, transport=api)
    assert api.requests == []


class FakeTime:
    def __init__(self, start: datetime) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


def test_realtime_posts_no_record_before_its_timestamp() -> None:
    records = generate(Scenario.HEALTHY, seed=7, start=START, duration=timedelta(minutes=5)).records
    api = FakeApi()
    clock = FakeTime(START)
    sent_at: list[tuple[datetime, str]] = []

    def transport(url: str, body: bytes, headers: Mapping[str, str]) -> tuple[int, bytes]:
        sent_at.extend(
            (clock.now(), item["source_event_id"]) for item in json.loads(body)["records"]
        )
        return api(url, body, headers)

    summary = post_records_realtime(
        records,
        api_url="http://api",
        tokens=TOKENS,
        transport=transport,
        sleep=clock.sleep,
        now=clock.now,
    )

    assert summary.accepted == len(records)
    assert summary.batches > 10
    occurred = {record.source_event_id: record.occurred_at for record in records}
    assert len(sent_at) == len(records)
    for at, source_event_id in sent_at:
        assert occurred[source_event_id] <= at
        assert at - occurred[source_event_id] <= timedelta(seconds=10)
