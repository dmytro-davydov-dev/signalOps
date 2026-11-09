"""Pure parts of the benchmark harness (P11-T04): record streams and summaries."""

from datetime import UTC, datetime, timedelta

from signalops.evaluation.benchmark import _bytes, live_records, percentiles, replay_records

START = datetime(2026, 11, 1, tzinfo=UTC)


def test_percentiles_of_nothing_and_of_a_sample() -> None:
    assert percentiles([]) == {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    summary = percentiles([float(n) for n in range(1, 101)])
    assert summary == {"count": 100, "p50": 51.0, "p95": 96.0, "p99": 100.0, "max": 100.0}


def test_replay_records_stop_at_the_count_with_distinct_ids_in_event_time_order() -> None:
    windows = list(replay_records(40_000, density=1, start=START, seed=5))
    records = [record for window in windows for record in window]
    assert len(records) == 40_000
    assert len(windows) == 2
    assert len({(r.source_id, r.source_event_id) for r in records}) == len(records)
    for window in windows:
        times = [r.occurred_at for r in window]
        assert times == sorted(times)
    # Windows follow each other in event time.
    assert windows[1][0].occurred_at >= START + timedelta(hours=6)


def test_live_records_approach_the_rate_from_the_start() -> None:
    records = live_records(50.0, timedelta(minutes=2), start=START, seed=7)
    assert len(records) == 6_000
    assert records[0].occurred_at >= START
    assert records[-1].occurred_at < START + timedelta(minutes=2)
    # Spread over the whole duration, not front-loaded.
    assert records[-1].occurred_at > START + timedelta(minutes=1, seconds=55)
    first_minute = sum(1 for r in records if r.occurred_at < START + timedelta(minutes=1))
    assert 2_700 <= first_minute <= 3_300
    assert len({(r.source_id, r.source_event_id) for r in records}) == len(records)


def test_docker_sizes_parse_in_both_unit_systems() -> None:
    assert _bytes("1.5GiB") == 1.5 * 1024**3
    assert _bytes("200MiB") == 200 * 1024**2
    assert _bytes("3kB") == 3000
    assert _bytes("12B") == 12
    assert _bytes("n/a") == 0.0
