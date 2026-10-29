from signalops.streaming.topics import DAY_MS, TOPICS


def test_topic_catalogue_matches_plan() -> None:
    by_name = {spec.name: spec for spec in TOPICS}
    assert len(by_name) == len(TOPICS), "duplicate topic names"
    assert {name: spec.partitions for name, spec in by_name.items()} == {
        "signals.raw.v1": 3,
        "signals.normalized.v1": 3,
        "incidents.changed.v1": 1,
        "investigations.requested.v1": 1,
        "investigations.completed.v1": 1,
        "processing.failed.v1": 1,
    }
    assert by_name["signals.raw.v1"].retention_ms == 3 * DAY_MS
    assert by_name["signals.normalized.v1"].retention_ms == 7 * DAY_MS


def test_topic_config_is_delete_policy_with_retention() -> None:
    config = TOPICS[0].config()
    assert config == {"retention.ms": str(TOPICS[0].retention_ms), "cleanup.policy": "delete"}
