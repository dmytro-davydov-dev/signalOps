import pytest

from signalops import __version__
from signalops.cli import main


def test_version_flag_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_missing_command_returns_usage_error() -> None:
    assert main([]) == 1


def test_init_topics_command_logs_result(monkeypatch: pytest.MonkeyPatch) -> None:
    import signalops.streaming.topics as topics

    monkeypatch.setenv("SIGNALOPS_DATABASE_URL", "postgresql+psycopg://u@db/x")
    monkeypatch.setattr(topics, "ensure_topics", lambda admin: ["signals.raw.v1"])
    from signalops.config import get_settings

    get_settings.cache_clear()
    try:
        assert main(["init-topics"]) == 0
    finally:
        get_settings.cache_clear()


def test_seed_command_runs_the_demo_seed(monkeypatch: pytest.MonkeyPatch) -> None:
    import signalops.persistence.seed as seed
    from signalops.persistence.database import Database

    calls: list[Database] = []

    async def fake_seed(database: Database) -> None:
        calls.append(database)

    monkeypatch.setenv("SIGNALOPS_DATABASE_URL", "postgresql+psycopg://u@db/x")
    monkeypatch.setattr(seed, "seed_demo", fake_seed)
    from signalops.config import get_settings

    get_settings.cache_clear()
    try:
        assert main(["seed"]) == 0
    finally:
        get_settings.cache_clear()
    assert len(calls) == 1
