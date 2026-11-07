import pytest
from pydantic import SecretStr, ValidationError

from signalops.config import Settings


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGNALOPS_DATABASE_URL", raising=False)
    with pytest.raises(ValidationError, match="database_url"):
        Settings()  # type: ignore[call-arg]


def test_reads_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIGNALOPS_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    monkeypatch.setenv("SIGNALOPS_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    settings = Settings()  # type: ignore[call-arg]
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.model_provider == "fake"
    assert "p@db" not in repr(settings)


def test_an_unknown_model_provider_is_rejected() -> None:
    with pytest.raises(ValidationError, match="model_provider"):
        Settings(database_url=SecretStr("postgresql+psycopg://u@db/x"), model_provider="paid")  # type: ignore[arg-type]


def test_non_postgres_database_url_is_rejected() -> None:
    settings = Settings(database_url=SecretStr("mysql://u@db/x"))
    with pytest.raises(ValidationError):
        settings.validated_database_url()
