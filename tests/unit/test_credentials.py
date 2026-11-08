"""Credential file, rotation without restart, expiry, and disjoint sets (P11-T02)."""

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from signalops.api.credentials import CredentialFile, CredentialStore, token_digest

WORKSPACE = uuid.UUID("0d9f6a52-5f0e-4e57-9f32-0a4c3b3f2e11")
SOURCE = uuid.UUID("1dee9492-8198-4ab9-af5e-9bcc709660fd")
OTHER_SOURCE = uuid.UUID("2a0c5a6e-8d1b-4b44-9f0e-4a2a1f6f7c20")
NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def _write(
    path: Path, operators: list[dict[str, object]], sources: list[dict[str, object]]
) -> None:
    path.write_text(json.dumps({"operators": operators, "sources": sources}))
    # Distinct mtimes even on coarse file systems.
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))


def _operator(credential_id: str, token: str, **extra: object) -> dict[str, object]:
    return {
        "id": credential_id,
        "workspace_id": str(WORKSPACE),
        "sha256": token_digest(token),
        **extra,
    }


def _source(credential_id: str, token: str, source: uuid.UUID = SOURCE) -> dict[str, object]:
    return {"id": credential_id, "source_id": str(source), "sha256": token_digest(token)}


def _store(
    path: Path | None, clock: Clock, **tokens: dict[uuid.UUID, SecretStr]
) -> CredentialStore:
    return CredentialStore(
        tokens.get("operators", {}),
        tokens.get("sources", {}),
        path,
        reload_seconds=5,
        clock=clock,
        now=lambda: NOW,
    )


def test_file_credentials_name_their_principal(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    _write(path, [_operator("alice-1", "op-secret")], [_source("logs-1", "src-secret")])
    store = _store(path, Clock())
    match = store.operator("op-secret")
    assert match is not None
    assert (match.credential_id, match.principal) == ("alice-1", WORKSPACE)
    source = store.source(SOURCE, "src-secret")
    assert source is not None
    assert source.credential_id == "logs-1"
    assert store.operator("src-secret") is None
    assert store.source(SOURCE, "op-secret") is None
    assert store.source(OTHER_SOURCE, "src-secret") is None


def test_rotation_takes_effect_without_restart(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    clock = Clock()
    _write(path, [_operator("alice-1", "old")], [])
    store = _store(path, clock)

    _write(path, [_operator("alice-1", "old"), _operator("alice-2", "new")], [])
    # Not re-read before the reload interval.
    assert store.operator("new") is None
    clock.value = 5
    assert store.operator("new") is not None
    assert store.operator("old") is not None

    _write(path, [_operator("alice-2", "new")], [])
    clock.value = 10
    assert store.operator("old") is None
    assert store.operator("new") is not None


def test_a_broken_edit_keeps_the_last_valid_set(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    clock = Clock()
    _write(path, [_operator("alice-1", "old")], [])
    store = _store(path, clock)
    path.write_text("{not json")
    clock.value = 5
    assert store.operator("old") is not None
    path.unlink()
    clock.value = 10
    assert store.operator("old") is not None


def test_expired_credentials_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    expired = (NOW - timedelta(seconds=1)).isoformat()
    later = (NOW + timedelta(days=1)).isoformat()
    _write(
        path,
        [_operator("a", "gone", not_after=expired), _operator("b", "fine", not_after=later)],
        [],
    )
    store = _store(path, Clock())
    assert store.operator("gone") is None
    assert store.operator("fine") is not None


def test_environment_tokens_still_work_alongside_the_file(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    _write(path, [_operator("alice-1", "file-token")], [])
    store = _store(
        path,
        Clock(),
        operators={WORKSPACE: SecretStr("env-token")},
        sources={SOURCE: SecretStr("env-source")},
    )
    env = store.operator("env-token")
    assert env is not None
    assert env.credential_id == f"env:operator:{WORKSPACE}"
    assert store.operator("file-token") is not None
    assert store.source(SOURCE, "env-source") is not None


def test_a_token_in_both_sets_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    with pytest.raises(ValidationError, match="must not equal"):
        CredentialFile.model_validate(
            {"operators": [_operator("a", "same")], "sources": [_source("b", "same")]}
        )
    _write(path, [_operator("a", "same")], [])
    with pytest.raises(ValueError, match="must not equal"):
        _store(path, Clock(), sources={SOURCE: SecretStr("same")})


def test_invalid_files_fail_at_startup(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    with pytest.raises(FileNotFoundError):
        _store(path, Clock())
    path.write_text(
        json.dumps(
            {"operators": [{"id": "a", "workspace_id": str(WORKSPACE), "sha256": "plaintext"}]}
        )
    )
    with pytest.raises(ValidationError):
        _store(path, Clock())
    path.write_text(
        json.dumps({"operators": [_operator("dup", "x"), _operator("dup", "y")], "sources": []})
    )
    with pytest.raises(ValidationError, match="unique"):
        _store(path, Clock())
