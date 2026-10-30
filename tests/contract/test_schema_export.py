"""Exported schemas stay current, and incompatible rewrites are refused."""

import json
from pathlib import Path
from typing import Any

import pytest

from signalops.cli import main
from signalops.contracts.compatibility import breaking_changes
from signalops.contracts.export import (
    SCHEMA_DIR,
    WEB_STATUS_MODULE,
    BreakingChangeError,
    stale_paths,
    write_all,
)

DIAGNOSIS = SCHEMA_DIR / "diagnosis" / "diagnosis.v1.json"


def test_committed_artifacts_match_the_contracts() -> None:
    # Fix with: uv run signalops export-schemas
    assert stale_paths() == []


def test_check_command_reports_up_to_date() -> None:
    assert main(["export-schemas", "--check"]) == 0


def test_status_mapping_artifacts_are_generated(tmp_path: Path) -> None:
    write_all(tmp_path)
    module = (tmp_path / WEB_STATUS_MODULE).read_text()
    assert 'completed: "diagnosed",' in module
    assert 'insufficient_evidence: "insufficient_evidence",' in module
    mapping = json.loads(
        (tmp_path / SCHEMA_DIR / "investigations" / "status-mapping.v1.json").read_text()
    )
    assert mapping["diagnosis_status_by_investigation_status"]["failed"] is None


def edit_json(path: Path, change: Any) -> None:
    document = json.loads(path.read_text())
    change(document)
    path.write_text(json.dumps(document))


def test_write_refuses_breaking_changes_and_writes_nothing(tmp_path: Path) -> None:
    write_all(tmp_path)
    target = tmp_path / DIAGNOSIS
    # Pretend the committed v1 had a property the current model lacks.
    edit_json(target, lambda d: d["properties"].update(reasoning={"type": "string"}))
    edited = target.read_text()
    (tmp_path / WEB_STATUS_MODULE).write_text("stale")

    with pytest.raises(BreakingChangeError, match="reasoning: property removed"):
        write_all(tmp_path)
    assert target.read_text() == edited
    assert (tmp_path / WEB_STATUS_MODULE).read_text() == "stale"

    written = write_all(tmp_path, allow_breaking=True)
    assert DIAGNOSIS in written
    assert stale_paths(tmp_path) == []


def test_write_accepts_additive_optional_fields(tmp_path: Path) -> None:
    write_all(tmp_path)
    # Pretend the committed v1 predates the optional missing_evidence field.
    edit_json(tmp_path / DIAGNOSIS, lambda d: d["properties"].pop("missing_evidence"))
    assert write_all(tmp_path) == [DIAGNOSIS]


def test_stale_paths_include_orphaned_schema_files(tmp_path: Path) -> None:
    write_all(tmp_path)
    orphan = tmp_path / SCHEMA_DIR / "events" / "signal.raw.v0.json"
    orphan.write_text("{}")
    assert stale_paths(tmp_path) == [orphan.relative_to(tmp_path)]


def obj(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required or []}


STRING = {"type": "string"}
LEVEL = {"$defs": {"Level": {"title": "Level", "enum": ["info", "error"]}}}


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (obj({"a": STRING}), obj({"a": STRING, "b": STRING}), []),
        (obj({"a": STRING, "b": STRING}), obj({"a": STRING}), ["$.b: property removed"]),
        (
            obj({"a": STRING}),
            obj({"a": STRING, "b": STRING}, ["b"]),
            ["$.b: property became required"],
        ),
        (
            obj({"a": STRING}),
            obj({"a": {"type": "integer"}}),
            ["$.a: type changed from string to integer"],
        ),
        (
            obj({"a": {"enum": ["x", "y"]}}),
            obj({"a": {"enum": ["x"]}}),
            ["$.a: values removed: 'y'"],
        ),
        (obj({"a": {"enum": ["x"]}}), obj({"a": {"enum": ["x", "y"]}}), []),
        (
            obj({"a": {"anyOf": [STRING, {"type": "null"}]}}),
            obj({"a": STRING}),
            ["$.a: alternative null removed"],
        ),
        (obj({"a": STRING}), obj({"a": {"anyOf": [STRING, {"type": "null"}]}}), []),
        (
            obj({"items": {"type": "array", "items": obj({"x": STRING})}}),
            obj({"items": {"type": "array", "items": obj({})}}),
            ["$.items[].x: property removed"],
        ),
        (
            LEVEL | obj({"level": {"$ref": "#/$defs/Level"}}),
            {"$defs": {"Level": {"title": "Level", "enum": ["error"]}}}
            | obj({"level": {"$ref": "#/$defs/Level"}}),
            ["$.level: values removed: 'info'"],
        ),
    ],
    ids=[
        "add-optional",
        "remove",
        "newly-required",
        "type",
        "enum-narrowed",
        "enum-widened",
        "nullable-removed",
        "nullable-added",
        "nested-array",
        "ref-enum",
    ],
)
def test_breaking_changes(old: dict[str, Any], new: dict[str, Any], expected: list[str]) -> None:
    assert breaking_changes(old, new) == expected


@pytest.mark.parametrize(
    "path",
    sorted(
        p
        for p in (Path(__file__).resolve().parents[2] / SCHEMA_DIR).rglob("*.json")
        if p.name not in {"openapi.v1.json", "status-mapping.v1.json"}
    ),
    ids=lambda p: p.name,
)
def test_committed_schemas_are_compatible_with_themselves(path: Path) -> None:
    schema = json.loads(path.read_text())
    assert breaking_changes(schema, schema) == []
