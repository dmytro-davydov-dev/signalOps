"""Every versioned contract fixture validates and round-trips unchanged."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from signalops.contracts.adapters import ADAPTER_INPUTS
from signalops.contracts.checkpoints import decode_checkpoint
from signalops.contracts.diagnosis import Diagnosis
from signalops.contracts.events import EventType, decode_event
from signalops.contracts.evidence import NormalizedSignal, SourceKind
from signalops.contracts.incidents import IncidentDetail
from signalops.contracts.investigations import InvestigationView
from signalops.contracts.progress import ProgressPage
from signalops.contracts.tools import TOOL_RESULT_ADAPTER
from signalops.contracts.workflow import ToolName

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1"


def _adapter_input(path: Path, document: dict[str, Any]) -> BaseModel:
    return ADAPTER_INPUTS[SourceKind(path.stem)].model_validate(document)


DECODERS: dict[str, Callable[[Path, dict[str, Any]], BaseModel]] = {
    "events": lambda _, document: decode_event(json.dumps(document).encode()),
    "adapter_inputs": _adapter_input,
    "evidence": lambda _, document: NormalizedSignal.model_validate(document),
    "incidents": lambda _, document: IncidentDetail.model_validate(document),
    "investigations": lambda _, document: InvestigationView.model_validate(document),
    "progress": lambda _, document: ProgressPage.model_validate(document),
    "checkpoints": lambda _, document: decode_checkpoint(document),
    "tool_results": lambda _, document: TOOL_RESULT_ADAPTER.validate_python(document),
    "diagnoses": lambda _, document: Diagnosis.model_validate(document),
}

FIXTURE_PATHS = sorted(FIXTURES.rglob("*.json"))


def test_every_fixture_directory_has_a_decoder() -> None:
    assert {path.parent.name for path in FIXTURE_PATHS} == set(DECODERS)


@pytest.mark.parametrize("path", FIXTURE_PATHS, ids=lambda p: str(p.relative_to(FIXTURES)))
def test_fixture_round_trips(path: Path) -> None:
    decode = DECODERS[path.parent.name]
    document = json.loads(path.read_text())
    model = decode(path, document)

    dumped = model.model_dump(mode="json")
    # Fixtures are stored in canonical form, so dumping reproduces them exactly.
    assert dumped == document
    assert decode(path, dumped) == model
    assert decode(path, json.loads(model.model_dump_json())) == model


def test_event_fixtures_cover_every_event_type() -> None:
    types = {
        json.loads(path.read_text())["event_type"] for path in (FIXTURES / "events").glob("*.json")
    }
    assert types == set(EventType)


def test_event_fixtures_survive_encode_and_decode() -> None:
    for path in (FIXTURES / "events").glob("*.json"):
        envelope = decode_event(path.read_bytes())
        assert decode_event(envelope.encode()) == envelope


def test_tool_result_fixtures_cover_every_tool() -> None:
    assert {path.stem for path in (FIXTURES / "tool_results").glob("*.json")} == set(ToolName)


def test_adapter_fixtures_cover_every_source_kind() -> None:
    assert {path.stem for path in (FIXTURES / "adapter_inputs").glob("*.json")} == set(SourceKind)
