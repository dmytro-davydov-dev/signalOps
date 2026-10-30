import json
from pathlib import Path
from typing import Any

from signalops.contracts.evidence import SourceKind
from signalops.persistence.seed import DEMO_SERVICES, DEMO_SOURCES, DEMO_WORKSPACE_ID

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contracts" / "v1"


def load(relative: str) -> Any:
    return json.loads((FIXTURES / relative).read_text())


def test_fixture_workspace_is_the_seeded_workspace() -> None:
    workspace_ids = {
        str(value)
        for path in FIXTURES.rglob("*.json")
        for key, value in _walk(json.loads(path.read_text()))
        if key == "workspace_id"
    }
    assert workspace_ids == {str(DEMO_WORKSPACE_ID)}


def test_fixture_services_and_source_are_seeded() -> None:
    seeded = {(service.name, service.environment): str(service.id) for service in DEMO_SERVICES}
    for service in load("incidents/incident_detail.json")["services"]:
        assert seeded[(service["name"], service["environment"])] == service["service_id"]
    raw = load("events/signal_raw.json")
    source = next(s for s in DEMO_SOURCES if s.kind == SourceKind.STRUCTURED_LOG)
    assert raw["source_id"] == str(source.id)
    assert raw["payload"]["source_kind"] == source.kind


def test_seed_identities_are_unique() -> None:
    assert len({s.id for s in DEMO_SOURCES} | {s.id for s in DEMO_SERVICES}) == len(
        DEMO_SOURCES
    ) + len(DEMO_SERVICES)
    assert {s.kind for s in DEMO_SOURCES} == set(SourceKind)


def _walk(node: Any) -> list[tuple[str, Any]]:
    if isinstance(node, dict):
        return [pair for key, value in node.items() for pair in [(key, value), *_walk(value)]]
    if isinstance(node, list):
        return [pair for item in node for pair in _walk(item)]
    return []
