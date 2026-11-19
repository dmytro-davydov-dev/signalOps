"""Scenario catalog version 2: variants, stage labels, and adversarial twins (P13-T01)."""

import json
from collections import Counter
from functools import cache
from pathlib import Path

import pytest

from signalops.contracts.evaluation import ScenarioFamily, ScenarioSplit
from signalops.contracts.roles import SPECIALIST_ROLES
from signalops.demo.generator import DemoDataset, ScenarioLabels, serialize_records
from signalops.evaluation.adversarial import PLANTS, plant_text
from signalops.evaluation.catalog import (
    CATALOG_V2,
    CATALOG_VERSION,
    ENTRIES_V2,
    CatalogEntry,
    definition_of,
    entries,
    fixture_documents,
    split_hash,
)
from tests.unit.test_demo_generator import LABEL_WORDS

ROOT = Path(__file__).resolve().parents[2]
V2_FIXTURES = ROOT / "fixtures" / "scenarios" / "v2"
V1_FIXTURES = ROOT / "fixtures" / "scenarios" / "v1"
INJECTIONS = ROOT / "fixtures" / "adversarial" / "v1" / "injections.json"
# The held-out split hash of catalog v1, as retained in run 7ac381af.
V1_HELD_OUT_HASH = "17ce501dbeefef8814a5fa91e506f8e9048f0204ed966a0114c058a1a337cae6"
DIAGNOSABLE_FAMILIES = {
    ScenarioFamily.DOWNSTREAM_TIMEOUT,
    ScenarioFamily.CONNECTION_POOL_EXHAUSTION,
    ScenarioFamily.DEPLOYMENT_REGRESSION,
    ScenarioFamily.MEMORY_PRESSURE,
    ScenarioFamily.MIXED,
}
ADVERSARIAL = [entry for entry in ENTRIES_V2 if entry.plants]


@cache
def dataset_of(entry: CatalogEntry) -> DemoDataset:
    return entry.dataset()


def test_v2_has_five_held_out_variants_per_diagnosable_family_and_35_in_all() -> None:
    held_out = entries(ScenarioSplit.HELD_OUT, CATALOG_V2)
    families = Counter(entry.family for entry in held_out)
    assert all(families[family] >= 5 for family in DIAGNOSABLE_FAMILIES), families
    assert len(held_out) >= 35
    development = entries(ScenarioSplit.DEVELOPMENT, CATALOG_V2)
    assert len(development) == 2 * len(entries(ScenarioSplit.DEVELOPMENT, CATALOG_VERSION))
    names = [(entry.name, entry.version) for entry in ENTRIES_V2]
    assert len(names) == len(set(names))


def test_v1_stays_registered_with_unchanged_hashes_and_no_stage_labels() -> None:
    held_out = [definition_of(entry) for entry in entries(ScenarioSplit.HELD_OUT, "1")]
    assert split_hash(held_out) == V1_HELD_OUT_HASH
    for entry in entries(ScenarioSplit.DEVELOPMENT, "1"):
        committed = ScenarioLabels.model_validate_json(
            (V1_FIXTURES / f"{entry.scenario}.labels.json").read_text()
        )
        definition = definition_of(entry)
        assert definition.dataset_hash == committed.dataset_hash, entry.name
        stored = definition.model_dump(mode="json")["expected_outcome"]
        assert not {"lead_roles", "expected_contradiction", "clean_twin"} & set(stored)


def test_v2_development_variants_repeat_v1_datasets() -> None:
    v1 = {entry.name: definition_of(entry).dataset_hash for entry in entries(version="1")}
    for entry in entries(ScenarioSplit.DEVELOPMENT, CATALOG_V2):
        if entry.name in v1:
            assert definition_of(entry).dataset_hash == v1[entry.name]


def test_committed_v2_fixtures_match_regeneration() -> None:
    # Regenerate with `signalops evaluation-catalog --out fixtures/scenarios/v2`.
    documents = fixture_documents(CATALOG_V2)
    assert sorted(path.name for path in V2_FIXTURES.glob("*.json")) == sorted(documents)
    for name, text in documents.items():
        assert (V2_FIXTURES / name).read_text() == text, name


@pytest.mark.parametrize("entry", ENTRIES_V2, ids=lambda entry: entry.name)
def test_stage_labels_follow_the_scenario(entry: CatalogEntry) -> None:
    expected = definition_of(entry, dataset_of(entry)).expected_outcome
    if expected.diagnosable:
        assert expected.lead_roles
        assert set(expected.lead_roles) <= set(SPECIALIST_ROLES)
    else:
        assert expected.lead_roles == ()
    if entry.scenario == "mixed-timeout-deploy":
        assert expected.expected_contradiction == "checkout"
    else:
        assert expected.expected_contradiction is None
    assert (expected.clean_twin is not None) == bool(entry.plants)


@pytest.mark.parametrize("entry", ENTRIES_V2, ids=lambda entry: entry.name)
def test_records_carry_no_label_words_except_planted_text(entry: CatalogEntry) -> None:
    planted = {plant_text(key) for key in entry.plants}
    for line in serialize_records(dataset_of(entry).records).decode().splitlines():
        record = json.loads(line)
        message = record["payload"].get("message")
        if message in planted:
            record["payload"]["message"] = ""
        text = json.dumps(record).lower()
        for word in LABEL_WORDS:
            assert word not in text, (entry.name, word)


@pytest.mark.parametrize("entry", ADVERSARIAL, ids=lambda entry: entry.name)
def test_adversarial_scenario_differs_from_its_twin_only_by_planted_text(
    entry: CatalogEntry,
) -> None:
    assert entry.twin is not None
    twin = entry.twin
    assert twin in ENTRIES_V2
    assert twin.split is entry.split
    assert not twin.plants
    attacked, clean = dataset_of(entry), dataset_of(twin)
    clean_ids = {record.source_event_id for record in clean.records}
    planted = [record for record in attacked.records if record.source_event_id not in clean_ids]
    # Every clean record is present, unchanged and in order.
    assert [r for r in attacked.records if r.source_event_id in clean_ids] == list(clean.records)
    assert len(planted) == len(entry.plants)
    assert sorted(str(r.payload["message"]) for r in planted) == sorted(
        plant_text(key) for key in entry.plants
    )
    expected = definition_of(entry, attacked).expected_outcome
    twin_expected = definition_of(twin, clean).expected_outcome
    assert expected.clean_twin == twin.name
    assert expected.model_copy(update={"clean_twin": None}) == twin_expected
    assert entry.family is ScenarioFamily.ADVERSARIAL


def test_adversarial_variants_span_diagnosable_and_insufficient_evidence_twins() -> None:
    twins = {entry.twin.family for entry in ADVERSARIAL if entry.twin}
    assert ScenarioFamily.INSUFFICIENT_EVIDENCE in twins
    assert twins & DIAGNOSABLE_FAMILIES
    for split in ScenarioSplit:
        assert any(entry.split is split for entry in ADVERSARIAL)


def test_embedded_plants_match_the_adversarial_fixture() -> None:
    cases = json.loads(INJECTIONS.read_text())["cases"]
    logs = {case["key"]: case["text"] for case in cases if case["target"] == "log"}
    assert {key: plant.text for key, plant in PLANTS.items()} == logs
