"""The versioned scenario catalog: development and held-out variants (P10-T03).

Every entry is synthetic: generated from a seed by `demo/generator.py`, and
labelled synthetic wherever it is stored or shown. Development variants use the
default fault parameters and seed 42, the same datasets the fixtures and tests
use while building rules and prompts. Held-out variants change the seed, the fault
window, its intensity, the run length, and for a deployment regression the
service. They are for reporting only: nothing is tuned on them.

The records are regenerated from each entry rather than stored. `dataset_hash`
pins them: registering an entry whose regenerated hash differs from the stored
version is refused, so a generator change cannot silently alter a scenario.
"""

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from signalops.contracts.evaluation import (
    ExpectedOutcome,
    FaultWindow,
    ScenarioDefinition,
    ScenarioFamily,
    ScenarioSplit,
)
from signalops.demo.generator import (
    DEFAULT_DURATION,
    DEFAULT_START,
    DEFAULT_VARIANT,
    GENERATOR_VERSION,
    DemoDataset,
    FaultVariant,
    Scenario,
    format_timestamp,
    generate,
)

CATALOG_VERSION: Final = "1"

FAMILY_BY_SCENARIO: Final = {
    Scenario.HEALTHY: ScenarioFamily.HEALTHY_NOISE,
    Scenario.DOWNSTREAM_TIMEOUT: ScenarioFamily.DOWNSTREAM_TIMEOUT,
    Scenario.POOL_EXHAUSTION: ScenarioFamily.CONNECTION_POOL_EXHAUSTION,
    Scenario.DEPLOYMENT_REGRESSION: ScenarioFamily.DEPLOYMENT_REGRESSION,
    Scenario.MEMORY_PRESSURE: ScenarioFamily.MEMORY_PRESSURE,
    Scenario.MIXED_TIMEOUT_DEPLOY: ScenarioFamily.MIXED,
    Scenario.UNEXPLAINED_ERRORS: ScenarioFamily.INSUFFICIENT_EVIDENCE,
}


@dataclass(frozen=True)
class CatalogEntry:
    name: str
    split: ScenarioSplit
    scenario: Scenario
    seed: int
    duration: timedelta = DEFAULT_DURATION
    variant: FaultVariant = DEFAULT_VARIANT
    start: datetime = DEFAULT_START
    version: str = CATALOG_VERSION

    @property
    def family(self) -> ScenarioFamily:
        return FAMILY_BY_SCENARIO[self.scenario]

    def dataset(self) -> DemoDataset:
        return generate(
            self.scenario,
            seed=self.seed,
            start=self.start,
            duration=self.duration,
            variant=self.variant,
        )


def _held_out(
    scenario: Scenario, seed: int, minutes: int, variant: FaultVariant = DEFAULT_VARIANT
) -> CatalogEntry:
    return CatalogEntry(
        f"{scenario}/held-out",
        ScenarioSplit.HELD_OUT,
        scenario,
        seed,
        duration=timedelta(minutes=minutes),
        variant=variant,
    )


ENTRIES: Final[tuple[CatalogEntry, ...]] = (
    *(
        CatalogEntry(f"{scenario}/dev", ScenarioSplit.DEVELOPMENT, scenario, 42)
        for scenario in Scenario
    ),
    _held_out(Scenario.HEALTHY, 1105, 12),
    _held_out(Scenario.DOWNSTREAM_TIMEOUT, 1101, 12, FaultVariant(0.35, 0.7, 0.85)),
    _held_out(Scenario.POOL_EXHAUSTION, 1102, 10, FaultVariant(0.45, 0.85, 0.6)),
    _held_out(
        Scenario.DEPLOYMENT_REGRESSION, 1103, 10, FaultVariant(0.3, 0.75, 0.7, target="payment")
    ),
    _held_out(Scenario.MEMORY_PRESSURE, 1104, 15, FaultVariant(0.4, 0.9, 0.8)),
    _held_out(Scenario.MIXED_TIMEOUT_DEPLOY, 1106, 10, FaultVariant(0.35, 0.75, 0.8)),
    _held_out(Scenario.UNEXPLAINED_ERRORS, 1107, 10, FaultVariant(0.4, 0.8, 0.6)),
)


def entries(split: ScenarioSplit | None = None) -> list[CatalogEntry]:
    return [entry for entry in ENTRIES if split is None or entry.split is split]


def definition_of(entry: CatalogEntry, dataset: DemoDataset | None = None) -> ScenarioDefinition:
    """The scenario row for an entry: its dataset hash, injection spec, and ground truth."""
    labels = (dataset or entry.dataset()).labels
    window = None
    if labels.fault_start is not None and labels.fault_end is not None:
        window = FaultWindow(start=labels.fault_start, end=labels.fault_end)
    variant = entry.variant
    return ScenarioDefinition(
        name=entry.name,
        version=entry.version,
        family=entry.family,
        split=entry.split,
        synthetic=labels.synthetic,
        dataset_ref=f"generator:v{GENERATOR_VERSION}",
        dataset_hash=labels.dataset_hash,
        record_count=labels.record_count,
        injection_spec={
            "generator_scenario": entry.scenario.value,
            "seed": entry.seed,
            "start": format_timestamp(entry.start),
            "duration_seconds": int(entry.duration.total_seconds()),
            "fault_start_fraction": variant.start_fraction,
            "fault_end_fraction": variant.end_fraction,
            "intensity": variant.intensity,
            "target": variant.target,
        },
        expected_outcome=ExpectedOutcome(
            incident_expected=labels.expected_incident,
            diagnosable=labels.diagnosable,
            root_cause_service=labels.root_cause_service,
            failure_type=labels.failure_type,
            affected_services=labels.affected_services,
            fault_window=window,
            supporting_event_ids=labels.supporting_event_ids,
        ),
    )


def split_hash(definitions: Iterable[ScenarioDefinition]) -> str:
    """SHA-256 over a split's scenario names, versions, and dataset hashes."""
    lines = sorted(f"{d.name}@{d.version}:{d.dataset_hash}" for d in definitions)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()
