"""Every catalog scenario, replayed in process, detects what its labels expect (P10-T03).

Fault scenarios open exactly one incident, on a service the fault affects, inside
the fault window; healthy noise opens none. Both catalog versions are replayed,
including catalog v2's adversarial variants (P13-T01). Run a few at a time to bound
the time without exhausting connections. `SIGNALOPS_CATALOG_SCENARIOS=name@version,...`
limits the check to some entries while tuning a variant.
"""

import asyncio
import os
import uuid

import pytest
from sqlalchemy import select

from signalops.contracts.evaluation import ReplayStatus
from signalops.evaluation.catalog import ENTRIES, CatalogEntry, definition_of
from signalops.persistence import database as db
from signalops.persistence.models import Incident, IncidentService, Service
from tests.integration.test_replay import _replay

pytestmark = pytest.mark.integration


async def _check(database: db.Database, entry: CatalogEntry) -> None:
    dataset = entry.dataset()
    expected = definition_of(entry, dataset).expected_outcome
    report = await _replay(database, dataset, uuid.uuid4())
    assert report.status is ReplayStatus.COMPLETED, entry.name
    assert report.accepted == dataset.labels.record_count, entry.name
    if not expected.incident_expected:
        assert report.incident_ids == (), entry.name
        return
    assert len(report.incident_ids) == 1, (entry.name, report.incident_ids)
    assert expected.fault_window is not None
    async with database.transaction() as session:
        incident = await session.get_one(Incident, report.incident_ids[0])
        services = set(
            await session.scalars(
                select(Service.name)
                .join(IncidentService, IncidentService.service_id == Service.id)
                .where(IncidentService.incident_id == incident.id)
            )
        )
    assert expected.fault_window.start <= incident.detected_at, entry.name
    assert incident.detected_at <= expected.fault_window.end + (
        expected.fault_window.end - expected.fault_window.start
    ), entry.name
    assert services, entry.name
    assert services <= set(expected.affected_services), (entry.name, services)


CONCURRENCY = 8
ONLY = {name for name in os.environ.get("SIGNALOPS_CATALOG_SCENARIOS", "").split(",") if name}
SELECTED = [entry for entry in ENTRIES if not ONLY or f"{entry.name}@{entry.version}" in ONLY]


async def test_every_catalog_scenario_detects_what_its_labels_expect(
    database: db.Database,
) -> None:
    slots = asyncio.Semaphore(CONCURRENCY)

    async def bounded(entry: CatalogEntry) -> None:
        async with slots:
            await _check(database, entry)

    results = await asyncio.gather(*(bounded(entry) for entry in SELECTED), return_exceptions=True)
    failures = [
        f"{entry.name}@{entry.version}: {result!r}"
        for entry, result in zip(SELECTED, results, strict=True)
        if isinstance(result, BaseException)
    ]
    assert failures == []
