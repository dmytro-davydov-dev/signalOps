import uuid
from datetime import UTC, datetime, timedelta
from itertools import permutations

import pytest

from signalops.contracts.common import ContractError
from signalops.detection.graph import GraphPolicy, downstream_names
from signalops.persistence.models import Signal
from signalops.persistence.repositories.graph import Observed

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _observed(offset: int) -> Observed:
    at = T0 + timedelta(minutes=offset)
    signal = uuid.UUID(int=offset + 1)
    return Observed(at, at + HOUR, signal, 1, at, signal)


def test_merge_gives_the_union_with_outer_signals_and_total_count() -> None:
    merged = _observed(10).merge(_observed(0)).merge(_observed(5))
    assert merged == Observed(
        T0,
        T0 + timedelta(minutes=10) + HOUR,
        uuid.UUID(int=1),
        3,
        T0 + timedelta(minutes=10),
        uuid.UUID(int=11),
    )


def test_merge_does_not_depend_on_order() -> None:
    results = set()
    for first, second, third in permutations(_observed(offset) for offset in (0, 5, 10)):
        results.add(first.merge(second).merge(third))
        results.add(first.merge(second.merge(third)))
    assert len(results) == 1


def test_merge_breaks_time_ties_by_signal() -> None:
    low, high = uuid.UUID(int=1), uuid.UUID(int=2)
    first = Observed(T0, T0 + HOUR, low, 1, T0, low)
    second = Observed(T0, T0 + HOUR, high, 1, T0, high)
    assert first.merge(second).first_signal == low
    assert second.merge(first).first_signal == low
    assert first.merge(second).last_signal == high


def test_downstream_names_are_distinct() -> None:
    signal = Signal(attributes={"record": {"downstream_services": ["a", "b", "a"]}})
    assert downstream_names(signal) == {"a", "b"}


@pytest.mark.parametrize(
    "attributes",
    [
        {},
        {"record": "text"},
        {"record": {}},
        {"record": {"downstream_services": "payment"}},
        {"record": {"downstream_services": [1]}},
    ],
)
def test_malformed_trace_record_is_a_contract_error(attributes: dict[str, object]) -> None:
    with pytest.raises(ContractError):
        downstream_names(Signal(attributes=attributes))


def test_policy_exposes_the_ttl() -> None:
    assert GraphPolicy(60).dependency_ttl == timedelta(minutes=1)
