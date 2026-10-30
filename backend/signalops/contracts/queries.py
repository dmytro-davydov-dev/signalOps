"""Evidence query responses: signal pages, trace lookups, and source quality counts.

Every response names the replay scope it was read from. The workspace comes
from the caller's operator credential and is never a request parameter.
"""

import uuid
from typing import Self

from pydantic import Field, NonNegativeInt, model_validator

from signalops.contracts.common import ContractModel, Key, ReplayScope, TraceId, UtcDatetime
from signalops.contracts.evidence import NormalizedSignal, QualityFlag, QualityStatus, SourceKind

# Hard ceiling on items in one response; the configured limit is at most this.
MAX_QUERY_ITEMS = 1_000


class TimeWindow(ContractModel):
    """Half-open interval: `since` inclusive, `until` exclusive."""

    since: UtcDatetime
    until: UtcDatetime

    @model_validator(mode="after")
    def _check_order(self) -> Self:
        if self.since >= self.until:
            raise ValueError("since must be earlier than until")
        return self


class SignalPage(ContractModel):
    """Signals by `occurred_at` descending, then ID descending."""

    replay_scope: ReplayScope
    # The window every page of this query reads; later pages reuse it from the cursor.
    window: TimeWindow
    items: tuple[NormalizedSignal, ...] = Field(max_length=MAX_QUERY_ITEMS)
    # Opaque; None on the last page.
    next_cursor: Key | None = None


class TraceEvidence(ContractModel):
    """Signals sharing a trace ID, by `occurred_at` ascending, then ID ascending."""

    trace_id: TraceId
    replay_scope: ReplayScope
    signals: tuple[NormalizedSignal, ...] = Field(min_length=1, max_length=MAX_QUERY_ITEMS)
    # True when the trace has more signals than the configured limit returned.
    truncated: bool


class SourceQuality(ContractModel):
    source_id: uuid.UUID
    # Catalog names are free text (sources.name is only non-empty).
    name: str = Field(min_length=1)
    kind: SourceKind
    enabled: bool
    total: NonNegativeInt
    # Every status, zero when absent.
    by_status: dict[QualityStatus, NonNegativeInt]
    # Only flags that occurred. A signal can carry several flags.
    by_flag: dict[QualityFlag, NonNegativeInt]

    @model_validator(mode="after")
    def _check_counts(self) -> Self:
        if set(self.by_status) != set(QualityStatus):
            raise ValueError("by_status must list every quality status")
        if sum(self.by_status.values()) != self.total:
            raise ValueError("by_status counts must sum to total")
        return self


class QualitySummary(ContractModel):
    """Per-source counts of signals ingested in the window (by `ingested_at`)."""

    replay_scope: ReplayScope
    window: TimeWindow
    # Every source in the workspace, including those with no signals.
    sources: tuple[SourceQuality, ...] = Field(max_length=MAX_QUERY_ITEMS)
