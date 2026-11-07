"""Output of each workflow node, stored in `agent_steps.output`.

A resumed worker validates stored output against these models before reusing it.
Output that fails is never run again, because a completed step cannot change, so
a change here that old rows would fail needs a new workflow version.
"""

import uuid

from pydantic import Field

from signalops.contracts.common import ContractModel, ShortText, UtcDatetime
from signalops.contracts.diagnosis import MAX_LIST_ITEMS


class IncidentBrief(ContractModel):
    """Only fields fixed at detection: severity may have moved past the evidence cutoff."""

    incident_id: uuid.UUID
    title: ShortText
    detected_at: UtcDatetime
    evidence_cutoff_at: UtcDatetime


class InvestigatorFindings(ContractModel):
    missing_evidence: tuple[ShortText, ...] = Field(min_length=1, max_length=MAX_LIST_ITEMS)
