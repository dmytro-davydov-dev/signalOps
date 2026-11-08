"""Workflow `d1`: the deterministic baseline, with no model (P10-T04).

It is the simple rule an on-call engineer might apply first: blame the service
that alerted. Among the signals the detector cited, it takes the service with
the most errors, then labels the failure from that service's own warnings and
errors by keyword: a deployment in the window, an exhausted pool, heap trouble,
a timeout, or plain elevated errors. With no cited errors it abstains.

It reads the same scope and evidence cutoff the agents' tools enforce, through
the same repositories, and cites only signals it read. It makes no model call,
so it costs nothing and is the bar the agent variants must beat. It does not
follow calls downstream, which is the point of comparing it.
"""

import uuid
from collections import Counter
from datetime import timedelta
from typing import Final

from signalops.agents.briefs import LOAD_INCIDENT
from signalops.agents.nodes import Node, NodeContext, Workflow
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus, RootCause
from signalops.contracts.evidence import Severity
from signalops.contracts.workflow import AgentRole
from signalops.persistence.repositories.catalog import CatalogRepository
from signalops.persistence.repositories.evidence import EvidenceRepository, SignalFilter

WORKFLOW_D1: Final = "d1"
LOOK_BACK: Final = timedelta(minutes=5)
DEPLOY_LOOK_BACK: Final = timedelta(minutes=15)
MAX_LOGS: Final = 200
MAX_CITATIONS: Final = 10
ERRORS: Final = (Severity.ERROR.value, Severity.CRITICAL.value)
CONFIDENCE: Final = 0.5

# First match wins, in this order.
KEYWORDS: Final = (
    ("connection_pool_exhaustion", ("pool exhausted",)),
    ("memory_pressure", ("outofmemory", "gc pause", "heap")),
    ("downstream_timeout", ("timed out", "timeout")),
)


def classify(messages: list[str], deployed: bool) -> str:
    """The failure type the keyword rule assigns."""
    if deployed:
        return "deployment_regression"
    lowered = [message.lower() for message in messages]
    for failure_type, words in KEYWORDS:
        if any(word in message for message in lowered for word in words):
            return failure_type
    return "elevated_errors"


async def conclude_by_rule(context: NodeContext) -> Diagnosis:
    run = context.run
    brief = context.output(LOAD_INCIDENT)
    cutoff = run.evidence_cutoff_at
    since = (brief.window_start or cutoff - LOOK_BACK) - LOOK_BACK
    async with context.database.transaction() as session:
        evidence = EvidenceRepository(session)
        cited = await evidence.known_signals(run.scope, brief.evidence_signal_ids, known_by=cutoff)
        errors = [row for row in cited.values() if row.severity in ERRORS and row.service_id]
        if not errors:
            return _abstain(run.workflow_version, brief.title)
        counts = Counter(row.service_id for row in errors if row.service_id is not None)
        top = max(counts.values())
        candidates = [service for service, count in counts.items() if count == top]
        services = await CatalogRepository(session).get_services(run.scope.workspace_id, candidates)
        service = min(services, key=lambda row: row.name)
        logs = await evidence.list_signals(
            run.scope,
            SignalFilter(
                since=since,
                until=cutoff,
                service_id=service.id,
                severities=(Severity.WARNING, Severity.ERROR, Severity.CRITICAL),
                known_by=cutoff,
            ),
            after=None,
            limit=MAX_LOGS,
        )
        deployments = await evidence.list_deployments(
            run.scope,
            service_id=service.id,
            since=since - DEPLOY_LOOK_BACK,
            until=cutoff,
            known_by=cutoff,
            limit=1,
        )
    failure_type = classify([row.message for row in logs], bool(deployments))
    citations: list[uuid.UUID] = []
    if deployments:
        citations.append(deployments[0].signal_id)
    citations += [row.id for row in errors if row.service_id == service.id]
    citations += [row.id for row in logs if row.severity in ERRORS]
    unique = list(dict.fromkeys(citations))[:MAX_CITATIONS]
    return Diagnosis(
        status=DiagnosisStatus.DIAGNOSED,
        summary=f"{service.name} raised the most detector-cited errors.",
        root_cause=RootCause(
            service_id=service.id,
            service_name=service.name,
            failure_type=failure_type,
            confidence=CONFIDENCE,
            evidence_signal_ids=tuple(unique),
        ),
        limitations=(
            "Deterministic baseline: blames the alerting service and does not follow calls.",
        ),
        workflow_version=run.workflow_version,
    )


def _abstain(workflow_version: str, title: str) -> Diagnosis:
    return Diagnosis(
        status=DiagnosisStatus.INSUFFICIENT_EVIDENCE,
        summary=f"No detector-cited errors for: {title}",
        root_cause=None,
        missing_evidence=("Error signals cited by the detector",),
        workflow_version=workflow_version,
    )


CONCLUDE_RULE: Final = Node("conclude", AgentRole.SYNTHESIZER, Diagnosis, conclude_by_rule)
DETERMINISTIC: Final = Workflow(WORKFLOW_D1, (LOAD_INCIDENT,), CONCLUDE_RULE)
