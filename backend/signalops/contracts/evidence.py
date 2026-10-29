"""Enumerations shared by evidence contracts and persistence CHECK constraints.

Adding a member requires a migration that widens the matching CHECK constraint.
"""

from enum import StrEnum

LIVE_REPLAY_SCOPE = "live"


class SourceKind(StrEnum):
    STRUCTURED_LOG = "structured_log"
    TRACE_SUMMARY = "trace_summary"
    DEPLOYMENT = "deployment"


class SignalKind(StrEnum):
    LOG = "log"
    TRACE = "trace"
    DEPLOYMENT = "deployment"


class Severity(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class QualityStatus(StrEnum):
    ACCEPTED = "accepted"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"
