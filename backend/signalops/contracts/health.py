"""Liveness and readiness response contracts."""

from typing import Literal

from pydantic import BaseModel


class LivenessResponse(BaseModel):
    status: Literal["ok"] = "ok"


class DependencyStatus(BaseModel):
    status: Literal["ok", "error"]
    latency_ms: float
    # Error category only (for example "timeout" or an exception class name).
    # Messages are omitted because driver errors can include hosts or credentials.
    error: str | None = None


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, DependencyStatus]
