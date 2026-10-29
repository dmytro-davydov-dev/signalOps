"""Liveness and readiness endpoints."""

from fastapi import APIRouter, Request, Response, status

from signalops.config import Settings
from signalops.contracts.health import LivenessResponse, ReadinessResponse
from signalops.dependencies import Resources, check_readiness

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live() -> LivenessResponse:
    """The process is running and serving requests."""
    return LivenessResponse()


@router.get(
    "/ready",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def ready(request: Request, response: Response) -> ReadinessResponse:
    """Every required dependency answered within the readiness timeout."""
    resources: Resources = request.app.state.resources
    settings: Settings = request.app.state.settings
    report = await check_readiness(resources.checks, settings.readiness_timeout_seconds)
    if report.status != "ready":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return report
