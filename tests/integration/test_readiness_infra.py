import socket

import pytest

from signalops.config import Settings
from signalops.dependencies import check_readiness, open_resources

pytestmark = pytest.mark.integration


def _unused_local_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_ready_against_real_infrastructure(infra_settings: Settings) -> None:
    async with open_resources(infra_settings) as resources:
        report = await check_readiness(resources.checks, infra_settings.readiness_timeout_seconds)
    assert report.status == "ready", report
    assert set(report.checks) == {"postgres", "redis", "kafka"}


async def test_unreachable_redis_is_reported_while_others_stay_ok(
    infra_settings: Settings,
) -> None:
    settings = infra_settings.model_copy(
        update={"redis_url": f"redis://127.0.0.1:{_unused_local_port()}/0"}
    )
    async with open_resources(settings) as resources:
        report = await check_readiness(resources.checks, settings.readiness_timeout_seconds)
    assert report.status == "not_ready"
    assert report.checks["redis"].status == "error"
    assert report.checks["postgres"].status == "ok"
    assert report.checks["kafka"].status == "ok"
