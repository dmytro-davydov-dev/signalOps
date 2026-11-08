"""FastAPI application factory."""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from signalops import __version__
from signalops.adapters.ingestion import IngestPolicy
from signalops.api import evaluations, evidence, health, incidents, ingest, investigations
from signalops.api.credentials import CredentialStore
from signalops.api.request_ids import RequestIdMiddleware
from signalops.config import Settings, get_settings
from signalops.dependencies import Resources, open_resources

logger = logging.getLogger(__name__)

ResourceFactory = Callable[[Settings], AbstractAsyncContextManager[Resources]]


def create_app(
    settings: Settings | None = None,
    resource_factory: ResourceFactory = open_resources,
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Opens clients only; durable work runs in worker processes.
        async with resource_factory(settings) as resources:
            app.state.resources = resources
            logger.info(
                "api started",
                extra={
                    "environment": settings.environment,
                    "model_provider": settings.model_provider,
                },
            )
            yield
        logger.info("api stopped")

    app = FastAPI(title="SignalOps API", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    # Fails at startup on a missing or invalid credentials file.
    app.state.credentials = CredentialStore(
        settings.operator_tokens,
        settings.ingest_tokens,
        settings.credentials_file,
        reload_seconds=settings.credentials_reload_seconds,
    )
    app.state.ingest_policy = IngestPolicy.from_settings(settings)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["GET", "PATCH", "POST"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "Last-Event-ID",
                "X-Request-ID",
            ],
            expose_headers=["X-Request-ID"],
        )
    # Outermost, so CORS preflights and errors carry the ID too.
    app.add_middleware(RequestIdMiddleware, max_body_bytes=settings.api_max_body_bytes)
    app.include_router(health.router)
    app.include_router(ingest.router)
    app.include_router(evidence.router)
    app.include_router(incidents.router)
    app.include_router(investigations.router)
    app.include_router(evaluations.router)
    return app
