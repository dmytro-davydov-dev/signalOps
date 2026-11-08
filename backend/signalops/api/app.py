"""FastAPI application factory."""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from signalops import __version__
from signalops.adapters.ingestion import IngestPolicy, SourceTokens
from signalops.api import evaluations, evidence, health, incidents, ingest, investigations
from signalops.api.auth import OperatorTokens
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
    app.state.source_tokens = SourceTokens(settings.ingest_tokens)
    app.state.ingest_policy = IngestPolicy.from_settings(settings)
    app.state.operator_tokens = OperatorTokens(settings.operator_tokens)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["GET", "PATCH", "POST"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "Last-Event-ID"],
        )
    app.include_router(health.router)
    app.include_router(ingest.router)
    app.include_router(evidence.router)
    app.include_router(incidents.router)
    app.include_router(investigations.router)
    app.include_router(evaluations.router)
    return app
