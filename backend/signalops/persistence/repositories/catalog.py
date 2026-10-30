"""Workspaces, sources, and services: the catalog that evidence refers to."""

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.evidence import SourceKind
from signalops.persistence.models import Service, Source, Workspace


class CatalogConflictError(Exception):
    """A catalog row exists under the same identity with different values."""


class CatalogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def ensure_workspace(self, workspace_id: uuid.UUID, name: str) -> Workspace:
        await self._session.execute(
            insert(Workspace).values(id=workspace_id, name=name).on_conflict_do_nothing()
        )
        workspace = await self._session.get(Workspace, workspace_id, populate_existing=True)
        if workspace is None or workspace.name != name:
            raise CatalogConflictError(f"workspace {workspace_id} exists with another name")
        return workspace

    async def ensure_source(
        self, *, source_id: uuid.UUID, workspace_id: uuid.UUID, name: str, kind: SourceKind
    ) -> Source:
        """Insert the source, or return the existing one with the same id, name, and kind."""
        await self._session.execute(
            insert(Source)
            .values(id=source_id, workspace_id=workspace_id, name=name, kind=kind)
            .on_conflict_do_nothing()
        )
        source = await self._session.scalar(
            select(Source)
            .where(Source.workspace_id == workspace_id, Source.name == name)
            .execution_options(populate_existing=True)
        )
        if source is None or source.id != source_id or source.kind != kind:
            raise CatalogConflictError(f"source {name!r} conflicts with an existing row")
        return source

    async def ensure_service(
        self, *, service_id: uuid.UUID, workspace_id: uuid.UUID, name: str, environment: str
    ) -> Service:
        """Insert the service, or return the existing one with the same id and identity."""
        await self._session.execute(
            insert(Service)
            .values(id=service_id, workspace_id=workspace_id, name=name, environment=environment)
            .on_conflict_do_nothing()
        )
        service = await self._session.scalar(
            select(Service)
            .where(
                Service.workspace_id == workspace_id,
                Service.name == name,
                Service.environment == environment,
            )
            .execution_options(populate_existing=True)
        )
        if service is None or service.id != service_id:
            raise CatalogConflictError(f"service {name!r} ({environment}) conflicts with a row")
        return service
