"""Alembic environment.

`signalops migrate` passes the database URL as a config attribute; the plain
`alembic` CLI falls back to SIGNALOPS_DATABASE_URL.
"""

from alembic import context
from sqlalchemy import create_engine, pool

import signalops.persistence.models  # noqa: F401  (registers tables on Base.metadata)
from signalops.config import get_settings
from signalops.persistence.base import Base

target_metadata = Base.metadata

# Schemas Alembic compares; `evaluation` holds ground truth (migration 0017).
MANAGED_SCHEMAS = frozenset({"public", "evaluation"})


def include_name(name: str | None, type_: str, _parent_names: object) -> bool:
    if type_ == "schema":
        return name is None or name in MANAGED_SCHEMAS
    return True


def database_url() -> str:
    url = context.config.attributes.get("database_url")
    return str(url) if url else get_settings().validated_database_url()


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        include_schemas=True,
        include_name=include_name,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(database_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,
            include_name=include_name,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
