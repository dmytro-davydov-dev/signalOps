"""Database login roles for the runtime and the evaluator.

Migration 0017 creates two group roles without login. `signalops_runtime` reads and
writes the application tables and has no privilege on the `evaluation` schema,
where ground truth lives. `signalops_evaluator` has both. The processes that run
investigations connect as a login member of the runtime group, so neither their
code nor a crafted query through their connection can read expected outcomes.

`provision_login_roles` creates or updates the login roles from configured
passwords. `signalops migrate` runs it after the upgrade, as the owner. A password
is sent as a quoted literal in `CREATE ROLE`, which takes no bind parameters, and
is never logged.
"""

import logging
from dataclasses import dataclass

from psycopg import sql
from sqlalchemy import Connection, create_engine, text

from signalops.config import Settings

logger = logging.getLogger(__name__)

RUNTIME_GROUP = "signalops_runtime"
EVALUATOR_GROUP = "signalops_evaluator"
GROUPS = (RUNTIME_GROUP, EVALUATOR_GROUP)


@dataclass(frozen=True)
class LoginRole:
    name: str
    password: str
    group: str


def configured_login_roles(settings: Settings) -> list[LoginRole]:
    roles = []
    if settings.db_runtime_password is not None:
        roles.append(
            LoginRole(
                settings.db_runtime_user,
                settings.db_runtime_password.get_secret_value(),
                RUNTIME_GROUP,
            )
        )
    if settings.db_evaluator_password is not None:
        roles.append(
            LoginRole(
                settings.db_evaluator_user,
                settings.db_evaluator_password.get_secret_value(),
                EVALUATOR_GROUP,
            )
        )
    return roles


def provision_login_roles(settings: Settings) -> list[str]:
    """Create or update each configured login role; return their names.

    Each login role belongs to exactly one group, so the runtime login never gains
    the evaluator's access, even if it held it before.
    """
    roles = configured_login_roles(settings)
    if not roles:
        return []
    engine = create_engine(settings.validated_database_url())
    try:
        with engine.begin() as connection:
            for role in roles:
                exists = connection.execute(
                    text("SELECT 1 FROM pg_roles WHERE rolname = :name"), {"name": role.name}
                ).scalar()
                verb = "ALTER" if exists else "CREATE"
                statement = sql.SQL(
                    verb + " ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOBYPASSRLS PASSWORD {}"
                ).format(sql.Identifier(role.name), sql.Literal(role.password))
                _execute(connection, statement)
                for group in GROUPS:
                    action = "GRANT {} TO {}" if group == role.group else "REVOKE {} FROM {}"
                    _execute(
                        connection,
                        sql.SQL(action).format(sql.Identifier(group), sql.Identifier(role.name)),
                    )
    finally:
        engine.dispose()
    names = [role.name for role in roles]
    logger.info("database login roles ready", extra={"roles": names})
    return names


def _execute(connection: Connection, statement: sql.Composed) -> None:
    # Composed SQL needs the driver's cursor; it runs in the same transaction.
    raw = connection.connection.dbapi_connection
    if raw is None:
        raise RuntimeError("the connection is closed")
    cursor = raw.cursor()
    try:
        cursor.execute(statement)
    finally:
        cursor.close()
