"""Run Alembic migrations programmatically for the one-shot migrate process."""

from pathlib import Path

from alembic import command
from alembic.config import Config

from signalops.config import Settings


def alembic_config(settings: Settings) -> Config:
    script_location = Path(settings.migrations_path)
    if not (script_location / "env.py").is_file():
        raise FileNotFoundError(f"no Alembic environment at {script_location.resolve()}")
    config = Config()
    config.set_main_option("script_location", str(script_location))
    # Passed as an attribute rather than an ini option so it never lands in a file.
    config.attributes["database_url"] = settings.validated_database_url()
    return config


def upgrade_to_head(settings: Settings) -> None:
    command.upgrade(alembic_config(settings), "head")
