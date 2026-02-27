"""Alembic migration environment for the PFM SQLite database."""

from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path
from typing import TYPE_CHECKING

from alembic import context
from sqlalchemy import engine_from_config, pool

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.base import Engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def _resolve_sqlalchemy_url() -> str:
    """Build SQLAlchemy URL from DATABASE_PATH env var."""
    db_path = Path(os.getenv("DATABASE_PATH", "data/pfm.db"))
    return f"sqlite:///{db_path}"


config.set_main_option("sqlalchemy.url", _resolve_sqlalchemy_url())


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    maybe_url = config.get_main_option("sqlalchemy.url")
    if maybe_url is None:
        maybe_url = ""
    context.configure(
        url=maybe_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def _run_migrations_with_connection(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    section = config.get_section(config.config_ini_section, {})
    connectable: Engine = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        _run_migrations_with_connection(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
