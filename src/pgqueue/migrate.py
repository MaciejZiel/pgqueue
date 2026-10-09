"""Programmatic access to the bundled Alembic migrations.

Alembic's online mode drives its own event loop, so these functions are
synchronous; from async code call them through ``asyncio.to_thread``.
"""

from __future__ import annotations

from alembic import command
from alembic.config import Config

from pgqueue.dsn import sqlalchemy_url


def alembic_config(dsn: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", "pgqueue:migrations")
    # ConfigParser interpolation would choke on '%' in passwords.
    cfg.set_main_option("sqlalchemy.url", sqlalchemy_url(dsn).replace("%", "%%"))
    return cfg


def upgrade(dsn: str, revision: str = "head") -> None:
    """Bring the pgqueue tables up to ``revision`` (default: latest)."""
    command.upgrade(alembic_config(dsn), revision)


def downgrade(dsn: str, revision: str) -> None:
    """Roll the pgqueue tables back to ``revision`` (``base`` drops them)."""
    command.downgrade(alembic_config(dsn), revision)
