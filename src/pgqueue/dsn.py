"""Helpers for the two spellings of a Postgres DSN.

asyncpg wants ``postgresql://``; SQLAlchemy needs the driver in the scheme
(``postgresql+asyncpg://``). Users pass either and we normalise.
"""

from __future__ import annotations

_PREFIXES = ("postgresql+asyncpg://", "postgresql://", "postgres://")


def asyncpg_dsn(dsn: str) -> str:
    """Return ``dsn`` in the form asyncpg accepts."""
    for prefix in _PREFIXES:
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    raise ValueError(f"not a PostgreSQL DSN: {dsn!r}")


def sqlalchemy_url(dsn: str) -> str:
    """Return ``dsn`` in the form SQLAlchemy's asyncpg dialect accepts."""
    return "postgresql+asyncpg://" + asyncpg_dsn(dsn)[len("postgresql://") :]
