"""Alembic environment: runs migrations over an asyncpg-backed async engine."""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from pgqueue.dsn import sqlalchemy_url
from pgqueue.schema import metadata

config = context.config
VERSION_TABLE = "pgqueue_alembic_version"


def _include_object(obj: object, name: str | None, type_: str, *_: object) -> bool:
    # pgqueue usually shares a database with the host application: only ever
    # look at our own tables when comparing or autogenerating.
    if type_ == "table":
        return bool(name and name.startswith("pgqueue_"))
    return True


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=metadata,
        version_table=VERSION_TABLE,
        include_object=_include_object,
        compare_type=True,
    )


def _run_sync(connection: Connection) -> None:
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    url = sqlalchemy_url(config.get_main_option("sqlalchemy.url") or "")
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        await connection.run_sync(_run_sync)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=metadata,
        version_table=VERSION_TABLE,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    existing = config.attributes.get("connection")
    if existing is not None:
        _run_sync(existing)
    else:
        asyncio.run(_run_async())
