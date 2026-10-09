from __future__ import annotations

import asyncio

import asyncpg
import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from pgqueue.dsn import sqlalchemy_url
from pgqueue.migrate import downgrade, upgrade
from pgqueue.schema import metadata

from .conftest import TEST_DSN, dsn_for_database, recreate_database

SCRATCH_DSN = dsn_for_database(TEST_DSN, "pgqueue_test_migrations")


async def _tables(dsn: str) -> set[str]:
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename LIKE 'pgqueue%'"
        )
        return {r["tablename"] for r in rows}
    finally:
        await conn.close()


async def test_upgrade_downgrade_roundtrip() -> None:
    await recreate_database(SCRATCH_DSN)
    await asyncio.to_thread(upgrade, SCRATCH_DSN)
    assert "pgqueue_jobs" in await _tables(SCRATCH_DSN)

    await asyncio.to_thread(downgrade, SCRATCH_DSN, "base")
    assert await _tables(SCRATCH_DSN) == {"pgqueue_alembic_version"}

    await asyncio.to_thread(upgrade, SCRATCH_DSN)
    assert "pgqueue_jobs" in await _tables(SCRATCH_DSN)


def _diff(connection: Connection) -> list[object]:
    def include(obj: object, name: str | None, type_: str, *_: object) -> bool:
        return type_ != "table" or bool(name and name.startswith("pgqueue_"))

    ctx = MigrationContext.configure(
        connection,
        opts={
            "compare_type": True,
            "include_object": include,
            "version_table": "pgqueue_alembic_version",
        },
    )
    return list(compare_metadata(ctx, metadata))


@pytest.mark.usefixtures("database")
async def test_migrations_match_schema_module() -> None:
    """The hand-written migrations and schema.py must never drift apart."""
    engine = create_async_engine(sqlalchemy_url(TEST_DSN))
    try:
        async with engine.connect() as conn:
            diff = await conn.run_sync(_diff)
    finally:
        await engine.dispose()
    assert diff == []
