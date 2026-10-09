from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
import pytest_asyncio

from pgqueue import PgQueue
from pgqueue.migrate import upgrade

TEST_DSN = os.environ.get(
    "PGQUEUE_TEST_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue_test"
)


def dsn_for_database(dsn: str, database: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit(parts._replace(path=f"/{database}"))


async def recreate_database(dsn: str) -> None:
    """Drop and create the database named in ``dsn`` (connecting via ``postgres``)."""
    name = urlsplit(dsn).path.lstrip("/")
    admin = await asyncpg.connect(dsn_for_database(dsn, "postgres"))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


@pytest_asyncio.fixture(scope="session")
async def database() -> str:
    """A freshly created and fully migrated test database (once per session)."""
    try:
        await recreate_database(TEST_DSN)
    except (OSError, asyncpg.PostgresError) as exc:  # pragma: no cover
        pytest.exit(
            f"cannot reach Postgres at {TEST_DSN}: {exc}\n"
            "Start it with `docker compose up -d postgres`",
            returncode=2,
        )
    await asyncio.to_thread(upgrade, TEST_DSN)
    return TEST_DSN


TABLES = ("pgqueue_jobs", "pgqueue_periodic", "pgqueue_workers")


@pytest_asyncio.fixture
async def clean_db(database: str) -> str:
    conn = await asyncpg.connect(database)
    try:
        existing = [
            t for t in TABLES if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", t)
        ]
        if existing:
            await conn.execute(f"TRUNCATE {', '.join(existing)} RESTART IDENTITY")
    finally:
        await conn.close()
    return database


@pytest_asyncio.fixture
async def app(clean_db: str) -> AsyncIterator[PgQueue]:
    queue = PgQueue(clean_db)
    async with queue:
        yield queue
