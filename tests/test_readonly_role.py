from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
import pytest_asyncio

from pgqueue import PgQueue
from pgqueue.admin import create_app

from .asgi import Client
from .conftest import dsn_for_database

ROLE = "pgqueue_test_ro"
SETUP = Path(__file__).resolve().parent.parent / "deploy" / "termux" / "setup-db.sh"


def _as_role(dsn: str, user: str, password: str, database: str | None = None) -> str:
    parts = urlsplit(dsn)
    host = f"{parts.hostname}:{parts.port}" if parts.port else str(parts.hostname)
    path = f"/{database}" if database else parts.path
    return urlunsplit(parts._replace(netloc=f"{user}:{password}@{host}", path=path))


@pytest_asyncio.fixture
async def readonly_dsn(app: PgQueue) -> AsyncIterator[str]:
    """A login role that can only SELECT from the test database."""
    database = urlsplit(app.dsn).path.lstrip("/")
    conn = await asyncpg.connect(app.dsn)
    try:
        await conn.execute(f"DROP ROLE IF EXISTS {ROLE}")
        await conn.execute(f"CREATE ROLE {ROLE} LOGIN PASSWORD 'ro'")
        await conn.execute(f'GRANT CONNECT ON DATABASE "{database}" TO {ROLE}')
        await conn.execute(f"GRANT USAGE ON SCHEMA public TO {ROLE}")
        await conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {ROLE}")
        yield _as_role(app.dsn, ROLE, "ro")
    finally:
        await conn.execute(f"DROP OWNED BY {ROLE}")
        await conn.execute(f"DROP ROLE IF EXISTS {ROLE}")
        await conn.close()


async def test_a_read_only_app_serves_the_dashboard_through_the_select_only_role(
    app: PgQueue, readonly_dsn: str
) -> None:
    await app.enqueue("demo.task", {"n": 1})
    admin = create_app(
        "postgresql://nobody:wrong@127.0.0.1:1/none", read_only=True, read_only_dsn=readonly_dsn
    )
    try:
        client = Client(admin)
        assert (await client.get("/api/live")).status == 200
        assert (await client.get("/api/jobs")).json()["jobs"][0]["task"] == "demo.task"
        assert (await client.get("/metrics")).status == 200
    finally:
        await admin.state.engine.dispose()

    conn = await asyncpg.connect(readonly_dsn)
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await conn.execute("UPDATE pgqueue_jobs SET status = 'failed'")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await conn.execute("DELETE FROM pgqueue_jobs")
    finally:
        await conn.close()


async def test_the_read_only_dsn_only_applies_to_a_read_only_app(
    app: PgQueue, readonly_dsn: str
) -> None:
    admin = create_app(app.dsn, read_only=False, read_only_dsn="postgresql://x:y@127.0.0.1:1/none")
    try:
        assert (await Client(admin).get("/api/live")).status == 200
    finally:
        await admin.state.engine.dispose()


async def test_the_read_only_dsn_can_come_from_the_environment(
    app: PgQueue, readonly_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PGQUEUE_READONLY_DSN", readonly_dsn)
    admin = create_app("postgresql://nobody:wrong@127.0.0.1:1/none", read_only=True)
    try:
        assert (await Client(admin).get("/api/live")).status == 200
    finally:
        await admin.state.engine.dispose()


@pytest.mark.skipif(shutil.which("psql") is None, reason="needs the psql client")
async def test_setup_script_creates_a_limited_owner_and_a_select_only_role(
    database: str,
) -> None:
    parts = urlsplit(database)
    env = {
        **os.environ,
        "PGHOST": str(parts.hostname),
        "PGPORT": str(parts.port or 5432),
        "PGUSER": str(parts.username),
        "PGPASSWORD": str(parts.password),
        "DEMO_DB_PASSWORD": "owner-pw",
        "DEMO_RO_PASSWORD": "ro-pw",
    }
    admin = await asyncpg.connect(dsn_for_database(database, "postgres"))
    try:
        for _ in range(2):  # idempotent
            await asyncio.to_thread(
                subprocess.run, ["bash", str(SETUP)], env=env, check=True, capture_output=True
            )
        limits = dict(
            await admin.fetch(
                "SELECT rolname, rolconnlimit FROM pg_roles WHERE rolname LIKE 'pgqueue_demo%'"
            )
        )
        assert limits == {"pgqueue_demo": 40, "pgqueue_demo_ro": 10}

        owner_dsn = _as_role(database, "pgqueue_demo", "owner-pw", "pgqueue_demo")
        ro_dsn = _as_role(database, "pgqueue_demo_ro", "ro-pw", "pgqueue_demo")
        owner = await asyncpg.connect(owner_dsn)
        await owner.execute("CREATE TABLE later (id int)")  # created after the grants
        await owner.execute("INSERT INTO later VALUES (1)")
        reader = await asyncpg.connect(ro_dsn)
        try:
            assert await reader.fetchval("SELECT count(*) FROM later") == 1
            with pytest.raises(asyncpg.PostgresError):
                await reader.execute("INSERT INTO later VALUES (2)")
            with pytest.raises(asyncpg.PostgresError):
                await reader.execute("CREATE TABLE x (id int)")
        finally:
            await reader.close()
            await owner.close()
        # No other role can connect to the demo database by default.
        assert not await admin.fetchval(
            "SELECT has_database_privilege('public', 'pgqueue_demo', 'CONNECT')"
        )
    finally:
        await admin.execute("DROP DATABASE IF EXISTS pgqueue_demo WITH (FORCE)")
        await admin.execute("DROP ROLE IF EXISTS pgqueue_demo_ro")
        await admin.execute("DROP ROLE IF EXISTS pgqueue_demo")
        await admin.close()
