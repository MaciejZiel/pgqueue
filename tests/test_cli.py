from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from pgqueue import PgQueue
from pgqueue.cli import build_parser, load_app

from .conftest import TEST_DSN, dsn_for_database, recreate_database
from .helpers import all_in, count, wait_until

ROOT = Path(__file__).resolve().parent.parent


async def _spawn(*args: str, dsn: str = TEST_DSN) -> asyncio.subprocess.Process:
    env = {**os.environ, "PGQUEUE_TEST_DSN": dsn, "PGQUEUE_DSN": dsn}
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "pgqueue",
        *args,
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )


async def test_migrate_command_creates_the_schema() -> None:
    dsn = dsn_for_database(TEST_DSN, "pgqueue_test_cli")
    await recreate_database(dsn)
    proc = await _spawn("migrate", dsn=dsn)
    output, _ = await asyncio.wait_for(proc.communicate(), 60)
    assert proc.returncode == 0, output.decode()

    async with PgQueue(dsn) as app:
        assert await app.pool.fetchval("SELECT to_regclass('pgqueue_jobs') IS NOT NULL")


async def test_sigterm_drains_in_flight_jobs(app: PgQueue) -> None:
    for _ in range(3):
        await app.enqueue("sample.sleep", {"seconds": 1.0})
    await app.enqueue("sample.sleep", {"seconds": 0.1}, delay=3600)

    proc = await _spawn("worker", "tests.sample_app:app", "--concurrency", "3")
    try:
        await wait_until(all_in(app, "running", 3), within=20)
        proc.send_signal(signal.SIGTERM)
        output, _ = await asyncio.wait_for(proc.communicate(), 10)
    finally:
        if proc.returncode is None:
            proc.kill()

    log = output.decode()
    assert proc.returncode == 0, log
    assert "received SIGTERM" in log
    assert await count(app, "succeeded") == 3  # finished, not abandoned
    assert await count(app, "queued") == 1


def test_load_app_errors() -> None:
    with pytest.raises(SystemExit, match="MODULE:ATTRIBUTE"):
        load_app("tests.sample_app")
    with pytest.raises(SystemExit, match="not a PgQueue"):
        load_app("tests.sample_app:sleep")


def test_parser_defaults() -> None:
    args = build_parser().parse_args(["worker", "x:app", "-q", "a,b", "-c", "50"])
    assert (args.queues, args.concurrency, args.lease, args.no_listen) == ("a,b", 50, 30.0, False)
