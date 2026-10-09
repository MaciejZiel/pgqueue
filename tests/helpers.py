from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from pgqueue import PgQueue
from pgqueue.worker import Worker


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], within: float = 10.0, interval: float = 0.02
) -> float:
    """Poll ``predicate`` until it is true; return the elapsed seconds."""
    start = time.monotonic()
    while not await predicate():
        if time.monotonic() - start > within:
            raise AssertionError(f"condition not met within {within}s")
        await asyncio.sleep(interval)
    return time.monotonic() - start


@asynccontextmanager
async def running(worker: Worker) -> AsyncIterator[Worker]:
    task = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(task, 30)


async def count(app: PgQueue, status: str) -> int:
    value: int = await app.pool.fetchval(
        "SELECT count(*) FROM pgqueue_jobs WHERE status = $1", status
    )
    return value


def all_in(app: PgQueue, status: str, n: int) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return await count(app, status) == n

    return check
