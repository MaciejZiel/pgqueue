from __future__ import annotations

import asyncio

from pgqueue import PgQueue
from pgqueue.worker import Worker

from .helpers import running, wait_until


async def test_worker_registers_heartbeats_and_deregisters(app: PgQueue) -> None:
    worker = Worker(
        app, queues=["a", "b"], concurrency=7, lease_seconds=0.3, worker_id="registry-test"
    )

    async def registered() -> bool:
        return bool(await app.pool.fetchval("SELECT count(*) FROM pgqueue_workers"))

    async with running(worker):
        await wait_until(registered)
        row = await app.pool.fetchrow("SELECT * FROM pgqueue_workers")
        assert row is not None
        assert (row["id"], row["queues"], row["concurrency"]) == ("registry-test", ["a", "b"], 7)
        first_seen = row["last_seen_at"]
        await asyncio.sleep(0.3)
        assert await app.pool.fetchval("SELECT last_seen_at FROM pgqueue_workers") > first_seen

    assert not await registered()


async def test_long_dead_workers_are_pruned(app: PgQueue) -> None:
    await app.store.register_worker("ghost", queues=["default"], concurrency=1, lease_seconds=30)
    await app.store.register_worker("alive", queues=["default"], concurrency=1, lease_seconds=30)
    await app.pool.execute(
        "UPDATE pgqueue_workers SET last_seen_at = now() - interval '2 hours' WHERE id = 'ghost'"
    )

    assert await app.store.prune_workers() == 1
    assert await app.pool.fetchval("SELECT array_agg(id) FROM pgqueue_workers") == ["alive"]
