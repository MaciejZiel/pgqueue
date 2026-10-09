from __future__ import annotations

import asyncio
import time

import asyncpg
from pydantic import BaseModel

from pgqueue import PgQueue
from pgqueue.schema import NOTIFY_CHANNEL
from pgqueue.worker import Worker

from .helpers import all_in, running, wait_until


class Item(BaseModel):
    n: int


async def _collect_notifications(dsn: str) -> tuple[asyncpg.Connection, list[str]]:
    received: list[str] = []
    conn = await asyncpg.connect(dsn)
    await conn.add_listener(NOTIFY_CHANNEL, lambda *args: received.append(args[3]))
    return conn, received


async def test_trigger_notifies_once_per_queue_for_ready_jobs(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    conn, received = await _collect_notifications(app.dsn)
    try:
        await work.enqueue_many([Item(n=i) for i in range(500)], queue="bulk")
        await work.enqueue(Item(n=1), queue="later", delay=3600)
        await work.enqueue(Item(n=1), queue="now")
        await asyncio.sleep(0.2)
    finally:
        await conn.close()

    # 500 inserts in one statement -> one notification; delayed jobs -> none.
    assert received == ["bulk", "now"]


async def test_requeue_by_update_notifies(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    job_id = await work.enqueue(Item(n=1), delay=3600)
    conn, received = await _collect_notifications(app.dsn)
    try:
        await app.pool.execute(
            "UPDATE pgqueue_jobs SET status = 'retrying', run_at = now() WHERE id = $1", job_id
        )
        await app.pool.execute("UPDATE pgqueue_jobs SET priority = 5 WHERE id = $1", job_id)
        await asyncio.sleep(0.2)
    finally:
        await conn.close()

    assert received == ["default"]


async def test_notification_wakes_an_idle_worker_immediately(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    worker = Worker(app, poll_interval=30)  # polling alone would take 30 s
    async with running(worker):
        assert worker.listener is not None
        await asyncio.wait_for(worker.listener.connected.wait(), 5)
        await asyncio.sleep(0.1)  # let the worker go idle

        started = time.monotonic()
        await work.enqueue(Item(n=1))
        await wait_until(all_in(app, "succeeded", 1), within=5)
        latency = time.monotonic() - started

    assert latency < 3.0  # versus a 30 s poll interval


async def test_notifications_for_other_queues_are_ignored(app: PgQueue) -> None:
    worker = Worker(app, queues=["mine"], poll_interval=30)
    worker._on_notify("someone-elses")
    assert not worker._wakeup.is_set()
    worker._on_notify("mine")
    assert worker._wakeup.is_set()


async def test_polling_keeps_working_when_listen_connection_dies(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    worker = Worker(app, poll_interval=0.2)
    async with running(worker):
        listener = worker.listener
        assert listener is not None
        await asyncio.wait_for(listener.connected.wait(), 5)

        # Kill every LISTEN connection from the server side.
        await app.pool.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()"
        )
        await work.enqueue(Item(n=1))
        await wait_until(all_in(app, "succeeded", 1), within=5)  # served by polling

        async def reconnected() -> bool:
            return listener.connections >= 2 and listener.connected.is_set()

        await wait_until(reconnected, within=10)


async def test_worker_without_listen_relies_on_polling(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    worker = Worker(app, poll_interval=0.1, listen=False)
    assert worker.listener is None
    async with running(worker):
        await work.enqueue(Item(n=1))
        await wait_until(all_in(app, "succeeded", 1), within=5)
