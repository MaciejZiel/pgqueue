from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from pydantic import BaseModel

from pgqueue import PgQueue, PgQueueError
from pgqueue.periodic import PeriodicScheduler
from pgqueue.worker import Worker

from .helpers import all_in, running, wait_until


class Report(BaseModel):
    kind: str


async def _make_due(app: PgQueue, name: str) -> None:
    await app.pool.execute(
        "UPDATE pgqueue_periodic SET next_run_at = now() - interval '1 second' WHERE name = $1",
        name,
    )


async def _next_run(app: PgQueue, name: str) -> datetime:
    value: datetime = await app.pool.fetchval(
        "SELECT next_run_at FROM pgqueue_periodic WHERE name = $1", name
    )
    return value


async def test_sync_registers_schedules(app: PgQueue) -> None:
    @app.periodic("*/5 * * * *", payload=Report(kind="daily"), queue="reports")
    @app.task()
    async def build(payload: Report) -> None: ...

    await PeriodicScheduler(app).sync()

    row = await app.pool.fetchrow("SELECT * FROM pgqueue_periodic")
    assert row is not None
    assert (row["name"], row["cron"], row["task"], row["queue"]) == (
        build.name,
        "*/5 * * * *",
        build.name,
        "reports",
    )
    assert row["payload"] == {"kind": "daily"}
    assert row["next_run_at"].minute % 5 == 0


async def test_due_schedule_fires_once_and_advances(app: PgQueue) -> None:
    @app.periodic("0 * * * *", name="hourly-cleanup")
    @app.task()
    async def cleanup() -> None: ...

    scheduler = PeriodicScheduler(app)
    await scheduler.sync()
    assert await scheduler.tick() == []  # not due yet

    await _make_due(app, "hourly-cleanup")
    [job_id] = await scheduler.tick()
    assert await scheduler.tick() == []

    job = await app.get_job(job_id)
    assert job is not None
    assert job.task == cleanup.name
    assert job.idempotency_key is not None
    assert job.idempotency_key.startswith("periodic:hourly-cleanup:")
    upcoming = await _next_run(app, "hourly-cleanup")
    assert upcoming.minute == 0
    assert upcoming > await app.pool.fetchval("SELECT now()")


async def test_concurrent_schedulers_fire_each_tick_once(app: PgQueue) -> None:
    @app.periodic("* * * * *", name="every-minute")
    @app.task()
    async def ping() -> None: ...

    await PeriodicScheduler(app).sync()
    async with PgQueue(app.dsn, pool_max_size=20) as other:
        other.schedules = app.schedules
        for _ in range(5):
            await _make_due(app, "every-minute")
            results = await asyncio.gather(*(PeriodicScheduler(other).tick() for _ in range(10)))
            assert sum(len(r) for r in results) == 1

    assert await app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs") == 5


async def test_missed_ticks_are_not_replayed(app: PgQueue) -> None:
    @app.periodic("* * * * *", name="frequent")
    @app.task()
    async def ping() -> None: ...

    scheduler = PeriodicScheduler(app)
    await scheduler.sync()
    await app.pool.execute("UPDATE pgqueue_periodic SET next_run_at = now() - interval '3 hours'")

    assert len(await scheduler.tick()) == 1  # one catch-up run, not 180
    assert await scheduler.tick() == []


async def test_changing_the_cron_expression_reschedules(app: PgQueue) -> None:
    @app.periodic("0 0 1 1 *", name="report")
    @app.task()
    async def report() -> None: ...

    await PeriodicScheduler(app).sync()
    yearly = await _next_run(app, "report")

    redeployed = PgQueue(app.dsn)
    redeployed._pool, redeployed._store = app.pool, app.store

    @redeployed.periodic("*/10 * * * *", name="report")
    @redeployed.task(name=report.name)
    async def report_v2() -> None: ...

    await PeriodicScheduler(redeployed).sync()
    assert await _next_run(app, "report") < yearly


async def test_worker_fires_periodic_jobs_and_runs_them(app: PgQueue) -> None:
    runs = 0

    @app.periodic("* * * * *", name="tick")
    @app.task()
    async def tick() -> None:
        nonlocal runs
        runs += 1

    worker = Worker(app, poll_interval=0.05, periodic_interval=0.05)
    async with running(worker):

        async def synced() -> bool:
            return bool(await app.pool.fetchval("SELECT count(*) FROM pgqueue_periodic"))

        await wait_until(synced)
        await _make_due(app, "tick")
        await wait_until(all_in(app, "succeeded", 1))

    assert runs == 1


def test_duplicate_schedule_names_are_rejected() -> None:
    app = PgQueue("postgresql://localhost/unused")

    @app.task()
    def job() -> None: ...

    app.periodic("@daily", name="x")(job)
    with pytest.raises(PgQueueError, match="already exists"):
        app.periodic("@hourly", name="x")(job)
