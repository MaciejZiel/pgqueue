from __future__ import annotations

import asyncio

from pydantic import BaseModel

from pgqueue import JobContext, JobStatus, PgQueue, RetryPolicy
from pgqueue.worker import Worker

from .helpers import all_in, running, wait_until


class Item(BaseModel):
    n: int


async def test_heartbeat_keeps_long_jobs_leased(app: PgQueue) -> None:
    """A job running for several lease periods is never taken away from a live worker."""
    attempts: list[int] = []

    @app.task()
    async def long_running(payload: Item, ctx: JobContext) -> None:
        attempts.append(ctx.attempt)
        await asyncio.sleep(1.5)

    job_id = await long_running.enqueue(Item(n=1))
    worker = Worker(app, poll_interval=0.05, lease_seconds=0.5, reap_interval=0.1)
    # A second worker reaping aggressively would steal the job if heartbeats lapsed.
    rival = Worker(app, queues=["other"], poll_interval=0.05, reap_interval=0.05)
    async with running(worker), running(rival):
        await wait_until(all_in(app, "succeeded", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert attempts == [1]
    assert job.attempts == 1
    assert job.heartbeat_at is not None
    assert job.started_at is not None
    assert job.heartbeat_at > job.started_at


async def test_jobs_of_a_crashed_worker_are_recovered(app: PgQueue) -> None:
    attempts: list[int] = []

    @app.task(retry=RetryPolicy(base=0, jitter=0))
    async def work(payload: Item, ctx: JobContext) -> None:
        attempts.append(ctx.attempt)

    job_id = await work.enqueue(Item(n=1))
    # A worker claims the job and then "crashes": it never heartbeats or completes.
    [orphan] = await app.store.claim(["default"], limit=1, worker_id="ghost", lease_seconds=0.2)

    async with running(Worker(app, poll_interval=0.05, reap_interval=0.1)):
        await wait_until(all_in(app, "succeeded", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert attempts == [2]  # the crashed attempt counted
    assert job.last_error is not None
    assert "lease expired: worker ghost" in job.last_error
    # The ghost coming back to life cannot overwrite the real outcome.
    assert not await app.store.complete(orphan, "ghost")


async def test_reaper_dead_letters_when_attempts_are_exhausted(app: PgQueue) -> None:
    @app.task(max_attempts=1)
    async def work(payload: Item) -> None: ...

    job_id = await work.enqueue(Item(n=1))
    await app.store.claim(["default"], limit=1, worker_id="ghost", lease_seconds=0.01)
    await asyncio.sleep(0.05)

    reaped = await app.store.reap_expired()

    assert reaped == [(job_id, JobStatus.DEAD, "ghost")]
    job = await app.get_job(job_id)
    assert job is not None
    assert job.status is JobStatus.DEAD
    assert job.finished_at is not None


async def test_reaper_ignores_live_leases(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue(Item(n=1))
    await app.store.claim(["default"], limit=1, worker_id="alive", lease_seconds=60)
    assert await app.store.reap_expired() == []


async def test_heartbeat_reports_lost_leases(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue_many([Item(n=1), Item(n=2)])
    jobs = await app.store.claim(["default"], limit=2, worker_id="w", lease_seconds=60)
    stolen = jobs[0].id
    await app.pool.execute("UPDATE pgqueue_jobs SET locked_by = 'thief' WHERE id = $1", stolen)

    held = await app.store.heartbeat([j.id for j in jobs], "w", lease_seconds=60)
    assert held == {jobs[1].id}
