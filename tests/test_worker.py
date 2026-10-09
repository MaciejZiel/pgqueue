from __future__ import annotations

import asyncio
import threading
import time

import pytest
from pydantic import BaseModel

from pgqueue import JobContext, JobStatus, PermanentError, PgQueue, Retry, RetryPolicy
from pgqueue.worker import Worker

from .helpers import all_in, count, running, wait_until

FAST_RETRY = RetryPolicy(base=0, jitter=0)


class Item(BaseModel):
    n: int


def make_worker(app: PgQueue, **kwargs: object) -> Worker:
    options: dict[str, object] = {"poll_interval": 0.05, "concurrency": 4}
    options.update(kwargs)
    return Worker(app, **options)  # type: ignore[arg-type]


async def test_processes_async_and_sync_handlers(app: PgQueue) -> None:
    @app.task()
    async def double(payload: Item) -> int:
        return payload.n * 2

    @app.task()
    def triple(payload: Item) -> dict[str, int]:
        return {"value": payload.n * 3}

    a = await double.enqueue(Item(n=2))
    b = await triple.enqueue(Item(n=2))

    async with running(make_worker(app)):
        await wait_until(all_in(app, "succeeded", 2))

    job_a, job_b = await app.get_job(a), await app.get_job(b)
    assert job_a is not None
    assert job_b is not None
    assert job_a.result == 4
    assert job_b.result == {"value": 6}
    assert job_a.attempts == 1


async def test_handler_receives_job_context(app: PgQueue) -> None:
    seen: list[tuple[int, int, str]] = []

    @app.task()
    async def inspect(payload: Item, ctx: JobContext) -> None:
        seen.append((ctx.job_id, ctx.attempt, ctx.worker_id))

    job_id = await inspect.enqueue(Item(n=1))
    worker = make_worker(app, worker_id="w-ctx")
    async with running(worker):
        await wait_until(all_in(app, "succeeded", 1))

    assert seen == [(job_id, 1, "w-ctx")]


async def test_failing_job_is_retried_until_dead(app: PgQueue) -> None:
    calls = 0

    @app.task(max_attempts=3, retry=FAST_RETRY)
    async def broken(payload: Item) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError(f"boom {calls}")

    job_id = await broken.enqueue(Item(n=1))
    async with running(make_worker(app)):
        await wait_until(all_in(app, "dead", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert calls == 3
    assert job.attempts == 3
    assert job.last_error is not None
    assert "RuntimeError: boom 3" in job.last_error
    assert "Traceback" in job.last_error


async def test_job_that_recovers_succeeds(app: PgQueue) -> None:
    attempts: list[int] = []

    @app.task(max_attempts=5, retry=FAST_RETRY)
    async def flaky(payload: Item, ctx: JobContext) -> str:
        attempts.append(ctx.attempt)
        if ctx.attempt < 3:
            raise ConnectionError("downstream unavailable")
        return "ok"

    job_id = await flaky.enqueue(Item(n=1))
    async with running(make_worker(app)):
        await wait_until(all_in(app, "succeeded", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert attempts == [1, 2, 3]
    assert job.result == "ok"
    assert job.last_error is not None  # the last failure is kept for debugging


async def test_permanent_error_fails_without_retrying(app: PgQueue) -> None:
    @app.task(max_attempts=5)
    async def charge(payload: Item) -> None:
        raise PermanentError("card declined")

    job_id = await charge.enqueue(Item(n=1))
    async with running(make_worker(app)):
        await wait_until(all_in(app, "failed", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert job.attempts == 1
    assert job.last_error is not None
    assert "card declined" in job.last_error


async def test_explicit_retry_uses_its_own_delay(app: PgQueue) -> None:
    @app.task(retry=RetryPolicy(base=3600, jitter=0))
    async def rate_limited(payload: Item, ctx: JobContext) -> None:
        if ctx.attempt == 1:
            raise Retry("429", delay=0)

    await rate_limited.enqueue(Item(n=1))
    async with running(make_worker(app)):
        await wait_until(all_in(app, "succeeded", 1))


async def test_payload_that_no_longer_validates_fails_permanently(app: PgQueue) -> None:
    await app.enqueue("schema-drift", {"n": "not a number"})

    @app.task(name="schema-drift")
    async def drift(payload: Item) -> None: ...

    async with running(make_worker(app)):
        await wait_until(all_in(app, "failed", 1))


async def test_unknown_task_is_retried_for_another_worker(app: PgQueue) -> None:
    job_id = await app.enqueue("not-deployed-here", {}, max_attempts=2)
    async with running(make_worker(app)):
        await wait_until(all_in(app, "retrying", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert job.last_error is not None
    assert "UnknownTaskError" in job.last_error


async def test_timeout_counts_as_a_failed_attempt(app: PgQueue) -> None:
    @app.task(timeout=0.05, max_attempts=1)
    async def slow(payload: Item) -> None:
        await asyncio.sleep(5)

    job_id = await slow.enqueue(Item(n=1))
    async with running(make_worker(app)):
        await wait_until(all_in(app, "dead", 1))

    job = await app.get_job(job_id)
    assert job is not None
    assert job.last_error is not None
    assert "TimeoutError" in job.last_error


async def test_concurrency_limit_is_respected(app: PgQueue) -> None:
    active = 0
    peak = 0

    @app.task()
    async def sleepy(payload: Item) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.1)
        active -= 1

    await sleepy.enqueue_many([Item(n=i) for i in range(20)])
    started = time.monotonic()
    async with running(make_worker(app, concurrency=5)):
        await wait_until(all_in(app, "succeeded", 20))
    elapsed = time.monotonic() - started

    assert peak == 5
    assert elapsed < 1.8  # 20 jobs x 100 ms / 5 slots = ~0.4 s; sequentially 2 s


async def test_sync_handlers_run_off_the_event_loop(app: PgQueue) -> None:
    threads: set[str] = set()
    lock = threading.Lock()
    active = 0
    peak = 0

    @app.task()
    def blocking(payload: Item) -> None:
        nonlocal active, peak
        with lock:
            threads.add(threading.current_thread().name)
            active += 1
            peak = max(peak, active)
        time.sleep(0.3)
        with lock:
            active -= 1

    await blocking.enqueue_many([Item(n=i) for i in range(4)])
    async with running(make_worker(app, concurrency=4)):
        await wait_until(all_in(app, "succeeded", 4))

    assert peak == 4  # four blocking calls overlapped
    assert all(name.startswith("pgqueue-sync") for name in threads)


async def test_stop_drains_in_flight_jobs(app: PgQueue) -> None:
    finished = 0

    @app.task()
    async def slow(payload: Item) -> None:
        nonlocal finished
        await asyncio.sleep(0.3)
        finished += 1

    await slow.enqueue_many([Item(n=i) for i in range(3)])
    await slow.enqueue_many([Item(n=i) for i in range(3)], delay=3600)
    worker = make_worker(app, concurrency=3, shutdown_timeout=10)
    task = asyncio.create_task(worker.run())
    await wait_until(all_in(app, "running", 3))

    worker.stop()
    await asyncio.wait_for(task, 5)

    assert finished == 3
    assert await count(app, "succeeded") == 3
    assert await count(app, "queued") == 3  # never claimed after stop()


async def test_shutdown_timeout_releases_unfinished_jobs(app: PgQueue) -> None:
    @app.task()
    async def stuck(payload: Item) -> None:
        await asyncio.sleep(60)

    job_id = await stuck.enqueue(Item(n=1))
    worker = make_worker(app, shutdown_timeout=0.2)
    task = asyncio.create_task(worker.run())
    await wait_until(all_in(app, "running", 1))

    worker.stop()
    await asyncio.wait_for(task, 5)

    job = await app.get_job(job_id)
    assert job is not None
    assert job.status is JobStatus.QUEUED
    assert job.attempts == 0  # an interrupted attempt is not charged
    assert job.last_error is not None
    assert "shut down" in job.last_error


def test_worker_validates_arguments() -> None:
    app = PgQueue("postgresql://localhost/unused")
    with pytest.raises(ValueError, match="concurrency"):
        Worker(app, concurrency=0)
    with pytest.raises(ValueError, match="queue"):
        Worker(app, queues=[])


async def test_forced_stop_does_not_wait_for_the_shutdown_timeout(app: PgQueue) -> None:
    @app.task()
    async def stuck(payload: Item) -> None:
        await asyncio.sleep(60)

    await stuck.enqueue(Item(n=1))
    worker = make_worker(app, shutdown_timeout=60)
    task = asyncio.create_task(worker.run())
    await wait_until(all_in(app, "running", 1))

    worker.stop()
    await asyncio.sleep(0.2)
    worker.shutdown_timeout = 0  # what a second SIGTERM does
    started = time.monotonic()
    await asyncio.wait_for(task, 5)

    assert time.monotonic() - started < 5  # instead of the 60 s shutdown timeout
    assert await count(app, "queued") == 1
