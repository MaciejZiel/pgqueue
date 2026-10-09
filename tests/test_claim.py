from __future__ import annotations

import asyncio
from collections import Counter

from pydantic import BaseModel

from pgqueue import JobStatus, PgQueue


class Item(BaseModel):
    n: int


async def test_claim_orders_by_priority_then_fifo(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    low_a = await work.enqueue(Item(n=1), priority=0)
    high = await work.enqueue(Item(n=2), priority=10)
    low_b = await work.enqueue(Item(n=3), priority=0)
    urgent = await work.enqueue(Item(n=4), priority=100)

    jobs = await app.store.claim(["default"], limit=10, worker_id="w1", lease_seconds=30)

    assert [j.id for j in jobs] == [urgent, high, low_a, low_b]


async def test_claim_marks_jobs_running_with_a_lease(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue(Item(n=1))
    [job] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)

    assert job.status is JobStatus.RUNNING
    assert job.attempts == 1
    assert job.locked_by == "w1"
    assert job.locked_until is not None
    assert job.started_at is not None
    assert (job.locked_until - job.started_at).total_seconds() == 30


async def test_claim_respects_limit_and_queue_filter(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    for i in range(5):
        await work.enqueue(Item(n=i), queue="a")
    await work.enqueue(Item(n=99), queue="b")

    first = await app.store.claim(["a"], limit=3, worker_id="w1", lease_seconds=30)
    second = await app.store.claim(["a"], limit=3, worker_id="w2", lease_seconds=30)
    third = await app.store.claim(["a"], limit=3, worker_id="w3", lease_seconds=30)
    both = await app.store.claim(["a", "b"], limit=3, worker_id="w4", lease_seconds=30)

    assert (len(first), len(second), len(third)) == (3, 2, 0)
    assert [j.queue for j in both] == ["b"]


async def test_running_jobs_are_not_claimed_again(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue(Item(n=1))
    assert len(await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)) == 1
    assert await app.store.claim(["default"], limit=1, worker_id="w2", lease_seconds=30) == []


async def test_concurrent_claimers_never_receive_the_same_job(app: PgQueue) -> None:
    """Fifty concurrent claim transactions over 500 rows: SKIP LOCKED partitions them."""

    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue_many([Item(n=i) for i in range(500)])

    async with PgQueue(app.dsn, pool_max_size=50) as wide:

        async def claimer(worker: int) -> list[int]:
            claimed: list[int] = []
            while batch := await wide.store.claim(
                ["default"], limit=7, worker_id=f"w{worker}", lease_seconds=30
            ):
                claimed.extend(j.id for j in batch)
            return claimed

        results = await asyncio.gather(*(claimer(i) for i in range(50)))

    counts = Counter(job_id for ids in results for job_id in ids)
    assert len(counts) == 500
    assert set(counts.values()) == {1}


async def test_complete_records_result_and_finish_time(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue(Item(n=1))
    [job] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)

    assert await app.store.complete(job, "w1", {"answer": 42})

    done = await app.get_job(job.id)
    assert done is not None
    assert done.status is JobStatus.SUCCEEDED
    assert done.result == {"answer": 42}
    assert done.finished_at is not None
    assert done.locked_until is None


async def test_complete_is_fenced_by_worker_and_attempt(app: PgQueue) -> None:
    """A worker that no longer holds the lease cannot overwrite the outcome."""

    @app.task()
    async def work(payload: Item) -> None: ...

    await work.enqueue(Item(n=1))
    [job] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)

    assert not await app.store.complete(job, "someone-else")

    # Simulate the lease being lost and the job re-claimed (attempt 2) by w2.
    await app.pool.execute(
        "UPDATE pgqueue_jobs SET status = 'queued', locked_by = NULL, locked_until = NULL"
    )
    [again] = await app.store.claim(["default"], limit=1, worker_id="w2", lease_seconds=30)
    assert again.attempts == 2

    assert not await app.store.complete(job, "w1")  # stale attempt 1 is rejected
    assert await app.store.complete(again, "w2")
    assert not await app.store.complete(again, "w2")  # and completion is not repeatable
