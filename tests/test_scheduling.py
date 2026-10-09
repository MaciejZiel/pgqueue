from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from pgqueue import PgQueue
from pgqueue.app import resolve_schedule


class Item(BaseModel):
    n: int


async def _seconds_until_run(app: PgQueue, job_id: int) -> float:
    value: float = await app.pool.fetchval(
        "SELECT extract(epoch FROM run_at - now()) FROM pgqueue_jobs WHERE id = $1", job_id
    )
    return value


async def _claimable(app: PgQueue) -> list[int]:
    jobs = await app.store.claim(["default"], limit=100, worker_id="w", lease_seconds=30)
    return [j.id for j in jobs]


async def test_delayed_job_is_invisible_until_due(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    later = await work.enqueue(Item(n=1), delay=120)
    as_timedelta = await work.enqueue(Item(n=2), delay=timedelta(minutes=5))
    now = await work.enqueue(Item(n=3))

    assert 115 < await _seconds_until_run(app, later) <= 120
    assert 295 < await _seconds_until_run(app, as_timedelta) <= 300
    assert await _claimable(app) == [now]

    await app.pool.execute("UPDATE pgqueue_jobs SET run_at = now() WHERE id = $1", later)
    assert await _claimable(app) == [later]


async def test_run_at_schedules_an_absolute_time(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    when = datetime.now(UTC) + timedelta(hours=1)
    job_id = await work.enqueue(Item(n=1), run_at=when)
    job = await app.get_job(job_id)
    assert job is not None
    assert job.run_at == when.replace(microsecond=job.run_at.microsecond)
    assert await _claimable(app) == []


async def test_past_run_at_is_due_immediately_and_ordered_first(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    fresh = await work.enqueue(Item(n=1))
    overdue = await work.enqueue(Item(n=2), run_at=datetime.now(UTC) - timedelta(hours=1))
    assert await _claimable(app) == [overdue, fresh]


async def test_enqueue_many_with_delay(app: PgQueue) -> None:
    @app.task()
    async def work(payload: Item) -> None: ...

    ids = await work.enqueue_many([Item(n=i) for i in range(3)], delay=60)
    assert len(ids) == 3
    assert await _claimable(app) == []


def test_resolve_schedule_validation() -> None:
    with pytest.raises(ValueError, match="either"):
        resolve_schedule(1, datetime.now(UTC))
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_schedule(None, datetime(2030, 1, 1))
    with pytest.raises(ValueError, match="non-negative"):
        resolve_schedule(-5, None)
    assert resolve_schedule(timedelta(seconds=90), None) == (None, 90.0)
