from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from pgqueue import JobStatus, PermanentError, PgQueue, Retry, RetryPolicy
from pgqueue.errors import PayloadError
from pgqueue.models import Job
from pgqueue.outcomes import MAX_ERROR_LENGTH, decide_failure, format_error


class Item(BaseModel):
    n: int


NO_JITTER = RetryPolicy(base=10, factor=2, jitter=0)


async def _claim_one(app: PgQueue) -> Job:
    [job] = await app.store.claim(["default"], limit=1, worker_id="w", lease_seconds=30)
    return job


def _job(attempts: int, max_attempts: int = 3) -> Job:
    now = datetime.now(UTC)
    return Job(
        id=1, queue="q", task="t", payload={}, status=JobStatus.RUNNING, priority=0,
        run_at=now, attempts=attempts, max_attempts=max_attempts, idempotency_key=None,
        last_error=None, result=None, locked_by="w", locked_until=now, heartbeat_at=now,
        created_at=now, started_at=now, finished_at=None,
    )  # fmt: skip


@pytest.mark.parametrize(
    ("attempts", "exc", "expected"),
    [
        (1, RuntimeError("boom"), (JobStatus.RETRYING, 10.0)),
        (2, RuntimeError("boom"), (JobStatus.RETRYING, 20.0)),
        (3, RuntimeError("boom"), (JobStatus.DEAD, 0.0)),
        (1, Retry(delay=2.5), (JobStatus.RETRYING, 2.5)),
        (1, Retry(), (JobStatus.RETRYING, 10.0)),
        (3, Retry(delay=1), (JobStatus.DEAD, 0.0)),
        (1, PermanentError("bad card"), (JobStatus.FAILED, 0.0)),
        (1, PayloadError("schema drift"), (JobStatus.FAILED, 0.0)),
    ],
)
def test_decide_failure(
    attempts: int, exc: BaseException, expected: tuple[JobStatus, float]
) -> None:
    outcome = decide_failure(_job(attempts), exc, NO_JITTER)
    assert (outcome.status, outcome.retry_in) == expected


def test_format_error_keeps_the_tail_of_long_tracebacks() -> None:
    try:
        raise ValueError("x" * (MAX_ERROR_LENGTH * 2) + " THE END")
    except ValueError as exc:
        text = format_error(exc)
    assert text.startswith("...\n")
    assert text.endswith("THE END")
    assert len(text) <= MAX_ERROR_LENGTH + 4


async def test_retry_then_dead_letter(app: PgQueue) -> None:
    @app.task(max_attempts=3)
    async def flaky(payload: Item) -> None: ...

    job_id = await flaky.enqueue(Item(n=1))

    for attempt in (1, 2):
        job = await _claim_one(app)
        assert job.attempts == attempt
        assert await app.store.fail(job, "w", status=JobStatus.RETRYING, error="boom", retry_in=0)
        retried = await app.get_job(job_id)
        assert retried is not None
        assert retried.status is JobStatus.RETRYING
        assert retried.last_error == "boom"
        assert retried.finished_at is None

    job = await _claim_one(app)
    assert job.attempts == 3
    assert await app.store.fail(job, "w", status=JobStatus.DEAD, error="boom 3")

    dead = await app.get_job(job_id)
    assert dead is not None
    assert dead.status is JobStatus.DEAD
    assert dead.finished_at is not None
    assert await app.store.claim(["default"], limit=1, worker_id="w", lease_seconds=30) == []


async def test_backoff_delays_the_next_claim(app: PgQueue) -> None:
    @app.task()
    async def flaky(payload: Item) -> None: ...

    job_id = await flaky.enqueue(Item(n=1))
    job = await _claim_one(app)
    await app.store.fail(job, "w", status=JobStatus.RETRYING, error="boom", retry_in=3600)

    assert await app.store.claim(["default"], limit=1, worker_id="w", lease_seconds=30) == []
    delay = await app.pool.fetchval(
        "SELECT extract(epoch FROM run_at - now()) FROM pgqueue_jobs WHERE id = $1", job_id
    )
    assert 3590 < delay <= 3600


async def test_fail_is_fenced(app: PgQueue) -> None:
    @app.task()
    async def flaky(payload: Item) -> None: ...

    await flaky.enqueue(Item(n=1))
    job = await _claim_one(app)
    assert not await app.store.fail(job, "intruder", status=JobStatus.FAILED, error="x")
    with pytest.raises(ValueError, match="not a failure status"):
        await app.store.fail(job, "w", status=JobStatus.SUCCEEDED, error="x")
