"""Randomised stress tests of the core guarantee: no job attempt runs twice.

Every handler invocation is recorded in a side table *by the handler itself*
(no unique constraint, so duplicates would be visible rather than rejected).
After the queue drains we check, for every job:

* it reached a terminal state (succeeded or dead);
* it was executed exactly ``attempts`` times, once per attempt number;
* if it succeeded, exactly one execution succeeded.

The scenario (number of workers, their concurrency, priorities, delays and
failure rate) is drawn from a seeded RNG, property-test style, so a failure
is reproducible from the seed printed in the test id. Hypothesis would be the
natural tool, but it is MPL-2.0 licensed, which this project avoids.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import BaseModel

from pgqueue import JobContext, PgQueue, RetryPolicy
from pgqueue.worker import Worker

from .helpers import running, wait_until

pytestmark = pytest.mark.stress
SEEDS = [int(s) for s in os.environ.get("PGQUEUE_STRESS_SEEDS", "1,2,3,4,5").split(",")]
ROOT = Path(__file__).resolve().parent.parent

EXECUTIONS_DDL = """
CREATE TABLE IF NOT EXISTS stress_executions (
    job_id bigint NOT NULL,
    attempt int NOT NULL,
    worker text NOT NULL,
    succeeded boolean NOT NULL
)
"""


class Work(BaseModel):
    fail_until_attempt: int
    sleep_ms: int


@dataclass(frozen=True)
class Scenario:
    workers: int
    concurrency: list[int]
    jobs: int
    max_attempts: int

    @classmethod
    def draw(cls, rng: random.Random) -> Scenario:
        workers = rng.randint(3, 8)
        return cls(
            workers=workers,
            concurrency=[rng.randint(1, 16) for _ in range(workers)],
            jobs=rng.randint(300, 700),
            max_attempts=rng.randint(2, 4),
        )


def make_app(dsn: str) -> PgQueue:
    """One PgQueue (own connection pool) per simulated worker process."""
    app = PgQueue(dsn, pool_max_size=20)

    @app.task(name="stress.work", retry=RetryPolicy(base=0.01, max_delay=0.05))
    async def work(payload: Work, ctx: JobContext) -> None:
        if payload.sleep_ms:
            await asyncio.sleep(payload.sleep_ms / 1000)
        ok = ctx.attempt > payload.fail_until_attempt
        await app.pool.execute(
            "INSERT INTO stress_executions VALUES ($1, $2, $3, $4)",
            ctx.job_id,
            ctx.attempt,
            ctx.worker_id,
            ok,
        )
        if not ok:
            raise RuntimeError(f"planned failure on attempt {ctx.attempt}")

    return app


async def _reset(app: PgQueue) -> None:
    await app.pool.execute(EXECUTIONS_DDL)
    await app.pool.execute("TRUNCATE stress_executions")


async def _drained(app: PgQueue) -> bool:
    pending: int = await app.pool.fetchval(
        "SELECT count(*) FROM pgqueue_jobs WHERE status NOT IN ('succeeded', 'dead')"
    )
    return pending == 0


async def _check_invariants(app: PgQueue, expected_jobs: int) -> Counter[str]:
    jobs = await app.pool.fetch("SELECT id, status::text, attempts FROM pgqueue_jobs")
    executions = await app.pool.fetch("SELECT job_id, attempt, succeeded FROM stress_executions")
    assert len(jobs) == expected_jobs

    attempts_seen: dict[int, list[int]] = defaultdict(list)
    successes: Counter[int] = Counter()
    for row in executions:
        attempts_seen[row["job_id"]].append(row["attempt"])
        successes[row["job_id"]] += row["succeeded"]

    for job in jobs:
        seen = sorted(attempts_seen[job["id"]])
        assert seen == list(range(1, job["attempts"] + 1)), (job, seen)
        if job["status"] == "succeeded":
            assert successes[job["id"]] == 1, job
        else:
            assert job["status"] == "dead", job
            assert successes[job["id"]] == 0, job
    return Counter(job["status"] for job in jobs)


@pytest.mark.parametrize("seed", SEEDS, ids=[f"seed={s}" for s in SEEDS])
async def test_no_attempt_is_ever_executed_twice(clean_db: str, seed: int) -> None:
    rng = random.Random(seed)
    scenario = Scenario.draw(rng)
    apps = [make_app(clean_db) for _ in range(scenario.workers)]
    for app in apps:
        await app.connect()
    producer = apps[0]
    try:
        await _reset(producer)
        task = producer.get_task("stress.work")
        for _ in range(scenario.jobs):
            roll = rng.random()
            fail_until = 0 if roll < 0.7 else rng.randint(1, scenario.max_attempts)
            await task.enqueue(
                Work(fail_until_attempt=fail_until, sleep_ms=rng.choice([0, 0, 1, 5, 20])),
                priority=rng.randint(-5, 5),
                max_attempts=scenario.max_attempts,
                delay=rng.choice([0, 0, 0, 0.2]),
            )

        workers = [
            Worker(
                app,
                concurrency=concurrency,
                poll_interval=0.05,
                worker_id=f"w{i}",
                run_periodic=False,
            )
            for i, (app, concurrency) in enumerate(zip(apps, scenario.concurrency, strict=True))
        ]
        async with contextlib.AsyncExitStack() as stack:
            for worker in workers:
                await stack.enter_async_context(running(worker))
            await wait_until(lambda: _drained(producer), within=120, interval=0.1)

        statuses = await _check_invariants(producer, scenario.jobs)
        assert statuses["succeeded"] > 0
        # Work really was spread across workers, not done by one of them.
        per_worker = await producer.pool.fetch(
            "SELECT worker, count(*) FROM stress_executions GROUP BY worker"
        )
        assert len(per_worker) >= 2
    finally:
        for app in apps:
            await app.close()


async def test_separate_worker_processes_never_share_a_job(app: PgQueue) -> None:
    """Four real OS processes competing for 2000 jobs."""
    await _reset(app)
    await app.pool.execute(
        "INSERT INTO pgqueue_jobs (task, payload) "
        "SELECT 'stress.record', '{}'::jsonb FROM generate_series(1, 2000)"
    )
    env = {**os.environ, "PGQUEUE_TEST_DSN": app.dsn}
    processes = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "pgqueue",
            "--log-level",
            "WARNING",
            "worker",
            "tests.sample_app:app",
            "--concurrency",
            "16",
            "--no-periodic",
            cwd=ROOT,
            env=env,
        )
        for _ in range(4)
    ]
    try:
        await wait_until(lambda: _drained(app), within=120, interval=0.2)
        assert all(proc.returncode is None for proc in processes), "a worker process died"
    finally:
        for proc in processes:
            proc.terminate()
        await asyncio.gather(*(proc.wait() for proc in processes))

    rows = await app.pool.fetch(
        "SELECT job_id, count(*) AS n FROM stress_executions GROUP BY job_id"
    )
    assert len(rows) == 2000
    assert {row["n"] for row in rows} == {1}
    pids = await app.pool.fetchval("SELECT count(DISTINCT worker) FROM stress_executions")
    assert pids == 4


async def test_jobs_survive_a_worker_losing_its_database(clean_db: str) -> None:
    """Chaos: one of three workers is cut off from Postgres mid-flight.

    Its in-flight jobs can neither complete nor heartbeat; their leases expire
    and the surviving workers' reapers hand them out again. Every job still
    ends up succeeded, and each job's attempt counter matches the executions
    that the database actually saw.
    """
    apps = [make_app(clean_db) for _ in range(3)]
    for app in apps:
        await app.connect()
    producer = apps[0]
    try:
        await _reset(producer)
        task = producer.get_task("stress.work")
        await task.enqueue_many(
            [Work(fail_until_attempt=0, sleep_ms=50) for _ in range(400)], max_attempts=5
        )
        workers = [
            Worker(
                app,
                concurrency=10,
                poll_interval=0.05,
                lease_seconds=1.0,
                reap_interval=0.2,
                worker_id=f"w{i}",
                run_periodic=False,
            )
            for i, app in enumerate(apps)
        ]
        victim = workers[2]
        async with contextlib.AsyncExitStack() as stack:
            for worker in workers:
                await stack.enter_async_context(running(worker))

            async def victim_busy() -> bool:
                return victim.in_flight == victim.concurrency

            await wait_until(victim_busy)
            stranded = victim.in_flight
            apps[2].pool.terminate()  # network partition: every query now fails

            await wait_until(lambda: _drained(producer), within=60, interval=0.1)

        jobs = await producer.pool.fetch("SELECT id, status::text, attempts FROM pgqueue_jobs")
        assert {j["status"] for j in jobs} == {"succeeded"}
        # Stranded jobs were recovered and retried. (A job whose completion
        # reached the server just before the cut-off is legitimately done.)
        recovered = [j for j in jobs if j["attempts"] > 1]
        assert 1 <= len(recovered) <= stranded
        recorded = await producer.pool.fetch(
            "SELECT job_id, count(*) AS n FROM stress_executions GROUP BY job_id"
        )
        assert len(recorded) == 400
        # At-least-once, made visible: only a stranded job may have executed
        # twice (its side effect landed, its completion did not).
        assert sum(1 for r in recorded if r["n"] > 1) <= stranded
    finally:
        for app in apps[:2]:
            await app.close()
