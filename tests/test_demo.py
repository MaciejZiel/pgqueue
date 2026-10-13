from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from dataclasses import replace

import pytest
import pytest_asyncio
from demo import retention, tasks
from demo.config import Settings
from demo.generator import Generator
from demo.server import build_app
from demo.supervisor import WorkerSupervisor, worker_command

from pgqueue import JobContext, PermanentError, PgQueue, Retry
from pgqueue.dsn import asyncpg_dsn
from pgqueue.models import Job
from pgqueue.worker import Worker

from .asgi import Client
from .helpers import running, wait_until


class FixedRandom:
    """Stands in for ``tasks.rng``: no sleeping, and a chosen failure roll."""

    def __init__(self, roll: float) -> None:
        self.roll = roll

    def uniform(self, a: float, b: float) -> float:
        return 0.0

    def random(self) -> float:
        return self.roll


@pytest.fixture
def settings(clean_db: str) -> Settings:
    return Settings(dsn=clean_db, burst_size=20, rate=20.0, max_backlog=1000)


@pytest_asyncio.fixture
async def demo_app(clean_db: str) -> AsyncIterator[PgQueue]:
    tasks.app.dsn = asyncpg_dsn(clean_db)
    await tasks.app.connect()
    yield tasks.app
    await tasks.app.close()


# -- settings ---------------------------------------------------------------------------------


def test_settings_defaults_and_overrides() -> None:
    env = {
        "PGQUEUE_DSN": "postgresql://x/y",
        "DEMO_PORT": "9000",
        "DEMO_ROOT_PATH": "/queue/",
        "DEMO_RATE": "2.5",
        "DEMO_CRASH_EVERY": "0",
    }

    settings = Settings.from_env(env)

    assert (settings.port, settings.root_path, settings.rate) == (9000, "/queue", 2.5)
    assert settings.crash_every == 0
    assert settings.workers == 3
    assert settings.host == "127.0.0.1"
    assert settings.respawn_after > settings.lease_seconds


def test_settings_require_a_database() -> None:
    with pytest.raises(SystemExit):
        Settings.from_env({})


# -- tasks ------------------------------------------------------------------------------------


async def test_charge_outcomes(monkeypatch: pytest.MonkeyPatch) -> None:
    job = Job.from_record(
        {
            "id": 7, "queue": "billing", "task": "billing.charge", "payload": {},
            "status": "running", "priority": 10, "run_at": None, "attempts": 1,
            "max_attempts": 4, "idempotency_key": None, "last_error": None, "result": None,
            "locked_by": "w", "locked_until": None, "heartbeat_at": None, "created_at": None,
            "started_at": None, "finished_at": None,
        }
    )  # fmt: skip
    ctx = JobContext(job=job, worker_id="w")
    payload = tasks.Charge(invoice_id=1, amount_cents=500)

    monkeypatch.setattr(tasks, "rng", FixedRandom(0.01))
    with pytest.raises(PermanentError):
        await tasks.charge.fn(payload, ctx)
    monkeypatch.setattr(tasks, "rng", FixedRandom(0.05))
    with pytest.raises(Retry):
        await tasks.charge.fn(payload, ctx)
    monkeypatch.setattr(tasks, "rng", FixedRandom(0.9))
    assert await tasks.charge.fn(payload, ctx) == {"charge_id": "ch_7", "amount_cents": 500}


def test_all_queues_are_declared() -> None:
    used = {task.options.queue for task in tasks.app.tasks.values()}
    assert used <= set(tasks.QUEUES)
    assert {schedule.name for schedule in tasks.app.schedules.values()} == {
        "reports.minutely",
        "reports.digest",
    }


# -- generator --------------------------------------------------------------------------------


def test_generator_pacing_carries_fractions(settings: Settings) -> None:
    generator = Generator(settings, rng=random.Random(1))

    assert sum(generator.due(2.5, 1.0) for _ in range(4)) == 10
    assert generator.due(0.0, 1.0) == 0
    rates = [generator.current_rate(t) for t in range(0, 200, 5)]
    assert min(rates) < settings.rate < max(rates)
    assert min(rates) >= 0


async def test_generator_enqueues_every_kind_into_its_queue(
    demo_app: PgQueue, settings: Settings
) -> None:
    generator = Generator(settings, rng=random.Random(2))

    for kind in ("email", "image", "charge", "webhook", "export"):
        await generator.enqueue_one(kind)

    rows = await demo_app.pool.fetch("SELECT queue, task FROM pgqueue_jobs ORDER BY id")
    assert [(r["queue"], r["task"]) for r in rows] == [
        ("emails", "emails.send_welcome"),
        ("images", "images.resize"),
        ("billing", "billing.charge"),
        ("webhooks", "webhooks.deliver"),
        ("reports", "reports.export"),
    ]


async def test_a_restarted_generator_still_enqueues_emails(
    demo_app: PgQueue, settings: Settings
) -> None:
    for _ in range(2):  # the second one is the demo after a restart: same sequence numbers
        await Generator(settings, rng=random.Random(1)).enqueue_one("email")
        await Generator(settings, rng=random.Random(1)).burst()

    emails = await demo_app.pool.fetchval(
        "SELECT count(*) FROM pgqueue_jobs WHERE task = 'emails.send_welcome'"
    )
    assert emails == 2 + 2 * settings.burst_size


async def test_generator_bursts_insert_many_jobs(demo_app: PgQueue, settings: Settings) -> None:
    generator = Generator(settings, rng=random.Random(3))

    inserted = await generator.burst()

    total = await demo_app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs")
    assert total == inserted >= settings.burst_size // 2
    assert generator.bursts == 1


async def test_generator_pauses_when_the_backlog_is_too_long(
    demo_app: PgQueue, settings: Settings
) -> None:
    generator = Generator(replace(settings, max_backlog=3), rng=random.Random(4))
    for _ in range(5):
        await generator.enqueue_one("email")

    task = asyncio.create_task(generator.run(tick=0.05))
    try:
        await wait_until(lambda: asyncio.sleep(0, result=generator.paused))
        before = await demo_app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs")
        await asyncio.sleep(0.5)
        after = await demo_app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs")
    finally:
        task.cancel()

    assert before == after == 5


async def test_generator_produces_work_that_workers_finish(
    demo_app: PgQueue, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks, "rng", FixedRandom(0.99))  # nothing fails, nothing sleeps
    generator = Generator(replace(settings, burst_every=3600, rate=50.0), rng=random.Random(5))
    worker = Worker(
        demo_app, queues=list(tasks.QUEUES), concurrency=8, poll_interval=0.05, run_periodic=False
    )

    async with running(worker):
        producer = asyncio.create_task(generator.run(tick=0.05))
        try:
            await wait_until(lambda: _succeeded_at_least(demo_app, 25))
        finally:
            producer.cancel()

    assert generator.enqueued >= 25


async def _succeeded_at_least(app: PgQueue, n: int) -> bool:
    return (
        int(await app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs WHERE status = 'succeeded'"))
        >= n
    )


# -- retention --------------------------------------------------------------------------------


async def test_sweep_deletes_old_and_excess_finished_jobs_only(demo_app: PgQueue) -> None:
    pool = demo_app.pool
    await pool.execute(
        """
        INSERT INTO pgqueue_jobs (queue, task, status, finished_at, started_at)
        SELECT 'default', 'demo.task', 'succeeded', now() - make_interval(secs => g * 100), now()
        FROM generate_series(1, 10) AS g
        """
    )
    await pool.execute("INSERT INTO pgqueue_jobs (queue, task, status) VALUES ('q', 't', 'queued')")
    await pool.execute(
        "INSERT INTO pgqueue_jobs (queue, task, status, run_at, attempts) "
        "VALUES ('q', 't', 'retrying', now() + interval '1 hour', 1)"
    )
    await demo_app.store.register_worker("old", queues=["q"], concurrency=1, lease_seconds=30)
    await pool.execute("UPDATE pgqueue_workers SET last_seen_at = now() - interval '5 minutes'")
    await demo_app.store.register_worker("fresh", queues=["q"], concurrency=1, lease_seconds=30)

    aged, over, workers = await retention.sweep(demo_app.store, max_age=550, max_rows=3)

    assert aged == 5  # finished more than 550 s ago
    assert over == 2  # five remain, three are allowed
    assert workers == 1
    statuses = {r["status"]: r["n"] for r in await pool.fetch(
        "SELECT status::text, count(*) AS n FROM pgqueue_jobs GROUP BY 1"
    )}  # fmt: skip
    assert statuses == {"succeeded": 3, "queued": 1, "retrying": 1}
    assert await pool.fetchval("SELECT id FROM pgqueue_workers") == "fresh"


async def test_the_table_stays_bounded_under_a_sustained_load(demo_app: PgQueue) -> None:
    pool = demo_app.pool
    for _ in range(4):
        await pool.execute(
            "INSERT INTO pgqueue_jobs (queue, task, status, finished_at) "
            "SELECT 'q', 't', 'succeeded', now() FROM generate_series(1, 500)"
        )
        await retention.sweep(demo_app.store, max_age=3600, max_rows=700)
        assert await pool.fetchval("SELECT count(*) FROM pgqueue_jobs") <= 700


async def test_the_cap_never_deletes_a_job_that_is_being_requeued(demo_app: PgQueue) -> None:
    pool = demo_app.pool
    await pool.execute(
        "INSERT INTO pgqueue_jobs (queue, task, status, finished_at) "
        "SELECT 'q', 't', 'dead', now() FROM generate_series(1, 3)"
    )
    oldest = await pool.fetchval("SELECT min(id) FROM pgqueue_jobs")
    async with pool.acquire() as conn:
        # Someone requeues the oldest dead job and commits while the sweep runs.
        transaction = conn.transaction()
        await transaction.start()
        await conn.execute(
            "UPDATE pgqueue_jobs SET status = 'queued', finished_at = NULL WHERE id = $1", oldest
        )
        sweep = asyncio.create_task(retention.sweep(demo_app.store, max_age=3600, max_rows=0))
        await asyncio.sleep(0.3)
        await transaction.commit()
        _, over, _ = await asyncio.wait_for(sweep, 10)
    assert over == 2
    assert await pool.fetchval("SELECT status::text FROM pgqueue_jobs WHERE id = $1", oldest) == (
        "queued"
    )


# -- supervisor -------------------------------------------------------------------------------


def test_worker_command_uses_the_demo_app_and_a_short_lease(settings: Settings) -> None:
    command = worker_command(settings)

    assert command[1:3] == ["-m", "pgqueue"]
    assert "demo.tasks:app" in command
    assert command[command.index("--lease") + 1] == str(settings.lease_seconds)
    assert command[command.index("--queues") + 1] == ",".join(tasks.QUEUES)


class FakeProcess:
    def __init__(self, pid: int, returncode: int | None = None) -> None:
        self.pid = pid
        self.returncode = returncode
        self.killed = False

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


def test_the_supervisor_never_kills_the_last_worker(settings: Settings) -> None:
    supervisor = WorkerSupervisor(settings, rng=random.Random(1))
    only = FakeProcess(1)
    supervisor.processes = {0: only, 1: FakeProcess(2, returncode=-9)}  # type: ignore[dict-item]

    assert supervisor.kill_one() is None
    assert not only.killed

    supervisor.processes = {0: only, 1: FakeProcess(2)}  # type: ignore[dict-item]
    victim = supervisor.kill_one()
    assert victim in (1, 2)
    assert supervisor.kills == 1


# -- server -----------------------------------------------------------------------------------


async def test_the_public_app_is_read_only_and_explains_itself(
    demo_app: PgQueue, settings: Settings
) -> None:
    app = build_app(replace(settings, banner="Public demo"), background=False)
    client = Client(app)
    async with app.router.lifespan_context(app):
        page = await client.get("/")
        live = await client.get("/api/live")
        requeue = await client.post("/api/jobs/1/requeue", headers={"X-PgQueue-Action": "1"})
        bulk = await client.post("/api/queues/billing/requeue", {"status": "dead"})

    assert page.status == 200
    assert "Public demo" in page.text
    assert live.json()["read_only"] is True
    assert requeue.status in (404, 405)
    assert bulk.status in (403, 404, 405)
    routes = {getattr(route, "path", "") for route in app.routes}
    assert not any("requeue" in path for path in routes)
