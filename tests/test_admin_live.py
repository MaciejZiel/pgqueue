from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from starlette.routing import Mount

from pgqueue import JobStatus, PgQueue
from pgqueue.admin import create_app, live, queries
from pgqueue.admin.live import LiveFeed

from .asgi import Client

ACTION = {"X-PgQueue-Action": "1"}


@pytest_asyncio.fixture
async def read_only(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn, read_only=True, banner="Live demo")
    yield admin_app
    await admin_app.state.feed.close()
    await admin_app.state.engine.dispose()


@pytest_asyncio.fixture
async def admin(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn, read_only=False)
    yield admin_app
    await admin_app.state.feed.close()
    await admin_app.state.engine.dispose()


async def _run(app: PgQueue, status: JobStatus, *, retries: int = 0) -> int:
    job_id = await app.enqueue("demo.task", {"n": 1}, max_attempts=retries + 1)
    [job] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)
    assert job.id == job_id
    if status is JobStatus.SUCCEEDED:
        await app.store.complete(job, "w1", {"ok": True})
    return job_id


async def test_read_only_mode_has_no_mutating_endpoints(app: PgQueue, read_only: FastAPI) -> None:
    job_id = await app.enqueue("demo.task", {}, max_attempts=1)
    [job] = await app.store.claim(["default"], limit=1, worker_id="w", lease_seconds=30)
    await app.store.fail(job, "w", status=JobStatus.DEAD, error="boom")
    client = Client(read_only)

    for path in (f"/api/jobs/{job_id}/requeue", "/api/queues/default/requeue?status=dead"):
        assert (await client.post(path, headers=ACTION)).status in (404, 405)
    stored = await app.get_job(job_id)
    assert stored is not None
    assert stored.status is JobStatus.DEAD

    assert (await client.get(f"/api/jobs/{job_id}")).status == 200
    page = (await client.get("/")).text
    assert 'data-read-only="true"' in page
    assert "Live demo" in page


async def test_default_mode_keeps_the_actions(app: PgQueue, admin: FastAPI) -> None:
    job_id = await app.enqueue("demo.task", {}, max_attempts=1)
    [job] = await app.store.claim(["default"], limit=1, worker_id="w", lease_seconds=30)
    await app.store.fail(job, "w", status=JobStatus.DEAD, error="boom")

    response = await Client(admin).post(f"/api/jobs/{job_id}/requeue", headers=ACTION)

    assert response.status == 200
    assert 'data-read-only="false"' in (await Client(admin).get("/")).text


async def test_read_only_from_environment(app: PgQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGQUEUE_ADMIN_READ_ONLY", "true")
    admin_app = create_app(app.dsn)
    try:
        assert (await Client(admin_app).post("/api/jobs/1/requeue", headers=ACTION)).status in (
            404,
            405,
        )
    finally:
        await admin_app.state.feed.close()
        await admin_app.state.engine.dispose()


async def test_live_snapshot_reports_latency_throughput_and_leases(
    app: PgQueue, admin: FastAPI
) -> None:
    await app.store.register_worker("w1", queues=["default"], concurrency=4, lease_seconds=30)
    # Two jobs that were due 2 s ago, picked up now and finished: pickup latency ~2 s.
    for _ in range(2):
        await app.enqueue("demo.task", {}, run_at=datetime.now(UTC) - timedelta(seconds=2))
    claimed = await app.store.claim(["default"], limit=2, worker_id="w1", lease_seconds=30)
    await asyncio.sleep(0.2)
    for job in claimed:
        await app.store.complete(job, "w1", {"ok": True})
    # One retried and then dead job, one still running and one waiting.
    dead = await app.enqueue("demo.task", {}, max_attempts=1)
    [job] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)
    assert job.id == dead
    await app.store.fail(job, "w1", status=JobStatus.DEAD, error="Traceback\nTimeoutError: slow")
    await app.enqueue("demo.task", {})
    await app.enqueue("demo.task", {})
    [running] = await app.store.claim(["default"], limit=1, worker_id="w1", lease_seconds=30)

    response = await Client(admin).get("/api/live")

    assert response.status == 200
    data = response.json()
    assert data["read_only"] is False
    assert data["totals"]["succeeded"] == 2
    assert data["totals"]["dead"] == 1
    assert data["totals"]["running"] == 1
    assert data["totals"]["queued"] == 1
    stats = data["stats"]
    assert stats["pickup"]["samples"] == 3
    assert 1.5 < stats["pickup"]["p50"] < 3.5
    assert stats["run"]["samples"] == 2
    assert 0.1 < stats["run"]["p50"] < 2
    assert stats["run"]["p95"] >= stats["run"]["p50"]
    assert stats["failures"] == 1
    assert len(stats["throughput"]) == stats["window_seconds"]
    assert sum(p["succeeded"] + p["failed"] for p in stats["throughput"]) == 3
    [lease] = data["running"]
    assert (lease["id"], lease["locked_by"]) == (running.id, "w1")
    assert lease["locked_until"] > data["now"]
    assert data["workers"][0]["id"] == "w1"
    top = data["feed"][0]
    assert {"id", "task", "status", "at"} <= top.keys()
    dead_item = next(item for item in data["feed"] if item["id"] == dead)
    assert dead_item["error"] == "TimeoutError: slow"


async def test_live_snapshot_of_an_empty_database(app: PgQueue, admin: FastAPI) -> None:
    data = (await Client(admin).get("/api/live")).json()

    assert data["stats"]["pickup"] == {"p50": None, "p95": None, "samples": 0}
    assert data["stats"]["jobs_per_second"] == 0
    assert data["running"] == data["feed"] == data["queues"] == []


async def test_feed_shares_one_computation_between_subscribers(
    app: PgQueue, admin: FastAPI
) -> None:
    feed: LiveFeed = admin.state.feed
    computations = 0
    original = feed._compute

    async def counting() -> bytes:
        nonlocal computations
        computations += 1
        return await original()

    feed._compute = counting  # type: ignore[method-assign]

    async def first_two(n: int) -> list[bytes | None]:
        received: list[bytes | None] = []
        async for payload in feed.subscribe():
            received.append(payload)
            if len(received) == 2:
                break
        return received

    results = await asyncio.wait_for(asyncio.gather(*(first_two(i) for i in range(10))), 10)

    assert all(len(r) == 2 and r[0] is not None for r in results)
    # Ten viewers, two snapshots each: about three computations, not twenty.
    assert computations <= 4
    assert feed.subscribers == 0
    await asyncio.sleep(0.05)
    count = computations
    await asyncio.sleep(1.3)
    assert computations == count  # nobody is watching, so nothing is computed


async def test_feed_caches_for_polling_clients(app: PgQueue, admin: FastAPI) -> None:
    feed: LiveFeed = admin.state.feed
    first = await feed.current()
    assert await feed.current() is first


async def test_stream_endpoint_refuses_when_full(app: PgQueue, admin: FastAPI) -> None:
    admin.state.feed = LiveFeed(admin.state.engine, read_only=False, max_subscribers=0)

    response = await Client(admin).get("/api/stream")

    assert response.status == 503


async def test_finished_counts_follow_new_jobs_between_full_counts(
    app: PgQueue, admin: FastAPI
) -> None:
    engine = admin.state.engine
    await app.pool.execute(
        "INSERT INTO pgqueue_jobs (queue, task, status, finished_at) "
        "SELECT 'q', 't', 'succeeded', now() - interval '1 hour' FROM generate_series(1, 5)"
    )
    finished = queries.FinishedCounts(max_age=3600)

    async def totals() -> dict[JobStatus, int]:
        async with engine.connect() as conn:
            return (await queries.snapshot(conn, read_only=True, finished=finished)).totals

    assert (await totals())[JobStatus.SUCCEEDED] == 5
    # Finished after the full count: found through the index, without a recount.
    await app.pool.execute(
        "INSERT INTO pgqueue_jobs (queue, task, status, finished_at) "
        "VALUES ('q', 't', 'succeeded', now()), ('q', 't', 'dead', now()), "
        "('q', 't', 'queued', NULL)"
    )
    counts = await totals()
    assert (counts[JobStatus.SUCCEEDED], counts[JobStatus.DEAD], counts[JobStatus.QUEUED]) == (
        6,
        1,
        1,
    )
    async with engine.connect() as conn:
        exact = (await queries.overview(conn)).totals
    assert counts == exact

    # Deletions show up at the next full count.
    await app.pool.execute(
        "DELETE FROM pgqueue_jobs WHERE finished_at < now() - interval '1 minute'"
    )
    assert (await totals())[JobStatus.SUCCEEDED] == 6
    finished.max_age = 0
    assert (await totals())[JobStatus.SUCCEEDED] == 1


async def test_a_stalled_viewer_does_not_block_the_others(
    app: PgQueue, admin: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live, "KEEPALIVE_SECONDS", 0.05)
    feed: LiveFeed = admin.state.feed
    gate = asyncio.Event()

    async def slow() -> bytes:
        await gate.wait()
        return b"{}"

    feed._compute = slow  # type: ignore[method-assign]
    stalled = feed.subscribe()
    # Nothing is computed yet, so the first item is a keep-alive. The viewer then stops
    # reading, as a slow client would, leaving its generator suspended.
    assert await asyncio.wait_for(anext(stalled), 1) is None
    gate.set()
    other = feed.subscribe()
    try:
        assert await asyncio.wait_for(anext(other), 2) == b"{}"
    finally:
        await other.aclose()
        await stalled.aclose()


async def test_read_only_app_exposes_only_safe_methods(read_only: FastAPI) -> None:
    """Enumerate every route, including those of included routers and the OpenAPI schema."""
    methods: dict[str, set[str]] = {}
    for path, operations in read_only.openapi()["paths"].items():
        methods.setdefault(path, set()).update(m.upper() for m in operations)

    def walk(routes: list[Any]) -> None:
        for route in routes:
            if isinstance(route, Mount):
                methods.setdefault(f"{route.path}/dashboard.css", set()).add("GET")  # static files
            elif hasattr(route, "methods") and hasattr(route, "path"):
                methods.setdefault(route.path, set()).update(route.methods or ())
            if hasattr(route, "routes") and not isinstance(route, Mount):
                walk(list(route.routes))

    walk(list(read_only.routes))
    unsafe = {path: m - {"GET", "HEAD"} for path, m in methods.items() if m - {"GET", "HEAD"}}
    assert unsafe == {}
    assert "/api/jobs/{job_id}/requeue" not in methods
    assert "/api/queues/{queue}/requeue" not in methods

    client = Client(read_only)
    for path in sorted(methods):
        concrete = path.replace("{job_id}", "1").replace("{queue}", "default")
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            response = await client.request(method, concrete, headers=ACTION)
            assert response.status in (404, 405), (method, concrete, response.status)
