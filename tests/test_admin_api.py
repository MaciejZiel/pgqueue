from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from fastapi import FastAPI
from pydantic import BaseModel

from pgqueue import JobStatus, PgQueue
from pgqueue.admin import create_app

from .asgi import Client

ACTION = {"X-PgQueue-Action": "1"}


class Item(BaseModel):
    n: int


@pytest_asyncio.fixture
async def admin(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn)
    yield admin_app
    await admin_app.state.engine.dispose()


@pytest_asyncio.fixture
async def client(admin: FastAPI) -> Client:
    return Client(admin)


async def _finish(app: PgQueue, status: JobStatus, *, queue: str = "default") -> int:
    """Create a job and drive it to ``status`` through the real store API."""
    job_id = await app.enqueue("demo.task", {"n": 1}, queue=queue, max_attempts=1)
    if status is JobStatus.QUEUED:
        return job_id
    [job] = await app.store.claim([queue], limit=1, worker_id="w", lease_seconds=30)
    if status is JobStatus.SUCCEEDED:
        await app.store.complete(job, "w", {"ok": True})
    elif status in (JobStatus.FAILED, JobStatus.DEAD, JobStatus.RETRYING):
        await app.store.fail(job, "w", status=status, error="Traceback...\nValueError: x")
    return job_id


async def test_overview_counts_jobs_per_queue_and_status(app: PgQueue, client: Client) -> None:
    await _finish(app, JobStatus.SUCCEEDED, queue="emails")
    await _finish(app, JobStatus.SUCCEEDED, queue="emails")
    await _finish(app, JobStatus.DEAD, queue="emails")
    await _finish(app, JobStatus.QUEUED, queue="reports")
    await app.store.register_worker("w1", queues=["emails"], concurrency=4, lease_seconds=30)

    response = await client.get("/api/overview", {"minutes": 5})

    assert response.status == 200
    data = response.json()
    queues = {q["queue"]: q for q in data["queues"]}
    assert queues["emails"]["counts"]["succeeded"] == 2
    assert queues["emails"]["counts"]["dead"] == 1
    assert queues["reports"]["counts"]["queued"] == 1
    assert queues["reports"]["lag_seconds"] >= 0
    assert data["totals"]["succeeded"] == 2
    assert len(data["throughput"]) == 5
    assert data["throughput"][-1] == {
        "minute": data["throughput"][-1]["minute"],
        "succeeded": 2,
        "failed": 1,
    }
    [worker] = data["workers"]
    assert (worker["id"], worker["alive"], worker["concurrency"]) == ("w1", True, 4)


async def test_list_jobs_filters_and_paginates(app: PgQueue, client: Client) -> None:
    ids = [await _finish(app, JobStatus.DEAD) for _ in range(5)]
    await _finish(app, JobStatus.SUCCEEDED)

    first = (await client.get("/api/jobs", {"status": "dead,failed", "limit": 3})).json()
    assert [j["id"] for j in first["jobs"]] == ids[::-1][:3]
    second = (
        await client.get(
            "/api/jobs", {"status": "dead", "limit": 3, "before_id": first["next_before_id"]}
        )
    ).json()
    assert [j["id"] for j in second["jobs"]] == ids[::-1][3:]
    assert second["next_before_id"] is None

    assert (await client.get("/api/jobs", {"status": "bogus"})).status == 422


async def test_get_job(app: PgQueue, client: Client) -> None:
    job_id = await _finish(app, JobStatus.FAILED)

    response = await client.get(f"/api/jobs/{job_id}")
    assert response.status == 200
    assert response.json()["last_error"].endswith("ValueError: x")
    assert (await client.get("/api/jobs/999999")).status == 404


async def test_requeue_dead_job_resets_attempts(app: PgQueue, client: Client) -> None:
    job_id = await _finish(app, JobStatus.DEAD)

    response = await client.post(f"/api/jobs/{job_id}/requeue", headers=ACTION)

    assert response.status == 200
    assert response.json()["status"] == "queued"
    job = await app.get_job(job_id)
    assert job is not None
    assert (job.status, job.attempts, job.finished_at) == (JobStatus.QUEUED, 0, None)
    assert job.last_error is not None  # history is kept


async def test_requeue_rejects_jobs_in_other_states(app: PgQueue, client: Client) -> None:
    done = await _finish(app, JobStatus.SUCCEEDED)

    assert (await client.post(f"/api/jobs/{done}/requeue", headers=ACTION)).status == 409
    assert (await client.post("/api/jobs/424242/requeue", headers=ACTION)).status == 404


async def test_state_changes_require_the_action_header(app: PgQueue, client: Client) -> None:
    job_id = await _finish(app, JobStatus.DEAD)
    assert (await client.post(f"/api/jobs/{job_id}/requeue")).status == 403


async def test_bulk_requeue_of_a_queue(app: PgQueue, client: Client) -> None:
    for _ in range(3):
        await _finish(app, JobStatus.DEAD, queue="emails")
    await _finish(app, JobStatus.DEAD, queue="other")

    response = await client.post("/api/queues/emails/requeue", {"status": "dead"}, headers=ACTION)
    assert response.json() == {"requeued": 3}
    assert (
        await client.post("/api/queues/emails/requeue", {"status": "succeeded"}, headers=ACTION)
    ).status == 409


async def test_basic_auth(app: PgQueue) -> None:
    admin_app = create_app(app.dsn, username="admin", password="s3cret")
    try:
        anonymous = Client(admin_app)
        assert (await anonymous.get("/api/overview")).status == 401
        assert (await anonymous.get("/healthz")).status == 200
        wrong = anonymous.with_basic_auth("admin", "nope")
        assert (await wrong.get("/api/overview")).status == 401
        right = anonymous.with_basic_auth("admin", "s3cret")
        assert (await right.get("/api/overview")).status == 200
    finally:
        await admin_app.state.engine.dispose()
