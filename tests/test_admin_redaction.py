from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI

from pgqueue import JobStatus, PgQueue
from pgqueue.admin import create_app
from pgqueue.admin.redact import public_error, worker_label

from .asgi import Client

TRACEBACK = (
    "Traceback (most recent call last):\n"
    '  File "/data/data/com.termux/files/home/pgqueue/demo/tasks.py", line 12, in charge\n'
    "    raise RuntimeError('boom')\n"
    "RuntimeError: card declined for /data/data/com.termux/files/usr/lib/python3.13/x.py"
)
WORKER = "localhost:4242:abc123"


def test_public_error_keeps_only_type_and_message_without_paths() -> None:
    error = public_error(TRACEBACK)
    assert error == "RuntimeError: card declined for <path>"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("", None),
        ("  \n ", None),
        ("ValueError: ratio 1/2 is odd", "ValueError: ratio 1/2 is odd"),
        ("OSError: C:\\Users\\me\\x.txt missing", "OSError: <path> missing"),
        (
            "lease expired: worker host.name:99:0a1b2c",
            f"lease expired: worker {worker_label('host.name:99:0a1b2c')}",
        ),
    ],
)
def test_public_error_edge_cases(raw: str | None, expected: str | None) -> None:
    assert public_error(raw) == expected


def test_public_error_is_bounded() -> None:
    assert len(public_error("E: " + "x" * 5000) or "") <= 300


def test_worker_label_is_stable_and_hides_the_host() -> None:
    assert worker_label(WORKER) == worker_label(WORKER)
    assert worker_label(WORKER) != worker_label("localhost:4242:abc124")
    assert "localhost" not in worker_label(WORKER)
    assert "4242" not in worker_label(WORKER)


@pytest_asyncio.fixture
async def public(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn, read_only=True)
    yield admin_app
    await admin_app.state.engine.dispose()


@pytest_asyncio.fixture
async def private(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn, read_only=False)
    yield admin_app
    await admin_app.state.engine.dispose()


async def _dead_job_on_worker(app: PgQueue) -> int:
    job_id = await app.enqueue("demo.task", {"n": 1}, max_attempts=1)
    [job] = await app.store.claim(["default"], limit=1, worker_id=WORKER, lease_seconds=30)
    await app.store.register_worker(WORKER, queues=["default"], concurrency=1, lease_seconds=30)
    await app.store.fail(job, WORKER, status=JobStatus.DEAD, error=TRACEBACK)
    return job_id


async def test_public_jobs_api_shows_only_the_last_error_line(
    app: PgQueue, public: FastAPI
) -> None:
    job_id = await _dead_job_on_worker(app)
    client = Client(public)

    for path in ("/api/jobs", f"/api/jobs/{job_id}"):
        body = (await client.get(path)).text
        assert "termux" not in body
        assert "Traceback" not in body
        assert "RuntimeError: card declined for <path>" in body
        assert "localhost" not in body
    assert (await client.get(f"/api/jobs/{job_id}")).json()["locked_by"] == worker_label(WORKER)


async def test_public_live_overview_and_metrics_hide_host_details(
    app: PgQueue, public: FastAPI
) -> None:
    await _dead_job_on_worker(app)
    client = Client(public)

    for path in ("/api/live", "/api/overview", "/metrics"):
        body = (await client.get(path)).text
        assert "termux" not in body, path
        assert "localhost" not in body, path
        assert "4242" not in body, path
        assert worker_label(WORKER) in body or path != "/api/overview"
    [worker] = (await client.get("/api/live")).json()["workers"]
    assert worker["id"] == worker_label(WORKER)
    assert worker["hostname"] is None
    assert worker["pid"] is None


async def test_the_full_detail_stays_in_the_database_and_the_admin_api(
    app: PgQueue, private: FastAPI
) -> None:
    job_id = await _dead_job_on_worker(app)
    client = Client(private)

    job = (await client.get(f"/api/jobs/{job_id}")).json()
    assert job["last_error"] == TRACEBACK
    assert job["locked_by"] == WORKER
    [worker] = (await client.get("/api/live")).json()["workers"]
    assert worker["id"] == WORKER
    assert worker["hostname"]
    assert isinstance(worker["pid"], int)
