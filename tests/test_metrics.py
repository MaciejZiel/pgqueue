from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from demo.config import Settings
from demo.server import build_app
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from prometheus_client.parser import text_string_to_metric_families
from pydantic import BaseModel

from pgqueue import JobStatus, PermanentError, PgQueue
from pgqueue.admin import create_app
from pgqueue.prometheus import WorkerMetrics
from pgqueue.worker import Worker

from .asgi import Client
from .helpers import all_in, running, wait_until


class Item(BaseModel):
    n: int


@pytest_asyncio.fixture
async def admin(app: PgQueue) -> AsyncIterator[FastAPI]:
    admin_app = create_app(app.dsn)
    yield admin_app
    await admin_app.state.engine.dispose()


def _samples(text: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for family in text_string_to_metric_families(text)
        for s in family.samples
    }


async def test_metrics_endpoint_reports_queue_state(app: PgQueue, admin: FastAPI) -> None:
    await app.enqueue("t", {}, queue="emails")
    await app.enqueue("t", {}, queue="emails")
    await app.enqueue("t", {}, queue="later", delay=3600)
    [job] = await app.store.claim(["emails"], limit=1, worker_id="w", lease_seconds=30)
    await app.store.complete(job, "w")
    await app.store.register_worker("w", queues=["emails"], concurrency=3, lease_seconds=30)

    response = await Client(admin).get("/metrics")

    assert response.status == 200
    assert response.headers["content-type"].startswith("text/plain")
    samples = _samples(response.text)
    assert samples[("pgqueue_jobs", (("queue", "emails"), ("status", "queued")))] == 1
    assert samples[("pgqueue_jobs", (("queue", "emails"), ("status", "succeeded")))] == 1
    assert samples[("pgqueue_jobs", (("queue", "later"), ("status", "queued")))] == 1
    assert samples[("pgqueue_queue_lag_seconds", (("queue", "emails"),))] >= 0
    assert ("pgqueue_queue_lag_seconds", (("queue", "later"),)) not in samples  # not due
    assert (
        samples[("pgqueue_jobs_finished_last_5m", (("queue", "emails"), ("status", "succeeded")))]
        == 1
    )
    assert samples[("pgqueue_worker_up", (("worker", "w"),))] == 1
    assert samples[("pgqueue_worker_processed_total", (("worker", "w"),))] == 0


async def test_worker_metrics_record_outcomes_and_durations(app: PgQueue) -> None:
    @app.task(name="ok")
    async def ok(payload: Item) -> None: ...

    @app.task(name="bad")
    async def bad(payload: Item) -> None:
        raise PermanentError("no")

    await ok.enqueue_many([Item(n=1), Item(n=2)])
    await bad.enqueue(Item(n=3))

    worker = Worker(app, poll_interval=0.05)
    metrics = WorkerMetrics(worker, registry=CollectorRegistry())
    async with running(worker):
        await wait_until(all_in(app, JobStatus.SUCCEEDED.value, 2))
        await wait_until(all_in(app, JobStatus.FAILED.value, 1))

    def value(name: str, **labels: str) -> float | None:
        return metrics.registry.get_sample_value(name, labels)

    assert value("pgqueue_worker_attempts_total", task="ok", outcome="succeeded") == 2
    assert value("pgqueue_worker_attempts_total", task="bad", outcome="failed") == 1
    assert (
        value("pgqueue_worker_attempt_duration_seconds_count", task="ok", outcome="succeeded") == 2
    )
    assert value("pgqueue_worker_concurrency") == worker.concurrency


async def test_public_mode_serves_no_metrics_and_no_api_docs(clean_db: str) -> None:
    app = create_app(clean_db, public=True)
    client = Client(app)
    async with app.router.lifespan_context(app):
        statuses = {
            path: (await client.get(path)).status
            for path in ("/metrics", "/docs", "/redoc", "/openapi.json")
        }
        dashboard = await client.get("/")
        live = await client.get("/api/live")

    assert statuses == {"/metrics": 404, "/docs": 404, "/redoc": 404, "/openapi.json": 404}
    assert dashboard.status == 200
    assert live.status == 200


async def test_public_mode_can_come_from_the_environment(
    clean_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PGQUEUE_ADMIN_PUBLIC", "1")
    app = create_app(clean_db)
    async with app.router.lifespan_context(app):
        assert (await Client(app).get("/metrics")).status == 404


async def test_the_demo_does_not_expose_metrics_or_docs(clean_db: str) -> None:
    app = build_app(Settings(dsn=clean_db), background=False)
    client = Client(app)
    async with app.router.lifespan_context(app):
        for path in ("/metrics", "/docs", "/redoc", "/openapi.json"):
            assert (await client.get(path)).status == 404, path
