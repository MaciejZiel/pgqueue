"""The demo's tasks: realistic durations and realistic failures.

Durations are simulated with sleeps; failures are drawn from ``rng`` so tests can
seed it. Run a worker for them with::

    pgqueue worker demo.tasks:app --queues emails,images,billing,webhooks,reports
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from pydantic import BaseModel, Field

from pgqueue import JobContext, PermanentError, PgQueue, Retry, RetryPolicy

QUEUES = ("billing", "emails", "images", "webhooks", "reports")

app = PgQueue(os.environ.get("PGQUEUE_DSN", "postgresql://pgqueue:pgqueue@localhost:56432/pgqueue"))

rng = random.Random()


class WelcomeEmail(BaseModel):
    user_id: int
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+$")


class ResizeImage(BaseModel):
    image_id: int
    width: int = Field(gt=0, le=4096)


class Charge(BaseModel):
    invoice_id: int
    amount_cents: int = Field(gt=0)


class Webhook(BaseModel):
    url: str
    event: str


class Export(BaseModel):
    report: str
    rows: int


@app.task(name="emails.send_welcome", queue="emails", max_attempts=5)
async def send_welcome(payload: WelcomeEmail) -> dict[str, str]:
    await asyncio.sleep(rng.uniform(0.08, 0.4))
    if rng.random() < 0.05:
        raise ConnectionError("SMTP server closed the connection")
    return {"message_id": f"<{payload.user_id}.{time.time_ns()}@demo>"}


@app.task(name="images.resize", queue="images", timeout=3, max_attempts=3)
def resize_image(payload: ResizeImage) -> dict[str, int]:
    # A sync handler: it runs on the worker's thread pool, not on the event loop.
    time.sleep(rng.uniform(0.3, 1.5))
    return {"image_id": payload.image_id, "width": payload.width}


@app.task(name="billing.charge", queue="billing", priority=10, max_attempts=4)
async def charge(payload: Charge, ctx: JobContext) -> dict[str, object]:
    await asyncio.sleep(rng.uniform(0.1, 0.6))
    roll = rng.random()
    if roll < 0.03:
        raise PermanentError(f"card declined for invoice {payload.invoice_id}")
    if roll < 0.11:
        # The payment provider asked us to back off: honour its Retry-After.
        raise Retry("429 Too Many Requests", delay=2)
    # ctx.job_id is a stable idempotency key towards the provider.
    return {"charge_id": f"ch_{ctx.job_id}", "amount_cents": payload.amount_cents}


@app.task(
    name="webhooks.deliver",
    queue="webhooks",
    max_attempts=3,
    retry=RetryPolicy(base=2, factor=2, max_delay=20),
)
async def deliver_webhook(payload: Webhook) -> int:
    await asyncio.sleep(rng.uniform(0.05, 0.8))
    if "flaky" in payload.url and rng.random() < 0.6:
        raise TimeoutError(f"POST {payload.url} timed out after 10s")
    return 204


@app.task(name="reports.export", queue="reports", max_attempts=2)
async def export_report(payload: Export) -> dict[str, object]:
    # Slow enough that a crashed worker is usually in the middle of one.
    await asyncio.sleep(rng.uniform(2.0, 6.0))
    return {"report": payload.report, "rows": payload.rows}


@app.periodic("* * * * *", name="reports.minutely")
@app.task(name="reports.rollup", queue="reports")
async def rollup(ctx: JobContext) -> str:
    await asyncio.sleep(rng.uniform(0.5, 1.5))
    return f"rolled up stats at attempt {ctx.attempt}"


@app.periodic("*/5 * * * *", name="reports.digest")
@app.task(name="reports.digest", queue="reports", max_attempts=3)
async def digest() -> str:
    await asyncio.sleep(rng.uniform(1.0, 3.0))
    if rng.random() < 0.2:
        raise ConnectionError("mail relay unavailable")
    return "digest sent"
