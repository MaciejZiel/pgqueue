"""Demo application: a handful of realistic-looking tasks with realistic failures.

Run a worker for it with::

    pgqueue worker examples.demo:app --queues default,emails,images,billing,webhooks
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from pydantic import BaseModel, Field

from pgqueue import JobContext, PermanentError, PgQueue, Retry, RetryPolicy

app = PgQueue(os.environ.get("PGQUEUE_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue"))


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


@app.task(name="emails.send_welcome", queue="emails", max_attempts=5)
async def send_welcome(payload: WelcomeEmail) -> dict[str, str]:
    await asyncio.sleep(random.uniform(0.05, 0.3))
    if random.random() < 0.05:
        raise ConnectionError("SMTP server closed the connection")
    return {"message_id": f"<{payload.user_id}.{time.time_ns()}@demo>"}


@app.task(name="images.resize", queue="images", timeout=5)
def resize_image(payload: ResizeImage) -> dict[str, int]:
    # A sync handler: runs on the worker's thread pool, not the event loop.
    time.sleep(random.uniform(0.1, 0.5))
    return {"image_id": payload.image_id, "width": payload.width}


@app.task(name="billing.charge", queue="billing", priority=10, max_attempts=4)
async def charge(payload: Charge, ctx: JobContext) -> dict[str, object]:
    await asyncio.sleep(random.uniform(0.05, 0.2))
    roll = random.random()
    if roll < 0.03:
        raise PermanentError(f"card declined for invoice {payload.invoice_id}")
    if roll < 0.10:
        # The payment provider asked us to back off: honour its Retry-After.
        raise Retry("429 Too Many Requests", delay=2)
    # ctx.job_id is a stable idempotency key towards the provider.
    return {"charge_id": f"ch_{ctx.job_id}", "amount_cents": payload.amount_cents}


@app.task(
    name="webhooks.deliver",
    queue="webhooks",
    max_attempts=3,
    retry=RetryPolicy(base=1, factor=2, max_delay=10),
)
async def deliver_webhook(payload: Webhook) -> int:
    await asyncio.sleep(random.uniform(0.02, 0.15))
    if "flaky" in payload.url and random.random() < 0.6:
        raise TimeoutError(f"POST {payload.url} timed out after 10s")
    return 204


@app.periodic("* * * * *", name="reports.minutely")
@app.task(name="reports.rollup")
async def rollup(ctx: JobContext) -> str:
    await asyncio.sleep(0.2)
    return f"rolled up stats at attempt {ctx.attempt}"
