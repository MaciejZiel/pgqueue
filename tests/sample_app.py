"""A tiny application used by the CLI tests (imported by a worker subprocess)."""

from __future__ import annotations

import asyncio
import os

from pydantic import BaseModel

from pgqueue import JobContext, PgQueue

app = PgQueue(
    os.environ.get("PGQUEUE_TEST_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue_test")
)


class Sleep(BaseModel):
    seconds: float


@app.task(name="sample.sleep")
async def sleep(payload: Sleep) -> float:
    await asyncio.sleep(payload.seconds)
    return payload.seconds


@app.task(name="stress.record")
async def record(ctx: JobContext) -> None:
    """Used by the multi-process stress test: log which process ran which job."""
    await app.pool.execute(
        "INSERT INTO stress_executions VALUES ($1, $2, $3, true)",
        ctx.job_id,
        ctx.attempt,
        str(os.getpid()),
    )


@app.task(name="stress.slow_record", max_attempts=20)
async def slow_record(payload: Sleep, ctx: JobContext) -> None:
    """Used by the SIGKILL test: log the start of every attempt, then take a while."""
    await app.pool.execute(
        "INSERT INTO stress_executions VALUES ($1, $2, $3, false)",
        ctx.job_id,
        ctx.attempt,
        str(os.getpid()),
    )
    await asyncio.sleep(payload.seconds)
