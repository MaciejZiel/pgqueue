"""Benchmark tasks: a no-op handler, so we measure the queue, not the work."""

from __future__ import annotations

import os

from pydantic import BaseModel

from pgqueue import PgQueue

app = PgQueue(
    os.environ.get("PGQUEUE_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue_bench"),
    pool_max_size=int(os.environ.get("BENCH_POOL_SIZE", "10")),
)


class Noop(BaseModel):
    n: int


@app.task(name="bench.noop", queue="bench")
async def noop(payload: Noop) -> None:
    return None
