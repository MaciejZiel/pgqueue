"""Keeps the demo database bounded: old finished jobs and dead workers are deleted."""

from __future__ import annotations

import asyncio
import logging

from pgqueue.store import JobStore

log = logging.getLogger("demo.retention")

TERMINAL = "('succeeded', 'failed', 'dead')"

# Delete the oldest finished rows beyond a fixed count, whatever their age: a burst can
# never make the table outgrow the cap between two runs by more than one burst. Rows are
# locked (skipping any held elsewhere) and their status checked again on deletion, so a
# job that left the finished state meanwhile (requeued, say) is never deleted.
_CAP = f"""
DELETE FROM pgqueue_jobs
WHERE id IN (
    SELECT id FROM pgqueue_jobs
    WHERE status IN {TERMINAL}
    ORDER BY id
    LIMIT greatest(
        0,
        (SELECT count(*) FROM pgqueue_jobs WHERE status IN {TERMINAL}) - $1::int
    )
    FOR UPDATE SKIP LOCKED
)
AND status IN {TERMINAL}
"""


async def sweep(store: JobStore, *, max_age: float, max_rows: int) -> tuple[int, int, int]:
    """One cleanup pass; returns ``(aged out, over the cap, workers forgotten)``."""
    aged = await store.prune(older_than_seconds=max_age)
    over = int((await store.pool.execute(_CAP, max_rows)).split()[-1])
    # A crashed worker stays visible as "lost" for a moment, then its row goes away.
    workers = await store.prune_workers(older_than_seconds=30)
    return aged, over, workers


async def run(store: JobStore, *, max_age: float, max_rows: int, interval: float) -> None:
    while True:
        try:
            aged, over, workers = await sweep(store, max_age=max_age, max_rows=max_rows)
            if aged or over or workers:
                log.info("retention: %d aged out, %d over the cap, %d workers", aged, over, workers)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("retention sweep failed; continuing")
        await asyncio.sleep(interval)
