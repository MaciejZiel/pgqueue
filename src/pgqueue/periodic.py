"""Cron-like periodic jobs, fired exactly once per tick across all workers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pgqueue.cron import CronExpression

if TYPE_CHECKING:
    from pgqueue.app import PgQueue, Task

log = logging.getLogger("pgqueue.periodic")


@dataclass(frozen=True, slots=True)
class PeriodicSchedule:
    name: str
    cron: CronExpression
    task: Task[Any]
    payload: dict[str, Any] = field(default_factory=dict)
    queue: str | None = None

    @property
    def target_queue(self) -> str:
        return self.queue or self.task.options.queue


_UPSERT = """
INSERT INTO pgqueue_periodic (name, cron, task, queue, payload, next_run_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (name) DO UPDATE
SET task = EXCLUDED.task,
    queue = EXCLUDED.queue,
    payload = EXCLUDED.payload,
    -- Keep the pending tick unless the schedule itself changed.
    next_run_at = CASE WHEN pgqueue_periodic.cron = EXCLUDED.cron
                       THEN pgqueue_periodic.next_run_at ELSE EXCLUDED.next_run_at END,
    cron = EXCLUDED.cron,
    updated_at = now()
"""

# Locks the due rows. A second worker ticking at the same moment skips them,
# and by the time it could lock them next_run_at has moved into the future.
_DUE = """
SELECT name, next_run_at, now() AS db_now
FROM pgqueue_periodic
WHERE name = ANY($1::text[]) AND next_run_at <= now()
FOR UPDATE SKIP LOCKED
"""

_ADVANCE = """
UPDATE pgqueue_periodic
SET next_run_at = $2, last_run_at = $3, last_job_id = $4, updated_at = now()
WHERE name = $1
"""


class PeriodicScheduler:
    """Turns due schedules into ordinary jobs. Every worker runs one.

    Each firing happens in one transaction that (1) locks the schedule row,
    (2) inserts the job with the idempotency key ``periodic:<name>:<tick>``
    and (3) advances ``next_run_at``. The row lock prevents double firing in
    the normal case; the idempotency key makes it impossible even if a
    transaction is retried. If every worker was down for a while, missed
    ticks are *not* replayed: the schedule fires once and moves on.
    """

    def __init__(self, app: PgQueue) -> None:
        self.app = app

    @property
    def schedules(self) -> dict[str, PeriodicSchedule]:
        return self.app.schedules

    async def sync(self) -> None:
        """Write this process's schedule definitions to the database."""
        if not self.schedules:
            return
        async with self.app.pool.acquire() as conn, conn.transaction():
            now: datetime = await conn.fetchval("SELECT now()")
            for s in self.schedules.values():
                await conn.execute(
                    _UPSERT,
                    s.name,
                    s.cron.expression,
                    s.task.name,
                    s.target_queue,
                    s.payload,
                    s.cron.next_after(now),
                )

    async def tick(self) -> list[int]:
        """Enqueue a job for every due schedule; return the new job ids."""
        if not self.schedules:
            return []
        fired: list[int] = []
        async with self.app.pool.acquire() as conn, conn.transaction():
            for row in await conn.fetch(_DUE, list(self.schedules)):
                schedule = self.schedules[row["name"]]
                tick: datetime = row["next_run_at"]
                job_id, created = await self.app.store.insert(
                    queue=schedule.target_queue,
                    task=schedule.task.name,
                    payload=schedule.payload,
                    priority=schedule.task.options.priority,
                    run_at=None,
                    max_attempts=schedule.task.options.max_attempts,
                    idempotency_key=f"periodic:{schedule.name}:{tick.isoformat()}",
                    connection=conn,
                )
                upcoming = schedule.cron.next_after(max(tick, row["db_now"]))
                await conn.execute(_ADVANCE, schedule.name, upcoming, tick, job_id)
                if created:
                    fired.append(job_id)
                    log.info("periodic %s fired job %d (next %s)", schedule.name, job_id, upcoming)
        return fired
