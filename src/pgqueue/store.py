"""The data-access layer for the worker hot path.

Everything here is a single SQL statement on an asyncpg pool. Statements are
hand-written (rather than built with SQLAlchemy) because they are few, they
are the performance-critical part of the system and their exact shape --
``FOR UPDATE SKIP LOCKED``, the fencing ``WHERE`` clauses -- *is* the
correctness argument, so it should be readable as SQL.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

import asyncpg
from asyncpg import Pool as JobPool

from pgqueue.models import Job
from pgqueue.schema import JobStatus

__all__ = ["JobPool", "JobStore", "create_pool", "init_connection"]


async def init_connection(conn: asyncpg.Connection) -> None:
    """Decode json/jsonb to Python objects on every pooled connection."""
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename, schema="pg_catalog", encoder=json.dumps, decoder=json.loads
        )


async def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> JobPool:
    pool: JobPool = await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, init=init_connection
    )
    return pool


_INSERT = """
INSERT INTO pgqueue_jobs (queue, task, payload, priority, run_at, max_attempts, idempotency_key)
VALUES ($1, $2, $3, $4,
        COALESCE($5::timestamptz, now() + make_interval(secs => $8::float8)), $6, $7)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
"""

_INSERT_MANY = """
INSERT INTO pgqueue_jobs (queue, task, payload, priority, run_at, max_attempts, idempotency_key)
SELECT $1, $2, t.payload, $3,
       COALESCE($4::timestamptz, now() + make_interval(secs => $8::float8)), $5, t.key
FROM unnest($6::jsonb[], $7::text[]) AS t(payload, key)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
"""

# The heart of the queue. The CTE locks up to $2 ready rows; SKIP LOCKED makes
# concurrent claimers pass over rows another transaction already holds instead
# of blocking on them, so N workers never receive the same row and never wait
# for each other. The outer UPDATE flips the rows to 'running' and stamps the
# lease in the same statement, so a claim is a single round trip.
_CLAIM = """
WITH next AS (
    SELECT id
    FROM pgqueue_jobs
    WHERE status IN ('queued', 'retrying')
      AND queue = ANY($1::text[])
      AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgqueue_jobs AS j
SET status = 'running',
    attempts = j.attempts + 1,
    locked_by = $3,
    locked_until = now() + make_interval(secs => $4),
    heartbeat_at = now(),
    started_at = now()
FROM next
WHERE j.id = next.id
RETURNING j.*
"""

# Fencing: a completion is only accepted from the worker that holds the
# *current* lease for the *current* attempt. A worker whose lease expired and
# whose job was re-claimed elsewhere matches zero rows and must drop its result.
_COMPLETE = """
UPDATE pgqueue_jobs
SET status = 'succeeded',
    result = $4,
    locked_until = NULL,
    finished_at = now()
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
"""

# Same fencing as _COMPLETE. 'retrying' rows get a future run_at (the backoff)
# and become claimable again once it passes; 'failed' and 'dead' are terminal.
_FAIL = """
UPDATE pgqueue_jobs
SET status = $4::pgqueue_job_status,
    run_at = CASE WHEN $4::pgqueue_job_status = 'retrying'
                  THEN now() + make_interval(secs => $5::float8) ELSE run_at END,
    last_error = $6::text,
    locked_until = NULL,
    finished_at = CASE WHEN $4::pgqueue_job_status = 'retrying' THEN NULL ELSE now() END
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
"""

# Graceful shutdown hands an unfinished job back without burning an attempt:
# the handler did not fail, the process is just going away.
_RELEASE = """
UPDATE pgqueue_jobs
SET status = 'queued',
    attempts = attempts - 1,
    run_at = now(),
    locked_until = NULL,
    last_error = 'released: worker ' || $2 || ' shut down before the job finished'
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
"""


class JobStore:
    """Thin, stateless wrapper around the queue's SQL statements."""

    def __init__(self, pool: JobPool) -> None:
        self.pool = pool

    async def insert(
        self,
        *,
        queue: str,
        task: str,
        payload: dict[str, Any],
        priority: int,
        run_at: datetime | None,
        max_attempts: int,
        idempotency_key: str | None,
        delay: float = 0.0,
    ) -> tuple[int, bool]:
        """Insert one job; return ``(id, created)``.

        With an idempotency key that already exists, nothing is inserted and
        the id of the existing job is returned with ``created=False``.
        ``run_at`` (absolute) wins over ``delay`` (seconds from the database's
        clock, which avoids trusting the producer's clock).
        """
        async with self.pool.acquire() as conn:
            job_id: int | None = await conn.fetchval(
                _INSERT,
                queue,
                task,
                payload,
                priority,
                run_at,
                max_attempts,
                idempotency_key,
                delay,
            )
            if job_id is not None:
                return job_id, True
            existing: int | None = await conn.fetchval(
                "SELECT id FROM pgqueue_jobs WHERE idempotency_key = $1", idempotency_key
            )
            if existing is None:  # pragma: no cover - conflicting row deleted in between
                return await self.insert(
                    queue=queue,
                    task=task,
                    payload=payload,
                    priority=priority,
                    run_at=run_at,
                    max_attempts=max_attempts,
                    idempotency_key=idempotency_key,
                    delay=delay,
                )
            return existing, False

    async def insert_many(
        self,
        *,
        queue: str,
        task: str,
        payloads: Sequence[dict[str, Any]],
        idempotency_keys: Sequence[str | None] | None,
        priority: int,
        run_at: datetime | None,
        max_attempts: int,
        delay: float = 0.0,
    ) -> list[int]:
        """Insert many jobs in one statement; return the ids that were created."""
        keys = list(idempotency_keys) if idempotency_keys is not None else [None] * len(payloads)
        if len(keys) != len(payloads):
            raise ValueError("idempotency_keys must be as long as payloads")
        rows = await self.pool.fetch(
            _INSERT_MANY,
            queue,
            task,
            priority,
            run_at,
            max_attempts,
            list(payloads),
            keys,
            delay,
        )
        return [row["id"] for row in rows]

    async def claim(
        self, queues: Sequence[str], *, limit: int, worker_id: str, lease_seconds: float
    ) -> list[Job]:
        """Atomically take up to ``limit`` ready jobs and lease them to ``worker_id``."""
        if limit <= 0:
            return []
        rows = await self.pool.fetch(_CLAIM, list(queues), limit, worker_id, lease_seconds)
        jobs = [Job.from_record(row) for row in rows]
        # UPDATE ... RETURNING does not preserve the CTE's order.
        jobs.sort(key=lambda j: (-j.priority, j.run_at, j.id))
        return jobs

    async def complete(self, job: Job, worker_id: str, result: Any = None) -> bool:
        """Mark a claimed job as succeeded. ``False`` means the lease was lost."""
        status = await self.pool.execute(_COMPLETE, job.id, worker_id, job.attempts, result)
        return bool(status == "UPDATE 1")

    async def fail(
        self,
        job: Job,
        worker_id: str,
        *,
        status: JobStatus,
        error: str,
        retry_in: float = 0.0,
    ) -> bool:
        """Record a failed attempt. ``False`` means the lease was lost."""
        if status not in (JobStatus.RETRYING, JobStatus.FAILED, JobStatus.DEAD):
            raise ValueError(f"not a failure status: {status}")
        result = await self.pool.execute(
            _FAIL, job.id, worker_id, job.attempts, status.value, retry_in, error
        )
        return bool(result == "UPDATE 1")

    async def release(self, job: Job, worker_id: str) -> bool:
        """Put a claimed job back in the queue without counting the attempt."""
        result = await self.pool.execute(_RELEASE, job.id, worker_id, job.attempts)
        return bool(result == "UPDATE 1")

    async def get(self, job_id: int) -> Job | None:
        row = await self.pool.fetchrow("SELECT * FROM pgqueue_jobs WHERE id = $1", job_id)
        return Job.from_record(row) if row is not None else None
