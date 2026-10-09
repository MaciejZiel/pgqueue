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
VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6, $7)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
"""

_INSERT_MANY = """
INSERT INTO pgqueue_jobs (queue, task, payload, priority, run_at, max_attempts, idempotency_key)
SELECT $1, $2, t.payload, $3, COALESCE($4, now()), $5, t.key
FROM unnest($6::jsonb[], $7::text[]) AS t(payload, key)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
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
    ) -> tuple[int, bool]:
        """Insert one job; return ``(id, created)``.

        With an idempotency key that already exists, nothing is inserted and
        the id of the existing job is returned with ``created=False``.
        """
        async with self.pool.acquire() as conn:
            job_id: int | None = await conn.fetchval(
                _INSERT, queue, task, payload, priority, run_at, max_attempts, idempotency_key
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
    ) -> list[int]:
        """Insert many jobs in one statement; return the ids that were created."""
        keys = list(idempotency_keys) if idempotency_keys is not None else [None] * len(payloads)
        if len(keys) != len(payloads):
            raise ValueError("idempotency_keys must be as long as payloads")
        rows = await self.pool.fetch(
            _INSERT_MANY, queue, task, priority, run_at, max_attempts, list(payloads), keys
        )
        return [row["id"] for row in rows]

    async def get(self, job_id: int) -> Job | None:
        row = await self.pool.fetchrow("SELECT * FROM pgqueue_jobs WHERE id = $1", job_id)
        return Job.from_record(row) if row is not None else None
