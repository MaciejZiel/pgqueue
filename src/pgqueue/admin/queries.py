"""Read and repair queries for the admin API, written with SQLAlchemy Core.

Unlike the worker hot path these queries are many, varied and composed from
optional filters, which is exactly what a query builder is good at.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, and_, case, func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from pgqueue.admin.schemas import (
    JobOut,
    JobPage,
    Overview,
    PeriodicOut,
    QueueStats,
    ThroughputPoint,
    WorkerOut,
)
from pgqueue.schema import READY_STATUSES, JobStatus, jobs, periodic, workers

REQUEUEABLE = (JobStatus.FAILED, JobStatus.DEAD, JobStatus.RETRYING)
FAILURE_STATUSES = (JobStatus.FAILED, JobStatus.DEAD)


async def overview(conn: AsyncConnection, *, minutes: int = 60) -> Overview:
    now: datetime = (await conn.execute(select(func.now()))).scalar_one()

    counts: dict[str, dict[JobStatus, int]] = {}
    for queue, status, n in await conn.execute(
        select(jobs.c.queue, jobs.c.status, func.count()).group_by(jobs.c.queue, jobs.c.status)
    ):
        counts.setdefault(queue, dict.fromkeys(JobStatus, 0))[JobStatus(status)] = n

    lag: dict[str, float] = {}
    for queue, oldest in await conn.execute(
        select(jobs.c.queue, func.min(jobs.c.run_at))
        .where(jobs.c.status.in_(READY_STATUSES), jobs.c.run_at <= func.now())
        .group_by(jobs.c.queue)
    ):
        lag[queue] = max(0.0, (now - oldest).total_seconds())

    queues = [
        QueueStats(queue=q, counts=c, lag_seconds=lag.get(q, 0.0))
        for q, c in sorted(counts.items())
    ]
    totals = {s: sum(q.counts[s] for q in queues) for s in JobStatus}
    return Overview(
        now=now,
        totals=totals,
        queues=queues,
        throughput=await throughput(conn, now=now, minutes=minutes),
        workers=await list_workers(conn),
        periodic=await list_periodic(conn),
    )


async def throughput(
    conn: AsyncConnection, *, now: datetime, minutes: int
) -> list[ThroughputPoint]:
    """Finished jobs per minute for the last ``minutes`` minutes (gaps filled with 0)."""
    minute = func.date_trunc("minute", jobs.c.finished_at).label("minute")
    rows = await conn.execute(
        select(
            minute,
            func.count().filter(jobs.c.status == JobStatus.SUCCEEDED),
            func.count().filter(jobs.c.status.in_(FAILURE_STATUSES)),
        )
        .where(
            jobs.c.status.in_((JobStatus.SUCCEEDED, *FAILURE_STATUSES)),
            jobs.c.finished_at >= func.now() - timedelta(minutes=minutes),
        )
        .group_by(minute)
    )
    buckets = {row[0]: (row[1], row[2]) for row in rows}
    current = now.replace(second=0, microsecond=0)
    points = []
    for i in range(minutes - 1, -1, -1):
        at = current - timedelta(minutes=i)
        succeeded, failed = buckets.get(at, (0, 0))
        points.append(ThroughputPoint(minute=at, succeeded=succeeded, failed=failed))
    return points


async def list_workers(conn: AsyncConnection) -> list[WorkerOut]:
    alive = workers.c.last_seen_at >= func.now() - func.make_interval(
        0, 0, 0, 0, 0, 0, workers.c.lease_seconds
    )
    rows = await conn.execute(select(workers, alive.label("alive")).order_by(workers.c.started_at))
    return [WorkerOut.model_validate(row._mapping) for row in rows]


async def list_periodic(conn: AsyncConnection) -> list[PeriodicOut]:
    rows = await conn.execute(select(periodic).order_by(periodic.c.next_run_at))
    return [PeriodicOut.model_validate(row._mapping) for row in rows]


async def list_jobs(
    conn: AsyncConnection,
    *,
    statuses: Sequence[JobStatus] = (),
    queue: str | None = None,
    task: str | None = None,
    before_id: int | None = None,
    limit: int = 50,
) -> JobPage:
    """Newest first, keyset-paginated on id (stable while jobs are inserted)."""
    conditions: list[ColumnElement[bool]] = []
    if statuses:
        conditions.append(jobs.c.status.in_(statuses))
    if queue:
        conditions.append(jobs.c.queue == queue)
    if task:
        conditions.append(jobs.c.task == task)
    if before_id is not None:
        conditions.append(jobs.c.id < before_id)
    rows = (
        await conn.execute(
            select(jobs).where(and_(True, *conditions)).order_by(jobs.c.id.desc()).limit(limit + 1)
        )
    ).all()
    page = [JobOut.model_validate(row._mapping) for row in rows[:limit]]
    return JobPage(jobs=page, next_before_id=page[-1].id if len(rows) > limit else None)


async def get_job(conn: AsyncConnection, job_id: int) -> JobOut | None:
    row = (await conn.execute(select(jobs).where(jobs.c.id == job_id))).first()
    return JobOut.model_validate(row._mapping) if row is not None else None


def _requeue_values() -> dict[str, object]:
    # A failed or dead job gets a fresh attempt budget; a retrying job just
    # skips the rest of its backoff and keeps its attempt count.
    return {
        "status": JobStatus.QUEUED,
        "attempts": case((jobs.c.status.in_(FAILURE_STATUSES), 0), else_=jobs.c.attempts),
        "run_at": func.now(),
        "finished_at": None,
        "locked_until": None,
    }


async def requeue_job(conn: AsyncConnection, job_id: int) -> JobOut | None:
    """Put a failed, dead or retrying job back in the queue to run now."""
    row = (
        await conn.execute(
            update(jobs)
            .where(jobs.c.id == job_id, jobs.c.status.in_(REQUEUEABLE))
            .values(**_requeue_values())
            .returning(*jobs.c)
        )
    ).first()
    return JobOut.model_validate(row._mapping) if row is not None else None


async def requeue_all(conn: AsyncConnection, *, queue: str, status: JobStatus) -> int:
    """Requeue every job of ``queue`` in ``status`` (e.g. drain the dead-letter state)."""
    if status not in REQUEUEABLE:
        raise ValueError(f"cannot requeue {status.value} jobs")
    result = await conn.execute(
        update(jobs)
        .where(jobs.c.queue == queue, jobs.c.status == status)
        .values(**_requeue_values())
    )
    return result.rowcount
