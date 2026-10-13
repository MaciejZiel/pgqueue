"""Read and repair queries for the admin API, written with SQLAlchemy Core.

Unlike the worker hot path these queries are many, varied and composed from
optional filters, which is exactly what a query builder is good at.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, and_, case, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from pgqueue.admin.schemas import (
    FeedItem,
    JobOut,
    JobPage,
    LatencyStats,
    LiveStats,
    Overview,
    PeriodicOut,
    QueueStats,
    RunningJob,
    Snapshot,
    ThroughputPoint,
    ThroughputSecond,
    WorkerOut,
)
from pgqueue.schema import (
    READY_STATUSES,
    TERMINAL_STATUSES,
    JobStatus,
    jobs,
    periodic,
    workers,
)

REQUEUEABLE = (JobStatus.FAILED, JobStatus.DEAD, JobStatus.RETRYING)
FAILURE_STATUSES = (JobStatus.FAILED, JobStatus.DEAD)


async def overview(conn: AsyncConnection, *, minutes: int = 60) -> Overview:
    now: datetime = (await conn.execute(select(func.now()))).scalar_one()

    queues, totals = await _queue_stats(conn, now)
    return Overview(
        now=now,
        totals=totals,
        queues=queues,
        throughput=await throughput(conn, now=now, minutes=minutes),
        workers=await list_workers(conn),
        periodic=await list_periodic(conn),
    )


ACTIVE_STATUSES = tuple(s for s in JobStatus if s not in TERMINAL_STATUSES)

Counts = dict[str, dict[JobStatus, int]]


async def _count(conn: AsyncConnection, *conditions: ColumnElement[bool]) -> Counts:
    counts: Counts = {}
    for queue, status, n in await conn.execute(
        select(jobs.c.queue, jobs.c.status, func.count())
        .where(and_(True, *conditions))
        .group_by(jobs.c.queue, jobs.c.status)
    ):
        counts.setdefault(queue, dict.fromkeys(JobStatus, 0))[JobStatus(status)] += n
    return counts


def _merge(*parts: Counts) -> Counts:
    merged: Counts = {}
    for part in parts:
        for queue, by_status in part.items():
            target = merged.setdefault(queue, dict.fromkeys(JobStatus, 0))
            for status, n in by_status.items():
                target[status] += n
    return merged


_COMMIT_SLACK = timedelta(seconds=5)


class FinishedCounts:
    """Counts of finished jobs that do not rescan the whole table every second.

    Finished jobs are almost all of the table, so counting them is a sequential
    scan (about 100 ms for a million rows). The live view counts them in full
    only every ``max_age`` seconds and in between adds the jobs that finished
    since, which the ``(status, finished_at)`` index finds cheaply. Deleted or
    requeued finished jobs show up at the next full count.
    """

    def __init__(self, max_age: float = 60.0) -> None:
        self.max_age = max_age
        self._base: Counts = {}
        self._cutoff: datetime | None = None
        self._counted_at = 0.0

    async def get(self, conn: AsyncConnection) -> Counts:
        if self._cutoff is None or time.monotonic() - self._counted_at >= self.max_age:
            # A few seconds of overlap, so a job whose transaction committed just after the
            # full count (with a finished_at from before it) is still picked up below.
            cutoff = (await conn.execute(select(func.now()))).scalar_one() - _COMMIT_SLACK
            self._base = await _count(
                conn,
                jobs.c.status.in_(TERMINAL_STATUSES),
                or_(jobs.c.finished_at < cutoff, jobs.c.finished_at.is_(None)),
            )
            self._cutoff = cutoff
            self._counted_at = time.monotonic()
        recent = await _count(
            conn, jobs.c.status.in_(TERMINAL_STATUSES), jobs.c.finished_at >= self._cutoff
        )
        return _merge(self._base, recent)


async def _queue_stats(
    conn: AsyncConnection, now: datetime, finished: FinishedCounts | None = None
) -> tuple[list[QueueStats], dict[JobStatus, int]]:
    if finished is None:
        counts = await _count(conn)
    else:
        # Active jobs are few and found through the (status, finished_at) index.
        counts = _merge(
            await _count(conn, jobs.c.status.in_(ACTIVE_STATUSES)), await finished.get(conn)
        )
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
    return queues, {s: sum(q.counts[s] for q in queues) for s in JobStatus}


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


# -- live view -----------------------------------------------------------------------------

_LATENCY = text(
    """
    SELECT
      count(*) AS samples,
      percentile_cont(ARRAY[0.5, 0.95]) WITHIN GROUP (
        ORDER BY greatest(0, extract(epoch FROM started_at - run_at))) AS pickup,
      percentile_cont(ARRAY[0.5, 0.95]) WITHIN GROUP (
        ORDER BY extract(epoch FROM finished_at - started_at))
        FILTER (WHERE status = 'succeeded') AS run,
      count(*) FILTER (WHERE status = 'succeeded') AS run_samples,
      coalesce(sum(greatest(attempts - 1, 0)), 0) AS retries,
      count(*) FILTER (WHERE status IN ('failed', 'dead')) AS failures
    FROM pgqueue_jobs
    WHERE status IN ('succeeded', 'failed', 'dead')
      AND finished_at >= now() - make_interval(secs => :window)
      AND started_at IS NOT NULL
    """
)


async def live_stats(conn: AsyncConnection, *, now: datetime, window: int = 120) -> LiveStats:
    """Per-second throughput and latency percentiles over the last ``window`` seconds.

    Everything is derived from the finished jobs' own timestamps, so it needs no
    extra bookkeeping and is right no matter how many workers exist. The
    percentiles cover the most recent minute; the throughput series the window.
    """
    second = func.date_trunc("second", jobs.c.finished_at).label("second")
    rows = await conn.execute(
        select(
            second,
            func.count().filter(jobs.c.status == JobStatus.SUCCEEDED),
            func.count().filter(jobs.c.status.in_(FAILURE_STATUSES)),
        )
        .where(
            jobs.c.status.in_((JobStatus.SUCCEEDED, *FAILURE_STATUSES)),
            jobs.c.finished_at >= func.now() - timedelta(seconds=window),
        )
        .group_by(second)
    )
    buckets = {row[0]: (row[1], row[2]) for row in rows}
    current = now.replace(microsecond=0)
    series = []
    for i in range(window - 1, -1, -1):
        at = current - timedelta(seconds=i)
        succeeded, failed = buckets.get(at, (0, 0))
        series.append(ThroughputSecond(second=at, succeeded=succeeded, failed=failed))
    # The current second is still filling up: average the ten complete ones before it.
    recent = series[-11:-1]
    per_second = sum(p.succeeded + p.failed for p in recent) / max(1, len(recent))

    row = (await conn.execute(_LATENCY, {"window": 60})).one()
    pickup = row.pickup or [None, None]
    run = row.run or [None, None]
    return LiveStats(
        window_seconds=window,
        jobs_per_second=per_second,
        throughput=series,
        pickup=LatencyStats(p50=pickup[0], p95=pickup[1], samples=row.samples),
        run=LatencyStats(p50=run[0], p95=run[1], samples=row.run_samples),
        retries=int(row.retries),
        failures=row.failures,
    )


async def running_jobs(conn: AsyncConnection, *, limit: int = 40) -> list[RunningJob]:
    """Jobs currently holding a lease, most recently started first."""
    rows = await conn.execute(
        select(
            jobs.c.id,
            jobs.c.task,
            jobs.c.queue,
            jobs.c.attempts,
            jobs.c.max_attempts,
            jobs.c.locked_by,
            jobs.c.started_at,
            jobs.c.heartbeat_at,
            jobs.c.locked_until,
        )
        .where(jobs.c.status == JobStatus.RUNNING)
        .order_by(jobs.c.started_at.desc())
        .limit(limit)
    )
    return [RunningJob.model_validate(row._mapping) for row in rows]


async def recent_activity(
    conn: AsyncConnection, *, limit: int = 25, lookback_ids: int = 400
) -> list[FeedItem]:
    """The most recently changed jobs among the newest ``lookback_ids`` ones.

    Restricting to a window of recent ids keeps this a primary-key range scan
    however large the table is.
    """
    at = func.coalesce(jobs.c.finished_at, jobs.c.started_at, jobs.c.created_at)
    # Fetched first and passed as a value: with a subquery the planner cannot see how
    # narrow the range is and plans a parallel scan of a large part of the table.
    newest = (await conn.execute(select(func.max(jobs.c.id)))).scalar_one()
    if newest is None:
        return []
    rows = await conn.execute(
        select(
            jobs.c.id,
            jobs.c.task,
            jobs.c.queue,
            jobs.c.status,
            jobs.c.attempts,
            jobs.c.max_attempts,
            at.label("at"),
            func.extract("epoch", jobs.c.finished_at - jobs.c.started_at).label("duration"),
            jobs.c.last_error,
        )
        .where(jobs.c.id > newest - lookback_ids)
        .order_by(at.desc(), jobs.c.id.desc())
        .limit(limit)
    )
    items = []
    for row in rows:
        error = row.last_error.strip().splitlines()[-1] if row.last_error else None
        items.append(
            FeedItem(
                id=row.id,
                task=row.task,
                queue=row.queue,
                status=row.status,
                attempts=row.attempts,
                max_attempts=row.max_attempts,
                at=row.at,
                duration=float(row.duration) if row.duration is not None else None,
                error=error,
            )
        )
    return items


async def snapshot(
    conn: AsyncConnection, *, read_only: bool, finished: FinishedCounts | None = None
) -> Snapshot:
    """Everything the live dashboard shows. Pass a long-lived ``finished`` to avoid
    recounting every finished job on each call."""
    now: datetime = (await conn.execute(select(func.now()))).scalar_one()
    queues, totals = await _queue_stats(conn, now, finished)
    return Snapshot(
        now=now,
        read_only=read_only,
        totals=totals,
        queues=queues,
        workers=await list_workers(conn),
        running=await running_jobs(conn),
        periodic=await list_periodic(conn),
        stats=await live_stats(conn, now=now),
        feed=await recent_activity(conn),
    )
