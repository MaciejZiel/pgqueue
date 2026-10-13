"""``/metrics``: queue state from the database in Prometheus text format.

Values are computed at scrape time from the tables, so they are correct no
matter how many workers or admin replicas exist, and need no state here.
"""

from __future__ import annotations

from collections.abc import Iterable

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from pgqueue.admin.redact import worker_label
from pgqueue.schema import READY_STATUSES, JobStatus, jobs, periodic, workers

FINISHED = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD)


class _Snapshot(Collector):
    def __init__(self, families: list[GaugeMetricFamily | CounterMetricFamily]) -> None:
        self.families = families

    def collect(self) -> Iterable[GaugeMetricFamily | CounterMetricFamily]:
        return self.families


async def render(conn: AsyncConnection, *, redact: bool = False) -> bytes:
    by_state = GaugeMetricFamily(
        "pgqueue_jobs", "Jobs in the table, by queue and status.", labels=["queue", "status"]
    )
    for queue, status, n in await conn.execute(
        select(jobs.c.queue, jobs.c.status, func.count()).group_by(jobs.c.queue, jobs.c.status)
    ):
        by_state.add_metric([queue, JobStatus(status).value], n)

    lag = GaugeMetricFamily(
        "pgqueue_queue_lag_seconds",
        "Age of the oldest job that is due but not yet claimed (0 when none).",
        labels=["queue"],
    )
    for queue, seconds in await conn.execute(
        select(jobs.c.queue, func.extract("epoch", func.now() - func.min(jobs.c.run_at)))
        .where(jobs.c.status.in_(READY_STATUSES), jobs.c.run_at <= func.now())
        .group_by(jobs.c.queue)
    ):
        lag.add_metric([queue], float(seconds))

    recent = GaugeMetricFamily(
        "pgqueue_jobs_finished_last_5m",
        "Jobs that reached a terminal state in the last five minutes.",
        labels=["queue", "status"],
    )
    for queue, status, n in await conn.execute(
        select(jobs.c.queue, jobs.c.status, func.count())
        .where(
            jobs.c.status.in_(FINISHED),
            jobs.c.finished_at >= func.now() - func.make_interval(0, 0, 0, 0, 0, 5),
        )
        .group_by(jobs.c.queue, jobs.c.status)
    ):
        recent.add_metric([queue, JobStatus(status).value], n)

    alive = GaugeMetricFamily(
        "pgqueue_worker_up",
        "1 if the worker heart-beat within its lease period.",
        labels=["worker"],
    )
    in_flight = GaugeMetricFamily(
        "pgqueue_worker_jobs_in_flight", "Jobs running on the worker.", labels=["worker"]
    )
    processed = CounterMetricFamily(
        "pgqueue_worker_processed", "Jobs a worker finished successfully.", labels=["worker"]
    )
    failed = CounterMetricFamily(
        "pgqueue_worker_failed", "Failed job attempts on a worker.", labels=["worker"]
    )
    is_alive = workers.c.last_seen_at >= func.now() - func.make_interval(
        0, 0, 0, 0, 0, 0, workers.c.lease_seconds
    )
    for row in await conn.execute(
        select(workers.c.id, workers.c.in_flight, workers.c.processed, workers.c.failed, is_alive)
    ):
        worker = worker_label(row[0]) if redact else row[0]
        alive.add_metric([worker], 1.0 if row[4] else 0.0)
        in_flight.add_metric([worker], row[1])
        processed.add_metric([worker], row[2])
        failed.add_metric([worker], row[3])

    next_run = GaugeMetricFamily(
        "pgqueue_periodic_next_run_timestamp_seconds",
        "When each periodic schedule fires next (Unix time).",
        labels=["name"],
    )
    for name, at in await conn.execute(select(periodic.c.name, periodic.c.next_run_at)):
        next_run.add_metric([name], at.timestamp())

    registry = CollectorRegistry(auto_describe=False)
    registry.register(
        _Snapshot([by_state, lag, recent, alive, in_flight, processed, failed, next_run])
    )
    return generate_latest(registry)
