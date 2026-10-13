"""Response models of the admin API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from pgqueue.schema import JobStatus


class JobOut(BaseModel):
    id: int
    queue: str
    task: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    run_at: datetime
    attempts: int
    max_attempts: int
    idempotency_key: str | None
    last_error: str | None
    result: Any
    locked_by: str | None
    locked_until: datetime | None
    heartbeat_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobPage(BaseModel):
    jobs: list[JobOut]
    next_before_id: int | None


class QueueStats(BaseModel):
    queue: str
    counts: dict[JobStatus, int]
    lag_seconds: float
    """Age of the oldest job that is due but not yet claimed."""


class ThroughputPoint(BaseModel):
    minute: datetime
    succeeded: int
    failed: int


class WorkerOut(BaseModel):
    id: str
    hostname: str
    pid: int
    queues: list[str]
    concurrency: int
    lease_seconds: float
    in_flight: int
    processed: int
    failed: int
    started_at: datetime
    last_seen_at: datetime
    alive: bool


class PeriodicOut(BaseModel):
    name: str
    cron: str
    task: str
    queue: str
    next_run_at: datetime
    last_run_at: datetime | None
    last_job_id: int | None


class Overview(BaseModel):
    now: datetime
    totals: dict[JobStatus, int]
    queues: list[QueueStats]
    throughput: list[ThroughputPoint]
    workers: list[WorkerOut]
    periodic: list[PeriodicOut]


class RequeueResult(BaseModel):
    requeued: int


class ThroughputSecond(BaseModel):
    second: datetime
    succeeded: int
    failed: int


class LatencyStats(BaseModel):
    """Percentiles in seconds over the jobs that finished within the window."""

    p50: float | None
    p95: float | None
    samples: int


class LiveStats(BaseModel):
    window_seconds: int
    jobs_per_second: float
    """Finished jobs per second, averaged over the last ten seconds."""
    throughput: list[ThroughputSecond]
    pickup: LatencyStats
    """Time between a job becoming due and a worker claiming it."""
    run: LatencyStats
    """Handler time of succeeded jobs, from claim to completion."""
    retries: int
    """Extra attempts (beyond the first) used by the jobs that finished in the window."""
    failures: int
    """Jobs that ended failed or dead within the window."""


class RunningJob(BaseModel):
    id: int
    task: str
    queue: str
    attempts: int
    max_attempts: int
    locked_by: str
    started_at: datetime
    heartbeat_at: datetime | None
    locked_until: datetime


class FeedItem(BaseModel):
    id: int
    task: str
    queue: str
    status: JobStatus
    attempts: int
    max_attempts: int
    at: datetime
    """When the job last changed state."""
    duration: float | None
    """Handler time in seconds, when the job has finished an attempt."""
    error: str | None
    """Last line of the last error."""


class Snapshot(BaseModel):
    """Everything the live dashboard shows, computed once and shared by all viewers."""

    now: datetime
    read_only: bool
    totals: dict[JobStatus, int]
    queues: list[QueueStats]
    workers: list[WorkerOut]
    running: list[RunningJob]
    periodic: list[PeriodicOut]
    stats: LiveStats
    feed: list[FeedItem]
