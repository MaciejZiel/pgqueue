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
