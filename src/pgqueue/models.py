"""Plain data objects returned by the public API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pgqueue.schema import JobStatus


@dataclass(frozen=True, slots=True)
class Job:
    """A snapshot of one row of ``pgqueue_jobs``."""

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

    @classmethod
    def from_record(cls, row: Mapping[str, Any]) -> Job:
        values = {name: row[name] for name in cls.__slots__}
        values["status"] = JobStatus(values["status"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class JobContext:
    """Passed to handlers that declare a second parameter.

    Gives the handler access to the job's identity and attempt number, which
    is what you need to write an idempotent side effect (for example by
    using ``job_id`` as an idempotency key towards a payment provider).
    """

    job: Job
    worker_id: str

    @property
    def job_id(self) -> int:
        return self.job.id

    @property
    def attempt(self) -> int:
        return self.job.attempts

    @property
    def is_last_attempt(self) -> bool:
        return self.job.attempts >= self.job.max_attempts
