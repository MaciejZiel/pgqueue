"""Database schema, declared once with SQLAlchemy Core.

The same ``metadata`` object backs the Alembic migrations and the read-side
queries used by the admin API. The worker hot path uses hand-written SQL on
asyncpg (see :mod:`pgqueue.sql`) but targets exactly these tables.
"""

from __future__ import annotations

import enum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    Identity,
    Index,
    Integer,
    MetaData,
    SmallInteger,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB


class JobStatus(enum.StrEnum):
    """Lifecycle of a job.

    ``queued``    waiting to be picked up (possibly scheduled for later)
    ``running``   claimed by a worker and holding a lease
    ``succeeded`` handler returned normally
    ``retrying``  last attempt failed, waiting for its backoff delay
    ``failed``    permanent failure (non-retryable error or invalid payload)
    ``dead``      every allowed attempt failed; parked in the dead-letter state
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYING = "retrying"
    FAILED = "failed"
    DEAD = "dead"


READY_STATUSES = (JobStatus.QUEUED, JobStatus.RETRYING)
TERMINAL_STATUSES = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD)

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "pk": "pk_%(table_name)s",
    }
)

job_status = Enum(
    JobStatus,
    name="pgqueue_job_status",
    values_callable=lambda e: [m.value for m in e],
)

jobs = Table(
    "pgqueue_jobs",
    metadata,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column("queue", Text, nullable=False, server_default="default"),
    Column("task", Text, nullable=False),
    Column("payload", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("status", job_status, nullable=False, server_default=JobStatus.QUEUED.value),
    Column("priority", SmallInteger, nullable=False, server_default="0"),
    Column("run_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("max_attempts", Integer, nullable=False, server_default="5"),
    Column("idempotency_key", Text, nullable=True, unique=True),
    Column("last_error", Text, nullable=True),
    Column("result", JSONB, nullable=True),
    Column("locked_by", Text, nullable=True),
    Column("locked_until", DateTime(timezone=True), nullable=True),
    Column("heartbeat_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
    CheckConstraint("attempts >= 0", name="attempts_non_negative"),
    CheckConstraint(
        "status <> 'running' OR (locked_by IS NOT NULL AND locked_until IS NOT NULL)",
        name="running_has_lease",
    ),
    # The claim query: ready jobs of a queue, by priority, then schedule, then FIFO.
    # Partial, so finished jobs (the vast majority of rows) never bloat it.
    Index(
        "ix_pgqueue_jobs_ready",
        "queue",
        text("priority DESC"),
        "run_at",
        "id",
        postgresql_where=text("status IN ('queued', 'retrying')"),
    ),
    # The reaper: running jobs whose lease has expired.
    Index(
        "ix_pgqueue_jobs_lease",
        "locked_until",
        postgresql_where=text("status = 'running'"),
    ),
    # Dashboard: recent failures and throughput.
    Index("ix_pgqueue_jobs_status_finished", "status", "finished_at"),
)

# One row per cron schedule. Rows are upserted by workers on start-up; the row
# lock taken when a schedule fires is what stops two workers firing it twice.
periodic = Table(
    "pgqueue_periodic",
    metadata,
    Column("name", Text, primary_key=True),
    Column("cron", Text, nullable=False),
    Column("task", Text, nullable=False),
    Column("queue", Text, nullable=False),
    Column("payload", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("next_run_at", DateTime(timezone=True), nullable=False),
    Column("last_run_at", DateTime(timezone=True), nullable=True),
    Column("last_job_id", BigInteger, nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Channel the triggers from migration 0002 notify; the payload is the queue name.
NOTIFY_CHANNEL = "pgqueue_jobs"
