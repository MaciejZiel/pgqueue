"""Create the jobs table.

Revision ID: 0001
Revises:
Create Date: 2026-10-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATUSES = ("queued", "running", "succeeded", "retrying", "failed", "dead")


def upgrade() -> None:
    job_status = postgresql.ENUM(*STATUSES, name="pgqueue_job_status")
    op.create_table(
        "pgqueue_jobs",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("queue", sa.Text(), server_default="default", nullable=False),
        sa.Column("task", sa.Text(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("status", job_status, server_default="queued", nullable=False),
        sa.Column("priority", sa.SmallInteger(), server_default="0", nullable=False),
        sa.Column(
            "run_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="5", nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("max_attempts >= 1", name="ck_pgqueue_jobs_max_attempts_positive"),
        sa.CheckConstraint("attempts >= 0", name="ck_pgqueue_jobs_attempts_non_negative"),
        sa.CheckConstraint(
            "status <> 'running' OR (locked_by IS NOT NULL AND locked_until IS NOT NULL)",
            name="ck_pgqueue_jobs_running_has_lease",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_pgqueue_jobs"),
        sa.UniqueConstraint("idempotency_key", name="uq_pgqueue_jobs_idempotency_key"),
    )
    op.create_index(
        "ix_pgqueue_jobs_ready",
        "pgqueue_jobs",
        ["queue", sa.text("priority DESC"), "run_at", "id"],
        postgresql_where=sa.text("status IN ('queued', 'retrying')"),
    )
    op.create_index(
        "ix_pgqueue_jobs_lease",
        "pgqueue_jobs",
        ["locked_until"],
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index("ix_pgqueue_jobs_status_finished", "pgqueue_jobs", ["status", "finished_at"])


def downgrade() -> None:
    op.drop_table("pgqueue_jobs")
    op.execute("DROP TYPE pgqueue_job_status")
