"""Add the worker registry shown on the dashboard.

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pgqueue_workers",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("hostname", sa.Text(), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("queues", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False),
        sa.Column("lease_seconds", sa.Float(), nullable=False),
        sa.Column("in_flight", sa.Integer(), server_default="0", nullable=False),
        sa.Column("processed", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("failed", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_pgqueue_workers"),
    )


def downgrade() -> None:
    op.drop_table("pgqueue_workers")
