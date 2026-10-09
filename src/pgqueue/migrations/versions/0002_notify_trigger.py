"""Notify listening workers when jobs become ready.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-10
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Inserts use a statement-level trigger with a transition table: a bulk insert
# of 10 000 jobs sends one notification per distinct queue rather than 10 000
# trigger calls. Updates (requeue from the dashboard, release on shutdown,
# reaper) use a row-level trigger restricted by WHEN, which Postgres evaluates
# without entering PL/pgSQL -- so the hot-path UPDATEs (claim, complete,
# heartbeat) pay almost nothing. Notifications are only delivered on commit
# and identical ones within a transaction are collapsed by Postgres.
#
# asyncpg runs one statement per execute, hence the lists.
UPGRADE = [
    """
CREATE FUNCTION pgqueue_notify_inserted() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('pgqueue_jobs', q.queue)
    FROM (SELECT DISTINCT queue FROM new_jobs WHERE run_at <= now()) AS q;
    RETURN NULL;
END
$$
""",
    """
CREATE TRIGGER pgqueue_jobs_notify_insert
AFTER INSERT ON pgqueue_jobs
REFERENCING NEW TABLE AS new_jobs
FOR EACH STATEMENT EXECUTE FUNCTION pgqueue_notify_inserted()
""",
    """
CREATE FUNCTION pgqueue_notify_updated() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('pgqueue_jobs', NEW.queue);
    RETURN NULL;
END
$$
""",
    """
CREATE TRIGGER pgqueue_jobs_notify_update
AFTER UPDATE OF status ON pgqueue_jobs
FOR EACH ROW
WHEN (NEW.status IN ('queued', 'retrying') AND NEW.run_at <= now())
EXECUTE FUNCTION pgqueue_notify_updated()
""",
]

DOWNGRADE = [
    "DROP TRIGGER pgqueue_jobs_notify_update ON pgqueue_jobs",
    "DROP FUNCTION pgqueue_notify_updated()",
    "DROP TRIGGER pgqueue_jobs_notify_insert ON pgqueue_jobs",
    "DROP FUNCTION pgqueue_notify_inserted()",
]


def upgrade() -> None:
    for statement in UPGRADE:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE:
        op.execute(statement)
