from __future__ import annotations

import pytest
from pydantic import BaseModel

from pgqueue import JobContext, JobStatus, PayloadError, PgQueue, PgQueueError


class Email(BaseModel):
    to: str
    subject: str = "hello"


async def test_enqueue_stores_validated_payload(app: PgQueue) -> None:
    @app.task(queue="emails", priority=3, max_attempts=7)
    async def send(payload: Email) -> None: ...

    job_id = await send.enqueue(Email(to="a@example.com"))

    job = await app.get_job(job_id)
    assert job is not None
    assert job.task == send.name
    assert job.queue == "emails"
    assert job.priority == 3
    assert job.max_attempts == 7
    assert job.status is JobStatus.QUEUED
    assert job.attempts == 0
    assert job.payload == {"to": "a@example.com", "subject": "hello"}


async def test_enqueue_accepts_dict_and_validates_it(app: PgQueue) -> None:
    @app.task()
    async def send(payload: Email) -> None: ...

    job_id = await send.enqueue({"to": "b@example.com"})
    job = await app.get_job(job_id)
    assert job is not None
    assert job.payload["to"] == "b@example.com"

    with pytest.raises(PayloadError):
        await send.enqueue({"subject": "missing recipient"})


async def test_per_call_overrides(app: PgQueue) -> None:
    @app.task(queue="a", priority=1)
    async def send(payload: Email) -> None: ...

    job_id = await send.enqueue(Email(to="x"), queue="b", priority=9, max_attempts=2)
    job = await app.get_job(job_id)
    assert job is not None
    assert (job.queue, job.priority, job.max_attempts) == ("b", 9, 2)


async def test_idempotency_key_deduplicates(app: PgQueue) -> None:
    @app.task()
    async def send(payload: Email) -> None: ...

    first = await send.enqueue(Email(to="x"), idempotency_key="welcome:42")
    second = await send.enqueue(Email(to="y"), idempotency_key="welcome:42")
    other = await send.enqueue(Email(to="z"), idempotency_key="welcome:43")

    assert first == second
    assert other != first
    assert await app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs") == 2
    job = await app.get_job(first)
    assert job is not None
    assert job.payload["to"] == "x"  # the first payload wins


async def test_enqueue_many_inserts_in_bulk_and_skips_duplicates(app: PgQueue) -> None:
    @app.task()
    async def send(payload: Email) -> None: ...

    ids = await send.enqueue_many([Email(to=f"u{i}") for i in range(100)])
    assert len(ids) == 100
    assert len(set(ids)) == 100

    again = await send.enqueue_many(
        [Email(to="dup"), Email(to="new")], idempotency_keys=["k1", "k2"]
    )
    repeat = await send.enqueue_many(
        [Email(to="dup"), Email(to="newer")], idempotency_keys=["k1", "k3"]
    )
    assert len(again) == 2
    assert len(repeat) == 1
    assert await app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs") == 103


async def test_enqueue_by_name_for_unregistered_task(app: PgQueue) -> None:
    job_id = await app.enqueue("billing.charge", {"amount": 5}, queue="billing")
    job = await app.get_job(job_id)
    assert job is not None
    assert (job.task, job.queue, job.payload) == ("billing.charge", "billing", {"amount": 5})


async def test_enqueue_by_name_validates_registered_task(app: PgQueue) -> None:
    @app.task(name="send-email")
    async def send(payload: Email) -> None: ...

    with pytest.raises(PayloadError):
        await app.enqueue("send-email", {"nope": 1})


async def test_task_without_payload(app: PgQueue) -> None:
    @app.task()
    def tick() -> None: ...

    job_id = await tick.enqueue()
    job = await app.get_job(job_id)
    assert job is not None
    assert job.payload == {}
    with pytest.raises(PayloadError):
        await tick.enqueue({"unexpected": True})


def test_handler_signature_is_checked() -> None:
    app = PgQueue("postgresql://localhost/unused")

    with pytest.raises(PgQueueError):

        @app.task()  # type: ignore[arg-type]
        async def bad(payload: dict[str, int]) -> None: ...

    @app.task()
    async def with_ctx(payload: Email, ctx: JobContext) -> None: ...

    assert with_ctx.payload_model is Email
    assert with_ctx.wants_context


def test_duplicate_task_names_are_rejected() -> None:
    app = PgQueue("postgresql://localhost/unused")

    @app.task(name="same")
    def first() -> None: ...

    with pytest.raises(PgQueueError, match="already registered"):

        @app.task(name="same")
        def second() -> None: ...


def test_using_the_queue_before_connect_is_an_error() -> None:
    app = PgQueue("postgresql://localhost/unused")
    with pytest.raises(PgQueueError, match="not connected"):
        _ = app.store


async def test_enqueue_joins_the_callers_transaction(app: PgQueue) -> None:
    """Jobs enqueued in a rolled-back transaction never exist (transactional outbox)."""

    @app.task()
    async def send(payload: Email) -> None: ...

    async with app.pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        doomed = await send.enqueue(Email(to="rollback"), connection=conn)
        await tx.rollback()

        async with conn.transaction():
            kept = await app.enqueue(send.name, {"to": "commit"}, connection=conn)

    assert await app.get_job(doomed) is None
    assert await app.get_job(kept) is not None
