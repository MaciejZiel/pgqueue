from __future__ import annotations

from pgqueue import JobStatus, PgQueue


async def test_prune_deletes_only_old_finished_jobs(app: PgQueue) -> None:
    for _ in range(7):
        await app.enqueue("t", {})
    jobs = await app.store.claim(["default"], limit=6, worker_id="w", lease_seconds=30)
    for job in jobs[:3]:
        await app.store.complete(job, "w")
    await app.store.fail(jobs[3], "w", status=JobStatus.DEAD, error="x")
    await app.store.fail(jobs[4], "w", status=JobStatus.RETRYING, error="x", retry_in=60)
    # jobs[5] stays running, the 7th job stays queued.
    await app.pool.execute("UPDATE pgqueue_jobs SET finished_at = now() - interval '8 days'")
    await app.pool.execute("UPDATE pgqueue_jobs SET finished_at = now() WHERE id = $1", jobs[2].id)

    deleted = await app.store.prune(older_than_seconds=7 * 86400, batch_size=2)

    assert deleted == 3  # two old succeeded + one old dead; the recent one stays
    remaining = await app.pool.fetch("SELECT status::text FROM pgqueue_jobs ORDER BY id")
    assert sorted(r["status"] for r in remaining) == [
        "queued",
        "retrying",
        "running",
        "succeeded",
    ]
