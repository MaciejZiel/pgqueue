"""Celery + Redis counterpart of bench.tasks, for the comparison runs.

The task does the minimum a benchmark needs: it records when it started and finished in
Redis with one pipelined round trip. pgqueue gets the same information from its own job
rows at no extra cost, so Celery is charged one extra Redis call per task here; the
write-up says so.
"""

from __future__ import annotations

import os
import time
from typing import Any

import redis
from celery import Celery

BROKER = os.environ.get("BENCH_REDIS_URL", "redis://localhost:56379/0")

app = Celery("bench", broker=BROKER)
app.conf.update(
    task_ignore_result=True,
    task_default_queue="bench",
    # Both are Celery's defaults, spelled out because they matter for the comparison:
    # early ack means a message is acknowledged when the worker receives it (at-most-once
    # on a crash), and each 32-thread process prefetches 4 x 32 messages.
    task_acks_late=False,
    worker_prefetch_multiplier=4,
    broker_connection_retry_on_startup=True,
)

_redis = redis.Redis.from_url(BROKER)


def _noop(payload: dict[str, Any]) -> None:
    started = time.time()
    pipe = _redis.pipeline(transaction=False)
    pipe.rpush("bench:pickup_s", started - payload["sent_at"])
    pipe.rpush("bench:finished", time.time())
    pipe.execute()


noop = app.task(name="bench.noop")(_noop)
