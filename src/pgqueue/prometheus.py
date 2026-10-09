"""Prometheus instrumentation for worker processes (needs the ``admin`` extra).

The admin's ``/metrics`` endpoint reports what the *database* knows (queue
depth, lag, per-worker counters). What only a worker process knows -- how
long handlers take -- is exported here, from each worker, typically on its
own port via ``pgqueue worker --metrics-port 9100``.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from pgqueue.models import Job
from pgqueue.worker import Worker

DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)


class WorkerMetrics:
    def __init__(self, worker: Worker, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.attempts = Counter(
            "pgqueue_worker_attempts",
            "Job attempts finished by this worker, by task and outcome.",
            ["task", "outcome"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "pgqueue_worker_attempt_duration_seconds",
            "Wall-clock duration of job attempts, by task and outcome.",
            ["task", "outcome"],
            buckets=DURATION_BUCKETS,
            registry=self.registry,
        )
        in_flight = Gauge(
            "pgqueue_worker_in_flight", "Jobs currently running.", registry=self.registry
        )
        in_flight.set_function(lambda: worker.in_flight)
        concurrency = Gauge(
            "pgqueue_worker_concurrency", "Configured concurrency.", registry=self.registry
        )
        concurrency.set(worker.concurrency)
        worker.on_job_done.append(self.observe)

    def observe(self, job: Job, outcome: str, seconds: float) -> None:
        self.attempts.labels(job.task, outcome).inc()
        self.duration.labels(job.task, outcome).observe(seconds)

    def serve(self, port: int, addr: str = "0.0.0.0") -> None:
        start_http_server(port, addr=addr, registry=self.registry)
