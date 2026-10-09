"""The worker: claims jobs, runs handlers concurrently and records outcomes.

Concurrency model
-----------------
A worker is a single asyncio event loop running up to ``concurrency`` jobs at
once. Async handlers run as tasks on the loop; sync handlers run on a
dedicated thread pool of the same size, so a blocking handler cannot starve
the loop that heart-beats the leases. Most background jobs are I/O bound
(HTTP calls, e-mail, database writes), and for those asyncio gives hundreds
of in-flight jobs per process for the cost of one connection pool. CPU-bound
work scales by running more worker *processes* -- each claims independently,
so there is no coordinator to scale.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import logging
import os
import signal
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import asyncpg
from pydantic_core import to_jsonable_python

from pgqueue.app import PgQueue, Task
from pgqueue.backoff import DEFAULT_RETRY_POLICY
from pgqueue.models import Job, JobContext
from pgqueue.notify import Listener
from pgqueue.outcomes import decide_failure, format_error
from pgqueue.periodic import PeriodicScheduler
from pgqueue.schema import NOTIFY_CHANNEL, JobStatus

log = logging.getLogger("pgqueue.worker")


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


class Worker:
    """Processes jobs from one or more queues until :meth:`stop` is called."""

    def __init__(
        self,
        app: PgQueue,
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 10,
        poll_interval: float = 1.0,
        lease_seconds: float = 30.0,
        shutdown_timeout: float = 30.0,
        worker_id: str | None = None,
        heartbeat_interval: float | None = None,
        reap_interval: float | None = None,
        listen: bool = True,
        run_periodic: bool = True,
        periodic_interval: float = 1.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if not queues:
            raise ValueError("at least one queue is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.app = app
        self.queues = list(queues)
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.shutdown_timeout = shutdown_timeout
        self.id = worker_id or default_worker_id()
        # Three heartbeats per lease: one can be lost or late without the
        # lease expiring under a healthy worker.
        self.heartbeat_interval = heartbeat_interval or lease_seconds / 3
        self.reap_interval = reap_interval or lease_seconds / 2
        self.periodic_interval = periodic_interval
        self.scheduler = PeriodicScheduler(app) if run_periodic else None
        self.listener: Listener | None = None
        if listen:
            self.listener = Listener(
                app.dsn, NOTIFY_CHANNEL, self._on_notify, on_reconnect=self.wake
            )

        self.processed = 0
        self.failed = 0
        self._running: dict[asyncio.Task[None], Job] = {}
        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._background: list[asyncio.Task[None]] = []
        # Called as hook(job, outcome, seconds) after every attempt; outcome is
        # succeeded, retrying, failed, dead or released. Used for metrics.
        self.on_job_done: list[Callable[[Job, str, float], None]] = []

    # -- control ------------------------------------------------------------------------------

    @property
    def in_flight(self) -> int:
        return len(self._running)

    def stop(self) -> None:
        """Stop claiming new jobs; :meth:`run` returns once in-flight jobs drain."""
        if not self._stopping.is_set():
            log.info("worker %s stopping, draining %d job(s)", self.id, self.in_flight)
        self._stopping.set()
        self._wakeup.set()

    def wake(self) -> None:
        """Skip the rest of the current poll interval and try to claim now."""
        self._wakeup.set()

    def _on_notify(self, queue: str) -> None:
        if queue in self.queues:
            self._wakeup.set()

    def install_signal_handlers(self) -> None:
        """SIGTERM/SIGINT drain gracefully; a second signal stops without waiting."""
        loop = asyncio.get_running_loop()

        def handle(signum: int) -> None:
            if self._stopping.is_set():
                log.warning("second %s: abandoning in-flight jobs", signal.Signals(signum).name)
                self.shutdown_timeout = 0
                self._wakeup.set()
            else:
                log.info("received %s", signal.Signals(signum).name)
                self.stop()

        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, handle, signum)

    # -- main loop ----------------------------------------------------------------------------

    async def run(self) -> None:
        """Run until :meth:`stop` is called, then drain in-flight jobs."""
        await self.app.connect()
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="pgqueue-sync"
        )
        await self.app.store.register_worker(
            self.id,
            queues=self.queues,
            concurrency=self.concurrency,
            lease_seconds=self.lease_seconds,
        )
        log.info(
            "worker %s started: queues=%s concurrency=%d", self.id, self.queues, self.concurrency
        )
        self._background = [
            asyncio.create_task(self._every(self.heartbeat_interval, self._heartbeat)),
            asyncio.create_task(self._every(self.reap_interval, self._reap)),
        ]
        if self.listener is not None:
            self._background.append(asyncio.create_task(self.listener.run()))
        if self.scheduler is not None and self.app.schedules:
            await self.scheduler.sync()
            self._background.append(
                asyncio.create_task(self._every(self.periodic_interval, self._fire_periodic))
            )
        try:
            await self._loop()
        finally:
            # Leases keep being extended while in-flight jobs drain.
            await self._drain()
            for task in self._background:
                task.cancel()
            await asyncio.gather(*self._background, return_exceptions=True)
            self._executor.shutdown(wait=False, cancel_futures=True)
            with contextlib.suppress(Exception):
                await self.app.store.deregister_worker(self.id)
            log.info(
                "worker %s stopped: processed=%d failed=%d", self.id, self.processed, self.failed
            )

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            free = self.concurrency - self.in_flight
            claimed = 0
            if free > 0:
                try:
                    jobs = await self.app.store.claim(
                        self.queues, limit=free, worker_id=self.id, lease_seconds=self.lease_seconds
                    )
                except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                    log.warning("claim failed (%s); retrying after poll interval", exc)
                    jobs = []
                for job in jobs:
                    self._start(job)
                claimed = len(jobs)
                if claimed == free:
                    # Possibly more work waiting: loop straight away (the next
                    # iteration blocks below if every slot is busy).
                    continue
            await self._wait_for_wakeup()

    async def _wait_for_wakeup(self) -> None:
        """Sleep until a slot frees up, a notification arrives, stop() or the poll interval."""
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(self.poll_interval):
                await self._wakeup.wait()
        self._wakeup.clear()

    def _start(self, job: Job) -> None:
        task = asyncio.create_task(self._execute(job), name=f"pgqueue-job-{job.id}")
        self._running[task] = job
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._running.pop(task, None)
        self._wakeup.set()  # a slot is free

    async def _drain(self) -> None:
        """Wait for in-flight jobs until they finish, the timeout passes or a forced stop."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.shutdown_timeout
        pending = set(self._running)
        while pending and self.shutdown_timeout > 0 and loop.time() < deadline:
            # Short slices so a second signal (shutdown_timeout = 0) takes effect quickly.
            _, pending = await asyncio.wait(pending, timeout=min(0.1, deadline - loop.time()))
        if pending:
            log.warning("releasing %d unfinished job(s)", len(pending))
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    # -- leases ------------------------------------------------------------------------------

    async def _every(self, interval: float, action: Callable[[], Awaitable[None]]) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await action()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("background %s failed; will retry", action.__name__)

    async def _heartbeat(self) -> None:
        await self.app.store.touch_worker(
            self.id, in_flight=self.in_flight, processed=self.processed, failed=self.failed
        )
        jobs = {job.id: job for job in self._running.values()}
        if not jobs:
            return
        held = await self.app.store.heartbeat(list(jobs), self.id, lease_seconds=self.lease_seconds)
        for job_id in jobs.keys() - held:
            log.warning(
                "lost the lease on job %d (%s); its outcome will be discarded",
                job_id,
                jobs[job_id].task,
            )

    async def _reap(self) -> None:
        for job_id, status, owner in await self.app.store.reap_expired():
            log.warning("recovered job %d from %s -> %s", job_id, owner, status.value)
        await self.app.store.prune_workers()
        self._wakeup.set()

    async def _fire_periodic(self) -> None:
        assert self.scheduler is not None
        await self.scheduler.tick()

    # -- running one job ---------------------------------------------------------------------

    async def _execute(self, job: Job) -> None:
        started = time.perf_counter()
        try:
            result = await self._invoke(job)
        except asyncio.CancelledError:
            await self._release(job)
            self._notify_hooks(job, "released", time.perf_counter() - started)
            raise
        except Exception as exc:
            self.failed += 1
            status = await self._record_failure(job, exc)
            self._notify_hooks(job, status.value, time.perf_counter() - started)
        else:
            self.processed += 1
            await self._record_success(job, result)
            self._notify_hooks(job, JobStatus.SUCCEEDED.value, time.perf_counter() - started)

    def _notify_hooks(self, job: Job, outcome: str, seconds: float) -> None:
        for hook in self.on_job_done:
            try:
                hook(job, outcome, seconds)
            except Exception:
                log.exception("on_job_done hook %r failed", hook)

    async def _invoke(self, job: Job) -> Any:
        task: Task[Any] = self.app.get_task(job.task)
        args: list[Any] = []
        if task.payload_model is not None:
            args.append(task.load_payload(job.payload))
        if task.wants_context:
            args.append(JobContext(job=job, worker_id=self.id))

        if task.is_async:
            call = task.fn(*args)
        else:
            loop = asyncio.get_running_loop()
            ctx = contextvars.copy_context()
            call = loop.run_in_executor(self._executor, functools.partial(ctx.run, task.fn, *args))
        if task.options.timeout is not None:
            return await asyncio.wait_for(call, task.options.timeout)
        return await call

    async def _record_success(self, job: Job, result: Any) -> None:
        try:
            stored = await self.app.store.complete(
                job, self.id, to_jsonable_python(result, fallback=repr)
            )
        except Exception:
            log.exception("could not record success of job %d; its lease will expire", job.id)
            return
        if not stored:
            log.warning("job %d finished after its lease was lost; result discarded", job.id)

    async def _record_failure(self, job: Job, exc: Exception) -> JobStatus:
        task = self.app.tasks.get(job.task)
        policy = task.options.retry if task is not None else DEFAULT_RETRY_POLICY
        outcome = decide_failure(job, exc, policy)
        level = logging.ERROR if outcome.status is not JobStatus.RETRYING else logging.WARNING
        log.log(
            level,
            "job %d (%s) attempt %d/%d failed: %r -> %s",
            job.id,
            job.task,
            job.attempts,
            job.max_attempts,
            exc,
            outcome.status.value,
        )
        try:
            await self.app.store.fail(
                job,
                self.id,
                status=outcome.status,
                error=format_error(exc),
                retry_in=outcome.retry_in,
            )
        except Exception:
            log.exception("could not record failure of job %d; its lease will expire", job.id)
        return outcome.status

    async def _release(self, job: Job) -> None:
        try:
            await self.app.store.release(job, self.id)
        except Exception:
            log.exception("could not release job %d; its lease will expire", job.id)
