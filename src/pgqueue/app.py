"""The public entry point: a :class:`PgQueue` application and its tasks."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Concatenate, Self, get_type_hints, overload

import asyncpg
from pydantic import BaseModel, ValidationError

from pgqueue.backoff import DEFAULT_RETRY_POLICY, RetryPolicy
from pgqueue.cron import CronExpression
from pgqueue.dsn import asyncpg_dsn
from pgqueue.errors import PayloadError, PgQueueError, UnknownTaskError
from pgqueue.models import Job, JobContext
from pgqueue.periodic import PeriodicSchedule
from pgqueue.store import JobPool, JobStore, create_pool

DEFAULT_QUEUE = "default"
DEFAULT_MAX_ATTEMPTS = 5


def resolve_schedule(
    delay: float | timedelta | None, run_at: datetime | None
) -> tuple[datetime | None, float]:
    """Normalise the two ways of scheduling a job to ``(run_at, delay_seconds)``."""
    if delay is not None and run_at is not None:
        raise ValueError("pass either delay or run_at, not both")
    if run_at is not None and run_at.tzinfo is None:
        raise ValueError("run_at must be timezone-aware")
    seconds = delay.total_seconds() if isinstance(delay, timedelta) else float(delay or 0)
    if seconds < 0:
        raise ValueError("delay must be non-negative")
    return run_at, seconds


@dataclass(frozen=True, slots=True)
class TaskOptions:
    queue: str = DEFAULT_QUEUE
    priority: int = 0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    timeout: float | None = None
    retry: RetryPolicy = DEFAULT_RETRY_POLICY


class Task[M: BaseModel | None]:
    """A registered handler plus everything needed to enqueue work for it.

    ``M`` is the pydantic model of the payload (``None`` for handlers that take
    no payload). Payloads are validated when they are enqueued *and* again when
    the worker picks them up, so a schema change between deploys surfaces as a
    clear, non-retryable ``failed`` job instead of an ``AttributeError`` deep in
    the handler.
    """

    def __init__(
        self,
        app: PgQueue,
        fn: Callable[..., Any],
        *,
        name: str,
        payload_model: type[BaseModel] | None,
        wants_context: bool,
        options: TaskOptions,
    ) -> None:
        self.app = app
        self.fn = fn
        self.name = name
        self.payload_model = payload_model
        self.wants_context = wants_context
        self.options = options
        self.is_async = inspect.iscoroutinefunction(fn)

    def __repr__(self) -> str:
        return f"<Task {self.name} queue={self.options.queue}>"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the handler directly, bypassing the queue (handy in unit tests)."""
        return self.fn(*args, **kwargs)

    # -- payloads ---------------------------------------------------------------------------

    def dump_payload(self, payload: M | dict[str, Any] | None) -> dict[str, Any]:
        if self.payload_model is None:
            if payload is not None:
                raise PayloadError(f"task {self.name!r} takes no payload")
            return {}
        if payload is None:
            raise PayloadError(f"task {self.name!r} requires a {self.payload_model.__name__}")
        try:
            model = (
                payload
                if isinstance(payload, self.payload_model)
                else self.payload_model.model_validate(payload)
            )
        except ValidationError as exc:
            raise PayloadError(f"invalid payload for {self.name!r}: {exc}") from exc
        return model.model_dump(mode="json")

    def load_payload(self, raw: dict[str, Any]) -> BaseModel | None:
        if self.payload_model is None:
            return None
        return self.payload_model.model_validate(raw)

    # -- enqueueing -------------------------------------------------------------------------

    async def enqueue(
        self,
        payload: M | dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        idempotency_key: str | None = None,
        delay: float | timedelta | None = None,
        run_at: datetime | None = None,
        connection: asyncpg.Connection | None = None,
    ) -> int:
        """Add one job for this task and return its id.

        ``delay`` (seconds or a timedelta) or ``run_at`` (an aware datetime)
        schedule the job for later; until then it is invisible to workers.

        If ``idempotency_key`` is given and a job with that key already exists,
        no new job is created and the existing job's id is returned.

        Pass an asyncpg ``connection`` with an open transaction to enqueue
        atomically with your own writes: if the transaction rolls back, the
        job never existed.
        """
        at, seconds = resolve_schedule(delay, run_at)
        job_id, _ = await self.app.store.insert(
            queue=queue or self.options.queue,
            task=self.name,
            payload=self.dump_payload(payload),
            priority=self.options.priority if priority is None else priority,
            run_at=at,
            delay=seconds,
            max_attempts=max_attempts or self.options.max_attempts,
            idempotency_key=idempotency_key,
            connection=connection,
        )
        return job_id

    async def enqueue_many(
        self,
        payloads: Sequence[M | dict[str, Any]],
        *,
        idempotency_keys: Sequence[str | None] | None = None,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        delay: float | timedelta | None = None,
        run_at: datetime | None = None,
    ) -> list[int]:
        """Add many jobs in a single INSERT; returns the ids actually created."""
        at, seconds = resolve_schedule(delay, run_at)
        return await self.app.store.insert_many(
            queue=queue or self.options.queue,
            task=self.name,
            payloads=[self.dump_payload(p) for p in payloads],
            idempotency_keys=idempotency_keys,
            priority=self.options.priority if priority is None else priority,
            run_at=at,
            delay=seconds,
            max_attempts=max_attempts or self.options.max_attempts,
        )


def _inspect_handler(fn: Callable[..., Any]) -> tuple[type[BaseModel] | None, bool]:
    """Work out the payload model and whether the handler wants a JobContext."""
    params = list(inspect.signature(fn).parameters.values())
    if len(params) > 2:
        raise PgQueueError(f"handler {fn.__qualname__} must take (payload[, ctx])")
    hints = get_type_hints(fn)
    payload_model: type[BaseModel] | None = None
    wants_context = False
    for param in params:
        hint = hints.get(param.name)
        if hint is JobContext:
            wants_context = True
        elif isinstance(hint, type) and issubclass(hint, BaseModel) and payload_model is None:
            payload_model = hint
        else:
            raise PgQueueError(
                f"parameter {param.name!r} of {fn.__qualname__} must be annotated with "
                "a pydantic model (the payload) or JobContext"
            )
    return payload_model, wants_context


class _TaskDecorator:
    def __init__(self, app: PgQueue, name: str | None, options: TaskOptions) -> None:
        self.app = app
        self.name = name
        self.options = options

    @overload
    def __call__[R](self, fn: Callable[[], R]) -> Task[None]: ...
    @overload
    def __call__[R](self, fn: Callable[[JobContext], R]) -> Task[None]: ...
    @overload
    def __call__[N: BaseModel, R](self, fn: Callable[Concatenate[N, ...], R]) -> Task[N]: ...

    def __call__(self, fn: Callable[..., Any]) -> Task[Any]:
        payload_model, wants_context = _inspect_handler(fn)
        name = self.name or f"{fn.__module__}.{fn.__qualname__}"
        task: Task[Any] = Task(
            self.app,
            fn,
            name=name,
            payload_model=payload_model,
            wants_context=wants_context,
            options=self.options,
        )
        self.app.register(task)
        return task


class PgQueue:
    """A queue application: the task registry plus a connection pool.

    >>> app = PgQueue("postgresql://localhost/app")
    >>> @app.task(queue="emails", max_attempts=8)
    ... async def send_welcome(payload: Welcome) -> None: ...
    >>> async with app:
    ...     await send_welcome.enqueue(Welcome(user_id=42))
    """

    def __init__(self, dsn: str, *, pool_min_size: int = 1, pool_max_size: int = 10) -> None:
        self.dsn = asyncpg_dsn(dsn)
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.tasks: dict[str, Task[Any]] = {}
        self.schedules: dict[str, PeriodicSchedule] = {}
        self._pool: JobPool | None = None
        self._store: JobStore | None = None

    # -- lifecycle --------------------------------------------------------------------------

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await create_pool(
                self.dsn, min_size=self.pool_min_size, max_size=self.pool_max_size
            )
            self._store = JobStore(self._pool)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._store = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def pool(self) -> JobPool:
        if self._pool is None:
            raise PgQueueError("PgQueue is not connected; call `await app.connect()` first")
        return self._pool

    @property
    def store(self) -> JobStore:
        if self._store is None:
            raise PgQueueError("PgQueue is not connected; call `await app.connect()` first")
        return self._store

    # -- registry ---------------------------------------------------------------------------

    def task(
        self,
        name: str | None = None,
        *,
        queue: str = DEFAULT_QUEUE,
        priority: int = 0,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        timeout: float | None = None,
        retry: RetryPolicy | None = None,
    ) -> _TaskDecorator:
        """Register a handler as a task.

        The handler may be sync or async. Its first parameter, if any, must be
        annotated with a pydantic model -- that is the payload type. A parameter
        annotated with :class:`JobContext` receives job metadata.

        Failed attempts are retried after ``retry`` backoff until
        ``max_attempts`` is reached, after which the job is ``dead``.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        options = TaskOptions(
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            timeout=timeout,
            retry=retry or DEFAULT_RETRY_POLICY,
        )
        return _TaskDecorator(self, name, options)

    def periodic[M: BaseModel | None](
        self,
        cron: str,
        *,
        name: str | None = None,
        payload: M | dict[str, Any] | None = None,
        queue: str | None = None,
    ) -> Callable[[Task[M]], Task[M]]:
        """Run a task on a cron schedule (UTC). Stack it on top of ``@app.task``::

        @app.periodic("*/15 * * * *")
        @app.task()
        async def refresh_exchange_rates() -> None: ...
        """
        expression = CronExpression.parse(cron)

        def decorate(task: Task[M]) -> Task[M]:
            schedule_name = name or task.name
            if schedule_name in self.schedules:
                raise PgQueueError(f"periodic schedule {schedule_name!r} already exists")
            self.schedules[schedule_name] = PeriodicSchedule(
                name=schedule_name,
                cron=expression,
                task=task,
                payload=task.dump_payload(payload),
                queue=queue,
            )
            return task

        return decorate

    def register(self, task: Task[Any]) -> None:
        if task.name in self.tasks:
            raise PgQueueError(f"task {task.name!r} is already registered")
        self.tasks[task.name] = task

    def get_task(self, name: str) -> Task[Any]:
        try:
            return self.tasks[name]
        except KeyError:
            raise UnknownTaskError(name) from None

    # -- untyped API ------------------------------------------------------------------------

    async def enqueue(
        self,
        task: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        idempotency_key: str | None = None,
        delay: float | timedelta | None = None,
        run_at: datetime | None = None,
        connection: asyncpg.Connection | None = None,
    ) -> int:
        """Enqueue by task name, e.g. from a process that does not import the handler.

        If the task is registered locally its payload model is still enforced.
        """
        registered = self.tasks.get(task)
        if registered is not None:
            return await registered.enqueue(
                payload,
                queue=queue,
                priority=priority,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                delay=delay,
                run_at=run_at,
                connection=connection,
            )
        at, seconds = resolve_schedule(delay, run_at)
        job_id, _ = await self.store.insert(
            queue=queue or DEFAULT_QUEUE,
            task=task,
            payload=payload or {},
            priority=priority or 0,
            run_at=at,
            delay=seconds,
            max_attempts=max_attempts or DEFAULT_MAX_ATTEMPTS,
            idempotency_key=idempotency_key,
            connection=connection,
        )
        return job_id

    async def get_job(self, job_id: int) -> Job | None:
        return await self.store.get(job_id)
