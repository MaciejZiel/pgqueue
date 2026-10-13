"""The public, read-only demo application."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from demo import retention, tasks
from demo.config import Settings
from demo.generator import Generator
from demo.supervisor import WorkerSupervisor
from pgqueue.admin import create_app

log = logging.getLogger("demo.server")


def build_app(settings: Settings, *, background: bool = True) -> FastAPI:
    """The library's admin, forced read-only, plus the workload that keeps it interesting.

    With ``background=False`` only the dashboard is served (used by tests and when the
    workload runs elsewhere).
    """
    app = create_app(
        settings.dsn,
        read_only=True,
        banner=settings.banner,
        read_only_dsn=settings.read_only_dsn,
    )
    inner = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        async with inner(application):
            running: list[asyncio.Task[None]] = []
            supervisor = WorkerSupervisor(settings)
            if background:
                await tasks.app.connect()
                generator = Generator(settings)
                application.state.generator = generator
                application.state.supervisor = supervisor
                running = [
                    asyncio.create_task(supervisor.run()),
                    asyncio.create_task(generator.run()),
                    asyncio.create_task(
                        retention.run(
                            tasks.app.store,
                            max_age=settings.retention_seconds,
                            max_rows=settings.max_finished,
                            interval=settings.janitor_interval,
                        )
                    ),
                ]
            try:
                yield
            finally:
                for task in running:
                    task.cancel()
                for task in running:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                if background:
                    await tasks.app.close()

    app.router.lifespan_context = lifespan
    return app
