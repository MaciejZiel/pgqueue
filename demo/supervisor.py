"""Runs the demo's workers as real processes and now and then kills one.

A SIGKILLed worker cannot release its jobs or deregister: its leases simply stop
being extended. After a lease period another worker's reaper recovers the jobs,
which is exactly the failure the lease mechanism exists for.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import signal
import sys

from demo.config import Settings
from demo.tasks import QUEUES

log = logging.getLogger("demo.supervisor")


def worker_command(settings: Settings) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pgqueue",
        "--log-level",
        "WARNING",
        "worker",
        "demo.tasks:app",
        "--queues",
        ",".join(QUEUES),
        "--concurrency",
        str(settings.concurrency),
        "--lease",
        str(settings.lease_seconds),
        "--shutdown-timeout",
        "5",
    ]


class WorkerSupervisor:
    def __init__(self, settings: Settings, *, rng: random.Random | None = None) -> None:
        self.settings = settings
        self.rng = rng or random.Random()
        self.processes: dict[int, asyncio.subprocess.Process] = {}
        self.kills = 0
        self._tasks: list[asyncio.Task[None]] = []

    async def _spawn(self) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            *worker_command(self.settings), cwd=self.settings.repo_root
        )

    async def _slot(self, index: int) -> None:
        """Keep one worker process alive; restart it after it exits or is killed."""
        while True:
            process = await self._spawn()
            self.processes[index] = process
            code = await process.wait()
            log.warning("worker slot %d exited with %s; restarting", index, code)
            await asyncio.sleep(self.settings.respawn_after if code == -signal.SIGKILL else 2.0)

    async def _chaos(self) -> None:
        every = self.settings.crash_every
        while True:
            await asyncio.sleep(every * self.rng.uniform(0.7, 1.3))
            self.kill_one()

    def kill_one(self) -> int | None:
        """SIGKILL one running worker; returns its pid."""
        alive = [p for p in self.processes.values() if p.returncode is None]
        if len(alive) < 2:  # never take the last worker down
            return None
        victim = self.rng.choice(alive)
        log.warning("killing worker pid %d with SIGKILL", victim.pid)
        victim.kill()
        self.kills += 1
        return victim.pid

    async def run(self) -> None:
        self._tasks = [asyncio.create_task(self._slot(i)) for i in range(self.settings.workers)]
        if self.settings.crash_every > 0:
            self._tasks.append(asyncio.create_task(self._chaos()))
        try:
            await asyncio.gather(*self._tasks)
        finally:
            await self.stop()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for process in self.processes.values():
            if process.returncode is None:
                process.terminate()
        for process in self.processes.values():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), 10)
            if process.returncode is None:
                process.kill()
