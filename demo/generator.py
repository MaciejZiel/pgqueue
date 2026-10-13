"""A synthetic workload: steady traffic, bursts, scheduled jobs, backpressure."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import Callable, Sequence

from demo import tasks
from demo.config import Settings

log = logging.getLogger("demo.generator")

# (weight, name) of what a request produces.
MIX: Sequence[tuple[int, str]] = (
    (38, "email"),
    (22, "image"),
    (20, "charge"),
    (15, "webhook"),
    (5, "export"),
)

READY = (
    "SELECT count(*) FROM pgqueue_jobs WHERE status IN ('queued', 'retrying') AND run_at <= now()"
)


class Generator:
    def __init__(
        self,
        settings: Settings,
        *,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.rng = rng or random.Random()
        self.clock = clock
        self.sequence = 0
        self.enqueued = 0
        self.bursts = 0
        self.paused = False
        self._carry = 0.0

    # -- what to enqueue ------------------------------------------------------------------------

    def _pick(self) -> str:
        weights = [w for w, _ in MIX]
        return self.rng.choices([name for _, name in MIX], weights)[0]

    async def enqueue_one(self, kind: str | None = None) -> int:
        kind = kind or self._pick()
        self.sequence += 1
        n = self.sequence
        if kind == "email":
            return await tasks.send_welcome.enqueue(
                tasks.WelcomeEmail(user_id=n, email=f"user{n}@example.com"),
                idempotency_key=f"welcome:{n}",
            )
        if kind == "image":
            width = self.rng.choice([320, 640, 1280, 2048])
            return await tasks.resize_image.enqueue(tasks.ResizeImage(image_id=n, width=width))
        if kind == "charge":
            cents = self.rng.randint(500, 50_000)
            return await tasks.charge.enqueue(tasks.Charge(invoice_id=n, amount_cents=cents))
        if kind == "export":
            rows = self.rng.randint(1_000, 90_000)
            return await tasks.export_report.enqueue(tasks.Export(report=f"export-{n}", rows=rows))
        host = self.rng.choice(["hooks.example.com", "flaky.example.net"])
        # A third of the webhooks are scheduled for later, to keep delayed jobs in view.
        delay = self.rng.choice([0, 0, 5, 15, 30])
        return await tasks.deliver_webhook.enqueue(
            tasks.Webhook(url=f"https://{host}/events/{n}", event="invoice.paid"), delay=delay
        )

    async def burst(self) -> int:
        """A traffic spike of one kind of job, inserted with a single statement."""
        size = self.settings.burst_size
        kind = self.rng.choice(["email", "image", "charge"])
        start = self.sequence
        self.sequence += size
        ids: list[int]
        if kind == "email":
            ids = await tasks.send_welcome.enqueue_many(
                [
                    tasks.WelcomeEmail(user_id=start + i, email=f"user{start + i}@example.com")
                    for i in range(size)
                ],
                idempotency_keys=[f"welcome:{start + i}" for i in range(size)],
            )
        elif kind == "image":
            ids = await tasks.resize_image.enqueue_many(
                [tasks.ResizeImage(image_id=start + i, width=640) for i in range(size // 2)]
            )
        else:
            ids = await tasks.charge.enqueue_many(
                [
                    tasks.Charge(invoice_id=start + i, amount_cents=self.rng.randint(500, 50_000))
                    for i in range(size)
                ]
            )
        self.bursts += 1
        self.enqueued += len(ids)
        log.info("burst: %d %s jobs", len(ids), kind)
        return len(ids)

    # -- pacing ---------------------------------------------------------------------------------

    def current_rate(self, elapsed: float) -> float:
        """Jobs per second now: a slow wave around the configured average."""
        return max(0.0, self.settings.rate * (1.0 + 0.5 * math.sin(elapsed / 25.0)))

    def due(self, rate: float, dt: float) -> int:
        """How many jobs to enqueue after ``dt`` seconds, carrying the fraction over."""
        self._carry += rate * dt
        count = int(self._carry)
        self._carry -= count
        return count

    async def backlog(self) -> int:
        row = await tasks.app.pool.fetchval(READY)
        return int(row)

    async def run(self, tick: float = 0.25) -> None:
        started = self.clock()
        next_burst = started + self.settings.burst_every * self.rng.uniform(0.3, 0.8)
        next_check = started
        previous = started
        while True:
            await asyncio.sleep(tick)
            now = self.clock()
            dt, previous = now - previous, now
            try:
                if now >= next_check:
                    # One cheap query per second keeps the demo from burying itself.
                    self.paused = await self.backlog() > self.settings.max_backlog
                    next_check = now + 1.0
                if self.paused:
                    continue
                if now >= next_burst:
                    await self.burst()
                    next_burst = now + self.settings.burst_every * self.rng.uniform(0.7, 1.3)
                for _ in range(self.due(self.current_rate(now - started), dt)):
                    await self.enqueue_one()
                    self.enqueued += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("generator step failed; continuing")
                await asyncio.sleep(2.0)
