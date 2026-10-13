"""Shared live snapshots for the dashboard.

Computing a snapshot costs a handful of queries. Doing that per viewer would
make the database load grow with the audience, which is not acceptable for a
public dashboard. :class:`LiveFeed` computes one snapshot per interval, no
matter how many browsers are subscribed (or polling), and does nothing at all
while nobody is looking.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncGenerator
from typing import Final

from sqlalchemy.ext.asyncio import AsyncEngine

from pgqueue.admin import queries

log = logging.getLogger("pgqueue.admin.live")

KEEPALIVE_SECONDS: Final = 15.0


class TooManySubscribers(Exception):
    """Raised when the configured number of concurrent streams is reached."""


class LiveFeed:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        read_only: bool,
        interval: float = 1.0,
        max_subscribers: int = 200,
    ) -> None:
        self._engine = engine
        self._read_only = read_only
        self._interval = interval
        self._max_subscribers = max_subscribers
        self._latest: bytes | None = None
        self._computed_at = 0.0
        self._version = 0
        self._changed = asyncio.Condition()
        self._producer: asyncio.Task[None] | None = None
        self._subscribers = 0
        self._compute_lock = asyncio.Lock()
        self._finished = queries.FinishedCounts()

    @property
    def max_subscribers(self) -> int:
        return self._max_subscribers

    @property
    def subscribers(self) -> int:
        return self._subscribers

    async def _compute(self) -> bytes:
        async with self._engine.connect() as conn:
            snapshot = await queries.snapshot(
                conn, read_only=self._read_only, finished=self._finished
            )
        return snapshot.model_dump_json().encode()

    async def current(self) -> bytes:
        """The latest snapshot, recomputed at most once per interval (for polling clients)."""
        async with self._compute_lock:
            if self._latest is None or time.monotonic() - self._computed_at >= self._interval:
                self._latest = await self._compute()
                self._computed_at = time.monotonic()
            return self._latest

    async def _produce(self) -> None:
        while True:
            started = time.monotonic()
            try:
                payload = await self._compute()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("could not compute the live snapshot; retrying")
                await asyncio.sleep(2.0)
                continue
            async with self._changed:
                self._latest = payload
                self._computed_at = time.monotonic()
                self._version += 1
                self._changed.notify_all()
            await asyncio.sleep(max(0.05, self._interval - (time.monotonic() - started)))

    async def subscribe(self) -> AsyncGenerator[bytes | None]:
        """Yield each new snapshot as JSON; ``None`` is a keep-alive tick."""
        if self._subscribers >= self._max_subscribers:
            raise TooManySubscribers
        self._subscribers += 1
        if self._producer is None or self._producer.done():
            self._producer = asyncio.create_task(self._produce())
        seen = 0

        def newer(last: int | None = None) -> bool:
            return self._version > (seen if last is None else last)

        try:
            while True:
                async with self._changed:
                    try:
                        await asyncio.wait_for(
                            self._changed.wait_for(newer),
                            KEEPALIVE_SECONDS,
                        )
                    except TimeoutError:
                        yield None
                        continue
                    seen = self._version
                    payload = self._latest
                if payload is not None:
                    yield payload
        finally:
            self._subscribers -= 1
            if self._subscribers == 0 and self._producer is not None:
                self._producer.cancel()
                self._producer = None

    async def close(self) -> None:
        if self._producer is not None:
            self._producer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._producer
            self._producer = None
