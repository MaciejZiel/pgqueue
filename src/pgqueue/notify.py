"""A resilient LISTEN connection that wakes workers when jobs are enqueued."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import asyncpg

log = logging.getLogger("pgqueue.notify")


class Listener:
    """Holds one dedicated connection in LISTEN mode and calls ``on_notify``.

    LISTEN/NOTIFY is an optimisation, never a correctness requirement:
    notifications are not durable (one sent while we are reconnecting is
    lost) and are not sent at all for jobs that become due later. Workers
    therefore keep polling; the listener only turns "up to one poll interval"
    of latency into "a few milliseconds" for the common case.
    """

    def __init__(
        self,
        dsn: str,
        channel: str,
        on_notify: Callable[[str], None],
        *,
        on_reconnect: Callable[[], None] | None = None,
        max_backoff: float = 30.0,
    ) -> None:
        self.dsn = dsn
        self.channel = channel
        self.on_notify = on_notify
        self.on_reconnect = on_reconnect
        self.max_backoff = max_backoff
        self.connected = asyncio.Event()
        self.connections = 0

    async def run(self) -> None:
        """Listen forever, reconnecting with backoff; cancel to stop."""
        backoff = 0.5
        while True:
            lost = asyncio.Event()
            conn: Any = None
            try:
                conn = await asyncpg.connect(self.dsn)
                conn.add_termination_listener(lambda _conn, lost=lost: lost.set())
                await conn.add_listener(self.channel, self._dispatch)
                self.connections += 1
                self.connected.set()
                if self.connections > 1:
                    log.info("LISTEN connection re-established")
                    if self.on_reconnect is not None:
                        # Anything enqueued while we were away was not seen.
                        self.on_reconnect()
                backoff = 0.5
                await self._wait_until_lost(conn, lost)
                log.warning("LISTEN connection lost; falling back to polling")
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                log.warning("LISTEN connection failed (%s); retrying in %.1fs", exc, backoff)
            finally:
                self.connected.clear()
                if conn is not None and not conn.is_closed():
                    conn.terminate()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff)

    async def _wait_until_lost(self, conn: Any, lost: asyncio.Event) -> None:
        # The termination callback only fires once asyncpg notices; a cheap
        # periodic query makes sure a silently dead TCP connection is noticed too.
        while not lost.is_set():
            try:
                async with asyncio.timeout(5):
                    await lost.wait()
            except TimeoutError:
                try:
                    async with asyncio.timeout(5):
                        await conn.execute("SELECT 1")
                except (TimeoutError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError):
                    return

    def _dispatch(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        self.on_notify(payload)
