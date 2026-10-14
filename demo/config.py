"""Demo settings, all read from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BANNER = (
    "Live demo: a synthetic workload runs on a single PostgreSQL. Traffic comes in bursts, "
    "some jobs fail and retry, and every couple of minutes a worker is killed with SIGKILL "
    "to show lease recovery. This page is read-only."
)


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    return float(raw) if raw not in (None, "") else default


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True, slots=True)
class Settings:
    dsn: str
    read_only_dsn: str | None = None
    """Used by the public dashboard when set: a role that can only SELECT."""
    host: str = "127.0.0.1"
    port: int = 8080
    root_path: str = ""
    banner: str = DEFAULT_BANNER

    workers: int = 3
    concurrency: int = 8
    lease_seconds: float = 10.0
    crash_every: float = 150.0
    """Seconds between simulated worker crashes; 0 disables them."""
    respawn_after: float = 14.0
    """Must exceed ``lease_seconds`` so the crashed worker's jobs are recovered first."""

    rate: float = 8.0
    """Average jobs per second enqueued outside bursts."""
    burst_every: float = 45.0
    burst_size: int = 240
    max_backlog: int = 1500
    """The generator pauses while more than this many due jobs wait."""

    retention_seconds: float = 600.0
    max_finished: int = 6000
    janitor_interval: float = 20.0

    repo_root: Path = Path(__file__).resolve().parent.parent

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        dsn = env.get("PGQUEUE_DSN")
        if not dsn:
            raise SystemExit(
                "set PGQUEUE_DSN, e.g. postgresql://demo:secret@127.0.0.1/pgqueue_demo"
            )
        defaults = cls(dsn=dsn)
        root_path = env.get("DEMO_ROOT_PATH", "").strip().rstrip("/")
        return cls(
            dsn=dsn,
            read_only_dsn=env.get("PGQUEUE_READONLY_DSN") or None,
            host=env.get("DEMO_HOST", defaults.host),
            port=_int(env, "DEMO_PORT", defaults.port),
            root_path=root_path,
            banner=env.get("DEMO_BANNER", defaults.banner),
            workers=_int(env, "DEMO_WORKERS", defaults.workers),
            concurrency=_int(env, "DEMO_CONCURRENCY", defaults.concurrency),
            lease_seconds=_float(env, "DEMO_LEASE_SECONDS", defaults.lease_seconds),
            crash_every=_float(env, "DEMO_CRASH_EVERY", defaults.crash_every),
            respawn_after=_float(env, "DEMO_RESPAWN_AFTER", defaults.respawn_after),
            rate=_float(env, "DEMO_RATE", defaults.rate),
            burst_every=_float(env, "DEMO_BURST_EVERY", defaults.burst_every),
            burst_size=_int(env, "DEMO_BURST_SIZE", defaults.burst_size),
            max_backlog=_int(env, "DEMO_MAX_BACKLOG", defaults.max_backlog),
            retention_seconds=_float(env, "DEMO_RETENTION_SECONDS", defaults.retention_seconds),
            max_finished=_int(env, "DEMO_MAX_FINISHED", defaults.max_finished),
            janitor_interval=_float(env, "DEMO_JANITOR_INTERVAL", defaults.janitor_interval),
        )
