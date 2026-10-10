"""Measure pgqueue throughput and latency against a real Postgres.

    docker compose up -d postgres
    uv run python -m bench.bench            # full run, prints a Markdown report
    uv run python -m bench.bench --quick    # smaller numbers, for a smoke test

Every scenario runs against a freshly truncated ``pgqueue_bench`` database.
Workers are separate OS processes started with the real CLI, exactly as in
production, with a no-op handler so that the numbers measure the queue's
own overhead (claim, heartbeat, completion), not handler work.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from bench.tasks import Noop, app, noop
from pgqueue.migrate import upgrade

ROOT = Path(__file__).resolve().parent.parent
DSN = os.environ.get(
    "PGQUEUE_BENCH_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue_bench"
)


@dataclass
class Result:
    scenario: str
    jobs: int
    seconds: float
    detail: str = ""

    @property
    def rate(self) -> float:
        return self.jobs / self.seconds


async def prepare_database() -> None:
    parts = urlsplit(DSN)
    name = parts.path.lstrip("/")
    admin = await asyncpg.connect(urlunsplit(parts._replace(path="/postgres")))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    await asyncio.to_thread(upgrade, DSN)


async def reset() -> None:
    await app.pool.execute("TRUNCATE pgqueue_jobs, pgqueue_workers RESTART IDENTITY")
    await app.pool.execute("VACUUM ANALYZE pgqueue_jobs")


async def timed(fn: Callable[[], Awaitable[object]]) -> float:
    start = time.perf_counter()
    await fn()
    return time.perf_counter() - start


# -- enqueue ------------------------------------------------------------------------------------


async def bench_enqueue_sequential(n: int) -> Result:
    await reset()

    async def run() -> None:
        for i in range(n):
            await noop.enqueue(Noop(n=i))

    return Result("enqueue, one at a time (1 client)", n, await timed(run))


async def bench_enqueue_concurrent(n: int, clients: int) -> Result:
    await reset()

    async def client(offset: int) -> None:
        for i in range(offset, n, clients):
            await noop.enqueue(Noop(n=i))

    async def run() -> None:
        await asyncio.gather(*(client(c) for c in range(clients)))

    return Result(f"enqueue, one at a time ({clients} concurrent clients)", n, await timed(run))


async def bench_enqueue_bulk(n: int, batch: int) -> Result:
    await reset()

    async def run() -> None:
        for start in range(0, n, batch):
            await noop.enqueue_many([Noop(n=i) for i in range(start, min(n, start + batch))])

    return Result(f"enqueue_many, batches of {batch}", n, await timed(run))


# -- processing ---------------------------------------------------------------------------------


def start_workers(count: int, concurrency: int, *extra: str) -> list[subprocess.Popen[bytes]]:
    # 8 pooled connections per worker (+1 for LISTEN) is plenty for no-op jobs and
    # keeps 8 workers under Postgres' default max_connections of 100.
    env = {**os.environ, "PGQUEUE_DSN": DSN, "BENCH_POOL_SIZE": "8"}
    return [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "pgqueue",
                "--log-level",
                "WARNING",
                "worker",
                "bench.tasks:app",
                "--queues",
                "bench",
                "--concurrency",
                str(concurrency),
                "--no-periodic",
                *extra,
            ],
            cwd=ROOT,
            env=env,
        )
        for _ in range(count)
    ]


def stop_workers(procs: list[subprocess.Popen[bytes]]) -> None:
    for proc in procs:
        proc.terminate()
    for proc in procs:
        proc.wait(timeout=60)


async def wait_registered(count: int) -> None:
    while await app.pool.fetchval("SELECT count(*) FROM pgqueue_workers") < count:
        await asyncio.sleep(0.05)


async def remaining() -> int:
    value: int = await app.pool.fetchval(
        "SELECT count(*) FROM pgqueue_jobs WHERE status <> 'succeeded'"
    )
    return value


async def bench_drain(n: int, workers: int, concurrency: int) -> Result:
    """Pre-load n jobs, then measure how fast ``workers`` processes drain them."""
    await reset()
    for first in range(0, n, 5000):
        await noop.enqueue_many([Noop(n=i) for i in range(first, min(n, first + 5000))])
    procs = start_workers(workers, concurrency)
    try:
        await wait_registered(workers)
        start = time.perf_counter()
        while await remaining():
            await asyncio.sleep(0.02)
        seconds = time.perf_counter() - start
    finally:
        stop_workers(procs)
    return Result(
        f"process backlog: {workers} worker process(es) x {concurrency} concurrency",
        n,
        seconds,
    )


async def bench_latency(rate: int, seconds: int, *, listen: bool) -> Result:
    """Steady trickle of jobs into idle workers: how long from enqueue to start?"""
    await reset()
    extra = () if listen else ("--no-listen",)
    procs = start_workers(2, 16, "--poll-interval", "1.0", *extra)
    total = rate * seconds
    try:
        await wait_registered(2)
        await asyncio.sleep(1)
        interval = 1 / rate
        start = time.perf_counter()
        for i in range(total):
            await noop.enqueue(Noop(n=i))
            delay = start + (i + 1) * interval - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
        while await remaining():
            await asyncio.sleep(0.05)
    finally:
        stop_workers(procs)
    rows = await app.pool.fetch(
        "SELECT extract(epoch FROM started_at - created_at) * 1000 AS pickup_ms FROM pgqueue_jobs"
    )
    pickup = sorted(float(r["pickup_ms"]) for r in rows)
    q = statistics.quantiles(pickup, n=100)
    detail = f"pickup latency p50 {q[49]:.1f} ms, p95 {q[94]:.1f} ms, p99 {q[98]:.1f} ms"
    mode = "LISTEN/NOTIFY + 1 s polling" if listen else "polling only (1 s interval)"
    return Result(f"latency at {rate} jobs/s, {mode}", total, seconds, detail)


# -- report ---------------------------------------------------------------------------------------


def hardware() -> dict[str, str]:
    def run(cmd: str) -> str:
        try:
            # Force English output: lscpu and free are localised.
            env = {**os.environ, "LC_ALL": "C"}
            return subprocess.run(
                cmd, shell=True, capture_output=True, text=True, check=False, env=env
            ).stdout.strip()
        except OSError:  # pragma: no cover
            return "unknown"

    cpu = run("lscpu | sed -n 's/^Model name:[ \\t]*//p'") or platform.processor()
    return {
        "cpu": f"{cpu} ({os.cpu_count()} logical CPUs)",
        "memory": run("free -h | awk '/^Mem/ {print $2}'") + " RAM",
        "disk": "NVMe SSD; Postgres 17 in Docker (default config) on the same machine",
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "postgres": run("docker exec pgqueue-postgres-1 postgres --version 2>/dev/null"),
        "load_before": run("cat /proc/loadavg | cut -d' ' -f1-3"),
    }


async def main(quick: bool) -> None:
    await prepare_database()
    await app.connect()
    hw = hardware()
    scale = 0.2 if quick else 1.0
    results: list[Result] = []
    try:
        results.append(await bench_enqueue_sequential(int(5_000 * scale)))
        results.append(await bench_enqueue_concurrent(int(20_000 * scale), clients=10))
        results.append(await bench_enqueue_bulk(int(100_000 * scale), batch=1000))
        for workers, concurrency in [(1, 32), (2, 32), (4, 32), (8, 32)]:
            results.append(await bench_drain(int(50_000 * scale), workers, concurrency))
        results.append(await bench_latency(100, 10 if not quick else 3, listen=True))
        results.append(await bench_latency(100, 10 if not quick else 3, listen=False))
    finally:
        await app.close()

    print("\n## Environment\n")
    for key, value in hw.items():
        print(f"- **{key}**: {value}")
    print("\n## Results\n")
    print("| Scenario | Jobs | Seconds | Jobs/s | Notes |")
    print("|---|---:|---:|---:|---|")
    for r in results:
        print(f"| {r.scenario} | {r.jobs:,} | {r.seconds:.2f} | {r.rate:,.0f} | {r.detail} |")
    out = ROOT / "bench" / "results.json"
    out.write_text(
        json.dumps(
            {"environment": hw, "results": [asdict(r) | {"rate": r.rate} for r in results]},
            indent=2,
        )
    )
    print(f"\nRaw numbers written to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    asyncio.run(main(parser.parse_args().quick))
