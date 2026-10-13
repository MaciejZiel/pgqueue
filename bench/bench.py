"""Reproducible benchmark: pgqueue on PostgreSQL, optionally next to Celery on Redis.

    make bench                      # full run: about an hour, writes bench/results.json
    make bench-quick                # smoke test with tiny numbers, results are meaningless
    uv run --group bench python -m bench.bench --runs 3 --skip-celery

What is measured (all on one machine, workers are separate OS processes):

* enqueue: latency per call and jobs/s for one client, ten concurrent clients and
  ``enqueue_many`` (pgqueue only, Celery has no bulk publish)
* drain: a backlog is loaded first, then 1/2/4/8 worker processes (32 slots each) empty
  it. Throughput is taken from the completion timestamps of the 20th to the 95th
  percentile job, so worker start-up stagger and the straggling tail are excluded.
* pickup: jobs arrive at a fixed rate into idle workers; latency is the time from
  enqueue to the first line of the handler.
* payload size: the drain scenario at 4 workers with 256 B, 2 KiB and 16 KiB payloads.

The handler is a no-op, so the numbers describe the queue, not your work. Every
scenario is repeated ``--runs`` times. The two systems run back to back with a cool-down
(``--cooldown``, default 30 s) in between, and the order alternates with the run number
(odd runs: pgqueue first, even runs: Celery first), so a systematic order effect cancels
out over an even number of runs. Each record stores its position, the 1-minute load average
before and after the measurement, and the raw per-run numbers are kept in the results file
(``schema_version`` 2; version 1 files have no such field and a fixed pgqueue-first order).
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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from bench.payload import SIZES, make_payload
from bench.tasks import Email, app, noop
from pgqueue.migrate import upgrade

ROOT = Path(__file__).resolve().parent.parent
DSN = os.environ.get(
    "PGQUEUE_BENCH_DSN", "postgresql://pgqueue:pgqueue@localhost:55432/pgqueue_bench"
)
REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://localhost:56379/0")
SLOTS = 32  # concurrent jobs per worker process, for both systems
WINDOW = (0.20, 0.95)  # share of the backlog that defines the steady-state window

Record = dict[str, Any]


def summarize(samples_ms: list[float]) -> dict[str, float]:
    q = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return {
        "p50": q[49],
        "p95": q[94],
        "p99": q[98],
        "max": max(samples_ms),
        "n": float(len(samples_ms)),
    }


def steady_rate(finished: list[float]) -> float:
    """Jobs/s between the 20th and the 95th percentile completion."""
    finished = sorted(finished)
    lo, hi = int(len(finished) * WINDOW[0]), int(len(finished) * WINDOW[1])
    return (hi - lo) / (finished[hi] - finished[lo])


def loadavg() -> str:
    return sh("cut -d' ' -f1 /proc/loadavg")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


# -- pgqueue side ---------------------------------------------------------------------------------


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


async def pg_reset() -> None:
    await app.pool.execute("TRUNCATE pgqueue_jobs, pgqueue_workers RESTART IDENTITY")
    await app.pool.execute("VACUUM ANALYZE pgqueue_jobs")


def pg_workers(count: int, *extra: str) -> list[subprocess.Popen[bytes]]:
    # 8 pooled connections per worker (+1 for LISTEN) keeps 8 workers well under
    # Postgres' default max_connections of 100.
    env = {**os.environ, "PGQUEUE_DSN": DSN, "BENCH_POOL_SIZE": "8"}
    cmd = [sys.executable, "-m", "pgqueue", "--log-level", "WARNING", "worker", "bench.tasks:app"]
    cmd += ["--queues", "bench", "--concurrency", str(SLOTS), "--no-periodic", *extra]
    return [subprocess.Popen(cmd, cwd=ROOT, env=env) for _ in range(count)]


def stop(procs: list[subprocess.Popen[bytes]]) -> None:
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


async def pg_wait_registered(count: int) -> None:
    deadline = time.monotonic() + 60
    while await app.pool.fetchval("SELECT count(*) FROM pgqueue_workers") < count:
        if time.monotonic() > deadline:
            raise TimeoutError("workers did not register")
        await asyncio.sleep(0.05)


async def pg_wait_done(total: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while await app.pool.fetchval("SELECT count(*) FROM pgqueue_jobs WHERE status <> 'succeeded'"):
        if time.monotonic() > deadline:
            raise TimeoutError("backlog did not drain")
        await asyncio.sleep(0.1)


async def pg_preload(n: int, size: int) -> None:
    for first in range(0, n, 5000):
        batch = [Email(**make_payload(i, size)) for i in range(first, min(n, first + 5000))]
        await noop.enqueue_many(batch)


async def pg_enqueue(n: int, size: int, clients: int, bulk: int) -> Record:
    await pg_reset()
    payloads = [Email(**make_payload(i, size)) for i in range(n)]
    lat: list[float] = []

    async def one(offset: int) -> None:
        for i in range(offset, n, clients):
            t = time.perf_counter()
            await noop.enqueue(payloads[i])
            lat.append((time.perf_counter() - t) * 1000)

    start = time.perf_counter()
    if bulk:
        for first in range(0, n, bulk):
            t = time.perf_counter()
            await noop.enqueue_many(payloads[first : first + bulk])
            lat.append((time.perf_counter() - t) * 1000)
    else:
        await asyncio.gather(*(one(c) for c in range(clients)))
    seconds = time.perf_counter() - start
    return {"jobs": n, "seconds": seconds, "rate": n / seconds, "latency_ms": summarize(lat)}


async def pg_drain(n: int, size: int, workers: int) -> Record:
    await pg_reset()
    await pg_preload(n, size)
    procs = pg_workers(workers)
    try:
        await pg_wait_registered(workers)
        await pg_wait_done(n, timeout=600)
    finally:
        stop(procs)
    rows = await app.pool.fetch(
        "SELECT extract(epoch FROM finished_at) AS f FROM pgqueue_jobs WHERE status = 'succeeded'"
    )
    return {"jobs": n, "rate": steady_rate([float(r["f"]) for r in rows])}


async def pg_pickup(rate: int, seconds: int, size: int, workers: int) -> Record:
    await pg_reset()
    procs = pg_workers(workers)
    total = rate * seconds
    payloads = [Email(**make_payload(i, size)) for i in range(total)]
    try:
        await pg_wait_registered(workers)
        await asyncio.sleep(1)
        interval = 1 / rate
        start = time.perf_counter()
        for i, payload in enumerate(payloads):
            await noop.enqueue(payload)
            delay = start + (i + 1) * interval - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
        await pg_wait_done(total, timeout=120)
    finally:
        stop(procs)
    rows = await app.pool.fetch(
        "SELECT extract(epoch FROM started_at - created_at) * 1000 AS ms FROM pgqueue_jobs"
    )
    return {"jobs": total, "latency_ms": summarize([float(r["ms"]) for r in rows])}


# -- Celery side ----------------------------------------------------------------------------------


class CeleryBench:
    def __init__(self) -> None:
        import redis
        from celery import Celery

        from bench import celery_tasks

        self.redis = redis.Redis.from_url(REDIS_URL)
        self.tasks = celery_tasks
        self.app: Celery = celery_tasks.app
        self.noop = celery_tasks.noop
        self.local = threading.local()

    def reset(self) -> None:
        self.redis.flushall()

    def workers(self, count: int) -> list[subprocess.Popen[bytes]]:
        env = {**os.environ, "BENCH_REDIS_URL": REDIS_URL}
        procs = [
            subprocess.Popen(
                [
                    *[sys.executable, "-m", "celery", "-A", "bench.celery_tasks", "worker"],
                    *["-P", "threads", "-c", str(SLOTS), "-Q", "bench", "-l", "WARNING"],
                    *["--without-gossip", "--without-mingle", "-n", f"w{i}@%h"],
                ],
                cwd=ROOT,
                env=env,
                stdout=subprocess.DEVNULL,  # the start-up banner
            )
            for i in range(count)
        ]
        deadline = time.monotonic() + 60
        while len(self.app.control.ping(timeout=1.0)) < count:
            if time.monotonic() > deadline:
                stop(procs)
                raise TimeoutError("celery workers did not start")
        return procs

    def send(self, i: int, size: int) -> None:
        payload = make_payload(i, size, sent_at=time.time())
        self.noop.apply_async(args=[payload], queue="bench")

    def wait_done(self, total: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while self.redis.llen("bench:finished") < total:
            if time.monotonic() > deadline:
                raise TimeoutError("celery backlog did not drain")
            time.sleep(0.1)

    def enqueue(self, n: int, size: int, clients: int) -> Record:
        self.reset()
        lat: list[float] = []

        def timed(i: int) -> None:
            t = time.perf_counter()
            self.send(i, size)
            lat.append((time.perf_counter() - t) * 1000)

        start = time.perf_counter()
        if clients == 1:
            for i in range(n):
                timed(i)
        else:
            with ThreadPoolExecutor(clients) as pool:
                list(pool.map(timed, range(n)))
        seconds = time.perf_counter() - start
        return {"jobs": n, "seconds": seconds, "rate": n / seconds, "latency_ms": summarize(lat)}

    def drain(self, n: int, size: int, workers: int) -> Record:
        self.reset()
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda i: self.send(i, size), range(n)))
        procs = self.workers(workers)
        try:
            self.wait_done(n, timeout=600)
        finally:
            stop(procs)
        finished = [float(v) for v in self.redis.lrange("bench:finished", 0, -1)]
        return {"jobs": n, "rate": steady_rate(finished)}

    def pickup(self, rate: int, seconds: int, size: int, workers: int) -> Record:
        self.reset()
        procs = self.workers(workers)
        total = rate * seconds
        try:
            time.sleep(1)
            interval = 1 / rate
            start = time.perf_counter()
            for i in range(total):
                self.send(i, size)
                delay = start + (i + 1) * interval - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            self.wait_done(total, timeout=120)
        finally:
            stop(procs)
        ms = [float(v) * 1000 for v in self.redis.lrange("bench:pickup_s", 0, -1)]
        return {"jobs": total, "latency_ms": summarize(ms)}


# -- orchestration --------------------------------------------------------------------------------


def sh(cmd: str) -> str:
    env = {**os.environ, "LC_ALL": "C"}
    out = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False, env=env)
    return out.stdout.strip()


async def environment(celery: bool) -> dict[str, str]:
    pg = await asyncpg.connect(DSN)
    try:
        version = await pg.fetchval("SHOW server_version")
        settings = {
            k: await pg.fetchval(f"SHOW {k}")
            for k in ("shared_buffers", "synchronous_commit", "fsync", "max_connections")
        }
    finally:
        await pg.close()
    info = {
        "cpu": sh("lscpu | sed -n 's/^Model name:[ \\t]*//p'") or platform.processor(),
        "logical_cpus": str(os.cpu_count()),
        "memory": sh("free -h | awk '/^Mem/ {print $2}'"),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "postgres": f"{version} {settings}",
        "load_average_at_start": sh("cut -d' ' -f1-3 /proc/loadavg"),
    }
    if celery:
        import celery as celery_pkg
        import redis

        r = redis.Redis.from_url(REDIS_URL)
        info["broker"] = (
            f"{r.info('server').get('server_name', 'redis')} {r.info('server')['redis_version']}"
        )
        info["celery"] = f"celery {celery_pkg.__version__}, redis-py {redis.__version__}"
        info["persistence"] = (
            str(r.config_get("save")) + " appendonly=" + str(r.config_get("appendonly"))
        )
    return info


async def main(args: argparse.Namespace) -> None:
    quick = args.quick
    runs = 1 if quick else args.runs
    jobs = 2_000 if quick else args.jobs
    rate = args.rate
    secs = 3 if quick else args.seconds
    sweep = [int(w) for w in args.workers.split(",")]
    size = SIZES[args.payload]
    celery = None if args.skip_celery else CeleryBench()
    cooldown = 0.0 if quick else args.cooldown

    await prepare_database()
    await app.connect()
    env = await environment(celery is not None)
    records: list[Record] = []

    async def measure(
        system: str, scenario: str, run: int, position: str, call: Any, **kw: Any
    ) -> None:
        load_before = loadavg()
        result = call()
        if asyncio.iscoroutine(result):
            result = await result
        rec = {
            "system": system,
            "scenario": scenario,
            "run": run,
            "position": position,
            **kw,
            **result,
            "load_1min_before": load_before,
            "load_1min": loadavg(),  # after the measurement; kept for the v1 readers
        }
        records.append(rec)
        short = {k: (round(v, 1) if isinstance(v, float) else v) for k, v in result.items()}
        log(f"{system:8} {scenario:10} run {run} {kw} {short}")

    async def both(run: int, scenario: str, pg_call: Any, celery_call: Any, **kw: Any) -> None:
        """Run the systems back to back; odd runs start with pgqueue, even runs with Celery."""
        calls = [("pgqueue", pg_call)]
        if celery:
            calls.append(("celery", celery_call))
        if run % 2 == 0:
            calls.reverse()
        for i, (system, call) in enumerate(calls):
            if i and cooldown:
                await asyncio.sleep(cooldown)  # let checkpointer and autovacuum settle
            await measure(system, scenario, run, "first" if i == 0 else "second", call, **kw)

    try:
        for run in range(1, runs + 1):
            log(f"run {run}/{runs} (load average 1 min: {loadavg()})")
            for name, clients, bulk in [("single", 1, 0), ("ten", 10, 0), ("bulk", 1, 1000)]:
                n = jobs // 4 if name != "bulk" else jobs * 2
                kw = {"payload": size, "variant": name}
                if name == "bulk":  # Celery has no bulk publish
                    await measure(
                        "pgqueue",
                        "enqueue",
                        run,
                        "only",
                        partial(pg_enqueue, n, size, clients, bulk),
                        **kw,
                    )
                    continue
                await both(
                    run,
                    "enqueue",
                    partial(pg_enqueue, n, size, clients, bulk),
                    celery and partial(celery.enqueue, n, size, clients),
                    **kw,
                )
            for w in sweep:
                await both(
                    run,
                    "drain",
                    partial(pg_drain, jobs, size, w),
                    celery and partial(celery.drain, jobs, size, w),
                    workers=w,
                    payload=size,
                )
            for w in sweep:
                await both(
                    run,
                    "pickup",
                    partial(pg_pickup, rate, secs, size, w),
                    celery and partial(celery.pickup, rate, secs, size, w),
                    workers=w,
                    payload=size,
                    arrival_rate=rate,
                )
            for label, psize in SIZES.items():
                n = jobs // 2
                await both(
                    run,
                    "drain-size",
                    partial(pg_drain, n, psize, 4),
                    celery and partial(celery.drain, n, psize, 4),
                    workers=4,
                    payload=psize,
                    label=label,
                )
    finally:
        env["load_average_at_end"] = sh("cut -d' ' -f1-3 /proc/loadavg")
        await app.close()
        out = Path(args.out)
        out.write_text(
            json.dumps(
                {
                    "schema_version": 2,  # 1 (no field): fixed order, one load value per record
                    "environment": env,
                    "parameters": {
                        "runs": runs,
                        "jobs": jobs,
                        "slots_per_worker": SLOTS,
                        "arrival_rate": rate,
                        "pickup_seconds": secs,
                        "workers": sweep,
                        "payload_bytes": size,
                        "quick": quick,
                        "counterbalanced": True,
                        "cooldown_s": cooldown,
                    },
                    "records": records,
                },
                indent=1,
            )
        )
        log(f"raw numbers written to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="tiny smoke run")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--jobs", type=int, default=20_000, help="backlog size for drain runs")
    parser.add_argument("--rate", type=int, default=200, help="arrivals/s for the pickup runs")
    parser.add_argument("--seconds", type=int, default=15, help="length of a pickup run")
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--payload", choices=list(SIZES), default="medium")
    parser.add_argument(
        "--cooldown",
        type=float,
        default=30.0,
        help="seconds to wait between the two systems of a pair (0 with --quick)",
    )
    parser.add_argument("--skip-celery", action="store_true")
    parser.add_argument("--out", default=str(ROOT / "bench" / "results.json"))
    asyncio.run(main(parser.parse_args()))
