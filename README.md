# pgqueue

[![CI](https://github.com/MaciejZiel/pgqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/MaciejZiel/pgqueue/actions/workflows/ci.yml)
![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776ab)
![PostgreSQL 16 | 17 | 18](https://img.shields.io/badge/postgres-16%20%7C%2017%20%7C%2018-336791)
![mypy strict](https://img.shields.io/badge/mypy-strict-2a6db2)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**A background job queue that runs on the PostgreSQL you already have.** No Redis, no
RabbitMQ, no second system to operate, back up and keep consistent with your data.

pgqueue is a small job queue library built around one PostgreSQL feature: `SELECT … FOR UPDATE SKIP LOCKED`. It gives you typed tasks, retries
with backoff, a dead-letter state, scheduled and cron jobs, crash recovery through leases,
LISTEN/NOTIFY wake-ups, a dashboard and Prometheus metrics, and it lets you enqueue a job
**in the same transaction** as the data it refers to.

![The live dashboard, recorded against the demo workload](docs/dashboard-live.gif)

```python
from pydantic import BaseModel
from pgqueue import PgQueue, PermanentError

app = PgQueue("postgresql://localhost/shop")


class Charge(BaseModel):
    invoice_id: int
    amount_cents: int


@app.task(queue="billing", max_attempts=5, priority=10)
async def charge(payload: Charge) -> dict[str, str]:
    if payload.amount_cents <= 0:
        raise PermanentError("nothing to charge")  # -> failed, no retries
    return {"charge_id": await provider.charge(payload.invoice_id)}


@app.periodic("0 3 * * *")  # every day at 03:00 UTC
@app.task()
async def nightly_report() -> None: ...


async with app:
    async with app.pool.acquire() as conn, conn.transaction():
        await conn.execute("UPDATE invoices SET status = 'charging' WHERE id = $1", 42)
        await charge.enqueue(Charge(invoice_id=42, amount_cents=1999), connection=conn)
        # the job exists if and only if this transaction commits
```

```console
$ pgqueue migrate
$ pgqueue worker shop.jobs:app --queues billing,default --concurrency 32
$ pgqueue admin --port 8080          # dashboard, JSON API and /metrics
```

## Contents

- [Why a queue in Postgres?](#why-a-queue-in-postgres)
- [Features](#features)
- [Quick start](#quick-start)
- [The live demo](#the-live-demo)
- [Self-hosting on Termux](#self-hosting-on-termux)
- [Architecture](#architecture)
- [Guarantees and failure semantics](#guarantees-and-failure-semantics)
- [Benchmarks](#benchmarks)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [When to use something else](#when-to-use-something-else)
- [Limitations](#limitations)
- [Development](#development)

## Why a queue in Postgres?

Most applications that need background jobs already have a PostgreSQL database. Adding a
broker means another service to run, monitor and secure, and, more subtly, a **dual-write
problem**: if you commit an order and then publish "send the confirmation e-mail" to Redis,
a crash between the two leaves either an order without an e-mail or an e-mail for an order
that was rolled back. Keeping the queue in the same database turns that into one atomic
transaction.

Postgres has had the primitive that makes this efficient since 9.5: `SKIP LOCKED` lets many
workers grab different rows of the same table concurrently without blocking each other. For
workloads of up to a few thousand jobs per second, a Postgres queue is often the simpler
option. pgqueue is an exploration of what it takes to make one robust: leases, fencing,
retries and crash recovery, with tests for each failure mode.

## Features

| | |
|---|---|
| **Typed tasks** | `@app.task` handlers declare a pydantic payload model; payloads are validated on enqueue and again on execution. mypy checks `enqueue()` calls against the model. Sync and async handlers. |
| **Claiming** | One round-trip `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)` per batch, walking a partial index per queue. Priorities, then schedule time, then FIFO. |
| **Retries** | Capped exponential backoff with jitter (`RetryPolicy`), `raise Retry(delay=…)` to honour a `Retry-After`, `raise PermanentError` to fail fast. |
| **Dead letters** | Jobs that exhaust `max_attempts` become `dead`; requeue one or a whole queue from the dashboard or API. |
| **Scheduling** | `delay=` / `run_at=` (database clock), cron-like `@app.periodic("*/5 * * * *")` fired exactly once per tick across all workers, without leader election. |
| **Leases** | Every running job holds a lease that workers heart-beat; a reaper in every worker recovers jobs of crashed or partitioned workers. |
| **Fencing** | Completions are accepted only from the current lease holder for the current attempt. A worker that comes back from a long GC pause cannot overwrite a newer result. |
| **Idempotency keys** | `enqueue(…, idempotency_key="welcome:42")` is deduplicated by a unique constraint. |
| **Transactional enqueue** | Pass your asyncpg connection: the job commits or rolls back with your writes. |
| **LISTEN/NOTIFY** | Idle workers wake within milliseconds; polling remains the fallback, so a lost notification costs latency, never a job. |
| **Graceful shutdown** | SIGTERM stops claiming and drains in-flight jobs; jobs still running after the timeout are released without being charged an attempt. A second signal stops immediately. |
| **Operations** | Admin API + dashboard, Prometheus metrics (database-level on the admin, handler durations on workers), worker registry, `pgqueue prune` retention. |
| **Migrations** | Alembic migrations ship in the package (own version table, safe next to your schema); `pgqueue migrate`. |

## Quick start

### The demo

```console
$ docker compose up
```

That builds one image and starts Postgres, runs the migrations, and launches the dashboard
on <http://localhost:8080>, two workers and a producer. The demo tasks in
[`examples/demo.py`](examples/demo.py) fail the way real ones do: flaky SMTP, a payment
provider answering `429` (`Retry`), declined cards (`PermanentError`) and a webhook endpoint
that times out often enough to fill the dead-letter state, so there is something to look at
and requeue. `docker compose down -v` removes everything.

### In your project

```console
$ pip install "pgqueue[admin] @ git+https://github.com/MaciejZiel/pgqueue"
$ export PGQUEUE_DSN=postgresql://user:pass@localhost/app
$ pgqueue migrate
```

Define tasks in a module, e.g. `app/jobs.py`:

```python
import os
from pydantic import BaseModel
from pgqueue import JobContext, PgQueue, Retry, RetryPolicy

app = PgQueue(os.environ["PGQUEUE_DSN"])


class Webhook(BaseModel):
    url: str
    event: str


@app.task(
    queue="webhooks",
    max_attempts=8,
    timeout=30,
    retry=RetryPolicy(base=2, factor=2, max_delay=3600),
)
async def deliver(payload: Webhook, ctx: JobContext) -> int:
    response = await http.post(
        payload.url, json={"event": payload.event}, headers={"Idempotency-Key": f"job-{ctx.job_id}"}
    )
    if response.status == 429:
        raise Retry("rate limited", delay=float(response.headers["Retry-After"]))
    response.raise_for_status()
    return response.status
```

Enqueue from anywhere (a web request handler, a script, another job):

```python
async with app:
    job_id = await deliver.enqueue(Webhook(url="https://…", event="invoice.paid"))
    await deliver.enqueue(Webhook(…), delay=60)                     # in a minute
    await deliver.enqueue(Webhook(…), idempotency_key="invoice:42") # at most once
    await deliver.enqueue_many([Webhook(…) for _ in range(10_000)]) # one INSERT
```

Run workers and the admin:

```console
$ pgqueue worker app.jobs:app -q webhooks,default -c 50 --metrics-port 9100
$ PGQUEUE_ADMIN_USERNAME=ops PGQUEUE_ADMIN_PASSWORD=… pgqueue admin --host 0.0.0.0
$ pgqueue prune --older-than-days 14      # e.g. from a nightly cron
```

## The live demo

`python -m demo` (or the Termux setup below) starts a self-contained showcase: the
dashboard in **read-only mode** plus a synthetic workload that keeps it alive. Everything in
the screenshots below is real: real workers, real jobs, real PostgreSQL.

![Full dashboard](docs/dashboard.png)

What is on the page:

- **Throughput and latency**: finished jobs per second (two-minute chart), and p50/p95 of the
  *pickup latency* (due until claimed) and the *run time*, computed from the jobs' own
  timestamps.
- **Queue depth** per queue over time, with running, retrying and dead counts and queue lag.
- **Workers** with heartbeat age, slots in use and lease length, plus every **running job**
  with a counting-down lease.
- A **live job feed** and the problem lists (retrying, dead, failed) with the last error.
- Periodic (cron) schedules and a **"How it works"** explainer with a diagram.

Updates arrive over server-sent events (`/api/stream`) with a polling fallback
(`/api/live`). One snapshot is computed per second and shared by all viewers, so the database
load does not depend on how many people are watching (100 concurrent streams cost the demo
process about 90 MB and under 3 % CPU on a laptop).

<img src="docs/dashboard-mobile.png" alt="The dashboard on a narrow mobile screen" width="260">

The workload ([`demo/`](demo), separate from the library) is deliberately lively:

- five job types with realistic durations: e-mails (SMTP flakiness), image resizing (sync
  handler on the thread pool, with a timeout), payments (`Retry-After`, declined cards),
  webhooks to a flaky endpoint (backoff, then dead letters) and slow report exports;
- two cron schedules (`* * * * *` and `*/5 * * * *`), delayed jobs, traffic bursts inserted
  with one `enqueue_many`, and backpressure so the demo never buries itself;
- **real worker processes**; every couple of minutes one is killed with `SIGKILL`. Its leases
  expire, another worker's reaper recovers the jobs (`lease expired: worker ... stopped
  heart-beating`), and the supervisor starts a replacement;
- bounded storage: finished jobs are deleted after ten minutes and capped at 6000 rows,
  dead workers are forgotten after 30 s.

The public dashboard exposes **no endpoint that changes state**: in read-only mode
(`create_app(dsn, read_only=True)` or `PGQUEUE_ADMIN_READ_ONLY=1`) the requeue routes are not
mounted at all, and the buttons are not rendered. Settings are environment variables, listed in
[`deploy/termux/env.example`](deploy/termux/env.example).

```console
$ export PGQUEUE_DSN=postgresql://user:pass@127.0.0.1/pgqueue_demo
$ python -m demo            # migrates, then serves http://127.0.0.1:8080
```

## Self-hosting on Termux

The demo runs as plain processes (no Docker) on a self-hosted server: here Termux on
aarch64, with PostgreSQL 18 and nginx. The scripts live in [`deploy/termux/`](deploy/termux). Roughly 270 MB of RAM for
the Python processes with the defaults (three workers).

```console
$ pkg install python postgresql nginx clang make rust libffi openssl git
$ git clone https://github.com/MaciejZiel/pgqueue ~/pgqueue && cd ~/pgqueue
$ deploy/termux/install.sh                                   # venv; compiles asyncpg and greenlet
$ DEMO_DB_PASSWORD=... deploy/termux/setup-db.sh             # role + database pgqueue_demo
$ mkdir -p ~/.config/pgqueue-demo
$ cp deploy/termux/env.example ~/.config/pgqueue-demo/env    # then edit the password and port
$ deploy/termux/start.sh                                     # try it by hand first
```

- **Start on boot**: copy `deploy/termux/boot/pgqueue-demo` to `~/.termux/boot/` (Termux:Boot);
  it waits for PostgreSQL and restarts the demo if it exits.
- **nginx**: include `nginx-zones.conf` in the `http` block and `nginx-location.conf` in the
  `server` block. It publishes the dashboard under `/pgqueue/`, serves the static files from
  disk, disables buffering for the event stream, limits requests and streams per visitor
  (keyed on `CF-Connecting-IP` behind a Cloudflare Tunnel, where every request comes from
  `127.0.0.1`) and caps the streams in total.
- **Public access**: put a Cloudflare Tunnel or Tailscale Funnel in front of nginx; the demo
  itself only listens on `127.0.0.1`.
- **Database**: the demo uses its own role and database and works with PostgreSQL 16 to 18
  (CI runs the test suite on all three).

## Architecture

```mermaid
flowchart LR
    subgraph producers["Producers"]
        web["web app<br/>enqueue() in its own transaction"]
        cron["periodic scheduler<br/>(inside every worker)"]
    end

    subgraph pg["PostgreSQL"]
        jobs[("pgqueue_jobs<br/>partial index on ready jobs")]
        trig{{"NOTIFY trigger"}}
        sched[("pgqueue_periodic")]
        reg[("pgqueue_workers")]
    end

    subgraph worker["Worker process (asyncio, N slots)"]
        loop["claim loop<br/>SKIP LOCKED batch claim"]
        run["handlers<br/>async on loop, sync on thread pool"]
        hb["heartbeat<br/>extends leases"]
        reaper["reaper<br/>recovers expired leases"]
        listen["LISTEN connection"]
    end

    admin["admin: FastAPI<br/>dashboard · JSON API · /metrics"]
    prom["Prometheus"]

    web -- INSERT --> jobs
    cron -- "lock tick, INSERT, advance" --> sched
    cron --> jobs
    jobs --> trig -- "pg_notify(queue)" --> listen
    listen -- wake --> loop
    loop -- "UPDATE … SKIP LOCKED RETURNING" --> jobs
    loop --> run
    run -- "fenced complete / fail" --> jobs
    hb -- "locked_until = now() + lease" --> jobs
    hb --> reg
    reaper -- "expired → retrying / dead" --> jobs
    admin -- "SQLAlchemy Core" --> jobs
    admin --> reg
    admin --> sched
    prom -- scrape --> admin
    prom -- scrape --> worker
```

A job's life:

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue (optionally delayed)
    queued --> running: claimed (lease starts, attempts + 1)
    running --> succeeded: handler returned
    running --> retrying: exception, attempts left (backoff)
    running --> retrying: lease expired (worker crashed) — reaper
    running --> failed: PermanentError / invalid payload
    running --> dead: exception, no attempts left
    running --> queued: graceful shutdown timeout (attempt not charged)
    retrying --> running: backoff elapsed, claimed again
    failed --> queued: requeue (dashboard / API)
    dead --> queued: requeue (dashboard / API)
    succeeded --> [*]: pruned after retention period
```

The code is small enough to read in an afternoon:

| Module | Responsibility |
|---|---|
| [`schema.py`](src/pgqueue/schema.py), [`migrations/`](src/pgqueue/migrations/versions) | Tables (SQLAlchemy Core) and the Alembic migrations, including the NOTIFY triggers |
| [`store.py`](src/pgqueue/store.py) | Every hot-path SQL statement, with the reasoning next to it |
| [`app.py`](src/pgqueue/app.py) | `PgQueue`, the `@task` decorator, typed `enqueue` |
| [`worker.py`](src/pgqueue/worker.py) | Claim loop, concurrency, heartbeats, reaper, shutdown |
| [`notify.py`](src/pgqueue/notify.py) | Self-healing LISTEN connection |
| [`periodic.py`](src/pgqueue/periodic.py), [`cron.py`](src/pgqueue/cron.py) | Cron parser and the exactly-once-per-tick scheduler |
| [`outcomes.py`](src/pgqueue/outcomes.py), [`backoff.py`](src/pgqueue/backoff.py) | What a failure turns into |
| [`admin/`](src/pgqueue/admin) | FastAPI admin, dashboard, `/metrics` |

## Guarantees and failure semantics

**Delivery is at-least-once.** A job is executed until one attempt is recorded as
succeeded, failed or dead. In the normal case every attempt runs exactly once; in the
failure cases below a handler can run again after a previous run's side effects already
happened. Handlers should therefore be idempotent, and `JobContext.job_id` is a natural
idempotency key to pass to external systems.

What is guaranteed, and tested against a real Postgres:

- **No two workers ever hold the same job at the same time.** Claims lock rows with
  `FOR UPDATE SKIP LOCKED` and flip them to `running` in the same statement.
  [`tests/test_stress.py`](tests/test_stress.py) runs randomised scenarios (3–8 workers with
  random concurrency, priorities, delays and failures; seeds are reproducible) plus four
  real worker processes over 2,000 jobs, logs every handler execution into an
  unconstrained table and asserts that every job ran **exactly once per attempt number**.
- **A stale worker cannot overwrite the truth.** `complete`, `fail` and `release` only
  match `id AND status = 'running' AND locked_by = me AND attempts = my_attempt`. If a
  worker loses its lease and the job is re-run elsewhere, the late result is dropped (and
  logged).
- **Crashed workers do not lose jobs.** Leases default to 30 s and are extended three times
  per lease period. If a worker is SIGKILLed, loses its network or stalls its event loop,
  its jobs' leases expire and any worker's reaper returns them to `retrying` (or `dead`).
  The crashed attempt counts, because the handler may have run. A chaos test cuts a worker
  off from the database mid-flight and checks that all its jobs still succeed.
- **Periodic jobs fire once per tick**, no matter how many workers run the scheduler: the
  schedule row is locked with `SKIP LOCKED`, the job is inserted with the idempotency key
  `periodic:<name>:<tick>` and `next_run_at` advances in the same transaction. Missed ticks
  after downtime are not replayed (one catch-up run).
- **Enqueue is atomic with your data** when you pass your connection, and idempotency keys
  make retried producers safe.

What happens when…

| Event | Outcome |
|---|---|
| handler raises | `retrying` with backoff, or `dead` after `max_attempts`; traceback kept in `last_error` |
| handler raises `PermanentError` or the payload no longer validates | `failed` immediately |
| handler exceeds `timeout` | counted as a failed attempt (`TimeoutError`) |
| task not registered in this worker (rolling deploy) | retried, so a newer worker can take it |
| SIGTERM | stop claiming, finish in-flight jobs (up to `--shutdown-timeout`), then release the rest as `queued` without charging the attempt |
| SIGKILL / OOM / power loss | leases expire, the reaper re-queues the jobs (attempt charged) |
| worker loses its DB connection | same as a crash for its in-flight jobs; the worker keeps retrying the claim and recovers |
| LISTEN connection drops | polling continues; the listener reconnects with backoff and triggers a claim |
| Postgres restarts | workers log errors and resume when it is back; committed jobs are durable |

## Benchmarks

Measured with [`bench/bench.py`](bench/bench.py) on the author's laptop, so read them as an
order of magnitude, not a promise:

- **CPU** 13th Gen Intel Core i5-13500H (12 cores / 16 threads), **RAM** 14 GiB, NVMe SSD
- PostgreSQL 17.11 in Docker with the **default configuration** (`shared_buffers = 128MB`,
  `synchronous_commit = on`) on the same machine as the workers and the load generator
- Python 3.12.13, Linux 7.0; the machine was not idle (load average ≈ 5 from other
  processes during the run)
- Workers are separate processes started through the real CLI; the handler is a no-op, so
  this measures the queue's own overhead (claim, lease, completion), not your work

| Scenario | Jobs | Time | Throughput |
|---|---:|---:|---:|
| `enqueue()`, one client, one job per transaction | 5,000 | 10.2 s | **490 jobs/s** |
| `enqueue()`, 10 concurrent clients | 20,000 | 13.9 s | **1,438 jobs/s** |
| `enqueue_many()`, batches of 1,000 | 100,000 | 2.5 s | **40,496 jobs/s** |
| drain a 50k backlog, 1 worker × 32 slots | 50,000 | 34.8 s | **1,436 jobs/s** |
| drain a 50k backlog, 2 workers × 32 slots | 50,000 | 19.0 s | **2,628 jobs/s** |
| drain a 50k backlog, 4 workers × 32 slots | 50,000 | 12.0 s | **4,185 jobs/s** |
| drain a 50k backlog, 8 workers × 32 slots | 50,000 | 7.6 s | **6,625 jobs/s** |

Pickup latency (enqueue → handler starts) for a steady 100 jobs/s into idle workers:

| Wake-up mechanism | p50 | p95 | p99 |
|---|---:|---:|---:|
| LISTEN/NOTIFY (+ 1 s polling fallback) | **4.1 ms** | 6.7 ms | 12.5 ms |
| polling only, 1 s interval | 483 ms | 932 ms | 991 ms |

Observations:

- Single-job enqueue is bound by commit latency (an fsync per transaction). Batching with
  `enqueue_many` is ~80× faster; enqueueing inside a transaction you are committing anyway
  costs nothing extra.
- Throughput scales nearly linearly up to 4 worker processes and keeps growing at 8; the
  ceiling on this machine is Postgres's per-row UPDATE + commit cost (two transactions per
  job: claim and completion).
- The benchmark caught a real bug: the first version of the claim query filtered with
  `queue = ANY($1)`, which cannot use the index for ordering, and degraded to a
  sequential scan + sort of the whole backlog (22 ms per claim at 50k jobs). Walking the
  index per queue through a `LATERAL` subquery brought that to ~0.1 ms and doubled
  throughput. Raw numbers are in [`bench/results.json`](bench/results.json).

Reproduce: `docker compose up -d postgres && uv run python -m bench.bench`.

## Design decisions and trade-offs

**Postgres as the broker.** Transactional enqueue, one system to back up and monitor,
SQL as the debugging tool (`SELECT * FROM pgqueue_jobs WHERE status = 'dead'`). The price:
every job is a few row writes and WAL, so the ceiling is thousands, not hundreds of
thousands, of jobs per second, and the queue shares I/O with your application.

**asyncio workers, not threads or prefork.** Typical background jobs wait on HTTP, SMTP or
the database. One event loop can keep hundreds of those in flight for the cost of one small
connection pool, and the heartbeat and reaper run on the same loop without locks. Sync
handlers still work: they run on a thread pool sized to the worker's concurrency, so they
cannot block the loop that keeps their leases alive. CPU-bound work scales by running more
worker processes, which is free because workers coordinate only through the database.

**Leases + fencing rather than holding a transaction open.** Some Postgres queues keep the
row locked for the whole job. That pins a connection per running job, blocks vacuum and
makes "is this job running?" invisible to the dashboard. pgqueue commits the claim, tracks
ownership in columns (`locked_by`, `locked_until`, `attempts`) and fences every write on
them, which also makes crash recovery an ordinary `UPDATE`.

**LISTEN/NOTIFY as a hint, not a channel.** Notifications are not durable and are not sent
for jobs that become due later, so workers still poll. Inserts notify through a
*statement-level* trigger (one notification per queue per bulk insert); updates use a
row-level trigger with a `WHEN` clause that Postgres evaluates without entering PL/pgSQL,
so claims, completions and heartbeats pay nothing.

**Hand-written SQL on the hot path, SQLAlchemy Core everywhere else.** The handful of
statements whose exact shape *is* the correctness argument (`SKIP LOCKED`, fencing,
reaping) live in `store.py` as SQL on asyncpg. The table definitions (and therefore the
Alembic migrations) and the admin's filter-heavy queries use SQLAlchemy Core. A test fails
if the migrations and the schema module ever drift apart.

**Attempts are charged on claim.** If a worker dies mid-job we cannot know whether the
side effect happened, so the attempt counts; a poison job that crashes its worker ends up
`dead` instead of crash-looping the fleet. The exception is a graceful shutdown timeout,
where the worker *knows* it interrupted the job and hands it back uncharged.

**No leader for cron.** Every worker ticks the scheduler; row locks plus an idempotency key
make double firing impossible, so there is no election to get wrong.

**Licensing.** All dependencies are permissively licensed (see
[`3rdparty_licenses.md`](3rdparty_licenses.md)). That is why the driver is asyncpg
(Apache-2.0) rather than psycopg (LGPL-3.0), why the admin tests use a 60-line ASGI client
rather than httpx (which pulls in certifi, MPL-2.0) and why the stress tests use seeded
randomness instead of Hypothesis (MPL-2.0).

## When to use something else

pgqueue is a personal project, not a replacement for established tools. Celery, RQ,
Dramatiq, Procrastinate and pg-boss are mature, widely used and solve problems pgqueue
deliberately does not. A rough guide:

- **pgqueue (or another Postgres-backed queue)** fits when you already run PostgreSQL,
  your volume is in the hundreds to low thousands of jobs per second, and you want to
  enqueue jobs in the same transaction as your application data.
- **[Celery](https://docs.celeryq.dev/)** is a good choice when you need higher throughput
  than a single database comfortably handles, workflow primitives (chains, groups, chords),
  or already operate RabbitMQ or Redis. It has a large ecosystem and years of production use.
- **[RQ](https://python-rq.org/)** is a simple, well-documented Redis-based queue; a good
  fit when Redis is already part of your stack and you want minimal setup.
- **[Procrastinate](https://procrastinate.readthedocs.io/)** and
  **[Dramatiq](https://dramatiq.io/)** (with a Postgres broker) are mature Python options
  built on the same idea as pgqueue; prefer them if you need a maintained library.
- **[pg-boss](https://github.com/timgit/pg-boss)** is the long-standing equivalent for
  Node.js.

What pgqueue focuses on is a small, readable codebase in which transactional enqueue,
typed payloads and the failure semantics above are explicit and covered by tests.

## Limitations

- **Throughput is bounded by Postgres writes**: a few thousand jobs per second on a laptop,
  two transactions per job. Not a fit for event streaming.
- **Commit latency bounds single-job enqueue**; use `enqueue_many` for bulk producers.
- **At-least-once, not exactly-once.** Handlers must be idempotent. A handler that blocks the
  event loop (sync code inside `async def`) for longer than its lease will be considered dead
  and run again elsewhere.
- **Timeouts cannot kill sync handlers.** A timed-out thread is abandoned, not interrupted.
- **Cron is UTC-only** and supports the standard five fields (no seconds, no `L`/`W`/`#`).
- **Finished jobs stay in the table** until `pgqueue prune` removes them; with high volume,
  run it regularly and keep autovacuum healthy. Idempotency keys are only unique among rows
  that still exist.
- **The admin counts jobs with a `GROUP BY` over the table**. The live view, shared by all
  viewers, counts active jobs every second but finished ones in full only once a minute
  (about 10 ms per snapshot instead of 130 ms with a million finished jobs). Fine for
  millions of rows, slow for hundreds of millions. Its auth is optional HTTP basic auth; put it behind your
  usual access controls.
- **No workflows** (chains, groups, job dependencies), **no per-queue rate limits** and no
  result streaming; return values are stored as JSON on the job row.
- **One database is one failure domain**: if Postgres is down, so is the queue (and,
  usually, the application it serves).

## Development

```console
$ docker compose up -d postgres         # Postgres 17 on localhost:55432
$ uv sync --all-extras
$ uv run pytest                          # 160+ tests against the real database, ~45 s
$ uv run ruff check . && uv run ruff format --check . && uv run mypy
```

Tests create their own databases (`pgqueue_test*`) on that server; set `PGQUEUE_TEST_DSN`
to point them elsewhere and `PGQUEUE_STRESS_SEEDS=1,2,…` to run more stress scenarios. CI
runs lint, strict mypy and the full suite against Postgres 16, 17 and 18, and builds the demo
image.

Project layout: `src/pgqueue` (library, CLI, admin), `tests`, `examples` (the compose demo),
`demo` (the public live demo: workload, workers, read-only server), `deploy/termux`
(self-hosting scripts), `bench` (benchmarks).

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md).

## License

[MIT](LICENSE) © 2026 Maciej Zieliński. Third-party components and their licenses are
listed in [`3rdparty_licenses.md`](3rdparty_licenses.md).
