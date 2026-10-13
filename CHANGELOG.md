# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `make bench`: a reproducible benchmark with a 1/2/4/8 worker sweep, enqueue and pickup latency
  percentiles, payload sizes, repeated runs and a Celery + Valkey comparison; results, charts
  and caveats in `docs/benchmarks.md`. It replaces the earlier single-run numbers.
- Dashboard favicon set (SVG, ICO, 32 px PNG, Apple touch icon) and `theme-color` tags; the admin app
  answers `/favicon.ico`.
- Dashboard rebuilt: live updates over server-sent events (polling fallback), throughput
  per second, pickup and run latency percentiles, queue depth history, workers with
  heartbeats and leases, running jobs with lease countdowns, a live job feed and a
  "How it works" explainer. Mobile friendly.
- Admin `read_only` mode (`create_app(read_only=True)` or `PGQUEUE_ADMIN_READ_ONLY=1`):
  endpoints that change state are not mounted and the dashboard hides the buttons.
- `GET /api/live` and `GET /api/stream`: one shared snapshot per second, independent of
  the number of viewers; `WorkerOut.lease_seconds`.
- `demo/`: public live demo with a synthetic workload (bursts, failures, retries, cron,
  simulated worker crashes), bounded retention and a worker supervisor; Termux
  self-hosting scripts in `deploy/termux/`.
- CI runs the test suite on PostgreSQL 16, 17 and 18.

### Security

- The read-only dashboard shows only the last line of job errors with absolute paths removed, and
  workers under a stable short label instead of host name and pid (API, live snapshot, metrics).
  `WorkerOut.hostname` and `pid` are `None` there.
- `read_only_dsn` / `PGQUEUE_READONLY_DSN`: the public dashboard can use a SELECT-only database role.
  `deploy/termux/setup-db.sh` creates it, limits connections and revokes `CONNECT` from `PUBLIC`.

### Fixed

- Job ids outside the bigint range (`/api/jobs/{id}`, `before_id`) now answer 422 instead of 500.
- The Termux boot script rotates its log (`logcap.sh`) instead of appending forever.

- Workers no longer log "lost the lease" for a job that finished while its heartbeat was
  in flight.
- The live dashboard no longer counts every finished job each second, and one stalled
  viewer can no longer hold up the stream for the others.
- Demo retention never deletes a finished job that is being requeued.

## [0.1.0] - 2026-10-13

First release.

### Added

- Jobs table and Alembic migrations that ship in the package (own version table).
- Task registry with typed pydantic payloads, validated on enqueue and on execution.
- Claiming with `FOR UPDATE SKIP LOCKED` through a partial ready index per queue, with
  fenced completions so a stale worker cannot overwrite a newer attempt.
- Retries with capped exponential backoff and jitter, `Retry` and `PermanentError`,
  and a dead-letter state.
- Delayed and scheduled jobs, and cron-like periodic jobs.
- Worker with concurrent execution, graceful shutdown, registration in the database,
  heartbeat leases and recovery of jobs from crashed workers.
- LISTEN/NOTIFY wake-ups with polling as the fallback.
- Enqueueing inside the caller's own transaction.
- `pgqueue` command line: `migrate`, `worker` and `admin`.
- Admin API (overview, job inspection, requeueing), a server-rendered dashboard,
  Prometheus metrics from the admin and from workers, and retention of finished jobs.
- Stress tests proving no job attempt is executed twice, a benchmark script and a Docker
  Compose demo (dashboard, two workers and a producer).
