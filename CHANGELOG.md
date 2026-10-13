# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
