# Benchmarks

What the numbers in the README come from, how to reproduce them, and how far to trust them.
Short version: 6 runs of every cell on one laptop with the order of the two systems
counterbalanced and a 30 s cool-down between them. pgqueue drained a 20,000-job backlog
faster than this Celery configuration in all 6 runs at 1, 2, 4 and 8 worker processes (about
3 times at 1 and 2 workers, 1.3 to 3.6 times at 4 and 8), and Celery was faster on single-job
enqueue and on pickup latency. The two are not equal in durability, and the machine was not
fully idle; see [How to read this](#how-to-read-this).

Run it yourself with `make bench ARGS="--runs 6"` (about 25 minutes including the
cool-downs; `make bench-quick` checks the setup in a few minutes and produces meaningless
numbers). The tables below are generated from [`bench/results.json`](../bench/results.json) by
`python -m bench.report` and the charts by `python -m bench.chart`; the numbers in the
prose are taken from the same file.

## Setup

| | |
|---|---|
| Machine | 13th Gen Intel Core i5-13500H, 16 logical CPUs, 14 GiB RAM, NVMe SSD, Linux 7.0 |
| PostgreSQL | 17.11 (`postgres:17-alpine`) in Docker, default configuration: `shared_buffers = 128MB`, `synchronous_commit = on`, `fsync = on` |
| Broker for the comparison | Valkey 8 (`valkey/valkey:8-alpine`, BSD-3-Clause fork of Redis; it reports itself as 7.2.4 for protocol compatibility), persistence off (`--save "" --appendonly no`) |
| Celery | 5.6.3 with redis-py 8.1.0, thread pool with 32 threads per process, prefetch multiplier 4 (so up to 128 messages reserved per process), **early ack (`acks_late=False`)**, no result backend, `ignore_result`, defaults otherwise |
| Python | 3.12.13 |
| Topology | Postgres or Valkey, the workers and the load generator all run on the same machine, workers as separate OS processes |
| Handler | no-op. This measures the queue, not your work |
| Job payload | JSON object of about 2 KiB (identifiers, short strings and a free-text body, deterministic). A 256 B and a 16 KiB variant are in the payload table |
| Concurrency | 32 job slots per worker process in both systems (asyncio tasks for pgqueue, threads for Celery) |
| Repetitions | 6 runs of every cell; the order of the two systems alternates between runs (pgqueue first in runs 1, 3 and 5, Celery first in runs 2, 4 and 6) with a 30 s cool-down between them |

Cells below are the **median over the 6 runs, with the minimum and maximum run in
brackets**. Raw per-run numbers, the load average at each measurement and the environment
are in `bench/results.json`.

### What is timed

* **Enqueue**: wall-clock time around each `enqueue()` call (pgqueue) or `apply_async()`
  call (Celery), measured by the client. The call returns after the job is committed in
  Postgres (with an fsync) or after the broker accepted the message into memory.
  `enqueue_many` inserts 1,000 jobs in one statement; Celery has no bulk publish, so it is
  not in that row.
* **Backlog throughput**: a backlog of 20,000 jobs is loaded first, then N worker
  processes start and drain it. The rate is taken between the 20th and the 95th
  percentile completion timestamp, so staggered worker start-up and the straggling tail do
  not count. pgqueue reads `finished_at` from its job rows; the Celery task records its own
  finish time in Valkey.
* **Pickup latency**: jobs arrive at a fixed 200 per second for 15 seconds (3,000 jobs)
  into idle workers. The two systems do not measure the same interval:
  * pgqueue: `started_at - created_at`, both from the database clock. `created_at` is
    set by the INSERT, before its fsynced commit, and `started_at` is set inside the claim
    statement (`store.py`, `started_at = now()`), before the worker has received the row.
    The interval therefore contains the insert's commit, the wake-up (LISTEN/NOTIFY, 1 s
    polling as fallback) and the claim round trip, but not the transfer of the row to the
    worker or the start of the handler.
  * Celery: `time.time()` taken in the producer just before `apply_async` serialises the
    message, compared with `time.time()` inside the handler. It contains serialisation,
    the publish, the delivery to the worker and the thread hand-over, but no durable commit.

  The two ratios in the pickup table are therefore not like-for-like and should not be read
  as a pure speed comparison.
* **Durability differs in the drain scenario too.** pgqueue commits the claim and the
  completion of every job with an fsync. Celery runs with early ack (`acks_late=False`): the
  message is acknowledged when the worker receives it, so a worker that dies mid-task loses
  the task (at-most-once on receipt), and Valkey has persistence off. The backlog throughput
  figures compare a durable at-least-once queue with a non-durable, at-most-once one.

The Celery task makes one pipelined Redis call to record its timestamps, which pgqueue does
not need because its job rows already hold them. Celery is charged that extra round trip.

## Results

### Enqueue

| Variant | System | jobs/s | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| 1 client, 1 job per call | pgqueue | 226 (212-231) | 4.4 (4.4-4.6) | 5.0 (4.8-5.4) | 5.7 (5.1-10.6) |
| 1 client, 1 job per call | Celery + Valkey | 1,251 (712-1,272) | 0.8 (0.8-1.4) | 1.0 (1.0-1.8) | 1.3 (1.3-1.9) |
| 10 concurrent clients | pgqueue | 395 (334-417) | 24.5 (23.8-29.9) | 30.1 (25.5-34.9) | 36.0 (27.5-42.7) |
| 10 concurrent clients | Celery + Valkey | 774 (730-791) | 9.4 (8.9-9.5) | 34.9 (33.4-38.0) | 56.6 (53.0-63.4) |
| `enqueue_many`, 1,000 per call (latency is per call) | pgqueue | 14,162 (9,413-15,559) | 68.3 (64.1-104.1) | 78.6 (66.8-120.9) | 104.2 (69.9-138.4) |

### Backlog throughput (jobs/s)

| Worker processes | pgqueue | Celery (thread pool, 32 threads, prefetch x4, early ack) |
|---:|---:|---:|
| 1 | 1,323 (1,182-1,347) | 441 (400-445) |
| 2 | 2,788 (2,431-2,907) | 840 (725-1,000) |
| 4 | 5,267 (3,870-5,577) | 2,950 (1,279-3,003) |
| 8 | 10,103 (6,927-10,424) | 6,650 (2,292-6,693) |

![Backlog throughput by number of worker processes, pgqueue and Celery](benchmarks-throughput.svg)

### Pickup latency at 200 jobs/s (ms)

| Worker processes | System | p50 | p95 | p99 | max |
|---:|---|---:|---:|---:|---:|
| 1 | pgqueue | 6.5 (6.1-7.1) | 11.6 (9.5-19.3) | 22.2 (17.5-77.4) | 76.9 (49.8-262.9) |
| 1 | Celery + Valkey | 3.4 (3.3-3.8) | 4.0 (3.7-4.5) | 4.5 (3.8-6.0) | 13.8 (9.9-29.3) |
| 2 | pgqueue | 4.4 (4.0-4.4) | 8.5 (6.1-12.4) | 12.8 (6.8-46.1) | 45.4 (26.0-148.5) |
| 2 | Celery + Valkey | 3.5 (3.3-4.0) | 3.9 (3.7-4.6) | 4.1 (4.0-5.3) | 6.7 (5.5-11.1) |
| 4 | pgqueue | 4.0 (3.9-4.1) | 7.4 (5.9-12.2) | 11.9 (8.3-22.7) | 31.4 (25.4-112.5) |
| 4 | Celery + Valkey | 3.5 (3.4-4.2) | 4.0 (3.8-4.6) | 4.6 (3.9-6.4) | 6.9 (5.6-7.7) |
| 8 | pgqueue | 3.9 (3.8-4.3) | 6.8 (5.8-9.9) | 9.0 (6.2-12.4) | 30.3 (26.2-34.6) |
| 8 | Celery + Valkey | 3.6 (3.5-4.2) | 4.4 (3.9-4.7) | 4.7 (4.0-5.8) | 7.1 (6.9-7.3) |

![Pickup latency by number of worker processes, pgqueue and Celery](benchmarks-latency.svg)

### Payload size, 4 workers, backlog throughput (jobs/s)

| Payload | pgqueue | Celery (thread pool, 32 threads, prefetch x4, early ack) |
|---|---:|---:|
| 256 B | 5,199 (3,973-5,362) | 3,038 (1,299-3,106) |
| 2,048 B | 4,728 (3,923-5,454) | 2,990 (1,320-3,016) |
| 16,384 B | 4,694 (3,369-4,930) | 2,549 (1,156-2,587) |

One-minute load average sampled at each measurement: min 0.8, median 1.7, max 11.2.

Results file schema 2: every record has the load average before and after the measurement
and its position in the pair. The rows labelled "Celery + Valkey" in the enqueue and pickup
tables are the same Celery configuration as in the column headers above.


## How to read this

**Backlog throughput: pgqueue was ahead in every run.** Ratio of pgqueue over Celery, jobs/s,
runs 1 to 6:

| Workers | Ratio in each run | Reading |
|---:|---|---|
| 1 | 2.85, 3.00, 2.99, 3.11, 3.01, 2.96 | about 3 times, very stable |
| 2 | 3.26, 3.26, 3.47, 3.31, 2.87, 3.35 | about 3.3 times, very stable |
| 4 | 1.30, 1.72, 1.80, 1.83, 1.92, 3.23 | 1.3 to 1.9 times; run 6 is 3.2 because Celery had a slow run |
| 8 | 1.54, 1.49, 1.50, 1.56, 3.62, 3.02 | about 1.5 times in runs 1 to 4; runs 5 and 6 are 3.6 and 3.0 because Celery had two slow runs |

The order did not matter: pgqueue was ahead when it ran first and when it ran second. The
payload scenario (4 workers) agrees: pgqueue was ahead in all 6 runs at 256 B, 2 KiB and
16 KiB (ratios 1.7 to 3.1 at 256 B and 1.8 to 3.1 at 16 KiB).

The drain figures at 4 and 8 workers have outliers on the Celery side: its 8-worker runs
were 6,648 to 6,693 jobs/s in runs 1 to 4 and 2,812 and 2,292 in runs 5 and 6, and its
4-worker run 6 was 1,279 against about 2,900 to 3,000 otherwise. I do not know why; the
load average before those runs (1.4 to 2.8) does not explain it. For that reason the ratio at
4 and 8 workers should be quoted as a range or as "about 1.5 times in the typical runs", not
as the median ratio, which is pulled up by the slow Celery runs.

**The machine was not idle.** The 1-minute load average sampled around the measurements was
0.8 at minimum, 1.7 at median and 11.2 at maximum on a 16-thread CPU. Before each drain
measurement it was between 0.8 and 3.6. That is far quieter than a first run that was taken
with a load of 4 to 38, and the spread between runs is now small for most cells, but it is
not a dedicated machine, and the peak of 11 shows something else ran at times.

**Single-job enqueue favours Celery, as it should.** A Valkey publish with persistence off
is a memory write; a pgqueue `enqueue()` is a durable commit. The median was 226 jobs/s for
pgqueue against 1,251 for Celery with one client, 395 against 774 with ten concurrent clients.
That gap is what durability costs. If you enqueue inside a transaction you are committing
anyway, the job rides on that commit and adds one INSERT. If you can batch, `enqueue_many`
amortizes the commit: 14,162 jobs/s with 1,000 jobs per call, which is higher than Celery's
single-job rate (Celery has no bulk publish, so there is no like-for-like number).

**Pickup latency is low for both at this rate, and Celery's is lower.** With idle workers
the median is 3.9 to 6.5 ms for pgqueue and 3.4 to 3.6 ms for Celery. pgqueue's p99 is 9 to
22 ms against 4.1 to 4.7 ms for Celery. The intervals are not equivalent (see "What is
timed": pgqueue's includes a durable commit, Celery's does not), so read this as "both are in
single-digit milliseconds at the median", not as a pure speed comparison. At 1 worker
pgqueue's median is highest (6.5 ms) and it falls as workers are added.

**Celery on one worker is slow here** (hundreds of jobs/s) because one process with a thread
pool is limited by the GIL, while an asyncio worker spends its time waiting on the
database. A prefork pool, or the gevent pool, would change this comparison. It was not
tried, so there is no number for it. The 3 times lead at 1 and 2 workers is largely this
effect.

## Why more workers help, and where SKIP LOCKED contention comes in

A pgqueue worker claims up to as many jobs as it has free slots with one statement
(`UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)`, see `store.py`). Without
`SKIP LOCKED`, two workers that select the same head of the queue would serialize: the
second one blocks until the first commits and then finds the rows taken. With it, a
claimer passes over rows another transaction has locked and takes the next ones in index
order, so N workers receive disjoint rows and never wait for each other.

The remaining cost of contention is skipping, not waiting. While other workers hold their
batches, the head of the ready-index is full of locked entries, and a claimer has to step
over them to find free rows. The more workers and slots, the more locked entries sit at the
head. Besides that, all workers insert and update rows of the same table and index pages,
and every completion is its own UPDATE and commit. In this benchmark the backlog throughput
went from 1,323 jobs/s with one worker to 2,788 with two, 5,267 with four and 10,103 with
eight, which is close to linear at the median. The 4-worker cell has a wider range
(3,870 to 5,577) than the others. With a 16-thread CPU shared with Postgres itself, a CPU or
disk limit and the claim-side skipping cannot be told apart from these runs, and eight
workers is as far as this benchmark went. This
benchmark did not instrument lock skipping, so the contention explanation above describes
how the mechanism works, not a measured cause of this curve. If you want to measure it,
compare `pg_stat_activity` wait events and the claim statement time (`pg_stat_statements`)
across worker counts on a dedicated machine.

## Caveats

* **One machine, one configuration.** Postgres, Valkey, the workers and the load generator
  compete for the same 16 threads and the same disk. On separate hosts the network adds
  latency and the CPU contention disappears. Neither is modeled here.
* **Not a dedicated machine.** See above. The load averages are stored with every measurement.
* **The ratios are not like-for-like.** Besides durability, the pickup intervals differ in
  what they contain (see "What is timed").
* **Six runs.** Enough to show the spread, not enough for confidence intervals.
* **No-op handler.** Real jobs spend time in their handler, which moves the bottleneck away
  from the queue. For handlers that take tens of milliseconds or longer, the queue is
  unlikely to be the limit at these rates.
* **Default Postgres.** `shared_buffers = 128MB` and the default WAL settings. Tuning
  would likely move the numbers, and it was not tried.
* **Durability is not equal, in enqueue and in drain.** pgqueue commits every job, every
  claim and every completion with an fsync; Valkey runs without persistence and Celery
  acknowledges early (`acks_late=False`), so a crash loses in-flight tasks. A Valkey with
  `appendonly yes`, or Celery with `acks_late=True`, would be slower; neither was measured.
* **Celery outliers.** Three of the Celery cells (4 workers run 6, 8 workers runs 5 and 6) were
  two to three times slower than its other runs for a reason that was not investigated.
* **Celery configuration.** One configuration (thread pool, 32 threads per process,
  prefetch multiplier 4, early ack, defaults otherwise). Other pools and settings were not
  measured. RQ was not benchmarked.
* **Steady-state window.** Backlog throughput excludes the first 20% and the last 5% of
  the completions. It is not the time to drain the whole backlog.
* **The dataset is small.** The 20,000-job backlog stays in memory; a queue table of tens
  of millions of rows, bloat, autovacuum and retention were not tested.
* **Single-job enqueue measures a sequential client.** The "10 concurrent clients" row shares
  one asyncpg pool (pgqueue) or ten threads (Celery); both are bound by the same machine.

## Method (schema 2)

`bench/bench.py` counterbalances the order and records the load; the numbers in this document
come from a run of it.

* **Counterbalanced order.** In every repeat, odd run numbers measure pgqueue first and
  even run numbers measure Celery first. Use an even `--runs` (the published run used 6)
  for an exactly balanced design.
* **Cool-down between the two systems of a pair.** `--cooldown SECONDS` (default 30, forced
  to 0 with `--quick`) pauses between them so the checkpointer and autovacuum started by
  the first measurement can settle instead of landing in the second one. It also adds
  about 30 s per pair to the run time.
* **Load before and after each measurement.** Each record has `load_1min_before` and
  `load_1min` (after), the 1-minute load average from `/proc/loadavg`, and `position`
  (`first`, `second`, or `only` for `enqueue_many`, which has no Celery counterpart).
* **Results file version.** `bench/results.json` has `schema_version: 2`, and `parameters`
  gains `counterbalanced` and `cooldown_s`. The checked-in file is the schema 2 run described
  here (2026-10-14, 6 runs). An older file without `schema_version` is schema 1; `bench.report`
  and `bench.chart` read both versions.

To rerun:

    make bench ARGS="--runs 6"

The published run took about 25 minutes. Check `uptime` first and do not run other jobs at
the same time, otherwise the load columns will just document the same problem again.
