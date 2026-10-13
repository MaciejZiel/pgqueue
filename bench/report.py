"""Print the Markdown tables of docs/benchmarks.md from bench/results.json.

    uv run python -m bench.report

Every cell is the median over the runs, with the minimum and maximum run in brackets.
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any

RESULTS = Path(__file__).resolve().parent.parent / "bench" / "results.json"
NAMES = {"pgqueue": "pgqueue", "celery": "Celery + Valkey"}
# Column headers say which Celery configuration the numbers belong to.
HEADS = {"pgqueue": "pgqueue", "celery": "Celery (thread pool, 32 threads, prefetch x4, early ack)"}


def get(rec: dict[str, Any], key: str) -> float:
    value: Any = rec
    for part in key.split("."):
        value = value[part]
    return float(value)


def cell(recs: list[dict[str, Any]], key: str, fmt: str) -> str:
    if not recs:
        return "n/a"
    vals = [get(r, key) for r in recs]
    med, lo, hi = statistics.median(vals), min(vals), max(vals)
    return f"{fmt.format(med)} ({fmt.format(lo)}-{fmt.format(hi)})"


def select(records: list[dict[str, Any]], **where: Any) -> list[dict[str, Any]]:
    return [r for r in records if all(r.get(k) == v for k, v in where.items())]


def main() -> None:
    data = json.loads(RESULTS.read_text())
    recs, params = data["records"], data["parameters"]
    systems = [s for s in NAMES if select(recs, system=s)]
    workers = params["workers"]
    print(f"Runs per cell: {params['runs']}. Cells are median (min-max).\n")

    print("### Enqueue\n")
    print("| Variant | System | jobs/s | p50 ms | p95 ms | p99 ms |")
    print("|---|---|---:|---:|---:|---:|")
    for variant, label in (
        ("single", "1 client, 1 job per call"),
        ("ten", "10 concurrent clients"),
        ("bulk", "`enqueue_many`, 1,000 per call (latency is per call)"),
    ):
        for s in systems:
            r = select(recs, system=s, scenario="enqueue", variant=variant)
            if r:
                cols = [cell(r, "rate", "{:,.0f}")]
                cols += [cell(r, f"latency_ms.{q}", "{:,.1f}") for q in ("p50", "p95", "p99")]
                print(f"| {label} | {NAMES[s]} | " + " | ".join(cols) + " |")

    print("\n### Backlog throughput (jobs/s)\n")
    print("| Worker processes | " + " | ".join(HEADS[s] for s in systems) + " |")
    print("|---:|" + "---:|" * len(systems))
    for w in workers:
        row = [
            cell(select(recs, system=s, scenario="drain", workers=w), "rate", "{:,.0f}")
            for s in systems
        ]
        print(f"| {w} | " + " | ".join(row) + " |")

    print(f"\n### Pickup latency at {params['arrival_rate']} jobs/s (ms)\n")
    print("| Worker processes | System | p50 | p95 | p99 | max |")
    print("|---:|---|---:|---:|---:|---:|")
    for w in workers:
        for s in systems:
            r = select(recs, system=s, scenario="pickup", workers=w)
            cols = [cell(r, f"latency_ms.{q}", "{:,.1f}") for q in ("p50", "p95", "p99", "max")]
            print(f"| {w} | {NAMES[s]} | " + " | ".join(cols) + " |")

    print("\n### Payload size, 4 workers, backlog throughput (jobs/s)\n")
    print("| Payload | " + " | ".join(HEADS[s] for s in systems) + " |")
    print("|---|" + "---:|" * len(systems))
    for label, size in (("small", 256), ("medium", 2048), ("large", 16384)):
        row = [
            cell(select(recs, system=s, scenario="drain-size", label=label), "rate", "{:,.0f}")
            for s in systems
        ]
        print(f"| {size:,} B | " + " | ".join(row) + " |")

    loads = [float(r["load_1min"]) for r in recs]
    if data.get("schema_version", 1) < 2:
        print(
            "\nResults file schema 1: the order was fixed (pgqueue first, then Celery) and "
            "one load value was stored per measurement, taken after it."
        )
    print(
        f"\nOne-minute load average sampled at each measurement: "
        f"min {min(loads):.1f}, median {statistics.median(loads):.1f}, max {max(loads):.1f}."
    )


if __name__ == "__main__":
    main()
