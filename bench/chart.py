"""Draw docs/benchmarks-*.svg from bench/results.json using only the standard library.

    make bench-chart

Points are medians over the repeated runs; the whiskers span the minimum to the maximum
run, so the spread is visible in the picture and not only in the tables.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results.json"
DOCS = ROOT / "docs"

# Okabe-Ito colours (colour-blind safe); every series also has its own marker and dash.
BLUE, ORANGE, GREEN, PURPLE, GREY, RED = (
    "#0072b2",
    "#d55e00",
    "#009e73",
    "#8e5a9b",
    "#6b7280",
    "#b3261e",
)
INK, MUTED, GRID, BG = "#1f2328", "#59636e", "#d8dee4", "#ffffff"
FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
MARKERS = ("circle", "square", "diamond", "triangle", "cross")


@dataclass
class Series:
    label: str
    color: str
    marker: str
    dash: str = ""
    points: list[tuple[float, float, float, float]] = field(default_factory=list)  # x, med, lo, hi


def _marker(kind: str, x: float, y: float, color: str) -> str:
    s = 4.5
    common = f'fill="{color}" stroke="{BG}" stroke-width="1.5"'
    if kind == "square":
        return f'<rect x="{x - s:.1f}" y="{y - s:.1f}" width="{2 * s}" height="{2 * s}" {common}/>'
    if kind == "diamond":
        pts = f"{x},{y - s - 1} {x + s + 1},{y} {x},{y + s + 1} {x - s - 1},{y}"
        return f'<polygon points="{pts}" {common}/>'
    if kind == "triangle":
        pts = f"{x},{y - s - 1} {x + s + 1},{y + s} {x - s - 1},{y + s}"
        return f'<polygon points="{pts}" {common}/>'
    if kind == "cross":
        return (
            f'<path d="M{x - s},{y - s} L{x + s},{y + s} M{x - s},{y + s} L{x + s},{y - s}" '
            f'stroke="{color}" stroke-width="2.5" fill="none"/>'
        )
    return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{s}" {common}/>'


def _nice_max(value: float) -> float:
    step = 10 ** (len(str(int(value))) - 1)
    for mult in (1, 2, 2.5, 5, 10):
        if value <= mult * step:
            return float(mult * step)
    return float(10 * step)


def line_chart(  # noqa: PLR0917
    title: str,
    subtitle: str,
    xlabels: list[str],
    series: list[Series],
    ylabel: str,
    note: str,
    *,
    fmt: str = "{:,.0f}",
) -> str:
    w, h = 860, 480
    left, right, top, bottom = 78, 190, 78, 70
    pw, ph = w - left - right, h - top - bottom
    ymax = _nice_max(max(p[3] for s in series for p in s.points) * 1.05)
    n = len(xlabels)

    def px(i: float) -> float:
        return left + pw * (i + 0.5) / n

    def py(v: float) -> float:
        return top + ph * (1 - v / ymax)

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'role="img" aria-labelledby="t d" font-family="{FONT}">',
        f'<title id="t">{escape(title)}</title><desc id="d">{escape(subtitle)}</desc>',
        f'<rect width="{w}" height="{h}" fill="{BG}"/>',
        f'<text x="{left}" y="30" font-size="18" font-weight="600" fill="{INK}">'
        f"{escape(title)}</text>",
        f'<text x="{left}" y="52" font-size="12.5" fill="{MUTED}">{escape(subtitle)}</text>',
    ]
    for k in range(5):
        v = ymax * k / 4
        y = py(v)
        out.append(
            f'<line x1="{left}" x2="{left + pw}" y1="{y:.1f}" y2="{y:.1f}" stroke="{GRID}"/>'
        )
        out.append(
            f'<text x="{left - 8}" y="{y + 4:.1f}" font-size="11.5" text-anchor="end" '
            f'fill="{MUTED}">{fmt.format(v)}</text>'
        )
    out.append(
        f'<text transform="translate(18 {top + ph / 2}) rotate(-90)" font-size="12" '
        f'text-anchor="middle" fill="{MUTED}">{escape(ylabel)}</text>'
    )
    for i, lab in enumerate(xlabels):
        out.append(
            f'<text x="{px(i):.1f}" y="{top + ph + 22}" font-size="12" text-anchor="middle" '
            f'fill="{MUTED}">{escape(lab)}</text>'
        )
    out.append(
        f'<text x="{left + pw / 2}" y="{top + ph + 44}" font-size="12" text-anchor="middle" '
        f'fill="{MUTED}">worker processes</text>'
    )
    for s in series:
        pts = [(px(x), py(m), py(lo), py(hi)) for x, m, lo, hi in s.points]
        dash = f' stroke-dasharray="{s.dash}"' if s.dash else ""
        if len(pts) > 1:
            path = " ".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in pts)
            out.append(
                f'<polyline points="{path}" fill="none" stroke="{s.color}" stroke-width="2"{dash}/>'
            )
        for x, y, ylo, yhi in pts:
            if abs(ylo - yhi) > 1:
                out.append(
                    f'<path d="M{x:.1f},{ylo:.1f} V{yhi:.1f} M{x - 4:.1f},{ylo:.1f} h8 '
                    f'M{x - 4:.1f},{yhi:.1f} h8" stroke="{s.color}" '
                    f'stroke-width="1.2" fill="none"/>'
                )
            out.append(_marker(s.marker, x, y, s.color))
    ly = top + 4
    for s in series:
        lx = left + pw + 24
        dash = f' stroke-dasharray="{s.dash}"' if s.dash else ""
        out.append(
            f'<line x1="{lx}" x2="{lx + 22}" y1="{ly}" y2="{ly}" stroke="{s.color}" '
            f'stroke-width="2"{dash}/>'
        )
        out.append(_marker(s.marker, lx + 11, ly, s.color))
        out.append(
            f'<text x="{lx + 30}" y="{ly + 4}" font-size="12" fill="{INK}">{escape(s.label)}</text>'
        )
        ly += 22
    out.append(f'<text x="{left}" y="{h - 12}" font-size="11" fill="{MUTED}">{escape(note)}</text>')
    out.append("</svg>")
    return "\n".join(out) + "\n"


def aggregate(
    records: list[dict[str, Any]], system: str, scenario: str, key: str, workers: list[int]
) -> list[tuple[float, float, float, float]]:
    """(x index, median, min, max) of ``key`` over runs, one point per worker count."""
    out = []
    for i, w in enumerate(workers):
        vals = [
            _get(r, key)
            for r in records
            if r["system"] == system
            and r["scenario"] == scenario
            and r.get("workers") == w
            and "label" not in r
        ]
        if vals:
            out.append((float(i), statistics.median(vals), min(vals), max(vals)))
    return out


def _get(rec: dict[str, Any], key: str) -> float:
    value: Any = rec
    for part in key.split("."):
        value = value[part]
    return float(value)


def build(results: dict[str, Any]) -> dict[str, str]:
    params, env, records = results["parameters"], results["environment"], results["records"]
    workers = list(params["workers"])
    labels = [str(w) for w in workers]
    runs = params["runs"]
    kib = params["payload_bytes"] / 1024
    note = (
        f"Median of {runs} run{'s' if runs != 1 else ''}, whiskers = min to max. "
        f"{env['cpu'].split(' (')[0]}; PostgreSQL, Valkey, workers and load generator share it."
    )
    systems = [
        ("pgqueue", "pgqueue (PostgreSQL)", BLUE, "circle", ""),
        ("celery", "Celery (Valkey)", ORANGE, "triangle", "6 4"),
    ]
    present = {r["system"] for r in records}
    drain = [
        Series(label, color, marker, dash, aggregate(records, s, "drain", "rate", workers))
        for s, label, color, marker, dash in systems
        if s in present
    ]
    charts = {
        "benchmarks-throughput.svg": line_chart(
            "Backlog throughput by worker processes",
            f"Jobs per second draining a pre-loaded backlog, no-op handler, {kib:g} KiB payload, "
            f"{params['slots_per_worker']} slots per worker",
            labels,
            drain,
            "jobs per second",
            note,
        )
    }
    lat: list[Series] = []
    for s, label, color, marker, _dash in systems:
        if s not in present:
            continue
        short = label.split(" (")[0]
        for q, qcolor, qmarker in (
            ("p50", color, marker),
            ("p99", GREY if s == "pgqueue" else RED, "square"),
        ):
            pts = aggregate(records, s, "pickup", f"latency_ms.{q}", workers)
            lat.append(Series(f"{short} {q}", qcolor, qmarker, "" if q == "p50" else "2 3", pts))
    charts["benchmarks-latency.svg"] = line_chart(
        "Pickup latency by worker processes",
        f"Enqueue to handler start at a steady {params['arrival_rate']} jobs/s, "
        f"{kib:g} KiB payload (lower is better)",
        labels,
        lat,
        "milliseconds",
        note,
        fmt="{:,.1f}",
    )
    return charts


def main() -> None:
    results = json.loads(RESULTS.read_text())
    for name, svg in build(results).items():
        (DOCS / name).write_text(svg)
        print(f"wrote docs/{name}")


if __name__ == "__main__":
    main()
