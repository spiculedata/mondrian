#!/usr/bin/env python3
"""Render docs/reports/perf-benchmark-2x2.md from the four per-cell JSON
files produced by PerfBenchmarkTest.

Cells:
  A = hsqldb   + legacy  -> target/perf-bench-hsqldb-legacy.json
  B = postgres + legacy  -> target/perf-bench-postgres-legacy.json
  C = hsqldb   + calcite -> target/perf-bench-hsqldb-calcite.json
  D = postgres + calcite -> target/perf-bench-postgres-calcite.json

Ratios of interest for the blog narrative:
  C/A  — Calcite overhead at toy scale
  D/B  — Calcite value on a real planner
  D/A  — full-rewrite net (scale + planner + emitter)
  D/C  — dataset-scale effect under Calcite

Stdlib-only. Reports medians; falls back to "n/a" for any missing cell.
"""
from __future__ import annotations

import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TARGET = REPO / "target"
OUT = REPO / "docs" / "reports" / "perf-benchmark-2x2.md"

CELLS = [
    ("A", "hsqldb", "legacy"),
    ("B", "postgres", "legacy"),
    ("C", "hsqldb", "calcite"),
    ("D", "postgres", "calcite"),
]


def load_cell(backend: str, emitter: str):
    path = TARGET / f"perf-bench-{backend}-{emitter}.json"
    if not path.exists():
        return None
    with open(path) as fh:
        return json.load(fh)


def fmt_ms(ns: float | None) -> str:
    if ns is None or (isinstance(ns, float) and math.isnan(ns)):
        return "n/a"
    ms = ns / 1e6
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    if ms >= 10:
        return f"{int(ms)}ms"
    return f"{ms:.1f}ms"


def fmt_ratio(num: float | None, den: float | None) -> str:
    if num is None or den is None or den == 0:
        return "n/a"
    r = num / den
    if r >= 100:
        return f"{r:.0f}x"
    if r >= 10:
        return f"{r:.1f}x"
    return f"{r:.2f}x"


def median_of(cell, name):
    if not cell:
        return None
    for q in cell["queries"]:
        if q["name"] == name:
            s = q.get("stats") or {}
            return s.get("median")
    return None


def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    if not xs:
        return None
    ln = [math.log(x) for x in xs]
    return math.exp(sum(ln) / len(ln))


def hw_note() -> str:
    bits = []
    try:
        bits.append(subprocess.check_output(
            ["uname", "-a"], text=True).strip())
    except Exception:
        bits.append(platform.platform())
    try:
        sw = subprocess.check_output(
            ["sw_vers"], text=True, stderr=subprocess.DEVNULL).strip()
        if sw:
            bits.append(sw.replace("\n", " / "))
    except Exception:
        pass
    return "\n".join(f"    {b}" for b in bits)


def main():
    cells = {label: load_cell(b, e) for label, b, e in CELLS}

    # Collect a unified ordered list of (corpus, name) across all cells.
    seen = []
    seen_set = set()
    for cell in cells.values():
        if not cell:
            continue
        for q in cell["queries"]:
            key = (q["corpus"], q["name"])
            if key not in seen_set:
                seen_set.add(key)
                seen.append(key)

    present = [label for label in "ABCD" if cells[label] is not None]
    missing = [label for label in "ABCD" if cells[label] is None]

    lines = []
    lines.append("# Mondrian-on-Calcite 2x2 performance benchmark")
    lines.append("")
    lines.append("Matrix: {HSQLDB ~87k rows, Postgres 86.8M rows} x "
                 "{legacy Mondrian SQL emitter, Calcite SQL emitter}. "
                 "Each cell runs every corpus MDX 3 warm-up + 5 timed "
                 "iterations with a cold Mondrian schema cache before "
                 "each iteration. Median of the 5 timed runs is reported.")
    lines.append("")
    lines.append("Scale factor: Postgres fact table is **1000x** the "
                 "HSQLDB fact table (86.8M vs 87k rows). Dimensions are "
                 "identical.")
    lines.append("")
    lines.append("Hardware:")
    lines.append("")
    lines.append("```")
    lines.append(hw_note())
    lines.append("```")
    lines.append("")
    if missing:
        lines.append(f"> **Note:** cells missing (no JSON found): "
                     f"{', '.join(missing)}. Columns in the table below "
                     f"will read `n/a` for those.")
        lines.append("")

    # Headline table
    lines.append("## Headline table (median ms per query)")
    lines.append("")
    lines.append("| corpus | query | A (hsqldb/legacy) | B (pg/legacy) "
                 "| C (hsqldb/calcite) | D (pg/calcite) | C/A | D/B | D/A |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")

    # Per-corpus accumulators
    corp_meds = {"smoke": {c: [] for c in "ABCD"},
                 "aggregate": {c: [] for c in "ABCD"},
                 "calc": {c: [] for c in "ABCD"},
                 "mvhit": {c: [] for c in "ABCD"}}

    callouts = []

    for corpus, name in seen:
        meds = {c: median_of(cells[c], name) for c in "ABCD"}
        for c in "ABCD":
            if meds[c] is not None:
                corp_meds.setdefault(corpus, {c: [] for c in "ABCD"})[c].append(meds[c])

        row = [corpus, f"`{name}`"]
        row += [fmt_ms(meds[c]) for c in "ABCD"]
        row += [
            fmt_ratio(meds["C"], meds["A"]),
            fmt_ratio(meds["D"], meds["B"]),
            fmt_ratio(meds["D"], meds["A"]),
        ]
        lines.append("| " + " | ".join(row) + " |")

        # Callout rule: any pairwise ratio among the four > 10x.
        vals = [(c, meds[c]) for c in "ABCD" if meds[c]]
        if len(vals) >= 2:
            xs = [v[1] for v in vals]
            if xs and min(xs) > 0 and max(xs) / min(xs) > 10:
                callouts.append((corpus, name, meds))
    lines.append("")

    # Ratios section
    lines.append("## Ratios of interest")
    lines.append("")
    lines.append("Geomean of the **per-query median-ms** ratios across "
                 "the corpus. Higher than 1 means numerator is slower.")
    lines.append("")

    def geomean_ratio(num_cell, den_cell):
        if not cells[num_cell] or not cells[den_cell]:
            return None
        rs = []
        for corpus, name in seen:
            a = median_of(cells[den_cell], name)
            b = median_of(cells[num_cell], name)
            if a and b and a > 0:
                rs.append(b / a)
        return geomean(rs)

    lines.append("| ratio | geomean | interpretation |")
    lines.append("|---|---:|---|")
    rdef = [
        ("C/A", "C", "A", "Calcite overhead at toy scale (HSQLDB)"),
        ("D/B", "D", "B", "Calcite speedup on real planner (Postgres)"),
        ("D/A", "D", "A", "Full-rewrite net (scale + planner + emitter)"),
        ("D/C", "D", "C", "Dataset-scale effect under Calcite"),
    ]
    for label, num, den, interp in rdef:
        gm = geomean_ratio(num, den)
        gm_str = f"{gm:.2f}x" if gm else "n/a"
        lines.append(f"| {label} | {gm_str} | {interp} |")
    lines.append("")

    # Per-corpus summary
    lines.append("## Per-corpus summary")
    lines.append("")
    lines.append("| corpus | cell | n | median-of-medians | geomean |")
    lines.append("|---|---|---:|---:|---:|")
    for corpus in ["smoke", "aggregate", "calc", "mvhit"]:
        for c in "ABCD":
            xs = corp_meds.get(corpus, {}).get(c, [])
            if not xs:
                lines.append(f"| {corpus} | {c} | 0 | n/a | n/a |")
                continue
            med = statistics.median(xs)
            gm = geomean(xs)
            lines.append(f"| {corpus} | {c} | {len(xs)} | "
                         f"{fmt_ms(med)} | {fmt_ms(gm)} |")
    lines.append("")

    # Callouts
    lines.append("## Callouts (>10x spread across cells)")
    lines.append("")
    if not callouts:
        lines.append("_None — all queries within a 10x band across the "
                     "four cells._")
    else:
        lines.append("| corpus | query | A | B | C | D |")
        lines.append("|---|---|---:|---:|---:|---:|")
        for corpus, name, meds in callouts:
            lines.append(
                f"| {corpus} | `{name}` | "
                + " | ".join(fmt_ms(meds[c]) for c in "ABCD") + " |")
    lines.append("")

    # Reproducing
    lines.append("## Reproducing")
    lines.append("")
    lines.append("```sh")
    lines.append("scripts/perf/run-bench-matrix.sh")
    lines.append("```")
    lines.append("")
    lines.append("Or one cell at a time, e.g. cell A (HSQLDB + legacy):")
    lines.append("")
    lines.append("```sh")
    lines.append("mvn -Pcalcite-harness -Dharness.runPerfBench=true \\")
    lines.append("    -Dmondrian.backend=legacy \\")
    lines.append("    -Dtest=PerfBenchmarkTest test")
    lines.append("python3 scripts/perf/render-bench-report.py")
    lines.append("```")
    lines.append("")

    # Raw source files
    lines.append("## Raw data")
    lines.append("")
    for label, b, e in CELLS:
        path = TARGET / f"perf-bench-{b}-{e}.json"
        status = "present" if path.exists() else "MISSING"
        lines.append(f"- Cell {label} ({b}/{e}): `{path.relative_to(REPO)}` — {status}")
    lines.append("")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
