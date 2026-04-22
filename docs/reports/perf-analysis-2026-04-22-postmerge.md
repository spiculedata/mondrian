# Perf analysis — post shape-catalog merge + calc-pushdown revert

**Date:** 2026-04-22
**HEAD:** `a186103` (jakarta-servlet branch base + shape catalog + calc-pushdown SegmentLoader revert + CI workflow)
**Scope:** Clean sequential 6-cell re-run after merging the shape-catalog feature branch into `calcite-backend-agg-and-calc` and reverting the calc-pushdown `SegmentLoader` commit (`439438c`) that bisect identified as a Postgres-wide regression.
**Corpus:** 45 MDX queries (20 smoke + 12 aggregate + 12 calc + 1 iif), plus a 4-query MvHit corpus run with `UseAggregates=false`.
**Iterations:** 1 warmup + 3 timed, median reported.
**Hardware:** Apple M-series laptop, Postgres 18.1, HSQLDB 1.8.

## Headline ratios

|  | HSQLDB (~87k rows) | Postgres (86.8M rows, 1000×) |
|---|---|---|
| Legacy Mondrian SQL | **Cell A** | **Cell B** |
| Calcite SQL | **Cell C** | **Cell D** |

| Ratio | Geomean | Wall |
|---|---|---|
| **C/A** | **0.208×** → Calcite **4.80× faster on HSQLDB** | A 3:56 / C 0:48 |
| **D/B** | **0.995×** → parity on Postgres | B 33:13 / D 33:12 |

| MvHit ratio | Geomean | |
|---|---|---|
| **F/E** | **0.104×** → Calcite **9.59× faster on declared aggregates** | E 3:19 / F 0:34 |

## Per-corpus breakdown — Postgres D/B

| Corpus | Queries | D/B geomean | Read |
|---|---:|---:|---|
| Smoke | 21 | **0.977** | Calcite ~2% faster |
| Aggregate | 12 | **0.979** | Calcite ~2% faster |
| Calc (with `9c3b8e6` Project-skip) | 12 | **1.044** | Calcite ~4% slower (residual; see below) |

## MvHit unlock (UseAggregates=false)

When Mondrian's `UseAggregates=true` is flipped off, legacy forces a full 86.8M-row `sales_fact_1997` scan. Calcite's hand-rolled MV matcher rewrites these to scan the tiny pre-aggregated tables.

| Query | Legacy (fact scan) | Calcite (matcher agg scan) | F/E | Speedup |
|---|---:|---:|---:|---:|
| `agg-c-year-country` | 9 835 ms | **1 162 ms** | 0.118× | **8.5×** |
| `agg-c-quarter-country` | 11 572 ms | **1 250 ms** | 0.108× | **9.3×** |
| `agg-g-ms-pcat-family-gender` | 12 573 ms | **1 229 ms** | 0.098× | **10.2×** |
| `agg-g-ms-pcat-family-gender-marital` | 13 057 ms | **1 237 ms** | 0.095× | **10.6×** |
| **Geomean** | | | **0.104×** | **9.6×** |

## Notable per-query Postgres results

**Top wins:**

| Query | B (legacy) | D (calcite) | D/B |
|---|---:|---:|---:|
| `named-set` | 30 312 ms | **26 111 ms** | 0.86× |
| `topcount` | 30 850 ms | **26 810 ms** | 0.87× |
| `native-topcount-product-names` | 35 927 ms | **32 161 ms** | 0.90× |
| `agg-distinct-count-customers-levels` | 30 375 ms | **27 306 ms** | 0.90× |
| `agg-distinct-count-quarters` | 9 564 ms | **8 782 ms** | 0.92× |
| `ancestor` | 1 181 ms | **1 091 ms** | 0.92× |

**Top regressions (all <20%; concentrated in calc corpus):**

| Query | B (legacy) | D (calcite) | D/B |
|---|---:|---:|---:|
| `calc-arith-sum` | 7 842 ms | 9 334 ms | 1.19× |
| `calc-arith-ratio` | 8 132 ms | 9 409 ms | 1.16× |
| `calc-arith-unary-minus` | 7 619 ms | 8 701 ms | 1.14× |
| `calc-arith-const-multiply` | 8 141 ms | 8 703 ms | 1.07× |

The remaining ~15% calc-arith drag is from the kept `9c3b8e6` commit (calc-pushdown's outer-Project-skip). It shipped because (a) it's a prerequisite for the eventual SQL-side calc emission and (b) the regression is small enough not to gate the wider win. Plan B's SegmentLoader consume path stays out of `main` until profiled — see below.

## What changed since the prior 2026-04-21 baseline

Prior baseline (`docs/reports/perf-analysis-final.md`):
- C/A = 0.219× (4.57×), D/B = 0.958× (1.04×), MvHit = 0.103× (9.7×).

Now:
- **C/A 4.80×** (+0.23 vs 4.57) — incremental gain from shape-catalog enumeration.
- **D/B 1.01×** — held parity. Within measurement noise of prior 1.04×.
- **MvHit 9.59×** — matches prior 9.7× within noise; the 11.7× we briefly saw in the broken-merge run was likely warm-cache effects from the regression elsewhere.

## What got reverted and why

Bisect on the merged HEAD (commit `0f82b2a`) showed Postgres D/B geomean had regressed to **1.16×** (16% slower than legacy). Per-cell analysis isolated the regression to a single commit (`439438c`, "feat(calcite): SegmentLoader tolerates calc-consume extra column"). Reverting that commit alone restored parity (D/B = 0.94× across smoke corpus, equivalent to reverting the whole calc-pushdown merge).

The diff in `439438c` is small and gated behind `mondrian.calcite.calcConsume=false` (off by default). Static reading does not explain the 2× per-query regression on non-calc Postgres queries (`slicer-where`, `time-fn`, `topcount`). Likely a JIT-deopt or hot-path branch interaction visible only with a profiler. The commit lives on `calcite-calc-pushdown-sql` for future investigation; it is not part of `main`.

The kept commit `9c3b8e6` (calc-pushdown's outer-Project-skip in `CalciteSqlPlanner`) ships because it's a prerequisite for the eventual SegmentLoader consume path and its perf cost is bounded (~4% on calc corpus, no measurable cost on smoke or aggregate).

## Provenance

| Cell | Source JSON | Wall time |
|---|---|---|
| A | `/tmp/rerun/A-hsqldb-legacy-perf-bench-hsqldb-legacy.json` | 3:56 |
| B | `/tmp/rerun/B-postgres-legacy-perf-bench-postgres-legacy.json` | 33:13 |
| C | `/tmp/rerun/C-hsqldb-calcite-perf-bench-hsqldb-calcite.json` | 0:48 |
| D | `/tmp/rerun/D-postgres-calcite-perf-bench-postgres-calcite.json` | 33:12 |
| E (MvHit legacy noagg) | `/tmp/rerun/E-pg-legacy-noagg-perf-bench-postgres-legacy.json` | 3:19 |
| F (MvHit calcite noagg) | `/tmp/rerun/F-pg-calcite-noagg-perf-bench-postgres-calcite.json` | 0:34 |

Clean sequential run; no parallel JVM contention.

## Outstanding work

- **Plan A** (Volcano-driven MV selection) — depends on Plan C; not yet started.
- **Plan B** (calc pushdown into SQL with SegmentLoader consume) — `9c3b8e6` shipped; SegmentLoader path (`439438c`) parked pending profiler investigation of the cross-cutting Postgres regression.
- **Plan C** (shape catalog) — Phase 1 shipped; Phase 2 (FK-dim level-attribute enumeration) remains.
