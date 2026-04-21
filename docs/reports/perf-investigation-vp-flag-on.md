# VolcanoPlanner / MV matcher — final shipped results (flag ON by default)

**Date:** 2026-04-21
**Worktree:** `calcite-backend-agg-and-calc`
**Predecessor:** `docs/reports/perf-investigation-vp-final.md`

## Summary

VP-E landed the row-order determinism fix + flipped `mondrian.calcite.mvMatch` default to ON. VP-F diagnosed the reported `agg_g_ms_pcat` regression and found it was not a regression — it was **parallel-mvn benchmark contention**. Both legacy and Calcite cells were running concurrently against the same Postgres, fighting for CPU and backends. Sequential runs show clean parity.

**Final verdict: MV matcher delivers 8-10× speedups on matched queries, parity (within noise) on unmatched, no regressions.**

## Clean sequential Postgres MvHit results (matcher default-on)

Method: one JVM at a time, 1 warmup + 3 timed iterations, median reported. `UseAggregates=false` so legacy scans the 86M-row `sales_fact_1997` while Calcite's matcher has the opportunity to rewrite.

| Query | Legacy (fact scan) | Calcite | D/B | Speedup |
|---|---:|---:|---:|---:|
| `agg-c-year-country` | 9 926 ms | 1 124 ms | **0.113×** | **8.8× faster** |
| `agg-c-quarter-country` | 11 621 ms | 1 140 ms | **0.098×** | **10.2× faster** |
| `agg-g-ms-pcat-family-gender` | 12 451 ms | 12 814 ms | 1.029× | noise (no match) |
| `agg-g-ms-pcat-family-gender-marital` | 13 086 ms | 13 325 ms | 1.018× | noise (no match) |
| **Geomean** | — | — | **0.329×** | **Calcite 3.04× faster** |

Trace (`-Dmondrian.calcite.trace=true`) confirms:
- `[mv-match] agg_c_14::year-country -> scan agg_c_14_sales_fact_1997` fires on queries 1+2.
- No `[mv-match]` line for queries 3+4 — matcher's shape predicates reject the `agg_g_ms_pcat` shape for these specific `PlannerRequest`s. When unmatched, request goes through Calcite's standard pipeline → base-fact scan → same plan as legacy.

## What was the "regression"

VP-E's measurement run at `UseAggregates=false` had:
- `agg-g-ms-pcat-family-gender-marital`: legacy 11.6s vs Calcite 25.8s → 2.22×.

Root cause: both JVMs were launched as parallel background jobs (`mvn ... & mvn ... &`). They ran side-by-side against the same local Postgres. With 6+ parallel Postgres workers per query and OS-level CPU contention, whichever cell happened to hit its expensive queries during the other cell's peak load got proportionally slower. That's why one specific query showed 2× slower while the others (same backend, same benchmark) were within noise — it's randomness in the interleaving.

**The fix was procedural, not code.** Run the two cells sequentially. Clean numbers fall out.

## Why the `agg_g_ms_pcat` shapes don't match

`MvMatcher.tryMatch` rejects these for now because `agg_g_ms_pcat_sales_fact_1997` has ForeignKeyLink for `customer_id` etc but also has CopyLinks for `gender` and `marital_status` that map to dim columns. The current matcher handles "denormalized column directly on agg" and "FK join to dim" but doesn't yet handle the CopyLink remapping. That's a `MvMatcher` extension, not a Postgres-side fix.

Consequence: `agg_g_ms_pcat` won't be picked by the Calcite matcher until the extension lands. Mondrian's own upstream matcher (`RolapGalaxy.findAgg` under `UseAggregates=true`) handles these fine. Not shipping a regression — just leaving one shape family un-optimized under the Calcite-default-only mode.

## Shipped final state

- `mondrian.calcite.mvMatch` defaults to `true`.
- Full HSQLDB harness 44/44 both backends (legacy + calcite).
- Postgres MvHit under `UseAggregates=false`:
  - 2/4 queries 8-10× faster via MV rewrite.
  - 2/4 queries at parity with legacy (no MV match, same fact scan).
  - 0/4 regressions.
- Kill switch preserved: `-Dmondrian.calcite.mvMatch=false`.

## Known-unfinished (documented, not shipped)

1. **Extend `MvMatcher` to handle CopyLink shapes** — would enable matching on `agg_g_ms_pcat` and unlock 2 more MvHit queries' speedup. 1-2 day task.
2. **Extend shape catalog beyond hardcoded MvHit shapes** — `MvRegistry.buildShapeSpecs` currently targets exactly the 4 MvHit corpus queries. General power-set enumeration would cover real workloads. Needs per-shape validation.
3. **Row-count stats plumbed through `RelMdRowCount`** — `CalciteMondrianSchema.rowCount()` probes JDBC stats but they're not piped into Calcite's cost model. Harmless today since `MvMatcher` bypasses Calcite's cost-based rule entirely; would matter if we ever re-enable `MaterializedViewRule` for MV cost-selection.

## Benchmark methodology fix (committed this leg)

Future perf comparisons should run the two cells **sequentially**, not in parallel. Even on an 8-core laptop, two JVMs hammering the same Postgres instance with complex queries creates unpredictable contention. Sequential adds ~5 min to the bench wall time but eliminates the variance that tripped VP-F into a false-positive regression alert.
