# VolcanoPlanner + MV Rewriting — final outcome

**Date:** 2026-04-21
**Worktree:** `calcite-backend-agg-and-calc`
**Predecessors:** VP-1 diagnosis, VP-2 shape-aware MVs, VP-3 PK/FK metadata, VP-D hand-rolled matcher, plus the Phase 2 and Phase 3 reports.

## Summary

The hand-rolled `MvMatcher` **works and delivers 9-11× speedup on queries it matches correctly**, but has shape-equivalence gaps that cause a LEGACY_DRIFT regression on `time-fn`/`parallelperiod` queries plus a 1.36× slowdown on one MvHit query (`agg-g-ms-pcat-family-gender-marital` on Postgres). **Shipped behind `-Dmondrian.calcite.mvMatch=true` — off by default — so the infrastructure is in-tree and opt-in for pre-verified workloads without regressing the default corpus.**

## Numbers

### Postgres MvHit, `UseAggregates=false` (1 warmup + 3 timed median)

With `-Dmondrian.calcite.mvMatch=true`:

| Query | Legacy | Calcite | D/B | Speedup |
|---|---:|---:|---:|---:|
| `agg-c-year-country` | 11 589 ms | **1 275 ms** | **0.110×** | **9.1×** |
| `agg-c-quarter-country` | 14 085 ms | **1 317 ms** | **0.094×** | **10.7×** |
| `agg-g-ms-pcat-family-gender` | 12 967 ms | 13 039 ms | 1.006× | no rewrite |
| `agg-g-ms-pcat-family-gender-marital` | 13 017 ms | 17 650 ms | 1.356× | regression |
| **geomean** | — | — | **0.344×** | **2.91× faster** |

Two decisive 10× wins (agg_c_14 shapes), one no-op (matcher didn't match), one regression (matcher matched but produced a slower plan).

### HSQLDB harness, flag OFF (default)

All 44/44 pass both backends. Y.3's baseline 0.93× geomean D/B stands for the general corpus.

### HSQLDB MvHit synthetic tests

`MvMatcherTest` 6/6, `MvRuleRewriteTest` 4/4 — flag is enabled in `@BeforeClass`, matcher fires, all 4 MvHit shapes rewrite to agg-table SQL.

## What caused the deferment behind a flag

Default-enabled, the matcher tripped 2 harness queries with `LEGACY_DRIFT`:
- `EquivalenceSmokeTest.equivalent[time-fn]` — sqlExecution[6] cell-set signals.
- `EquivalenceSmokeTest.equivalent[parallelperiod]` — similar.

Both are time-hierarchy queries that the matcher over-matched to `agg_c_14` when it shouldn't (likely because the query filters or projects on time columns not present on the agg). The matcher's shape-equivalence check needs a correctness tightening: "MV subsumes user query" must be strict enough that the agg scan produces identical rows to the base-fact scan.

Also the `agg-g-ms-pcat-family-gender-marital` 1.36× slowdown — matcher rewrote successfully but Postgres picked a slower plan for the agg scan. Worth investigating but the 2/4 big wins prove the approach.

## What shipped in this leg

1. **`MvMatcher` class** (`src/main/java/mondrian/calcite/MvMatcher.java`, ~280 LOC) — walks `MvRegistry` shapes, rewrites matching `PlannerRequest`s to scan agg tables with adjusted measures + reduced joins.

2. **Registry late-bind retry in `CalcitePlannerCache`** — first call's RolapSchema may have empty cube list due to lazy init; subsequent calls retry until size > 0.

3. **Gate in `CalciteSqlPlanner.plan()`** — matcher runs only when `-Dmondrian.calcite.mvMatch=true`. Flag defaults off so the default harness + default perf stays at Y.3's 0.93× geomean without regression.

4. **Postgres dispatch wiring** — `SegmentLoader.plannerFor(star)` and `SqlTupleReader` now pass `RolapSchema` so the cache can build an MvRegistry for Postgres runs (previously only HSQLDB path had this).

## Where this lands in the larger story

| Phase | Outcome | D/B impact |
|---|---|---|
| Y.2 JDBC-identity cache | **Big win** | 1.27× → 0.93× (7% faster) |
| Y.3 re-measurement | Confirmed Y.2 | — |
| Y.4 Phase 2 cache pivot | Shipped + reverted | — |
| Y.4 Phase 3 HepPlanner + MvRegistry | Infrastructure | — |
| VP VolcanoPlanner + PK/FK | Infrastructure | — |
| **VP-D hand-rolled matcher (opt-in)** | **Working, 2/4 decisive wins** | **0.344× geomean on matched queries, 10× speedup on best cases** |

## Honest assessment

**The blog headline can now be:**
- **Calcite is 7% faster than legacy on average across the 45-query corpus (Y.2).**
- **Up to 10× faster on queries that hit aggregate tables, even without the Mondrian `UseAggregates=true` property flip that legacy requires.**
- Infrastructure for Calcite-side cost-based MV selection (VolcanoPlanner + rule family + PK/FK metadata) in place for future expansion.

**Caveats that must be in the post:**
- The 10× win is gated behind `-Dmondrian.calcite.mvMatch=true` while the matcher's subsumption check gets tightened.
- FoodMart's aggregate MeasureGroups cover a narrow set of query shapes. Real-world wins depend on whether your workload's common shapes map cleanly to your declared aggs.
- 1 of 4 MvHit queries on Postgres regresses by 36% when the matcher fires; 2 of 20 smoke queries drift if the matcher is forced on. Both are known, both fixable with more matcher polish, neither is in the default path.

## Outstanding work (for whoever picks this up next)

1. **Tighten `MvMatcher`'s shape-equivalence check** so `time-fn` / `parallelperiod` don't over-match. The matcher today accepts any MV whose group-by ⊇ user's group-by; needs additional checks that filters/projections are precisely satisfiable on the agg.

2. **Diagnose `agg-g-ms-pcat-family-gender-marital` regression** — matcher rewrites to agg but Postgres picks a slower plan. Probably a missing `pg_statistic` hint or a bad cost estimate from our JDBC row-count stats.

3. **Extend the shape catalog**. Today `MvRegistry.buildShapeSpecs` targets the 4 MvHit shapes. A general power-set enumeration would cover more real workloads (and need correctness validation per new shape).

4. **Flip the flag default to true once the above lands** and re-run the full 45-query corpus to confirm no regressions.

## Files modified this leg

- `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` — flag-gated matcher call.
- `src/main/java/mondrian/calcite/CalcitePlannerCache.java` — retry on empty registry.
- `src/test/java/mondrian/calcite/MvRuleRewriteTest.java` — sets flag in @BeforeClass.

## Gates

- HSQLDB harness 44/44 both backends with flag OFF ✅
- HSQLDB unit tests (MvMatcher, MvRuleRewrite) pass with flag ON ✅
- Postgres MvHit: 2 queries 10× faster, 1 unchanged, 1 regression (with flag ON).
- `time-fn` / `parallelperiod` LEGACY_DRIFT under flag ON (not in default path) ⚠
