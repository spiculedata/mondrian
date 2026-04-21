# VolcanoPlanner MV rule — honest outcome

**Date:** 2026-04-21
**Worktree:** `calcite-backend-agg-and-calc`
**Predecessors:** `perf-investigation-volcano-mv-diagnosis.md` (VP-1 — rule fails substitution), VP-2 (shape-aware MVs), VP-3 (PK/FK metadata — test-level rewrite succeeds)

## Summary

The MV rule **fires and rewrites correctly when called directly** with hand-built `PlannerRequest`s (`MvRuleRewriteTest` 4/4 green — Calcite emits `FROM "agg_c_14_sales_fact_1997"` instead of `FROM "sales_fact_1997"`). But it **does NOT fire at runtime** through the `SegmentLoader → CalcitePlannerAdapters.fromSegmentLoad → CalciteSqlPlanner.plan()` dispatch path — the bench still shows `FROM "sales_fact_1997"` on every segment load, identical wall times vs legacy.

**Geomean D/B with `UseAggregates=false`: 0.985×** — no improvement from the MV work.

## What did ship (kept)

1. **VolcanoPlanner stage** (`46027e0`) — wired correctly in `CalciteSqlPlanner.plan()`. Runs after Hep. Graceful fallback on error. Kill switch `-Dmondrian.calcite.volcano=false`.

2. **MvRegistry with shape-aware MVs** (`0bea1eb`) — 5 per-query-shape materializations registered per agg MeasureGroup (up from 1 full-star per agg). Shapes chosen to match the 4 MvHit corpus queries.

3. **PK/FK metadata synthesis** (`37874d0`) — `CalciteMondrianSchema.DecoratingJdbcSchema` + `StatisticTable` wrap reflected JDBC tables with synthesised primary keys + foreign-key constraints so Calcite's Goldstein-Larson duplication check can verify MV substitutions preserve row counts. Required on HSQLDB (no PKs declared) and harmless on Postgres (FKs don't exist there either since we loaded from HSQLDB script).

4. **Dispatch-seam wiring** (this commit) — `SegmentLoader.plannerFor(star)` and `SqlTupleReader.plannerFor(ds, schema)` now pass `RolapSchema` through to `CalcitePlannerCache.plannerFor(ds, schema)`. Cache late-binds MV registry if first caller had no schema (e.g. `SqlStatisticsProvider`'s cardinality probe).

5. **4/4 unit-level MV rewrites pass** — `MvRuleRewriteTest` exercises `CalciteSqlPlanner.plan()` directly with hand-built requests; Calcite emits agg-table SQL.

## What's unresolved

The runtime `PlannerRequest` built by `CalcitePlannerAdapters.fromSegmentLoad` has a **different structural shape** from the hand-built requests in `MvRuleRewriteTest`. When fed through the same planner, it produces the same RelNode shape as the MV's defining query at the SQL level (`FROM "sales_fact_1997" JOIN ...`) — but internally something differs enough that `SubstitutionVisitor` can't match.

Suspect causes (not diagnosed — would take another 4-6h):
- Runtime request may carry extra filters/projections from Mondrian's slicer context or group-by levels that tests don't include.
- `GroupingSetsList`'s interaction with `CalcitePlannerAdapters.fromSegmentLoad` may produce a subtly different RelNode structure than synthesised tests.
- RexNode representation differences (e.g. literals typed as `INTEGER` vs `SMALLINT`) can block `SubstitutionVisitor` equivalence even when user + MV SQL print identically.

A focused diagnostic would be: capture the exact runtime `RelNode` at `CalciteSqlPlanner.plan()` entry, compare via `RelOptUtil.toString()` against the same shape's MV defining query. Find the RexNode-level diff.

## Why we're stopping here

Time budget for "push for real MV firing" (option 2 from the end-of-Y.4 summary) has been consumed: VP-1 diagnosis (2h), VP-2 shape-aware MVs (1 day), VP-3 PK/FK metadata (2h), this dispatch wiring fix (1h). That's the 2-3 day budget.

**Pragmatic next step:** VP-4 would run the focused RelNode diff diagnostic above. 4-6h more. Either it identifies a surgical fix (surfaced shape difference, fixable in 1h) or it surfaces a structural mismatch requiring `CalcitePlannerAdapters.fromSegmentLoad` refactor (1-2 days).

**Alternative:** accept that Calcite's general-purpose MV framework doesn't fit Mondrian's runtime shape and pivot to **option D** (hand-rolled matcher). A custom pre-Hep rule that checks each `PlannerRequest` against `MvRegistry` shapes and rewrites by swapping `factTable` to the agg-table name + adjusting measures. Deterministic, reliable, bypasses Calcite's fragile subsumption logic. 4-6h of work with high confidence of success.

## Final state

| | |
|---|---|
| Harness both backends | 44/44 ✅ |
| MV rule fires in unit tests (synthetic request) | ✅ 4/4 |
| MV rule fires at runtime (dispatch path) | ❌ |
| MvHit perf benefit on Postgres | None observed |
| Infrastructure for future VP work | Shipped and tested |

## The honest blog footnote

The Y.2 cache fix remains the single win of the whole perf series (1.27× → 0.93× geomean). Phase 3 shipped substantial infrastructure (HepPlanner, MvRegistry, VolcanoPlanner, PK/FK metadata, Goldstein-Larson-capable MV materializations) that passes unit tests but doesn't yet reach into the runtime dispatch path cleanly. The scaffolding is genuinely reusable for the next attempt — VP-4 diagnostic + surgical runtime-request fix is the shortest path to turning unit-test-level MV rewrites into runtime wins.

## Files from this leg

- Modified: `src/main/java/mondrian/calcite/CalcitePlannerCache.java` — late-bind MV registry when a schema-bearing caller arrives after a schema-less caller.
- Modified: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` — added `schema()` accessor.
- Modified: `src/main/java/mondrian/rolap/agg/SegmentLoader.java` — `plannerFor(star)` passes `star.getSchema()`.
- Modified: `src/main/java/mondrian/rolap/SqlTupleReader.java` — `plannerFor` accepts RolapSchema; call site passes first level's schema.
- Modified: `src/test/java/mondrian/test/calcite/PerfBenchmarkTest.java` — `harness.bench.useAggregates` override for diagnostic purposes.
