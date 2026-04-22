# Shape-Enumeration Ship Decision — Phase 1 (2026-04-21)

Plan of record: `docs/plans/2026-04-21-shape-catalog-enumeration.md`.

## Decision

- **Phase-1 default:** `maxSubsetSize = 4`, enumerator output **dormant**.
- **Override:** `-Dmondrian.calcite.mvMaxSubsetSize=<int>` (min 1; see `ShapeEnumerator.defaultMaxSubsetSize`).
- **Dormancy:** enumerated shapes are built, deduped, and logged but registered NEITHER as Calcite `RelOptMaterialization`s NOR as `MvMatcher`-visible specs. Filter-agg correctness gates them.

## Why dormant, not live

Two compounding issues, both exposed by the 20-query `EquivalenceSmokeTest` during Task 5:

1. **Filter-agg grain, implicit slicer.** FoodMart's declared aggregates (`agg_c_14`, `agg_g_ms_pcat`) are year=1997 filter-aggs. Mondrian adds an implicit `[Time].[Year]` to every `PlannerRequest.groupBy`. Exact-size matching on a year-prefixed enumerated shape rewrites a query that legacy never rewrote — e.g. `crossjoin{gender, marital}` (+ implicit year) rewrites onto `agg_g_ms_pcat` instead of `sales_fact_1997`. Rowset values agree; cell-set byte checksum diverges because the rewrite emits rows in a different iteration order than the base-fact scan. `LEGACY_DRIFT` false-positive against the corpus golden.

2. **Calcite rollup.** `SubstitutionVisitor.go(tableRel)` does rollup-aware matching: an MV at `{year, gender, marital}` can satisfy a user query at `{gender, marital}` by SUM-ing across year. Over a year-filtered agg that's semantically wrong (it returns 1997 totals for all-time queries). This is a true correctness issue, not a checksum one.

Fixing either needs filter-agg predicate metadata — `RolapMeasureGroup` doesn't currently expose its slicing predicate to `MvRegistry`. That's the Phase-2 prerequisite.

## What Phase 1 ships

- `MeasureGroupShapeInspector` — copy-linked columns per MG.
- `ShapeEnumerator` — power-set enumeration with year-prefix gate (requires `the_year` when MG copy-links it; proxy for filter-agg, see `ShapeEnumerator` Javadoc).
- `MvRegistry.dedupeByCoverage` — cross-MG dedup on `(sorted group-cols, measure-set)`, smaller agg wins.
- `GroupColKey` + `MvRegistry.shapesFor` — O(1) matcher lookup (dormant shapes don't use it, but structure is in place for Phase 2).
- `MvMatcher.shapeLookupsPerProcess` — per-process counter.
- Registry startup log: `[mv-registry] N MeasureGroups → M shapes (dedup: K kept, D dropped), L materialized, E matcher-only (Phase-1 enumerated)`.

## What Phase 2 must land before lifting the gate

1. Expose `MeasureGroup`-level slicing predicate in `MvRegistry` (either by reading the MG's fact-table predicate or by probing the agg).
2. Encode the predicate in every enumerated `ShapeSpec` so the matcher can require the user request's filter to cover it — otherwise decline the match.
3. Teach `collectFactToDimLinks` to walk snowflake paths (`fact → product → product_class`) so FK-reachable dim attributes (e.g. `product_class.product_family`) show up without hand-curation.
4. Re-run `EquivalenceSmokeTest` with shapes live; `maxSubsetSize=4` assumed unchanged unless Phase-2 perf measurement suggests otherwise.

## Perf benchmark — deferred

Per plan Task 7, the ship decision between `maxSubsetSize ∈ {4, 5}` vs disabled is made against the 45-query corpus × Postgres 1000× / HSQLDB. The knob is inert while shapes are dormant, so there's nothing to measure yet. Defer to Phase 2's activation PR. For now:

- **Phase-1 default:** `maxSubsetSize = 4`.
- **Rationale:** matches the plan's stated default; bounds worst-case catalog size at `C(n,1)+C(n,2)+C(n,3)+C(n,4)` per MG; stays under a hundred shapes per MG for FoodMart's 5-copy-link aggs; preserves headroom for Phase-2's year-prefix + FK-dim additions.

## Tests exercising the Phase-1 pipeline

- `MeasureGroupShapeInspectorTest` — 2 tests, FoodMart copy-link surfacing.
- `ShapeEnumeratorTest` — 3 tests, power-set shape geometry + year gate.
- `MvRegistryDedupTest` — 4 tests, cross-MG dedup tiebreakers.
- `MvRegistryTest`, `MvMatcherTest`, `MvRuleRewriteTest`, `MvHitTest`, `EquivalenceSmokeTest` — regression-green against Phase-1 dormant wiring.
