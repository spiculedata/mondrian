# Calcite Backend Foundations — Checkpoint #4 Report

**Date:** 2026-04-19
**Worktree:** `calcite-backend-foundations`
**Design ref:** `docs/plans/2026-04-19-calcite-backend-rewrite-design.md` §Cutover checkpoints
**Plan ref:** `docs/plans/2026-04-19-calcite-backend-foundations.md`

## Status after Task 9

`mondrian.backend` default is now `calcite`. `-Dmondrian.backend=legacy` remains a kill switch.

## Harness results

| Run | Result |
|---|---|
| Default (Calcite) — `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test` | 34/34 |
| Legacy — `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test` | 34/34 |

## How the current Calcite path works

`SqlTupleReader` and `SegmentLoader` dispatch on `MondrianBackend.current().isCalcite()`. The Calcite branches call `CalcitePlannerAdapters.fromTupleRead` / `fromSegmentLoad`, which currently always throw `UnsupportedTranslation`. The dispatch catch-block then falls back to the legacy `SqlQuery.toString()` output, which is byte-identical to what the legacy branch emits. Net result: harness parity with legacy, *but the Calcite routing is exercised on every query*.

## Task 10 outcome — 0/20 end-to-end (with strong gap analysis)

Task 10 was structured as gap-analysis-first; the option to land "0 queries actually translated" was explicitly green-lit if the gap report was strong. That is the outcome here, and the rest of this document explains *why* and lays out the worktree-#2 shopping list.

### Targeted query: `basic-select`

```
select {[Measures].[Unit Sales]} on columns from Sales
```

The simplest MDX in the corpus. Recorded golden (`src/test/resources/calcite-harness/golden/basic-select.json`) shows it produces exactly two SQL executions on cold cache:

1. **Cardinality probe** (seq 0) — emitted by `SqlStatisticsProvider.getColumnCardinality`:
   ```sql
   select count(distinct "the_year") from "time_by_day"
   ```

2. **Segment load** (seq 1) — emitted by `SegmentLoader.createExecuteSql`:
   ```sql
   select "time_by_day"."the_year" as "c0",
          sum("sales_fact_1997"."unit_sales") as "m0"
   from "sales_fact_1997" as "sales_fact_1997",
        "time_by_day" as "time_by_day"
   where "time_by_day"."the_year" = 1997
     and "sales_fact_1997"."time_id" = "time_by_day"."time_id"
   group by "time_by_day"."the_year"
   ```

### Gap analysis

#### Gap A: harness compares legacy SQL byte-for-byte

This is the **decisive blocker** for Task 10's stated success criterion. `EquivalenceHarness.compareAgainstGolden` (gate 1, "LEGACY_DRIFT") asserts `gSql.equals(ae.sql)` for every captured execution. The golden SQL is comma-style implicit join produced by `mondrian.rolap.sql.SqlQuery`. Calcite's `RelToSqlConverter` produces ANSI-style `JOIN ... ON ...` with standard formatting. Those strings will never be byte-equal.

Concretely: even a perfect translation of `basic-select`'s segment-load that returns identical cell values would still fail the harness because the captured SQL string differs. This affects every query in the corpus equally. **The worktree-#1 harness was designed to verify the `SqlInterceptor` pass-through seam, not Calcite SQL substitution.**

The harness needs a new mode for the Calcite era — see worktree-#2 shopping list below.

#### Gap B: cardinality probe is not on the dispatch seam

The seq-0 SQL for `basic-select` (`select count(distinct "the_year") from "time_by_day"`) is emitted by `mondrian.spi.impl.SqlStatisticsProvider`. That code path is invoked from `RolapStar.Column.getCardinality` and goes straight to JDBC; it does **not** flow through `SqlTupleReader` or `SegmentLoader` and therefore is not intercepted by the worktree-#1 dispatch seam. To route it through the planner we need a third dispatch site (`CalcitePlannerAdapters.fromCardinalityProbe`) or a refactor that funnels statistics queries through `SegmentLoader`.

#### Gap C: `SqlTupleReader` dispatch site has rich enough info, but `PlannerRequest` is too narrow

The `targets` parameter visible at the dispatch site exposes `Target.getLevel().getKeyExp() / getNameExp() / getOrdinalExp() / getCaptionExp()`, the `RolapHierarchy`, the source members, native filters (`SqlConstraint`), and the level-to-table joins via `RolapStar`. So the *information* is there. What is *missing* in `PlannerRequest`:

- **Multi-column projection per level** — Mondrian's level-members SELECT projects key, name, ordinal, caption, parent, and properties columns; `PlannerRequest.projections` is a flat list with no semantics about "which column is the key". Translating the level-members query needs the request shape to model "ordered tuple of named expressions" rather than a column list.
- **Multi-table joins along a hierarchy snowflake** — `PlannerRequest.Join` is a single `dimTable + factKey + dimKey`. Hierarchy snowflakes (e.g. Customers -> City -> State -> Country) need a chain of joins, plus arbitrary join expressions (not just equi-join on a single column).
- **DISTINCT projection** — level-member queries typically `select distinct`. No representation in `PlannerRequest`.
- **`ORDER BY ordinal_exp` semantics** — needed for level-members.
- **`SqlConstraint` translation** — `MemberChildrenConstraint`, `TupleConstraint`, `RolapNativeFilter.SetConstraint` etc.; each is a Mondrian-level abstract predicate that the legacy path appends to the `SqlQuery` via `addConstraint`. Translating each variant into `PlannerRequest.Filter` (currently equality-only) is a multi-week task.

#### Gap D: `SegmentLoader` dispatch site is also rich enough, but the request shape is too narrow

`groupingSetsList` exposes `getStar()`, `getDefaultColumns()` (RolapStar.Column[]), `getDefaultSegments()` (each carries an `aggMeasure` with aggregator + expression), `getDefaultPredicates()` (StarColumnPredicate[]), and the rollup column list. Plenty to drive a translation. What `PlannerRequest` cannot yet express:

- **`StarColumnPredicate` shapes beyond equality** — `LiteralStarPredicate`, `ListColumnPredicate` (IN), `RangeColumnPredicate`, `ValueColumnPredicate`, `LiteralStarColumnPredicate.FALSE`. Today only `Filter(Column, literal)` exists.
- **Compound predicates** — `compoundPredicateList: List<StarPredicate>`. AND/OR trees of column predicates emitted by `RolapAggregationManager.makeCompoundGroup`. No representation in `PlannerRequest`.
- **Distinct-count measures** — `RolapAggregator.DistinctCount` becomes `count(distinct expr)`, with the additional kicker that some dialects route it through `SegmentLoader.optimizePredicates` and the `MultipleDistinctCount` rewrite (subqueries / UNION). `Measure.AggFn` has no `DISTINCT_COUNT`.
- **Grouping sets / rollup** — `useGroupingSet=true` requires `GROUPING SETS ((…),(…))` which `RelBuilder` supports but `PlannerRequest` does not model.
- **Snowflake joins** — same as Gap C: a level can sit on a chain of joined dimension tables.
- **Aggregator-expression evaluation** — `RolapAggregator.SumOfDistinctCount`, custom user-defined aggregators, and column expressions that are computed (e.g. `case when … end`) rather than plain column refs. `Measure` only carries an `AggFn` enum + a column ref.
- **`MondrianDef.Expression` rendering** — `RolapStar.Column.expression` is a `RolapSchema.PhysColumn` that can be a plain column or a calculated expression. The planner schema is a vanilla `JdbcSchema` reflection — it knows nothing about Mondrian computed columns.

#### Gap E: `CalciteMondrianSchema` is JDBC-reflection-only

`CalciteMondrianSchema` builds a `JdbcSchema` from the connection's DataSource. That gets us tables and column types for free, but it has no knowledge of:

- Mondrian schema-XML overrides (`<Column type="Numeric">` declarations).
- `<View>` / `<InlineTable>` dimension sources.
- Level closure tables.
- Aggregate tables (`<AggName>`, `<AggPattern>`).
- Cube join graphs (Mondrian computes the spanning tree from `<DimensionUsage>` foreign keys; Calcite would need to be told).

For `basic-select` the JDBC reflection is *sufficient* (sales_fact_1997 and time_by_day are both physical tables). For most of the rest of the corpus it is not — `Customers` (snowflake), aggregate tables, and any cube using `<View>` would fail.

### What I tried

I did not write speculative translation code that would have to be deleted. The unit-test suites (`SqlTupleReaderCalciteBackendTest`, `SegmentLoaderCalciteBackendTest`, `CalciteSqlPlannerTest`) already prove the dispatch seam, the planner, the schema adapter, and the dialect map all work in isolation. Wiring them together in `CalcitePlannerAdapters.fromSegmentLoad` for `basic-select` would have produced a `PlannerRequest`/`RelNode` we could render — but Gap A means it could not be plugged into the harness without breaking the byte-equal SQL gate, and the rendering itself adds no information beyond what `CalciteSqlPlannerTest` already demonstrates.

### Fallback counter observation

After running the smoke + aggregate corpus (`mvn -Pcalcite-harness -Dtest='Equivalence*Test' test`), `CalcitePlannerAdapters.unsupportedFallbackCount()` increments on every dispatched call from both `SqlTupleReader` and `SegmentLoader`. The harness does not expose the counter directly, but the unit tests in `Sql{TupleReader,SegmentLoader}CalciteBackendTest` confirm the wiring. The fallback is the *only* code path exercised today, which is the expected worktree-#1 state.

## Worktree-#2 shopping list (in priority order)

1. **Harness mode flag: `--cells-only` (Gap A).** Add a second comparison mode to `EquivalenceHarness` that compares cell-set + per-execution `(rowCount, checksum)` only, ignoring SQL string equality. Activate it when `MondrianBackend.current().isCalcite()` and translation succeeded for at least one execution. Existing byte-equal mode stays the default for legacy regression coverage. Without this, no Calcite translation can pass the harness.

2. **Wire `CalcitePlannerAdapters.fromSegmentLoad` for the simplest shape** (Gap D, partial). One fact + one inner equi-join + one equality filter + sum of one numeric column + group-by one column. This is exactly `basic-select`'s segment 1. Expected outcome with item 1 in place: 1/20 truly end-to-end on Calcite. Scope:
   - Detect "single grouping set, no rollup, all measures are SUM, all predicates are single-column equality, only one dim joined".
   - Build a `PlannerRequest` from `groupingSetsList.getStar()` + `getDefaultColumns()` + `getDefaultPredicates()` + `getDefaultSegments()`.
   - Return it; let the dispatch site swap `pair.left` for `planner.plan(req)`.

3. **`PlannerRequest` extensions for distinct-count + IN predicates** (Gap D). Adds `Measure.AggFn.DISTINCT_COUNT` and `Filter` variants for `IN (literal-list)` and `column = column`. Unlocks `distinct-count`, `slicer-where`, `time-fn`, `aggregate-measure`.

4. **Snowflake/multi-join shape** (Gaps C+D). Replace `Join` single-edge with `List<JoinEdge>` and add fact-relative path resolution from `RolapStar.Column.getTable()`. Unlocks `crossjoin`, `topcount`, `order`, `filter`, `format-string` (all of which join through `product` -> `product_class`).

5. **`SqlConstraint` -> `PlannerRequest.Filter` translator family** (Gap C). One per known constraint subtype. The legacy `SqlConstraint.addConstraint(SqlQuery, ...)` calls are the spec; translate the same logical predicate into `PlannerRequest`. Unlocks the `RolapNative*` corpus queries (`native-cj-*`, `native-filter-*`, `native-topcount-*`).

6. **Cardinality-probe dispatch** (Gap B). Add `CalcitePlannerAdapters.fromCardinalityProbe(RolapStar.Column)` and route `SqlStatisticsProvider` through it. Once routed, the planner can render `select count(distinct <col>) from <table>`. Tiny patch, but needed for *every* corpus query because the seq-0 probe shows up in nearly all goldens.

7. **`CalciteMondrianSchema` upgrade** (Gap E). Replace the `JdbcSchema` reflection with a Mondrian-aware schema that respects `<Column type=>` overrides and `<View>` definitions; subsequently teach it about aggregate tables. This is a big-ticket item but most of the corpus does not need it.

8. **Compound predicates + grouping sets** (Gap D). Needed for `agg-distinct-count-*` and any query that hits the `RolapAggregationManager.makeCompoundGroup` branch. Last because they're the most complex and the smallest absolute win.

### Summary

Worktree #1 ships its stated promise: Calcite is the default backend, the dispatch seam is in place, the foundations (schema adapter, planner, dialect map, translation API, fallback counter, harness) are tested in isolation, and parity with legacy is preserved through the fallback. Zero corpus queries actually generate their SQL via `CalciteSqlPlanner` end-to-end. The blocker for "even one" is not implementation effort on the translator — it is a harness-mode mismatch (Gap A) compounded by `PlannerRequest` being deliberately kept narrow in worktree #1. Both are tractable in worktree #2 and the priority-ordered shopping list above is the roadmap.

## Policy change: no fallback

After the initial checkpoint-4 landing, the "catch `UnsupportedTranslation` and fall back to legacy `SqlQuery.toString()`" branch was removed. The rule going forward is:

- **`backend=calcite`:** every SQL string on the wire is Calcite-generated. `UnsupportedTranslation` propagates to the caller — the query (and its test) fails. The Calcite path never consults `mondrian.spi.impl.*Dialect`; dialect lookup goes through `CalciteDialectMap.forDataSource(DataSource)`, which reads the database product name straight off `DatabaseMetaData` via JDBC.
- **`backend=legacy`:** unchanged — the kill switch preserves the legacy path in its entirety.

### Rationale

Once worktree #4 deletes `SqlQuery`, `SqlQueryBuilder`, and `mondrian.spi.impl.*Dialect`, there is no fallback target to fall back to. Keeping the catch-and-fallback during development was masking the true coverage gap; production deployments cannot silently depend on which shapes the translator happens to cover. Making `UnsupportedTranslation` a hard failure surfaces that gap as an enumerable list of test failures (the worktree-#2 shopping list) rather than as subtly-different SQL behaviour at runtime.

### Consequences

- `unsupportedFallbackCount()` / `record*Fallback()` are renamed to `unsupportedCount()` / incremented-at-throw-site — they are pure observability now, not a fallback signal.
- Under `backend=calcite`, running the smoke + aggregate corpus is mostly red. Only the two translatable shapes today (`basic-select`'s segment-load, the cardinality probe) survive. Everything else throws.
- Legacy harness (`-Dmondrian.backend=legacy`) stays 34/34 — that is the only absolute gate.
- Full post-change pass/fail distribution and the bucketed worktree-#2 shopping list live in [`2026-04-19-calcite-backend-foundations-checkpoint4b.md`](2026-04-19-calcite-backend-foundations-checkpoint4b.md).
