# Calcite Backend Foundations — Checkpoint #4b Report

**Date:** 2026-04-19
**Worktree:** `calcite-backend-foundations`
**Supersedes:** pass-through state recorded in `2026-04-19-calcite-backend-foundations-checkpoint4.md`
**Policy reference:** `checkpoint4.md § Policy change: no fallback`

## Summary

The catch-and-fall-back-to-legacy branch has been removed from the Calcite
dispatch seams in `SqlTupleReader`, `SegmentLoader`, and
`SqlStatisticsProvider`. `UnsupportedTranslation` now propagates. Dialect
lookup under `backend=calcite` reads `DatabaseMetaData.getDatabaseProductName()`
directly via `CalciteDialectMap.forDataSource(DataSource)`; the Calcite
path no longer consults any `mondrian.spi.impl.*Dialect` class for any
purpose.

The resulting red-under-Calcite state is **the correct worktree-#1 end
state** and the shopping list below is honest.

## Harness state

| Run                                                                                 | Result            |
|-------------------------------------------------------------------------------------|-------------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test`    | **34/34 pass**    |
| `mvn -Pcalcite-harness                        -Dtest='Equivalence*Test' test`      | **0/34 pass**     |
| `mvn -Pcalcite-harness -Dtest='HarnessMutationTest' test` (Calcite)                 | **0/1 pass**      |
| `mvn -Pcalcite-harness -Dtest='mondrian.calcite.**' test` (unit-level)              | 32/32 pass        |
| `mvn -Pcalcite-harness -Dtest='mondrian.test.calcite.BasicSelectEndToEndTest' test` | 1/1 pass (expects loud fail) |

Legacy 34/34 is the only absolute gate and it holds.

## Per-query Calcite pass/fail distribution

All 20 smoke-corpus queries, all 11 aggregate-corpus queries, all 3
`EquivalenceHarnessTest` self-tests, and the `HarnessMutationTest`
sweep throw `UnsupportedTranslation` at the same call site on every run:

```
CalcitePlannerAdapters.fromTupleRead (tuple-read translation is deferred)
  at SqlTupleReader.prepareTuples:510
  at SqlTupleReader.readMembers:653
  at SqlMemberSource.getMembersInLevel
  at SqlMemberSource.getRootMembers
  at SmartMemberReader.getRootMembers
  at RolapSchemaLoader$NamelessAssignDefaultMember.deriveDefaultMember
  at RolapSchemaLoader.createCube
```

i.e. every query in the corpus needs the Sales cube's default members
resolved, which emits a level-members SELECT through `SqlTupleReader`,
which dispatches through `fromTupleRead`, which is not yet implemented.

**Pass list (Calcite, equivalence corpus):** empty.
**Fail list (Calcite, equivalence corpus):** all 34.

The basic-select shape *will* translate end-to-end at the segment-load
site (`CalcitePlannerAdapters.fromSegmentLoad` is wired and covered by
`CalciteSqlPlannerTest`) and the cardinality probe shape *will* translate
at the probe site (`fromCardinalityProbe`, covered by
`CardinalityProbeEndToEndTest`) — but neither is reachable end-to-end
because the schema-init tuple-read throws first.

## Shopping list for worktree #2 — bucketed by first-throw cause

Under the no-fallback policy, every corpus query throws at the same first
cause. The worktree-#2 work is therefore bucketed by *what the translator
would have to implement to make that throw not happen*, ordered by unlock
cardinality:

### Bucket 1: level-members read (default-member resolution) — unlocks 34/34

Signature: `CalcitePlannerAdapters.fromTupleRead` is called from
`SqlTupleReader.prepareTuples` with a single `Target` backed by a
top-of-hierarchy level.

The legacy SQL that needs to match is a level-members projection:
`select distinct "key-col", "name-col", "ordinal-col", "caption-col" from "dim-table" order by "ordinal-col"`.

For the Sales cube's hierarchies:
- `[Measures]`: trivial (driven from XML, no SQL).
- `[Time]`: key=`the_year`, name=`the_year`, ordinal=`the_year`, on `time_by_day`, projection only, no join.
- `[Product]`: key=`product_family`, name=`product_family`, on `product_class`, projection only, no join (root level).
- `[Store]`: key=`store_country`, on `store`, projection only.
- `[Customers]`: key=`country`, on `customer`, projection only.
- `[Promotions]`: key=`promotion_name`, on `promotion`.
- `[Education Level]`, `[Gender]`, `[Marital Status]`, `[Yearly Income]`: key=column on `customer`.

All at root-level are single-table projections, no joins, with `DISTINCT`
and `ORDER BY`. `PlannerRequest` today does not model `DISTINCT` or
`ORDER BY`; this bucket requires:
- `PlannerRequest.Builder.distinct(true)`.
- `PlannerRequest.Builder.orderBy(Column)`.
- `fromTupleRead` implementation that maps `Target` → root-level PhysColumn references.

**Count:** 34 queries blocked by this single bucket.

### Bucket 2 (speculative, not exercised yet)

These are not yet observed as first-throws because bucket 1 blocks them.
They will appear as soon as bucket 1 is implemented. Bucketed from
gap analysis in checkpoint #4:

- **Multi-column level projection** (key/name/ord/caption/parent). Count: ~20 queries (every level-members read beyond trivial root).
- **Snowflake / multi-hop join** (Product → Product_Class, Customers → City → State → Country). Count: ~10 queries (crossjoin, topcount, order, filter, format-string, native-cj-usa-product-names, native-topcount-product-names, native-filter-product-names, agg-distinct-count-customers-levels, agg-crossjoin-gender-states).
- **IN-list predicate** (`ListColumnPredicate` with >1 value). Count: ~6 queries (slicer-where, agg-distinct-count-two-states, agg-distinct-count-quarters, time-fn, topcount, order).
- **Distinct-count measure** (`RolapAggregator.DistinctCount` → `count(distinct …)` in segment-load). Count: ~8 queries (every `agg-distinct-count-*` plus `distinct-count`).
- **SqlConstraint** family translation (`MemberChildrenConstraint`, `TupleConstraint`, `RolapNativeFilter.SetConstraint`). Count: ~3 queries (native-cj-*, native-filter-*, native-topcount-*).
- **Compound predicates** (AND/OR trees from `makeCompoundGroup`). Count: ~2 queries (agg-distinct-count-measure-tuple, agg-distinct-count-particular-tuple).
- **Grouping sets / rollup**. Count: speculative — no corpus query appears to exercise this today but it is in `SegmentLoader` and will surface when cube rollup columns appear.

## Call sites surgically updated (mondrian.spi.Dialect product-name read)

Two call sites previously read the product name off a Mondrian
`mondrian.spi.Dialect` instance; both have been switched to
`CalciteDialectMap.forDataSource(DataSource)`:

1. `mondrian/rolap/agg/SegmentLoader.java` — `plannerFor(RolapStar)` had
   `star.getSqlQueryDialect().getDatabaseProduct().name()`. Replaced with
   `CalciteDialectMap.forDataSource(star.getDataSource())`.

2. `mondrian/spi/impl/SqlStatisticsProvider.java` — `plannerFor(DataSource,
   Dialect)` had `dialect.getDatabaseProduct().name()`. Replaced with
   `CalciteDialectMap.forDataSource(dataSource)` and the `Dialect`
   argument dropped.

No other call sites under the Calcite branches of the dispatch seams
consult `mondrian.spi.impl.*Dialect`.

## Files changed

| File | Delta |
|------|-------|
| `src/main/java/mondrian/calcite/CalciteDialectMap.java` | +29 / -1 (new `forDataSource`) |
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` | +39 / -57 (renamed counters, removed fallback accessors) |
| `src/main/java/mondrian/rolap/SqlTupleReader.java` | +7 / -17 (removed try/catch fallback) |
| `src/main/java/mondrian/rolap/agg/SegmentLoader.java` | +18 / -48 (removed try/catch fallback; switched to `forDataSource`) |
| `src/main/java/mondrian/spi/impl/SqlStatisticsProvider.java` | +20 / -39 (removed try/catch fallback; switched to `forDataSource`) |
| `src/test/java/mondrian/calcite/SqlTupleReaderCalciteBackendTest.java` | renamed counter API calls |
| `src/test/java/mondrian/calcite/SegmentLoaderCalciteBackendTest.java` | renamed counter API calls |
| `src/test/java/mondrian/calcite/CardinalityProbeEndToEndTest.java` | renamed counter API calls |
| `src/test/java/mondrian/test/calcite/BasicSelectEndToEndTest.java` | inverted to `expected=UnsupportedTranslation` contract |
| `docs/plans/2026-04-19-calcite-backend-foundations-checkpoint4.md` | added `§ Policy change: no fallback` |
| `docs/plans/2026-04-19-calcite-backend-foundations-checkpoint4b.md` | new (this file) |

## Conclusion

The dispatch seams are closed. Every SQL string the Calcite path produces
is Calcite-generated; there is no silent downgrade path to legacy.
Translator coverage for the corpus is 0/34 and the reason is enumerable:
one single bucket (level-members read at schema init) blocks the entire
corpus. Worktree #2 opens with "implement `fromTupleRead` for
single-table root-level projections with DISTINCT + ORDER BY" and the
corpus light will start coming on.
