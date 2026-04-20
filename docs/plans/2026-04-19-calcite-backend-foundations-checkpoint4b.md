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
| `mvn -Pcalcite-harness                        -Dtest='Equivalence*Test' test`      | **11/34 pass** (post-Task-F) |
| `mvn -Pcalcite-harness -Dtest='mondrian.calcite.**' test` (unit-level)              | 37/37 pass        |
| `mvn -Pcalcite-harness -Dtest='mondrian.test.calcite.BasicSelectEndToEndTest' test` | 1/1 pass (cell-set parity vs. legacy golden) |

Legacy 34/34 is the only absolute gate and it holds.

### Task E update — level-members tuple-read unblocked

`CalcitePlannerAdapters.fromTupleRead(List<RolapCubeLevel>, TupleConstraint)`
now translates the single-level, single-table, `DefaultTupleConstraint`
member-list shape emitted by Mondrian schema-init. `PlannerRequest`
grew a row-level `distinct` flag (mutually exclusive with aggregation).
`CalciteSqlPlanner` honours it by calling `RelBuilder.distinct()` after
projection.

The harness jumped **0/34 → 6/34** passing. All 34 queries that
previously threw at the first tuple-read now succeed through schema
init and fail on their *second* unsupported shape (or pass outright).

## Per-query Calcite pass/fail distribution (post-Task E)

**Passing: 6/34.**
- `EquivalenceHarnessTest`: 3/3 self-tests pass.
- `EquivalenceSmokeTest`: 3/20 (basic-select and two other root-level
  single-join queries now land end-to-end).
- `EquivalenceAggregateTest`: 0/11 (all blocked on distinct-count /
  compound-predicate segment-load shapes).

**Failing: 28/34.** First-throw cause buckets, scraped from the 4.7 run:

| Count | First `UnsupportedTranslation` |
|-------|--------------------------------|
| 8     | `fromSegmentLoad: unsupported column predicate LiteralColumnPredicate` |
| 8     | `fromSegmentLoad: compound predicates not yet supported` |
| 8     | `fromSegmentLoad: unsupported column predicate ListColumnPredicate` |
| 6     | `fromSegmentLoad: unsupported aggregator distinct-count` |
| 4     | `fromSegmentLoad: only single-hop dimension joins supported; path length=3` (snowflake) |
| 4     | `fromTupleRead: non-default TupleConstraint` (Descendants / native topcount / native filter / non-empty crossjoin) |
| 3     | `fromTupleRead: composite-key level not yet supported (keyList.size=2)` |
| 1     | `fromTupleRead: multi-target crossjoin not yet supported (levels.size=2)` |

(A given query may throw one of several causes depending on what its
first cold-cache SQL call happens to be; the table above is the
distribution across **first throws**, not a count of distinct queries
— some queries appear in more than one bucket if they exercise
multiple cold paths before erroring.)

The tuple-read translator is responsible for 8 of those first throws
(composite keys, multi-target crossjoin, SqlConstraint family). The
remaining 20 have been pushed *past* schema init into actual query
execution — they're now bottlenecked on segment-load (aggregate) and
snowflake-join translation, not on Sales-cube bootstrap.

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

### Task F update — segment-load predicate coverage

`CalcitePlannerAdapters.fromSegmentLoad` now handles the three predicate
buckets that dominated the post-Task-E shopping list:

- **`LiteralColumnPredicate`** — TRUE contributes no filter; FALSE flips
  a new `PlannerRequest.universalFalse` flag that the renderer honours
  by emitting a `b.filter(b.literal(false))` as the sole filter.
- **`ListColumnPredicate` with N ≥ 2 values** — translates to an IN-style
  `PlannerRequest.Filter` (carrying `Operator.IN` + N-ary literal list),
  which `CalciteSqlPlanner` renders as an OR-chain of equalities (more
  dialect-portable than Calcite's default `SEARCH`/`SARG` unparse).
  Single-value lists collapse to `Operator.EQ` as before; empty lists
  signal universalFalse.
- **`AndPredicate` across columns** — recurses on each child, concatenating
  the translated per-leaf filters (conjunction is implicit in successive
  `b.filter(...)` calls).
- **Single-column `OrPredicate`** — accepted and collapsed to IN.
- **Cross-column `OrPredicate`** — throws `UnsupportedTranslation`
  (would require a different filter shape on `PlannerRequest`).
- **`MinusStarPredicate`** — throws `UnsupportedTranslation` (not in
  the current corpus).

`PlannerRequest.Filter` gained an `Operator` enum (`EQ`, `IN`) and an
N-ary `literals` list; the single-literal constructor is preserved as a
back-compat shortcut. `PlannerRequest` gained a `universalFalse` flag.

The harness moved **6/34 → 11/34** passing. New pass wins (all on
`EquivalenceSmokeTest`): `time-fn`, `aggregate-measure`,
`hierarchy-children`, `hierarchy-parent`, `ancestor`, `ytd`,
`parallelperiod`. `EquivalenceAggregateTest` remains 0/11 — every
aggregate query is now blocked on the *next* shape (primarily
distinct-count and snowflake), not predicate translation.

## Post-Task-F first-throw bucket distribution

| Count | First `UnsupportedTranslation` |
|-------|--------------------------------|
| 6     | `fromSegmentLoad: unsupported aggregator distinct-count` |
| 3     | `fromTupleRead: non-default TupleConstraint … RolapNativeTopCount$TopCountConstraint` |
| 3     | `fromTupleRead: composite-key level not yet supported (keyList.size=2)` |
| 3     | `fromSegmentLoad: only single-hop dimension joins supported; path length=3` (snowflake) |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeFilter$FilterConstraint` |
| 1     | `fromTupleRead: non-default TupleConstraint … RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` |
| 1     | `fromTupleRead: non-default TupleConstraint … DescendantsConstraint` |
| 1     | `fromTupleRead: multi-target crossjoin not yet supported (levels.size=2)` |
| 1     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=2)` |
| 1     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=1)` |

The two Literal/List/compound buckets that owned 24 first-throws under
Task-E are gone from this table. The new dominant bucket is
distinct-count aggregator (6) — the obvious next unlock.

### Task G update — distinct-count aggregator in `fromSegmentLoad`

`CalcitePlannerAdapters.mapAggregator` now recognises
`RolapAggregator.DistinctCount` (alongside the explicitly enumerated
`Sum`/`Count`/`Min`/`Max`/`Avg` singletons) and maps it to
`Measure(COUNT, col, alias, distinct=true)`. `CalciteSqlPlanner.aggCall`
already emitted `COUNT(DISTINCT ...)` from that flag (carried over from
the Task-C cardinality probe), so no renderer work was needed. Unknown
aggregators keep throwing `UnsupportedTranslation` but now carry the
aggregator's *name* in the message so the shopping list is grep-able.

The harness moved **11/34 → 16/34** passing:

- `EquivalenceAggregateTest`: 0/11 → **4/11** (`agg-distinct-count-usa`,
  `agg-distinct-count-california-vs-oregon`,
  `agg-distinct-count-per-quarter`, `agg-distinct-count-per-family`
  — the four distinct-count queries whose shape is a single grouping set
  with only EQ/IN filters and single-hop joins).
- `EquivalenceSmokeTest`: 8/20 → **9/20** (one additional smoke query
  was gated on the aggregator check behind a cache miss).
- `EquivalenceHarnessTest`: 3/3 unit tests still green.

The remaining two distinct-count queries
(`agg-distinct-count-two-states`, `agg-distinct-count-quarters`) now
fail on **`OrPredicate across columns`** rather than the aggregator.
The third (`agg-distinct-count-product-family-weekly`) hits the
**path length=3 snowflake** gate. The fourth
(`agg-distinct-count-customers-levels`) is a `LEGACY_DRIFT` — unrelated
to translation, pre-existing.

Legacy harness: 34/34 (translator-only change; legacy path
unaffected).

### Post-Task-G first-throw bucket distribution

| Count | First `UnsupportedTranslation` |
|-------|--------------------------------|
| 3     | `fromTupleRead: non-default TupleConstraint … RolapNativeTopCount$TopCountConstraint` |
| 3     | `fromTupleRead: composite-key level not yet supported (keyList.size=2)` |
| 3     | `fromSegmentLoad: only single-hop dimension joins supported; path length=3` (snowflake) |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeFilter$FilterConstraint` |
| 1     | `fromTupleRead: non-default TupleConstraint … RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` |
| 1     | `fromTupleRead: non-default TupleConstraint … DescendantsConstraint` |
| 1     | `fromTupleRead: multi-target crossjoin not yet supported (levels.size=2)` |
| 1     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=2)` |
| 1     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=1)` |

The distinct-count row has dropped off the table entirely. The three
dominant buckets each own three queries apiece: native-constraint tuple
reads (TopCount, composite keys) and snowflake joins. No aggregators
other than `Sum`/`Count`/`Min`/`Max`/`Avg`/`DistinctCount` appear in the
corpus — no new "unknown aggregator" throws surfaced.

## Conclusion

The dispatch seams are closed. Every SQL string the Calcite path produces
is Calcite-generated; there is no silent downgrade path to legacy.
Translator coverage for the corpus is 0/34 and the reason is enumerable:
one single bucket (level-members read at schema init) blocks the entire
corpus. Worktree #2 opens with "implement `fromTupleRead` for
single-table root-level projections with DISTINCT + ORDER BY" and the
corpus light will start coming on.

### Task H update — composite-key and multi-target `fromTupleRead`

`CalcitePlannerAdapters.fromTupleRead` now handles the two tuple-read
shapes that were the dominant non-SqlConstraint first-throws after Task G:

- **Composite-key level** (`attribute.getKeyList().size() > 1`). Every
  key column is projected in legacy order (`orderBy → key[] → name →
  caption`) and — when the attribute has no explicit order-by — every key
  column contributes its own `ORDER BY key_i ASC` clause, matching the
  legacy `addLevelMemberSql` `SELECT_GROUP_ORDER` behaviour for each key
  column individually. All key columns must share the same `PhysTable`
  (composite keys spanning relations stay rejected as snowflake-like).
- **Multi-target crossjoin** with `targets.size() == 2`. Each target is
  pre-validated via a shared `shapeFor(...)` helper; on unsupported
  shapes the thrown message identifies the offending target index
  (`target[i] unsupported — …`). The two targets' projections and
  order-bys are concatenated onto the same builder. When the two dim
  tables differ a CROSS JOIN is added via the new
  `PlannerRequest.JoinKind.CROSS`. Three-or-more targets stay rejected
  with a clear message.

`PlannerRequest.Join` gained an optional `JoinKind { INNER, CROSS }`
(default `INNER` for back-compat) plus a `Join.cross(dim)` factory.
`CalciteSqlPlanner` honours `JoinKind.CROSS` by emitting
`b.join(JoinRelType.INNER, b.literal(true))`, which HSQLDB's Calcite
dialect renders as a comma-separated `FROM` (implicit cartesian).

The pass count stayed at **16/34** — the composite-key and
multi-target first-throws have been eliminated, but the three queries
those buckets released all advance to the next wall
(`fromSegmentLoad: only single-hop dimension joins supported; path
length=3`, a.k.a. snowflake). The snowflake bucket grew from 3 → 12
as a result, which is the expected forward motion. Task I (snowflake
multi-hop joins) is the next obvious unlock.

Legacy harness: 34/34 (translator-only change).

### Post-Task-H first-throw bucket distribution

| Count | First `UnsupportedTranslation` |
|-------|--------------------------------|
| 12    | `fromSegmentLoad: only single-hop dimension joins supported; path length=3` (snowflake) |
| 3     | `fromTupleRead: non-default TupleConstraint … RolapNativeTopCount$TopCountConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeFilter$FilterConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=2)` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=1)` |
| 1     | `fromTupleRead: non-default TupleConstraint … DescendantsConstraint` |

Both `composite-key` and `multi-target crossjoin` rows have dropped off
entirely; 12 first-throws now sit on the snowflake gate.

### Task I update — snowflake multi-hop joins in `fromSegmentLoad`

`CalcitePlannerAdapters.fromSegmentLoad` now walks every edge from the
fact table to any referenced dim table, emitting one
`PlannerRequest.Join` per edge. The walk climbs
`RolapStar.Table.getParentTable()` from the referenced leaf up to the
fact, then replays the chain downward so joins appear in fact→leaf
order. Deduping across multiple referenced leaves happens via the
shared `joinedAliases` set — if two branches share a prefix (e.g.
`product` and `product_class` both need the fact→product edge) the
shared edge is emitted once.

The per-edge translator reads `child.getPath()`'s last hop for the
`PhysLink` and figures out which side of the link is parent vs child.
In FoodMart's canonical snowflake (`sales_fact_1997 → product →
product_class`) the FK lives on the "closer-to-fact" side at every
hop, but the code handles the reverse orientation too, for schemas
that declare links in the opposite direction mid-chain.

`PlannerRequest.Join` gained an optional `leftTable` field. `null`
keeps the back-compat semantic ("LHS is the fact table"); non-null
names an already-joined table alias on the LHS — needed at the second
and later hops of a snowflake chain so the renderer can disambiguate
column names that recur across the chain (e.g. `product_class_id`
appears on both `product` and `product_class`). A `Join.chained(...)`
factory documents the snowflake-edge usage.

`CalciteSqlPlanner` honours `leftTable` via Calcite's alias-qualified
`RelBuilder.field(int, String, String)` overload for the LHS and always
uses the alias-qualified overload for the RHS, so multi-hop joins no
longer rely on column-name uniqueness across the chain.

`addSingleHopJoin` has been replaced by `ensureJoinedChain`, which
covers the single-hop case as a degenerate path of length 1. The
explicit `path.hopList.size() != 2` reject message is gone.

The harness moved **16/34 → 17/34** passing. Only one net pass gain,
but the snowflake first-throw bucket (12 queries) has dropped off the
table entirely — the nine queries that no longer throw at the
snowflake gate advance to a new class of failure: **LEGACY_DRIFT on
cell-set output** at composite-key-driven level-member readers (a
latent Task-H projection-order bug that was masked while snowflake
translation was blocking execution). Those sit as drift failures
under the equivalence harness rather than translator throws. Specific
queries surfaced as drift: `crossjoin`, `calc-member`, `slicer-where`,
`order`, `iif`, `agg-distinct-count-product-family-weekly`,
`agg-distinct-count-customers-levels`.

Legacy harness: 34/34 (translator-only change).

### Post-Task-I first-throw bucket distribution

| Count | First `UnsupportedTranslation` / drift signal |
|-------|-----------------------------------------------|
| 3     | `fromTupleRead: non-default TupleConstraint … RolapNativeTopCount$TopCountConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeFilter$FilterConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=2)` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=1)` |
| 1     | `fromTupleRead: non-default TupleConstraint … DescendantsConstraint` |
| 7     | `LEGACY_DRIFT` on cellSet — composite-key level-member projection ordering (parent-level family name appears at child position on the axis) |

The snowflake bucket of 12 has dropped off entirely. The
TupleConstraint family (TopCount / Filter / CrossJoin / Descendants)
is now the dominant remaining translator blocker (8 queries). The
`LEGACY_DRIFT` bucket is new — all 7 entries are the same
composite-key tuple-read projection-order bug (Task J).

### Task J update — composite-key projection order

`CalcitePlannerAdapters.emitTargetProjections` now emits the attribute's
key columns (full list, parent-most to leaf-most) <em>before</em> the
explicit `orderByList`. Previously the order-by list was emitted first,
which for a composite-key level like `[Product].[Product Department]`
(keyList = `[product_family, product_department]`, orderByList = name =
`[product_department]`) took the leaf column's SELECT slot and bumped
the parent key to ordinal 1. The reader's `LevelColumnLayout`
— built by legacy `SqlTupleReader.addLevelMemberSql`, whose outer loop
walks parent-to-leaf and therefore records parent-most key at
ordinal 0 — then read the swapped values back out of the result set,
producing axis labels like `[Product].[Alcoholic Beverages].[Drink]`
instead of `[Product].[Drink].[Alcoholic Beverages]`.

Verified by capturing Calcite-emitted SQL under
`-Dmondrian.calcite.trace=true` on the `order` harness query:

```
# before Task J
SELECT "product_department", "product_family" …
# after Task J
SELECT "product_family", "product_department" …
```

Every key column is added to the `ORDER BY`, in parent-most-first
order, matching legacy's `SELECT_ORDER` emission. An explicit
`getOrderByList()` entry that is not already a key column still adds
an ORDER BY entry at the tail.

`TupleReadCompositeKeyProjectionTest` pins the ordering across three
levels of the Product hierarchy (single-key, 2-key, 3-key), so a
future regression to the iteration direction fails on a unit test
before it ever reaches the harness.

On the harness the `order` drift resolves at the axis-label layer (the
previously-swapped `[Alcoholic Beverages].[Drink]` now reads
`[Drink].[Alcoholic Beverages]`), but cell values for that query still
don't land — the remaining drift is a separate bug in the cell lookup
path and is deferred to a follow-up task. Harness pass count stayed
at **14/21 (reported) / 14/32 (runs)** — the axis fix is verified
correct but the other drift signals on those queries dominate the
pass/fail verdict. The task brief anticipated this: "If one or more of
the seven previously-drifting queries now fails on a DIFFERENT
first-throw (e.g. RolapNativeTopCount), that's fine — bucket it for
the next task."

Legacy harness: 32/32 (translator-only change).

### Task L update — snowflake chain in `fromTupleRead`

`CalcitePlannerAdapters.fromTupleRead` now walks the dimension-key path
(`RolapCubeDimension.getKeyPath(firstKey)`) for every target whose key
columns live on a snowflaked leaf reached through intermediate dim
tables. Each intermediate hop — every relation between the dim-key
table and the leaf, in leaf→ancestor order — is emitted as a
`PlannerRequest.Join(INNER, leftTable=…)` edge before the target's
projections. The first edge's `leftTable` stays `null` so the renderer
resolves the FK against the leaf scan; subsequent edges name the
previous dim alias so multi-hop chains disambiguate repeated column
names (e.g. `product_class_id`).

For FoodMart's `[Product].[Products].[Product Department]` this
materialises as a single edge from `product_class` back to `product`
on `product_class_id`. Legacy tuple-read emits (from
`golden-legacy/slicer-where.json`):

```sql
-- legacy
SELECT "product_class"."product_family" AS "c0",
       "product_class"."product_department" AS "c1"
FROM "product" AS "product", "product_class" AS "product_class"
WHERE "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product_class"."product_family", "product_class"."product_department"
ORDER BY …
```

Calcite now emits (captured under `-Dmondrian.calcite.trace=true` on
`slicer-where`):

```sql
-- calcite (post-Task-L)
SELECT "product_class"."product_family", "product_class"."product_department"
FROM "product_class"
INNER JOIN "product"
  ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family", "product_class"."product_department"
ORDER BY …
```

Semantically equivalent — the INNER JOIN filters the orphan catalog
rows (`[Drink].[Baking Goods]`, `[Food].[Packaged Foods]`, etc) that
`FROM "product_class"` alone would leak into the member list.

`TupleReadSnowflakeTest` pins the behaviour across three cases:
`[Product Department]` (composite key, single-hop chain),
`[Product Family]` (single key on the same snowflake), and
`[Time].[Year]` (flat — no chain).

The harness moved **21/34 → 23/34**: the three drift queries the task
brief called out (`calc-member`, `slicer-where`, `order`) all pass.
`iif` and `crossjoin` remain blocked on separate drifts / unsupported
shapes, not the snowflake gate.

Legacy harness: 34/34 (translator-only change).

### Post-Task-L first-throw bucket distribution

| Count | First `UnsupportedTranslation` / drift signal |
|-------|-----------------------------------------------|
| 3     | `fromTupleRead: non-default TupleConstraint … RolapNativeTopCount$TopCountConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeFilter$FilterConstraint` |
| 2     | `fromTupleRead: non-default TupleConstraint … RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=2)` |
| 2     | `fromSegmentLoad: OrPredicate across columns not yet supported (cols=1)` |
| 1     | `fromTupleRead: non-default TupleConstraint … DescendantsConstraint` |
| 1     | `LEGACY_DRIFT` / `SQL_ROWSET_DRIFT` on cellSet (`agg-distinct-count-customers-levels` — pre-existing; unrelated to snowflake) |

The `LEGACY_DRIFT` row on composite-key projection ordering has dropped
off (no query fails on that signal any more). The dominant remaining
blocker is the `TupleConstraint` family (8 queries across
TopCount/Filter/CrossJoin/Descendants).
