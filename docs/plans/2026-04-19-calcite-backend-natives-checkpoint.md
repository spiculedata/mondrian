# Calcite Backend Natives — Checkpoint (Tasks N + O)

**Date:** 2026-04-19
**Worktree:** `calcite-backend-natives`
**Base:** `calcite-backend-foundations` @ `320d911` (checkpoint-4b).

## Task N summary

First worktree-#2 unlock: teach `CalcitePlannerAdapters.fromTupleRead` to
translate `RolapNativeCrossJoin$NonEmptyCrossJoinConstraint` — the
simplest `SqlContextConstraint`-derived `TupleConstraint` subclass that
was topping the post-checkpoint-4b shopping list (2 queries).

The new translation builds a **fact-rooted** `PlannerRequest` (in
contrast to the dim-rooted shape used by `DefaultTupleConstraint`):

- **FROM:** the measure group's fact table.
- **Joins:** every dim table referenced by any target level's hierarchy
  walk, stitched in fact→leaf order via the existing
  `ensureJoinedChain` helper (reused verbatim from segment-load).
  Snowflake intermediates (e.g. `product_class` for
  `[Product].[Product Name]`) are picked up by walking the level's
  attribute key/name/orderBy columns and joining every table they
  reference.
- **SELECT + GROUP BY:** per target, walk every non-all level in the
  target's hierarchy root→leaf, emitting
  `(orderByList, keyList, nameExp, captionExp)` as both projections and
  group-by entries (with dedup by table+column). This reproduces the
  SELECT column layout that legacy `SqlTupleReader.addLevelMemberSql`
  builds into `LevelColumnLayout` — preserved because Mondrian's
  positional tuple-reader reads the Calcite SQL using the same layout
  object that the legacy SQL generator populated.
- **WHERE:**
  - Per `CrossJoinArg` filter: `MemberListCrossJoinArg` with N members
    → EQ (single) or IN-list on the arg level's leaf key column.
    `DescendantsCrossJoinArg` with `member == null` → no filter.
  - Slicer filter: for every non-all, non-measure, non-default evaluator
    member not already pinned by a CJ arg, emit an EQ filter on the
    member's leaf key column. Default-member exclusion mirrors legacy
    `SqlConstraintUtils.removeCalculatedAndDefaultMembers` — e.g. the
    Sales cube's `[Time].[1997]` default does NOT contribute a
    `the_year = 1997` WHERE.
- **ORDER BY:** same walk as SELECT, emitting the same column order.
  Legacy NECJ SQL has no `ORDER BY` but HSQLDB's natural row order for
  the legacy FROM/WHERE shape happens to match. Calcite's
  `RelBuilder`-rendered join topology is different and HSQLDB returns
  rows in a different order; Mondrian's `RolapNativeSet` does not
  re-sort natives (a long-standing TODO in `RolapNativeSet.java:39`)
  so without explicit ORDER BY the axis order drifts. Emitting the
  hierarchy's natural ordering pins it deterministically.

### CrossJoinArg subclasses handled

| Subclass                    | Status | Notes                                             |
|-----------------------------|--------|---------------------------------------------------|
| `MemberListCrossJoinArg`    | OK     | Single-member → EQ; multi-member → IN-list.       |
| `DescendantsCrossJoinArg`   | OK (level.members only) | Rejects real-member descendants. |
| other subclasses            | reject | `UnsupportedTranslation` with concrete class name.|

### SetConstraint subclasses handled

Currently narrowed to `RolapNativeCrossJoin$NonEmptyCrossJoinConstraint`
only. The sibling `SetConstraint` subclasses
`RolapNativeTopCount$TopCountConstraint` and
`RolapNativeFilter$FilterConstraint` both extend the same base but need
additional surface (TopCount needs the sort measure projected + LIMIT;
Filter needs a HAVING-like measure predicate), so they still throw
`UnsupportedTranslation`. Class-name gate avoids brittleness if the
hierarchy grows.

### Files changed (line-count deltas)

| File | Delta |
|------|-------|
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java`  | +320 / -2  |
| `src/main/java/mondrian/calcite/CalciteSqlPlanner.java`       | +15 / -1   |
| `src/main/java/mondrian/rolap/RolapNativeSet.java`            | +10 / -1   |

The RolapNativeSet edit is a two-line surface bump: `SetConstraint`
visibility `protected` → `public` (so `mondrian.calcite` can do
`instanceof` dispatch), and a new `getArgs()` getter. The brief calls
for getters over field-access; the file is scheduled for deletion in
worktree #4 anyway.

`CalciteSqlPlanner` gained a `fieldRef` helper that uses the
table-qualified `RelBuilder.field(String, String)` overload when the
`PlannerRequest.Column.table` is non-null — needed because
`product_id` lives on both `sales_fact_1997` and `product` and the
unqualified lookup picked fact's column, producing wrong GROUP BY
columns. Applies to filter/groupBy paths; projections/orderBy already
reference aggregate output aliases so stay unqualified.

## Harness state

| Run                                                                                 | Result            |
|-------------------------------------------------------------------------------------|-------------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test`    | **34/34 pass**    |
| `mvn -Pcalcite-harness                          -Dtest='Equivalence*Test' test`    | **27/34 pass** (was 26/34) |

Net +1 pass: `EquivalenceAggregateTest#equivalent[native-cj-usa-product-names]`.

### Legacy-SQL reference (target-1 query)

```sql
-- legacy NECJ: select NON EMPTY Crossjoin({[Store].[USA]},
--                                         [Product].[Product Name].members) on 0 from Sales
select
  "store"."store_country"            as "c0",
  "product_class"."product_family"   as "c1",
  "product_class"."product_department" as "c2",
  "product_class"."product_category" as "c3",
  "product_class"."product_subcategory" as "c4",
  "product"."brand_name"             as "c5",
  "product"."product_name"           as "c6",
  "product"."product_id"             as "c7"
from "sales_fact_1997", "store", "product", "product_class"
where ("store"."store_country" = 'USA')
  and "sales_fact_1997"."store_id"      = "store"."store_id"
  and "sales_fact_1997"."product_id"    = "product"."product_id"
  and "product"."product_class_id"      = "product_class"."product_class_id"
group by "store"."store_country",
         "product_class"."product_family",
         …,
         "product"."product_id"
```

Captured Calcite SQL (identical shape, INNER JOIN + explicit ORDER BY):

```sql
SELECT "store"."store_country", "product_class"."product_family", …,
       "product"."product_id"
FROM "sales_fact_1997"
INNER JOIN "store"         ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "product"       ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id"
                              = "product_class"."product_class_id"
WHERE "store"."store_country" = 'USA'
GROUP BY "store"."store_country", "product"."product_id", "product"."brand_name",
         "product"."product_name", "product_class"."product_subcategory",
         "product_class"."product_category", "product_class"."product_department",
         "product_class"."product_family"
ORDER BY "store"."store_country", "product_class"."product_family",
         "product_class"."product_department", "product_class"."product_category",
         "product_class"."product_subcategory", "product"."brand_name",
         "product"."product_name", "product"."product_id"
```

### Second NECJ query — not yet landing

`EquivalenceSmokeTest#equivalent[non-empty-rows]`
(`NON EMPTY [Store].[Store Name].members`) now reaches translation
successfully and Calcite emits:

```sql
SELECT "store"."store_country", "store"."store_state",
       "store"."store_city", "store"."store_name"
FROM "sales_fact_1997"
INNER JOIN "store" ON …
GROUP BY "store"."store_name", "store"."store_city",
         "store"."store_state", "store"."store_country"
ORDER BY …
```

But legacy emits 12 columns (country/state/city/name + 8 level
properties: store_type, store_manager, sqft columns, coffee_bar,
street_address, etc). Our hierarchy walk only picks up
attribute.keyList/nameExp/captionExp; it does NOT enumerate level
**properties**. Mondrian's `LevelColumnLayout` was populated from
legacy's 12-column shape, so the tuple-reader fails with
`types cardinality != column count 4` on assertion.

Fix is straightforward — walk
`RolapCubeLevel.getProperties()` and emit each property's PhysColumn
— but belongs in a follow-up task so this commit lands cleanly.
Added to the next shopping list.

## Post-Task-N first-throw bucket distribution

| Count | First `UnsupportedTranslation` / signal |
|-------|-----------------------------------------|
| 3     | `fromTupleRead: RolapNativeTopCount$TopCountConstraint` |
| 2     | `fromTupleRead: RolapNativeFilter$FilterConstraint` |
| 1     | `fromTupleRead: DescendantsConstraint` |
| 1     | `types cardinality != column count` (non-empty-rows — level properties missing from NECJ projection) |

The NECJ first-throw (2 queries) from checkpoint-4b is gone.
TopCount (3) is now the dominant translator blocker. The `non-empty-rows`
failure is a coverage gap inside the NECJ path (level properties)
rather than a constraint-shape gate; fixing it drops that 1 into the
pass column without any new shape being unblocked.

## Guardrails holding

- Legacy harness: 34/34.
- No touching `SqlQuery` / `mondrian.spi.impl.*Dialect` / `aggmatcher/`.
- No fallback; `UnsupportedTranslation` still propagates.
- `RolapNative*` edits limited to a 2-line visibility bump + getter
  (file scheduled for deletion in worktree #4).

## Next tasks (ordered by unlock cardinality)

1. ~~**TopCountConstraint**~~ — landed as Task O.
2. **FilterConstraint** — unlocks 2 queries
   (`filter`, `native-filter-product-names`).
   Needs a HAVING-like predicate on the measure.
3. **Level properties in NECJ projection** — unlocks `non-empty-rows`.
4. **DescendantsConstraint** — unlocks `descendants`.

---

## Task O summary (2026-04-19)

Extend the Task-N gate to accept
`RolapNativeTopCount$TopCountConstraint` and translate the two extras
it carries on top of the `SetConstraint` base:

1. **Sort-measure projection.** The constraint's `orderByExpr` is a
   `MemberExpr` whose member is (for the currently-supported shape) a
   `RolapStoredMeasure` with a real fact-table column and one of
   {Sum, Count, Min, Max, Avg, DistinctCount} as its aggregator.
   Translated to a `PlannerRequest.Measure` with alias `m0`. The
   renderer's post-aggregate reprojection (Task K fix) places it AFTER
   the group-by columns so the SELECT layout matches legacy's shape:
   dim keys first, sort measure last.
2. **Primary ORDER BY.** Inserted BEFORE the per-target dim-key
   ORDER BYs (which become tiebreakers), with direction taken from
   `constraint.isAscending()`. TopCount → DESC, BottomCount → ASC.

No `LIMIT` clause is emitted — Mondrian's `RolapNativeTopCount`
sets `setMaxRows(count)` on the outer `SetEvaluator`, which propagates
to `SqlStatement.execute` and becomes a JDBC
`Statement.setMaxRows(count)` call. HSQLDB honours that via the
driver, regardless of which SQL engine emitted the query. Confirmed
by inspecting the legacy goldens: no `LIMIT` / `FETCH FIRST` present.

Unsupported order-by shapes throw `UnsupportedTranslation` with the
offending expression / member / column class in the message:

- Non-`MemberExpr` (e.g. arithmetic, tuple, calculated expression).
- `MemberExpr` over a non-stored measure (calculated measure).
- Stored measure whose expression is a non-real `PhysColumn`
  (`PhysCalcColumn`).
- Aggregator outside the supported enum.

### Dispatch gate

Was: class-name equality check on `NonEmptyCrossJoinConstraint`.
Now: `instanceof SetConstraint` + either NECJ class-name OR
`instanceof TopCountConstraint`. TopCount dispatches via the actual
type because its class was promoted from package-private to `public`
in this task (file is scheduled for deletion in worktree #4, so the
surface bump is transient).

### Files changed

| File | Delta |
|------|-------|
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java`  | +150 / -12 |
| `src/main/java/mondrian/rolap/RolapNativeTopCount.java`       | +18 / -1   |

### Harness state

| Run                                                                              | Result         |
|----------------------------------------------------------------------------------|----------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test` | **34/34 pass** |
| `mvn -Pcalcite-harness                          -Dtest='Equivalence*Test' test` | **30/34 pass** (was 27/34) |

Net +3 pass: `topcount`, `named-set`, `native-topcount-product-names`.

### Post-Task-O first-throw bucket distribution

| Count | First `UnsupportedTranslation` / signal |
|-------|-----------------------------------------|
| 2     | `fromTupleRead: RolapNativeFilter$FilterConstraint` |
| 1     | `fromTupleRead: DescendantsConstraint` |
| 1     | `types cardinality != column count` (non-empty-rows — level properties) |

TopCount is cleared. FilterConstraint (Task P) is next, then the two
non-constraint-shape blockers.

---

## Task P summary (2026-04-19)

Extend the Task-N/O gate to accept
`RolapNativeFilter$FilterConstraint` and translate its `filterExpr`
into a HAVING predicate on the tuple-read aggregate.

1. **`PlannerRequest.Having` value type** — carries a
   `(Measure, Comparison, Object literal)` triple. The new
   `Comparison` enum covers the six MDX infix comparators
   (`>, <, >=, <=, =, <>`). Builder `.addHaving(h)` appends to a
   new `havings` list (defaults empty).
2. **`CalciteSqlPlanner` HAVING emission.** Each `Having` adds its
   `measure` to the aggregate's `AggCall` list with a stable alias
   (`h0..hN`), then a `b.filter()` applies the comparison to the
   aggregate output. Calcite's `RelToSqlConverter` renders
   `Filter-over-Aggregate` on dim-only projections as a proper
   `HAVING` clause — verified against the filter.json golden. The
   HAVING-only measures are dropped from the final SELECT by the
   existing post-aggregate reprojection (which only re-exposes
   `req.measures`, not the HAVING list).
3. **`CalcitePlannerAdapters.addFilterHaving()`.** Narrow
   translator: the filter expression must be a
   `ResolvedFunCall` whose `FunDef.name` is one of the six
   comparators, with exactly two args — LHS a `MemberExpr` over a
   `RolapStoredMeasure` with a real `PhysRealColumn` + supported
   aggregator (same gate as the TopCount sort-measure translator),
   RHS a numeric `Literal`. Richer shapes (compound boolean,
   arithmetic, calculated measures, two-measure comparisons) throw
   `UnsupportedTranslation` with the concrete class name.

### SQL comparison

| Backend | HAVING rendering |
|---------|------------------|
| Legacy  | `having (sum("sales_fact_1997"."unit_sales") > 10000)` |
| Calcite | `HAVING SUM("sales_fact_1997"."unit_sales") > 10000` |

Semantically identical. SQL_DRIFT under the strict comparator is
advisory per the worktree-#2 guardrails.

### Files changed

| File | Delta |
|------|-------|
| `src/main/java/mondrian/calcite/PlannerRequest.java`          | +40 / -0   |
| `src/main/java/mondrian/calcite/CalciteSqlPlanner.java`       | +35 / -0   |
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java`  | +145 / -10 |
| `src/main/java/mondrian/rolap/RolapNativeFilter.java`         | +9 / -1    |
| `src/test/java/mondrian/calcite/CalciteSqlPlannerTest.java`   | +35 / -1   |

### Harness state

| Run                                                                              | Result         |
|----------------------------------------------------------------------------------|----------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test` | **34/34 pass** |
| `mvn -Pcalcite-harness                          -Dtest='Equivalence*Test' test` | **32/34 pass** (was 30/34) |

Net +2 pass: `filter`, `native-filter-product-names`.

### Post-Task-P first-throw bucket distribution

| Count | First `UnsupportedTranslation` / signal |
|-------|-----------------------------------------|
| 1     | `fromTupleRead: DescendantsConstraint` |
| 1     | `types cardinality != column count` (non-empty-rows — level properties) |

FilterConstraint is cleared. The two remaining blockers are both
non-SetConstraint shapes.

## Task Q — `DescendantsConstraint` fromTupleRead

Target: multi-parent Descendants hop (`Descendants(<member>, <leaf-level>)`
two levels deep) now translates via the tuple-read path instead of
throwing `non-default TupleConstraint not yet supported`.

### What landed

- `DescendantsConstraint` promoted from package-private to `public` and
  two getters added (`getParentMembers`, `getMcc`) so the
  `mondrian.calcite` translator can read the constraint's state without
  reflection. Scheduled for deletion in worktree #4.
- New branch `translateDescendantsConstraintTupleRead` in
  `CalcitePlannerAdapters`:
  - Single-target only; parents must share a level that is an
    ancestor of the target on the same hierarchy.
  - Projections walk every non-all level from root to target,
    emitting each level's attribute keyList (dedup by alias+name).
    Mirrors legacy `addLevelMemberSql`'s root-down walk so
    `SqlTupleReader.Target`'s column layout still lines up with the
    result columns.
  - Parent predicate is a `TupleFilter` over the parent-level's
    attribute keyList, one row per parent with
    `member.getKeyAsList()` values. Single-parent / single-key
    degenerates to EQ at render time.
  - Snowflake-parent-filter (parent key on a non-leaf dim table) is
    rejected with `UnsupportedTranslation`; the corpus doesn't
    exercise it.
- New unit test `TupleReadDescendantsTest` covers the empty-parent and
  multi-target rejection paths; positive path rides the calcite-harness
  run of the `descendants` corpus entry.

### Legacy vs Calcite SQL (Descendants hop, parents = Q1..Q4)

Legacy (`golden-legacy/descendants.json` seq=1):

```
select "time_by_day"."the_year" as "c0",
       "time_by_day"."quarter" as "c1",
       "time_by_day"."month_of_year" as "c2"
from "time_by_day" as "time_by_day"
where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1'
    or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2'
    or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q3'
    or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q4')
group by "time_by_day"."the_year",
         "time_by_day"."quarter",
         "time_by_day"."month_of_year"
order by ... the_year ASC, ... quarter ASC, ... month_of_year ASC
```

Calcite (traced from `EquivalenceSmokeTest#equivalent[descendants]`):

```
SELECT "the_year", "quarter", "month_of_year"
FROM "time_by_day"
WHERE "the_year" = 1997 AND "quarter" = 'Q1'
   OR "the_year" = 1997 AND "quarter" = 'Q2'
   OR "the_year" = 1997 AND "quarter" = 'Q3'
   OR "the_year" = 1997 AND "quarter" = 'Q4'
GROUP BY "the_year", "quarter", "month_of_year"
ORDER BY "the_year", "quarter", "month_of_year"
```

Semantically identical; rowset shape + checksum match.

### Files changed

| File | Delta |
|------|-------|
| `src/main/java/mondrian/rolap/DescendantsConstraint.java`    | +14 / -1 |
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` | +170 / -0 |
| `src/test/java/mondrian/calcite/TupleReadDescendantsTest.java` | new    |

### Harness state

| Run                                                                              | Result         |
|----------------------------------------------------------------------------------|----------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test` | **34/34 pass** |
| `mvn -Pcalcite-harness                          -Dtest='Equivalence*Test' test` | **33/34 pass** (was 32/34) |

Net +1 pass: `descendants`.

### Post-Task-Q first-throw bucket distribution

| Count | First `UnsupportedTranslation` / signal |
|-------|-----------------------------------------|
| 1     | `types cardinality != column count` (non-empty-rows — level properties) |

DescendantsConstraint is cleared. Only `non-empty-rows` remains — the
NECJ level-properties gap tracked as Task R.

---

## Task R — NECJ projects level properties (non-empty-rows)

### Symptom

`EquivalenceSmokeTest#equivalent[non-empty-rows]` threw
`types cardinality != column count 4`. The NECJ translator projected
4 columns (store_country, store_state, store_city, store_name) but
`LevelColumnLayout` expected 12 — legacy's SELECT list also pulls
every `<Property>` attribute defined on the leaf `[Store Name]`
level (store_type, store_manager, store_sqft, grocery_sqft,
frozen_sqft, meat_sqft, coffee_bar, store_street_address).

### Fix

`CalcitePlannerAdapters.emitNecjTargetProjections` now mirrors the
trailing `getExplicitProperties()` loop of
`SqlTupleReader.addLevelMemberSql`: for every explicit property on
each non-all level, emit the property attribute's key column as
both projection and GROUP BY. `collectNecjRelations` updated in
lockstep so property-bearing tables join the request.

### Files changed

| File | Delta |
|------|-------|
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` | +29 / -2 |

### Harness state

| Run                                                                              | Result         |
|----------------------------------------------------------------------------------|----------------|
| `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test` | **34/34 pass** |
| `mvn -Pcalcite-harness                          -Dtest='Equivalence*Test' test` | **34/34 pass** |

Worktree #2 complete: Calcite 34/34, Legacy 34/34.
