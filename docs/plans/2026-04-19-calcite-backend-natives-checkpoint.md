# Calcite Backend Natives — Checkpoint (Task N)

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

1. **TopCountConstraint** — unlocks 3 queries
   (`topcount`, `named-set`, `native-topcount-product-names`).
   Needs the sort-measure added to SELECT + a LIMIT clause at render.
2. **FilterConstraint** — unlocks 2 queries
   (`filter`, `native-filter-product-names`).
   Needs a HAVING-like predicate on the measure.
3. **Level properties in NECJ projection** — unlocks `non-empty-rows`.
4. **DescendantsConstraint** — unlocks `descendants`.
