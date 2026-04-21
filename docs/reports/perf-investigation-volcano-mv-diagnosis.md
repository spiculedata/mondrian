# MV rule diagnosis — why `agg-c-year-country` does not rewrite

**Date:** 2026-04-19
**Branch:** `calcite-backend-agg-and-calc`
**HEAD:** `46027e0 feat(calcite): VolcanoPlanner stage with MaterializedViewRule + row-count probe`
**Subject query:** MvHit corpus entry `agg-c-year-country`
**Target MV:** `agg_c_14_sales_fact_1997`
**Captured via:** `src/test/java/mondrian/calcite/MvRuleDiagnosticTest.java`

## TL;DR

The MaterializedViewRule family is registered, fires, and hands the user
tree + MV's defining query to Calcite's `SubstitutionVisitor`. The
visitor returns **zero substitutions**. Root cause: the `queryRel` that
`MvRegistry` emits for each aggregate `MeasureGroup` is a **full-star
snowflake** — `Aggregate[{7 FK/copy cols}](fact JOIN product JOIN store
JOIN promotion JOIN customer JOIN time_by_day)` — whereas the user
query is a **minimal 2-dim star** — `Aggregate[{store_country,
the_year}](fact JOIN store JOIN time_by_day)`. `SubstitutionVisitor`
in Calcite 1.41 can handle a *subset* of query-MV shape divergence
(aggregate rollup on a shared group set, join-elimination on FK/PK
with uniqueness) but not the triple mismatch we have here: different
join shape, different join count, and group keys referencing
dimension-side columns the MV never projects in an analysable form.
Diagnosis is (b) in the task's classification — **rule fires,
SubstitutionVisitor finds no match**.

## 1. User query RelNode (post-Hep, pre-Volcano)

```
rowType = RecordType(VARCHAR(30) store_country,
                     SMALLINT the_year,
                     DECIMAL(19, 4) NOT NULL m0) NOT NULL

LogicalAggregate(group=[{17, 36}], m0=[SUM($7)])
  LogicalJoin(condition=[=($1, $32)], joinType=[inner])
    LogicalJoin(condition=[=($4, $8)], joinType=[inner])
      JdbcTableScan(table=[[foodmart, sales_fact_1997]])
      JdbcTableScan(table=[[foodmart, store]])
    JdbcTableScan(table=[[foodmart, time_by_day]])
```

- **Shape:** `Aggregate(Join(Join(fact, store), time_by_day))`.
- **Group keys:** `$17` = `store.store_country`, `$36` =
  `time_by_day.the_year` — dimension-side columns.
- **Measure:** `SUM($7)` = `SUM(sales_fact_1997.unit_sales)`.
- **Joins:** two equi-joins on FK/PK (`sales_fact_1997.store_id =
  store.store_id`, `sales_fact_1997.time_id = time_by_day.time_id`).

The Hep stage changed only one thing: it dropped the outer identity
`Project` that `CalciteSqlPlanner.build` leaves above the Aggregate
(see `PROJECT_REMOVE`). Structure otherwise matches what
`RelBuilder` emits. Post-Hep is what Volcano sees.

## 2. MV `queryRel` for `agg_c_14_sales_fact_1997`

```
queryRel.rowType = RecordType(
    INTEGER NOT NULL product_id, INTEGER NOT NULL customer_id,
    INTEGER NOT NULL promotion_id, INTEGER NOT NULL store_id,
    SMALLINT the_year, SMALLINT month_of_year, VARCHAR(30) quarter,
    BIGINT NOT NULL fact_count,
    DECIMAL(19, 4) NOT NULL unit_sales,
    DECIMAL(19, 4) NOT NULL store_cost,
    DECIMAL(19, 4) NOT NULL store_sales) NOT NULL

LogicalAggregate(group=[{0, 2, 3, 4, 87, 90, 91}],
    fact_count=[COUNT()],
    unit_sales=[SUM($7)], store_cost=[SUM($6)], store_sales=[SUM($5)])
  LogicalJoin(condition=[true], joinType=[inner])               -- time_by_day (CopyLink)
    LogicalJoin(condition=[=($2, $54)], joinType=[inner])        -- customer (FK)
      LogicalJoin(condition=[=($3, $47)], joinType=[inner])      -- promotion (FK)
        LogicalJoin(condition=[=($4, $23)], joinType=[inner])    -- store (FK)
          LogicalJoin(condition=[=($0, $9)], joinType=[inner])   -- product (FK)
            JdbcTableScan(table=[[foodmart, sales_fact_1997]])
            JdbcTableScan(table=[[foodmart, product]])
          JdbcTableScan(table=[[foodmart, store]])
        JdbcTableScan(table=[[foodmart, promotion]])
      JdbcTableScan(table=[[foodmart, customer]])
    JdbcTableScan(table=[[foodmart, time_by_day]])
```

- **Shape:** `Aggregate(Join⁵(fact, product, store, promotion, customer,
  time_by_day))`.
- **Group keys:** `{0, 2, 3, 4, 87, 90, 91}` — fact-side FK columns
  (`product_id`, `promotion_id`, `customer_id`, `store_id`) plus
  `time_by_day.the_year`, `time_by_day.month_of_year`,
  `time_by_day.quarter` (CopyLink-derived).
- **Time join has `condition=[true]`.** That's a pure-CopyLink
  cartesian, emitted by `MvRegistry.buildMaterialization` (lines
  360–367) because the CopyLink metadata carries no FK/PK pair.
- The output row type (11 columns) is completely disjoint from the
  user query's output row type (3 columns).

## 3. Row-type comparison

| side | columns |
|------|---------|
| user query | `store_country : VARCHAR(30)`, `the_year : SMALLINT`, `m0 : DECIMAL(19,4)` |
| MV `queryRel` | `product_id, customer_id, promotion_id, store_id, the_year, month_of_year, quarter, fact_count, unit_sales, store_cost, store_sales` |
| MV `tableRel` | `product_id, customer_id, store_id, promotion_id, month_of_year, quarter, the_year, store_sales, store_cost, unit_sales, fact_count` |

The user tree never projects `product_id`, `customer_id`, `promotion_id`,
`month_of_year`, `quarter`, `fact_count`, `store_cost`, `store_sales`.
It *does* project `store_country`, which the MV neither groups on nor
exposes anywhere in its tree. So even if `SubstitutionVisitor` could
match the join/aggregate skeleton, rollup post-match would still need
to drop seven grouping dimensions (a many-to-one rollup through
`fact_count`) **and** prove that `store_country` is a functional
dependency of `store_id` — information Calcite does not have because
the `JdbcTable` advertises no unique keys / no PK-FK metadata.

## 4. Where the rule bails — evidence

The diagnostic test ran `SubstitutionVisitor(mv.queryRel, userRel)`
directly (mirroring the call `MaterializedViewAggregateRule` makes
inside Volcano) and reported:

```
=== SubstitutionVisitor direct probe ===
substitutions returned: 0
```

The full Volcano pass produced:

```
=== Volcano stage ===
identity-changed? false
text-equal-to-hep? true
```

i.e. Volcano ran, no MV rule fired its `transformTo`, and the best
expression came back structurally identical to the Hep output (same
`LogicalAggregate(... Join ... Join ... fact ... store ...
time_by_day)` tree).

**Classification:** option **(b)** from the task — MV rule fires but
`SubstitutionVisitor` can't find a substitution because the two trees
are structurally too far apart for Calcite 1.41's rollup + join-elim
machinery to bridge.

There is a *secondary* finding: the "focused Volcano run vs agg_c_14
only" block in the diagnostic threw `AssertionError` at
`VolcanoPlanner.changeTraits:497`. That is a cluster-consistency
assertion — the focused `VolcanoPlanner` was constructed without the
Mondrian schema's cluster, so `changeTraits` on a rel from a different
cluster trips Calcite's invariant. Not a production issue: the
production `runVolcano` path reuses the input rel's own cluster (via
`planner.setRoot(rooted)` where `rooted = planner.changeTraits(input,
input.getTraitSet())`), which the main Volcano block in the diagnostic
also exercises successfully. The AE is an artifact of the focused
probe's setup and does not change the diagnosis.

### Why SubstitutionVisitor gives up

Three independent structural gaps, any one sufficient to kill the match:

1. **Extra joined tables in the MV.** `agg_c_14`'s defining query
   joins `product`, `promotion`, `customer` — none of which appear in
   the user tree. Calcite's join-elimination inside
   `SubstitutionVisitor` can drop an extra join on the *user* side
   when an FK/PK uniqueness proof exists; it does not synthesise
   extra joins on the *MV* side to match a sparser user tree, nor
   does the JDBC metadata carry the uniqueness proofs it would need
   even if it tried.
2. **Group-key mismatch.** MV groups by `product_id, customer_id,
   promotion_id, store_id, the_year, month_of_year, quarter`. User
   groups by `store_country, the_year`. `store_country` is not in the
   MV's group set — to roll up from MV to user, the rule would have
   to prove `store_country` is functionally determined by the MV's
   `store_id` group column, which requires PK + FK metadata on the
   `store` table that Calcite's JDBC reflection doesn't surface.
3. **Mondrian's SQL never joins store.** The actual SQL Mondrian
   emits for this query projects `store.store_country` after a
   `fact JOIN store` — but the MV's defining query keeps `store_id`
   as a group key and never projects `store_country`. Even with
   PK-FK metadata, the rule would need to re-join `store` on top of
   the MV to resolve `store_country` (a "materialized-view-and-also
   -a-dim-join-after" rewrite). Calcite 1.41's
   `MaterializedView*Rule` family does not do that.

## 5. Fix strategy recommendation — **A (simplify MV shape)**

The MV's defining query is over-engineered. Mondrian-4's aggregate
`MeasureGroup` metadata gives us enough to emit a **per-query-shape**
family of MVs where each MV's `queryRel` joins only the dims the
user-query class actually touches and groups on the dim-side columns
Mondrian actually projects. Concretely:

- Replace the single full-star `queryRel` per agg table with **N**
  `RelOptMaterialization` entries, one per dim combination the agg
  table can answer. Each `queryRel` is a minimal
  `Aggregate(Join(fact, <only the dims this shape uses>))` with group
  keys expressed as `dim.<level-column>` — the same columns Mondrian's
  SQL generator projects.
- Each `tableRel` stays the plain `Scan(aggTable)` — that side is
  correct already.
- Side effect: the registry grows from 4 MVs to ≈20–30. Volcano's MV
  rule is quadratic in registry size but linear per match attempt, so
  on real FoodMart this stays sub-millisecond.

### Why A and not B/C/D

- **B (canonicalize)**: helps with `condition=[true]` join
  normalisation, but does not fix the group-key / projected-column
  mismatch. We would still need a functional-dependency proof
  Calcite doesn't have.
- **C (patch SubstitutionVisitor)**: out of scope; patching Calcite
  in a fork is a maintenance anchor and fights the framework.
- **D (hand-roll the matcher)**: viable but trades one matcher
  (`RolapGalaxy.findAgg`, which already works and the harness uses)
  for another on the Calcite side. The point of going through
  Calcite's MV rule was to ride Calcite's cost model; a hand-rolled
  matcher throws that away. If we're going to hand-roll, option D
  equals "don't use Calcite's MV rule at all" — which is fine as a
  fallback but should be a conscious retreat, not a first move.

### Estimated complexity of option A

- **Registry rewrite**: 1 day. Replace `MvRegistry.buildMaterialization`
  with a shape-aware enumerator that walks
  `RolapMeasureGroup.dimensionMap3` and, for every subset of present
  dims (bounded by a cap, e.g. 3), emits one MV `queryRel` pinned to
  that subset + the dim-side level column(s) the agg exposes for each
  dim. The `copyColumnList` gives the dim-side column names directly.
- **Testing**: 0.5 day. Extend `MvRegistryTest` to assert the
  expected per-shape MV count, extend `CalciteSqlPlannerVolcanoTest`
  to assert the `agg_c_14` rewrite actually fires on `agg-c-year-country`,
  keep existing MvHit corpus passing byte-for-byte (cell-set parity).
- **Harness**: 0.5 day. `MvHitTest` under Calcite should now accept
  `agg_c_14_sales_fact_1997` in the FROM clause (it already does —
  the acceptable-set is `AGG_C_FAMILY`). No corpus change needed.

**Total estimate: 2 engineering days.** Matches the task's prompt
("does option 2 still look like a 2–3 day task?") — yes, and this
diagnosis sharpens it: the work is *data-shape engineering* in
`MvRegistry`, not Calcite-internals spelunking.

## 6. Artifacts

- Diagnostic test: `src/test/java/mondrian/calcite/MvRuleDiagnosticTest.java`.
  Kept in-tree as regression against future Calcite upgrades — if
  Calcite 1.42+ lands a more aggressive `SubstitutionVisitor`, this
  test's captured output will change and flag re-evaluation.
- Captured full diagnostic run: reproduce with
  `mvn -Dtest=MvRuleDiagnosticTest test`. Output (≈280 lines) goes to
  `stdout` under the JUnit runner; the key blocks (user tree, MV
  trees, SubstitutionVisitor result) are quoted verbatim above.

<!-- end -->
