# VolcanoPlanner MV Selection Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Get Calcite's cost-based `MaterializedViewRule` to fire on Mondrian-generated RelNodes so the planner picks the best-fit aggregate automatically, replacing (or complementing) the hand-rolled `MvMatcher`.

**Architecture:** Move aggregate selection from the current pre-plan `MvMatcher` (shape-exact, HepPlanner, no cost) to a proper `VolcanoPlanner` pass with `MaterializedView*Rule` registered and row-count statistics wired through `RelMdRowCount`. Keep `MvMatcher` as a fast-path fallback when Volcano declines.

**Tech Stack:** Calcite 1.41 (VolcanoPlanner, SubstitutionVisitor, MaterializedViewOnlyAggregateRule, RelOptMaterialization), Mondrian RolapStar statistics.

**Prereqs:** Plan C (shape catalog enumeration) lands first — Volcano needs a catalog of candidate materializations, not a handful of hardcoded shapes.

---

### Task 1: Capture baseline — confirm Volcano currently no-ops

**Files:**
- Read: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` (plan-phase entry)
- Read: `src/main/java/mondrian/calcite/MvRegistry.java`

**Step 1:** Add a `-Dmondrian.calcite.volcanoMv=true` system-property gate around the Volcano pass (default off, keeps fallback safe).

**Step 2:** Write a failing test `CalciteVolcanoMvTest.volcanoRewritesToAggregate()` that:
- Loads FoodMart with 4 aggregate MeasureGroups declared
- Runs the `agg-c-year-country` MDX with `-Dmondrian.calcite.mvMatch=false -Dmondrian.calcite.volcanoMv=true`
- Asserts the generated SQL's FROM clause references `agg_c_14_sales_fact_1997`, not `sales_fact_1997`

**Step 3:** Run: `mvn test -Dtest=CalciteVolcanoMvTest#volcanoRewritesToAggregate`
Expected: FAIL — SQL still targets fact table.

**Step 4:** Commit the failing test.

```bash
git add src/test/java/mondrian/calcite/CalciteVolcanoMvTest.java
git commit -m "test: failing baseline for Volcano MV selection"
```

---

### Task 2: Build RelOptMaterialization list from MvRegistry

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` (new `buildMaterializations(MvRegistry)` helper)
- Create: `src/main/java/mondrian/calcite/VolcanoMvPass.java`

**Step 1:** For each `ShapeSpec` in `MvRegistry`, build a `RelOptMaterialization`:
- `tableRel` = `LogicalTableScan` of the aggregate table (via `CalciteMondrianSchema`)
- `queryRel` = `LogicalAggregate` over the fact table + joins matching the shape's `groups`/`joins`
- `qualifiedName` = `[schema, aggTableName]`

**Step 2:** Register the `MvRegistry` → `List<RelOptMaterialization>` projection in `VolcanoMvPass.materializationsFor(schema)`.

**Step 3:** Write test `VolcanoMvPassTest.buildsOneMaterializationPerShape()` — assert size matches `MvRegistry.size()` and each materialization's `queryRel` is a valid `Aggregate(Project(Join*(TableScan)))` shape.

**Step 4:** Run: `mvn test -Dtest=VolcanoMvPassTest`. Fix until green.

**Step 5:** Commit.

```bash
git add src/main/java/mondrian/calcite/VolcanoMvPass.java src/main/java/mondrian/calcite/CalciteSqlPlanner.java src/test/java/mondrian/calcite/VolcanoMvPassTest.java
git commit -m "feat(calcite): build RelOptMaterialization list from MvRegistry"
```

---

### Task 3: Wire RelMdRowCount for fact + aggregate tables

**Files:**
- Create: `src/main/java/mondrian/calcite/RolapStatisticProvider.java`
- Modify: `src/main/java/mondrian/calcite/CalciteMondrianSchema.java` (attach statistic to each table)

**Why:** Without row counts, `MaterializedViewRule` cannot cost-compare a 2.6k-row aggregate scan vs an 86M-row fact scan. It needs `Statistic.getRowCount()` to prefer the MV.

**Step 1:** Implement `RolapStatisticProvider implements Statistic` returning the `RolapStar.Table`'s cached row count (falls back to `DatabaseMetaData` `getTables` size estimate if unknown).

**Step 2:** Modify `CalciteMondrianSchema` so each `TableImpl` returns a `RolapStatisticProvider` via `getStatistic()`.

**Step 3:** Test `RolapStatisticProviderTest.factIsOrdersOfMagnitudeLargerThanAgg()` asserting `factRowCount > 100 * aggRowCount` after loading FoodMart 1000×.

**Step 4:** Run tests, commit.

```bash
git commit -m "feat(calcite): provide row-count statistics to planner"
```

---

### Task 4: Run VolcanoPlanner with MaterializedViewRule

**Files:**
- Modify: `src/main/java/mondrian/calcite/VolcanoMvPass.java`

**Step 1:** Inside `VolcanoMvPass.rewrite(RelNode input, List<RelOptMaterialization> mvs)`:
- Create `VolcanoPlanner` with `RelOptCluster` sharing the input's trait def registry
- Register rules: `MaterializedViewOnlyAggregateRule.INSTANCE`, `MaterializedViewProjectAggregateRule.INSTANCE`, plus `CoreRules.PROJECT_MERGE`, `CoreRules.FILTER_PROJECT_TRANSPOSE`
- `planner.addMaterialization(mv)` for each
- `planner.setRoot(input)`, run `planner.findBestExp()`
- Return the best plan (or input unchanged on timeout/failure)

**Step 2:** Add 5-second `planner.setCancelFlag()` watchdog — if it exceeds budget, fall back to input and log `mondrian.calcite.volcanoMv.timeout` counter.

**Step 3:** Re-run Task 1's test. Expected: PASS — SQL now references `agg_c_14_sales_fact_1997`.

**Step 4:** Commit.

```bash
git commit -m "feat(calcite): fire MaterializedViewRule via VolcanoPlanner"
```

---

### Task 5: Equivalence harness + full corpus regression

**Files:**
- Modify: `src/test/java/mondrian/calcite/EquivalenceAggregateTest.java` (parameterize over `volcanoMv={false,true}`)

**Step 1:** Add the 4 MvHit corpus queries under `volcanoMv=true` to the equivalence harness. Assert cell-level parity vs legacy.

**Step 2:** Run full corpus:
```bash
mvn test -Dtest=EquivalenceAggregateTest -Dmondrian.calcite.volcanoMv=true -Dmondrian.calcite.mvMatch=false
```
Expected: all cells match legacy, all 4 MvHit queries rewrite.

**Step 3:** If LEGACY_DRIFT on any query, diagnose via `DEBUG_MV=true`, fix rule config, re-run. Don't accept false-positive rewrites — if Volcano rewrites a query MvMatcher declines, confirm the cells match before accepting.

**Step 4:** Commit green run.

```bash
git commit -m "test: Volcano MV selection passes equivalence + MvHit corpus"
```

---

### Task 6: Fallback chain + defaults

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java`

**Step 1:** Order: try VolcanoMvPass first (if enabled), then MvMatcher, then pass-through. Log which path fired via `mondrian.calcite.rewritePath` counter (`volcano` / `matcher` / `none`).

**Step 2:** Run 45-query perf benchmark with `volcanoMv=true,mvMatch=true` vs `mvMatch=true` only. Expected: same wall time or better, no regressions > 5%.

**Step 3:** If parity, flip `volcanoMv` default to `true` in a follow-up commit. If Volcano adds ≥ 500ms per query (planner overhead), keep default `false` and ship as opt-in.

**Step 4:** Document the decision in `docs/reports/volcano-mv-ship-decision.md`.

**Step 5:** Commit.

```bash
git commit -m "feat(calcite): fallback chain volcano → matcher → passthrough"
```

---

## Risks

- **Planner overhead:** Volcano + rules on every query may cost 100-500ms. Budget gate (Task 4) caps it.
- **Rule non-determinism:** `MaterializedViewRule` has known edge cases around project-merge ordering. Keep MvMatcher as the primary path until Volcano is proven stable on the full corpus.
- **Row-count accuracy:** RolapStar stats may be stale post-ETL. Low-risk — stale stats still order-of-magnitude correct.

## Done When

- 4/4 MvHit queries rewrite via Volcano with cells matching legacy
- Full 45-query equivalence corpus passes with `volcanoMv=true`
- Perf delta vs matcher-only path within ±5% geomean on Postgres 1000×
- Decision doc captures ship/opt-in stance
