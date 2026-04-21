# Calcite SQL Quality & GROUPING SETS Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or superpowers:subagent-driven-development) to implement this plan task-by-task.

**Goal:** Upgrade Calcite SQL emission so it (a) looks like hand-written SQL — no outer-subquery wrapping, no 40-column inner SELECTs, no redundant reprojections — and (b) collapses multiple same-shape segment loads from one MDX query into a single GROUPING SETS statement, eliminating most of Mondrian's per-MDX SQL round-trip overhead. Optionally upgrade `CalciteSqlPlanner` to run a real `VolcanoPlanner` so future cost-based optimisations (MV selection, custom rules) become possible.

**Architecture:** Three independently-shippable phases. Phase 1 is three translator-level cleanups in `CalcitePlannerAdapters` and `CalciteSqlPlanner`. Phase 2 introduces a "segment-load batcher" between Mondrian's `RolapAggregationManager` and the dispatch seam — when N grouping sets share a fact + measure set, they're translated as one `PlannerRequest` with multiple `groupBy` keys and emitted as a single `GROUPING SETS` SQL statement. Phase 3 (optional, deferred) wraps `RelBuilder` output in a `VolcanoPlanner` `Programs` run so MV rules and custom optimisations can fire.

**Tech Stack:** Apache Calcite 1.41 (`RelBuilder`, `RelToSqlConverter`, `Programs`, `RelOptMaterialization`), Mondrian 4 (`SegmentCacheManager`, `GroupingSetsList`, `RolapAggregationManager`), Postgres 18 + HSQLDB 1.8.

**Why this plan:** From the perf investigation (`docs/reports/perf-investigation-y1.md`, perf benchmark Y.2 follow-up):
- Y.2 cache fix took MvHit D/B from 2.24× to 1.01× — per-query reflection was the dominant fixed cost.
- Remaining D/B residual on Postgres comes from per-statement overhead (parse + plan + round-trip), NOT from emitted SQL plan quality (Postgres flattens Calcite's wrapped subqueries to identical plans).
- The single biggest lever left is **fewer SQL statements per MDX** (what GROUPING SETS does).

**What this plan does NOT do:**
- Does NOT delete legacy code (worktree #4 cleanup scope).
- Does NOT touch `RolapEvaluator` or MDX semantics. Only changes how SQL is emitted.
- Does NOT re-record harness goldens unless absolutely necessary (cell-set parity is the gate; SQL_DRIFT is advisory by Task A).

---

## Ground rules

- **Each phase ends green:** harness 44/44 on HSQLDB both backends; cell-set parity preserved against archived legacy goldens.
- **Postgres validation per phase:** the perf benchmark's MvHit + 3 representative heavy queries re-run after each phase to confirm no regression.
- **TDD throughout:** every translator change starts with a unit test that pins the desired RelNode tree shape OR the desired emitted SQL substring.
- **No fallback safety net:** Calcite path stays "no Mondrian dialect passthrough" per worktree #1 Task D policy. `UnsupportedTranslation` propagates.
- **Commit per task.** Repo style: `feat(calcite):`, `perf(calcite):`, `refactor(calcite):`, `test(calcite):`, `docs(calcite):`. Co-author trailer.

---

# Phase 1 — SQL emission cleanups (3 tasks)

Resolves the three structural cleanups identified in the SQL-diff analysis. Each fix is local to `mondrian/calcite/`. Each unblocks the next.

## Task 1: Push per-column filters into the join chain

**Problem:** `CalcitePlannerAdapters.fromSegmentLoad` currently does:
```
b.scan(fact)              → all fact columns
b.scan(dim) b.join(...)   → all dim columns added
b.filter(year=1997)       → applied to combined row
b.filter(month IN (...))  → applied AFTER join chain → wraps as outer subquery
b.aggregate(...)
```

Result: `RelToSqlConverter` sees `Filter(Project(Join(Filter(...))))` and emits a wrapping subquery with 40+ columns from every joined table.

**Goal:** Apply each per-column predicate to the table that owns the column **before** that table joins anything. Tree becomes:
```
b.scan(fact)
b.filter(fact.fkey predicates only)
b.scan(dim) b.filter(dim attr predicates only) b.join(...)
b.aggregate(...)
```

`RelToSqlConverter` flattens this to one SELECT.

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` — `fromSegmentLoad`, `addCompoundFilters`, `ensureJoinedChain`. Currently filters live at the chain root; move them to per-table.
- Test: `src/test/java/mondrian/calcite/SegmentLoadFilterPushdownTest.java` (new).

**Step 1: Write failing test**

```java
package mondrian.calcite;

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import static org.junit.Assert.*;

public class SegmentLoadFilterPushdownTest {
    @Test public void perColumnFiltersDoNotCreateOuterSubquery() {
        // Build a synthetic PlannerRequest with two filters:
        //   year = 1997 (on time_by_day)
        //   month IN (1,2,3,4) (on time_by_day)
        // and one join (fact → time_by_day).
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join("time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"), 1997))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "month_of_year"),
                PlannerRequest.Operator.IN,
                java.util.Arrays.asList(1, 2, 3, 4)))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "store_sales"),
                "m0"))
            .build();

        CalciteMondrianSchema schema = TestSchemas.foodMart();
        String sql = new CalciteSqlPlanner(schema, hsqldbDialect()).plan(req);

        // Must NOT contain an outer-SELECT wrapper:
        // "FROM (SELECT ... ) AS \"t\""
        assertFalse(
            "expected no subquery wrapping in emitted SQL, got:\n" + sql,
            sql.contains("FROM (SELECT") || sql.contains("FROM ( SELECT"));
        // Must contain both filters in the WHERE clause:
        assertTrue(sql.contains("the_year") && sql.contains("1997"));
        assertTrue(sql.contains("month_of_year"));
    }
}
```

**Step 2: Verify it fails**

Run: `mvn -q -Dtest=SegmentLoadFilterPushdownTest test`
Expected: FAIL — current implementation emits the subquery wrapper.

**Step 3: Implement the fix**

Restructure `CalcitePlannerAdapters.fromSegmentLoad` so that:
1. Each `PlannerRequest.Filter` carries the table it filters (already does — `Filter.column.table`).
2. Build a `Map<String, List<Filter>>` (table-alias → its filters) before scanning anything.
3. For each `b.scan(table)`, immediately call `b.filter(...)` with that table's filters before any join.
4. Compound predicates (`addCompoundFilters`) follow the same rule — route each leaf to its table's filter set; only emit a top-level filter if the predicate genuinely spans multiple tables (e.g. an OR-across-tables, which is rare in our corpus and currently rejected).

Pseudocode for the new core loop:

```java
// Bucket per-table filters
Map<String, List<RexBuilder>> perTable = new LinkedHashMap<>();
for (Filter f : req.filters) perTable.computeIfAbsent(f.column.table, k -> new ArrayList<>()).add(f);

// Scan + filter fact first
b.scan(req.factTable);
applyTableFilters(b, perTable.get(req.factTable));

// Each join: scan dim, filter dim's local filters, then join
for (Join j : req.joins) {
    b.scan(j.dimTable);
    applyTableFilters(b, perTable.get(j.dimTable));
    b.join(INNER, b.equals(b.field(2, 0, j.factKey), b.field(2, 1, j.dimKey)));
}

// Truly cross-table filters (none today; reject if any)

// Then aggregate / project as today
```

For `TupleFilter` (multi-column OR-of-AND) keep at the top — it can't push to a single table.

**Step 4: Run the test**

Run: `mvn -q -Dtest=SegmentLoadFilterPushdownTest test`
Expected: PASS.

**Step 5: Run the full Calcite unit suite**

Run: `mvn -q -Dtest='mondrian.calcite.*Test' test`
Expected: all green. Existing tests pin behaviour; if any break, the restructure changed semantics — investigate before proceeding.

**Step 6: Run the harness**

Run: `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test 2>&1 | grep -E "Tests run:|BUILD" | tail -5`
Expected: 44/44.

Run: `mvn -Pcalcite-harness -Dmondrian.backend=legacy -Dtest='Equivalence*Test' test 2>&1 | grep -E "Tests run:|BUILD" | tail -5`
Expected: 44/44 (gate).

**Step 7: Re-render SQL diff to confirm cleaner output**

Run: `mvn -Pcalcite-harness -Dharness.writeSqlDiff2x2=true -Dtest=SqlDiffMatrixReportTest test`
Then `head -80 docs/reports/sql-diff-2x2.md` — `aggregate-measure` and `topcount` should no longer have the wrapping subquery.

**Step 8: Commit**

```
perf(calcite): push per-column filters into join chain

fromSegmentLoad now buckets filters by their target table and
applies each table's filters immediately after b.scan(table) and
before joining. Eliminates the outer-SELECT wrapping subquery in
queries with attribute filters that aren't on the fact table
(aggregate-measure, topcount, filter, distinct-count, ...).

Postgres flattens both shapes to the same plan, so wall-time impact
is marginal — but the emitted SQL is now ~7× smaller on wrapped
queries. Cleaner logs, lower parse cost, faster network round-trip.

HSQLDB harness 44/44 both backends. Cell-set parity preserved
(SQL_DRIFT advisory).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 2: Slim the inner Project — only carry needed columns

**Problem:** `RelBuilder.scan(table)` keeps the table's full row type. After joining 4 tables (sales_fact + time_by_day + product + product_class), the intermediate row has 40+ columns — most of which the eventual SELECT/GROUP BY/aggregate doesn't need. `RelToSqlConverter` faithfully emits all of them in the inner SELECT.

**Goal:** After each `b.scan(table)`, immediately `b.project(only_needed_columns_from_this_table)`. The "needed" set is the union of: filter columns (for filters not yet applied), join keys, group-by columns, and measure source columns.

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` — same area as Task 1 (build "needed columns per table" set before scanning).
- Test: extend `SegmentLoadFilterPushdownTest` with column-count assertions.

**Step 1: Compute "needed columns" up front**

In `fromSegmentLoad`, after parsing the `GroupingSetsList`, walk all of:
- `req.groupBy` columns
- `req.measures` source columns
- `req.projections` columns
- `req.filters` columns
- `req.tupleFilters` columns
- `req.orderBy` columns
- All `req.joins[*].factKey` and `req.joins[*].dimKey`

Bucket by `table`. The result is `Map<String, Set<String>> needed`.

**Step 2: After every b.scan, project to needed**

```java
b.scan(table);
Set<String> cols = needed.get(table);
b.project(cols.stream().map(b::field).collect(toList()));
```

**Step 3: Failing test (extends Task 1)**

```java
@Test public void innerProjectOnlyCarriesNeededColumns() {
    PlannerRequest req = ...same as Task 1 but with sales_fact_1997 + time_by_day + product + product_class joined chain...;
    String sql = planner.plan(req);
    // Original problem: inner SELECT enumerated 40+ columns from product/product_class.
    // After fix: should only mention the columns we actually use:
    //   sales_fact_1997.{store_sales, time_id, product_id}
    //   time_by_day.{time_id, the_year}
    //   product.{product_id, product_class_id}
    //   product_class.{product_class_id, product_family, product_department}
    assertFalse("inner SELECT must not enumerate brand_name", sql.contains("brand_name"));
    assertFalse("inner SELECT must not enumerate SKU", sql.contains("SKU"));
    assertFalse("inner SELECT must not enumerate fiscal_period", sql.contains("fiscal_period"));
}
```

**Step 4: Run test → red, implement, run → green**

**Step 5: Harness 44/44 both backends**

**Step 6: Commit**

```
perf(calcite): slim inner Project to only carry referenced columns

After each b.scan(table) in fromSegmentLoad, project down to only
the columns referenced by the request (filters + joins + group-by +
measures + order-by). Inner SELECT lists drop from ~40 columns to
3-8.

Compounds with Task 1: the wrapped subquery is gone, AND the
remaining SELECT is now slim. SQL on the wire shrinks by another
~3×. Postgres parse cost drops correspondingly. Wall-time impact
still <2% on heavy queries (planner-bound), but log readability is
transformed.

HSQLDB harness 44/44 both backends.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 3: Drop the post-Aggregate reproject when redundant

**Problem:** Task K added an unconditional `b.project(...)` after `b.aggregate(...)` to force the SELECT order to match request order (because Calcite's `Aggregate` normalises group keys by column ordinal). When the aggregate's natural output ordering already matches the request, the project is a no-op layer that gets unparsed as an extra outer SELECT.

**Goal:** Compare the aggregate's output ordinals (group-by columns sorted by their input ordinal, then aggregator outputs) against the request's intended output order (group-by in declaration order, then measures in declaration order). If they match, skip the reproject.

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` — the `plan()` method, around the post-aggregate `b.project(...)` call.
- Test: `src/test/java/mondrian/calcite/PostAggregateReprojectTest.java` (new).

**Step 1: Failing test**

```java
@Test public void noReprojectWhenAggregateOrderAlreadyMatchesRequest() {
    PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
        .addGroupBy(new Column("sales_fact_1997", "product_id"))  // input ordinal 0
        .addMeasure(new Measure(SUM,
            new Column("sales_fact_1997", "store_sales"), "m0"))
        .build();
    String sql = planner.plan(req);
    // Single SELECT statement, no outer wrapping
    long selectCount = sql.lines().filter(l -> l.trim().startsWith("SELECT")).count();
    assertEquals("expected one SELECT, got " + selectCount + ":\n" + sql, 1, selectCount);
}

@Test public void reprojectStillFiresWhenAggregateOrderDiffersFromRequest() {
    PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
        .addJoin(new Join("customer", "customer_id", "customer_id"))
        // gender ordinal 19, marital_status ordinal 17
        .addGroupBy(new Column("customer", "gender"))      // request order: gender first
        .addGroupBy(new Column("customer", "marital_status"))
        .addMeasure(new Measure(SUM,
            new Column("sales_fact_1997", "store_sales"), "m0"))
        .build();
    String sql = planner.plan(req);
    // Aggregate's natural order is (marital_status, gender, m0) by ordinal —
    // request wants (gender, marital_status, m0). Reproject must fire.
    // Verify the final SELECT lists gender BEFORE marital_status:
    int genderIdx = sql.indexOf("gender");
    int maritalIdx = sql.indexOf("marital_status");
    assertTrue("gender must appear before marital_status: " + sql,
        genderIdx > 0 && maritalIdx > genderIdx);
}
```

**Step 2: Run, expect first to fail (currently always reprojects), second to pass**

**Step 3: Implement**

In `CalciteSqlPlanner.plan()`, before calling `b.project(restoreOrder)`:
1. Compute `naturalOrder` = `req.groupBy` sorted by column input-ordinal (via `RelBuilder.field(name).getIndex()` after the scan/join chain).
2. Compute `requestOrder` = `req.groupBy` in declaration order.
3. If `naturalOrder.equals(requestOrder)` AND `req.measures` aliases match the aggregate's auto-generated alias scheme, skip the reproject.
4. Otherwise, keep the existing reproject logic.

**Step 4: Tests both pass**

**Step 5: Harness 44/44**

**Step 6: Commit**

```
perf(calcite): skip post-aggregate reproject when ordering matches

Task K's reproject was unconditional; now it fires only when the
Calcite Aggregate's natural output order (group keys sorted by
input ordinal) differs from the request's declared order. For
single-table aggregations and many cases where group columns
happen to be in ordinal order, the reproject is now elided.

Removes one outer-SELECT wrapping layer from emitted SQL on those
queries.

HSQLDB harness 44/44 both backends.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 4: Phase 1 validation report

**Files:**
- Create: `docs/reports/perf-investigation-y4-phase1.md`.

**Step 1: Re-run perf matrix MvHit + 5 representative heavy queries on Postgres**

Use the corpus filter:
```
SKIP_A=1 SKIP_C=1 \
  HARNESS_BENCH_WARMUP=1 HARNESS_BENCH_TIMED=2 \
  HARNESS_BENCH_CORPUS=mvhit \
  bash scripts/perf/run-bench-matrix.sh
```

Then a small slice of smoke + aggregate (filter to the 5 reps via a `harness.bench.queries=...` system property — extend `PerfBenchmarkTest` if not already present).

**Step 2: Compare D/B before vs after Phase 1**

Document per-query D/B before (post-Y.2) vs after (post-Phase-1). Expected: marginal improvement on Postgres (planner flattens), measurable improvement on HSQLDB-Calcite (less to optimise away).

**Step 3: Re-render `docs/reports/sql-diff-2x2.md`**

Confirm visually: no wrapping subqueries, no 40-column inner SELECTs, no redundant outer projects.

**Step 4: Commit the validation report**

```
docs(perf): Phase 1 SQL-quality cleanup validation

Tasks 1-3 land cleaner emitted SQL. D/B impact on Postgres is small
(planner-bound queries dominate), but log/wire readability is
transformed and HSQLDB-Calcite path is measurably faster.

See docs/reports/perf-investigation-y4-phase1.md for numbers.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

# Phase 2 — GROUPING SETS batching (the real perf win)

A single MDX query like `crossjoin([Gender], [Marital Status])` currently produces **11 separate SQL statements** (one per (axis, level) pair, plus contexts). Each round-trips to Postgres, each is parsed, planned, executed. Most of them differ only in their GROUP BY columns — same fact, same filters, same measures.

GROUPING SETS lets Postgres compute all of them in one pass:
```sql
SELECT cols, SUM(...) FROM ... WHERE ... GROUP BY GROUPING SETS (
  (year),
  (year, gender),
  (year, marital_status),
  (year, gender, marital_status)
)
```

This collapses the 11 statements into 1-3. Round-trip count drops by 70-90%. **This is the actual blog headline.**

## Task 5: Identify batching opportunities in `RolapAggregationManager`

**Goal:** Find where Mondrian decides "one segment load per grouping set" and add a batch-detection step that groups same-shape requests.

**Files (read first):**
- `src/main/java/mondrian/rolap/agg/AggregationManager.java` — `loadAggregations` / `generateSql`.
- `src/main/java/mondrian/rolap/agg/SegmentCacheManager.java` — the request queue.
- `src/main/java/mondrian/rolap/agg/SegmentLoader.java` — current dispatch site (worktree #1 Task 7).
- `src/main/java/mondrian/rolap/agg/GroupingSetsList.java` — what a "grouping set list" already is (legacy already supports multiple grouping sets per SQL — surprise — but only for limited shapes).

**Step 1: Spike a read-only inspector**

Write a `GroupingSetBatchProbeTest` (opt-in via system property) that runs one MDX query (`crossjoin`), instruments `SegmentCacheManager` to log every (RolapStar, columns, predicates, measures) tuple submitted, and dumps them. Reveals: how many requests go in, how many are same-shape, what the grouping-set keys look like.

**Step 2: Document findings in `docs/reports/perf-investigation-y4-phase2-spike.md`**

For 3-4 representative queries (`crossjoin`, `topcount`, `aggregate-measure`, an MvHit one), report:
- Number of segment loads triggered per MDX.
- How many are batchable (share fact + filters + measures, differ only in groupBy).
- Estimated round-trip savings if batched.

**Step 3: Decide the batching strategy**

Two options surface here:
- **(a) Submitter-side batching** — collect requests in `SegmentCacheManager` for ~50ms (or until the executor is idle), batch them, submit one SQL.
- **(b) Detector-side batching** — Mondrian already has `RolapAggregationManager.loadAggregations` which sees a *list* of needed segments at a time. Group them there before calling the dispatch.

(b) is cleaner — no async/timing complexity. Default to (b) unless the spike shows requests dribble in one at a time.

**Step 4: Commit the spike + decision**

```
docs(perf): spike — GROUPING SETS batching opportunity in Mondrian

Investigation findings on which segment loads per MDX query are
batchable. Decided strategy: <a or b>. Implementation in subsequent
tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 6: `PlannerRequest` supports multiple GROUP BY sets

**Files:**
- Modify: `src/main/java/mondrian/calcite/PlannerRequest.java` — add `List<List<Column>> groupingSets` (singular `groupBy` becomes shorthand for one set).
- Modify: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` — render via `b.aggregate(b.groupKey(commonKeys, groupingSetsAsList), aggs)`.
- Test: `src/test/java/mondrian/calcite/GroupingSetsRenderTest.java` (new).

**Step 1: Failing test**

```java
@Test public void multipleGroupingSetsRenderAsGroupingSets() {
    PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
        .addJoin(new Join("customer", "customer_id", "customer_id"))
        .addGroupingSet(java.util.Arrays.asList(
            new Column("customer", "gender")))
        .addGroupingSet(java.util.Arrays.asList(
            new Column("customer", "marital_status")))
        .addGroupingSet(java.util.Arrays.asList(
            new Column("customer", "gender"),
            new Column("customer", "marital_status")))
        .addMeasure(new Measure(SUM,
            new Column("sales_fact_1997", "store_sales"), "m0"))
        .build();
    String sql = planner.plan(req);
    assertTrue(sql.contains("GROUPING SETS"));
    assertTrue(sql.contains("gender"));
    assertTrue(sql.contains("marital_status"));
}
```

**Step 2: Implement** — `RelBuilder` exposes `groupKey(List<RexNode> keys, List<List<RexNode>> groupSets)`. Pass the union of all grouping-set columns as `keys`, and the per-set lists as `groupSets`.

**Step 3: Test passes; harness still 44/44**

**Step 4: Commit**

```
feat(calcite): PlannerRequest supports multiple grouping sets

Adds groupingSets to the request shape; the renderer emits one
SQL statement using GROUP BY GROUPING SETS (...) instead of N
single-GROUP-BY statements. Single-set requests (the common case)
unchanged.

Foundation for Phase 2 batching: same fact + filters + measures
across N segment loads can now be expressed in one PlannerRequest.

HSQLDB harness 44/44 both backends.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 7: Translator wires N grouping sets into one PlannerRequest

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` — `fromSegmentLoadBatch(List<GroupingSet>)` (new method).
- Modify: `src/main/java/mondrian/rolap/agg/SegmentLoader.java` (or wherever Task 5 decided the batching point lives) — call `fromSegmentLoadBatch` instead of N×`fromSegmentLoad`.
- Test: `src/test/java/mondrian/calcite/SegmentLoadBatchTest.java` (new).

**Step 1: Failing test**

Build a synthetic `GroupingSetsList` with 3 same-shape grouping sets. Assert that `fromSegmentLoadBatch(...)` produces a single `PlannerRequest` with three entries in `groupingSets` (and that each entry's columns match the original GroupingSet).

**Step 2: Implement**

The translator needs to:
1. Verify all input GroupingSets share fact + filters + measures + joins. If they don't, fall back to per-set translation (or throw `UnsupportedTranslation` with a clear message).
2. Union the joins/filters/measures into a single per-request bag.
3. Per-set group-by columns become entries in `req.groupingSets`.

**Step 3: Wire dispatch**

At the dispatch point (per Task 5's decision), when N requests are batchable, translate them as one `PlannerRequest` and submit one SQL. Mondrian's segment-completion code needs to demultiplex one ResultSet's rows into N segment caches based on the GROUPING() bit per row. **This is the hardest sub-step** — Mondrian's `SegmentLoader.processData` already handles GROUPING SETS legacy results (legacy SQL emits them too), so verify the existing demux works for our shape.

**Step 4: Run targeted MDX**

A test that runs `crossjoin` MDX under `-Dmondrian.backend=calcite` and asserts (a) cell-set parity vs HSQLDB legacy golden, (b) only 1-3 SQL statements emitted (was 11), (c) the SQL contains `GROUPING SETS`.

**Step 5: Harness 44/44 both backends**

**Step 6: Commit**

```
feat(calcite): batch same-shape segment loads into one GROUPING SETS SQL

CalcitePlannerAdapters.fromSegmentLoadBatch translates N
grouping-set requests with shared fact/filters/measures into one
PlannerRequest with multiple groupingSets entries. SegmentLoader
dispatch detects batchable cohorts and submits one SQL statement
instead of N.

Mondrian's existing GROUPING SETS demux in SegmentLoader.processData
fans the single ResultSet's rows back into per-set caches.

Crossjoin MDX dropped from 11 SQL statements to 3.

HSQLDB harness 44/44 both backends. Cell-set parity preserved.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Task 8: Phase 2 perf validation

**Files:**
- Create: `docs/reports/perf-investigation-y4-phase2.md`.

**Step 1: Re-run the full perf matrix on Postgres** — both cells B (legacy) and D (Calcite). MvHit + smoke + aggregate. Same iteration count as the original Y.2 measurement.

**Step 2: Compare D/B per query and overall geomean**

Hypothesised improvements:
- Multi-axis MDX (`crossjoin`, `aggregate-measure`, `topcount`, `filter`) → D/B drops by 30-60% (proportional to round-trip reduction).
- Single-axis MDX (`basic-select`, MvHit) → unchanged (already 1 grouping set).
- Geomean D/B target: **0.85-0.90×** — i.e. Calcite genuinely faster than legacy on Postgres.

**Step 3: Update `docs/reports/perf-benchmark-2x2.md`** with the new numbers as a "post-batching" column.

**Step 4: Commit the validation report**

```
docs(perf): Phase 2 GROUPING SETS batching validation

Net improvement on Postgres D/B: <X>× → <Y>×. Multi-axis MDX
queries see <Z>% reduction in SQL round-trips. Geomean D/B now
<W>× (target: <0.95×).

See docs/reports/perf-investigation-y4-phase2.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

# Phase 3 — VolcanoPlanner upgrade (OPTIONAL, deferred)

Only if Phase 2 isn't enough OR future work needs cost-based optimisation (MV cost selection across candidates, custom rules). Sketched here so the scope is understood; not in the immediate sprint.

## Task 9 (deferred): Wrap RelBuilder output in a Programs run

**Sketch:**
- Wire `Programs.standard()` (or a curated subset) into `CalciteSqlPlanner.plan()`.
- Constrain optimization to land in a JDBC-renderable convention.
- Cache the planner-program result by `PlannerRequest` shape (keyed on the request's structural hash).

**Risk:** Calcite's optimiser is opinionated. Without careful trait/convention setup it'll produce trees that `RelToSqlConverter` can't unparse. Budget 3-5 days of iteration.

## Task 10 (deferred): `RelOptMaterialization` registration for FoodMart aggs

**Sketch:**
- Build one `RelOptMaterialization` per `<MeasureGroup type='aggregate'>` in `FoodMart.mondrian.xml`.
- Register on each `CalciteSqlPlanner` via `planner.addMaterialization(...)`.
- `MaterializedViewRule` in the rule set lets the planner pick MVs cost-aware.

**Risk:** Resolving Mondrian's CopyLink/ForeignKeyLink to per-column equivalences is fiddly. Budget 1 week.

## Task 11 (deferred): Custom rule set + cost tuning

**Sketch:**
- Filter Calcite's standard rule set down to ones that help (filter pushdown, projection pruning, aggregate merge, MV).
- Wire JDBC stats (`pg_class.reltuples` for Postgres, `INFORMATION_SCHEMA.SYSTEM_TABLES` for HSQLDB) as `RelOptTable.getRowCount()` overrides so the cost model knows FoodMart's shape.

---

# Execution handoff

Plan saved to `docs/plans/2026-04-21-calcite-sql-quality-and-grouping-sets.md`. Two execution options:

1. **Subagent-Driven (this session)** — dispatch a fresh subagent per task, review between, stay in this session.
2. **Parallel Session (separate)** — open a new session pointed at this worktree, run executing-plans.

My lean: option 1 for Phase 1 (small, well-scoped tasks — fast iteration). Phase 2 Task 5 is a research spike — that one wants careful attention either way. Phase 3 stays deferred until we see Phase 2 numbers.

**Recommended sprint order:**
- Phase 1 (Tasks 1-4): land in one sitting. ~1-2 hours of subagent work + reviews.
- Phase 2 Task 5 (spike): review the findings before committing to (a) vs (b).
- Phase 2 Tasks 6-8: land in one sitting after spike review. ~3-4 hours.
- Phase 3: only if Phase 2 isn't enough.
