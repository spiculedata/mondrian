# Y.4 Phase 2 Task 5 — GROUPING SETS batching spike

Branch: `calcite-backend-agg-and-calc`
Head: `cea67a6 docs(perf): post-Y.2 D/B re-measurement — Calcite now 7% faster geomean`
Harness: HSQLDB FoodMart, default Mondrian backend.
Probe: `src/test/java/mondrian/test/calcite/GroupingSetBatchProbeTest.java`
(opt-in via `-Dharness.probeGroupingSets=true`)

## 1. Why we are spiking

Phase 1 / Y.3 EXPLAIN ANALYZE established that on Postgres the residual
D/B > 1.10× queries run identical SQL plans on legacy and Calcite — the
gap is **per-statement Java overhead**, amplified by Mondrian emitting
2–11 SQL statements per MDX. The next perf lever is collapsing
same-shape segment loads into a single
`GROUP BY GROUPING SETS (...)` statement. Before picking an
implementation site we need to answer two questions:

  * Q1 — Does Mondrian's segment-load pipeline already hand a
    multi-member `GroupingSetsList` to `SegmentLoader.createExecuteSql`
    in our reference configuration? (If yes, we only need to teach
    the Calcite translator to handle it.)
  * Q2 — How many same-shape cohorts are actually present in
    representative MDX queries — i.e. what is the realistic upper bound
    on statements we can save?

## 2. Data-flow finding (code inspection, no probe needed)

The batching machinery already exists in Mondrian, but it is gated off
in our reference config:

  1. `FastBatchingCellReader.loadAggregations` (L870–886) calls
     `groupBatches(batchList)` **only if**
     `shouldUseGroupingFunction()` returns true
     (`FastBatchingCellReader.java:563`):

     ```java
     return MondrianProperties.instance().EnableGroupingSets.get()
         && dialect.supportsGroupingSets();
     ```

  2. `EnableGroupingSets` defaults to **false**
     (`src/main/prop/mondrian/olap/MondrianProperties.xml:1176`).
  3. `JdbcDialectImpl.supportsGroupingSets()` returns **false**
     (`JdbcDialectImpl.java:827`). Only Db2, Greenplum, Oracle,
     Phoenix, and Teradata override it. **HSQLDB and PostgreSQL
     inherit `false`** — even though Postgres 9.5+ supports the
     construct natively.
  4. When the guard is off, `FastBatchingCellReader.Batch.loadAggregation`
     (L1270) installs a `GroupingSetsCollector(false)` whose
     `useGroupingSets()` returns false. `Aggregation.load`
     (`Aggregation.java:143–156`) therefore always dispatches to
     `SegmentLoader.load(...)` with a single-element
     `List<GroupingSet>`.
  5. Downstream, `SegmentLoader.loadImpl` wraps that list in
     `new GroupingSetsList(singleton)`, whose `useGroupingSet` is set
     to `groupingSets.size() > 1` — always false on our config.
  6. The batching path **is already wired** for dialects where the
     gate is open: `CompositeBatch.load` (L1048–1066) collects
     `GroupingSetsCollector(true)` across a detailed batch and its
     rollup summaries, then submits
     `getSegmentLoader().load(cellRequestCount, collector.getGroupingSets(), ...)`.
     That list arrives at `SegmentLoader.load` with size N>1 and
     `GroupingSetsList.useGroupingSet` becomes true.

The Calcite translator side has the opposite problem — it explicitly
rejects the multi-grouping-set input today:

```
src/main/java/mondrian/calcite/CalcitePlannerAdapters.java:1701
    if (groupingSetsList.useGroupingSets()) {
        throw new UnsupportedTranslation(
            "fromSegmentLoad: GROUPING SETS rollup not yet supported");
    }
```

So: **the detector/batcher in `FastBatchingCellReader.groupBatches`
already does exactly what we want, provided dialect+toggle both say
yes. Strategy (c) is structurally the right answer — the missing
pieces are on either side of the existing primitive, not inside it.**

## 3. Probe results (HSQLDB, default Mondrian backend)

All 5 MDX were executed under `FoodMartCapture.executeCold` with a
fresh schema cache, and every JDBC statement was classified via
`SqlCapture`. A statement counts as a segment load iff it contains
both a `GROUP BY` clause and at least one
`SUM / COUNT / MIN / MAX / AVG` aggregate. Cohort key
= `(factTable, normalised WHERE signature, sorted aggregate list)`.
Two segment loads that share a cohort key differ only in their
group-by columns and are `GROUPING SETS`-batchable.

| MDX                                     | total stmts | segloads | cohorts | post-batch stmts | SQL with `GROUPING SETS` |
|-----------------------------------------|------------:|---------:|--------:|-----------------:|-------------------------:|
| crossjoin                               |          11 |        1 |       1 |               11 |                        0 |
| topcount                                |           5 |        2 |       2 |                5 |                        0 |
| aggregate-measure                       |           8 |        1 |       1 |                8 |                        0 |
| agg-distinct-count-customers-levels     |          12 |        2 |       2 |               12 |                        0 |
| non-empty-rows                          |           4 |        1 |       1 |                4 |                        0 |

### 3.1 Decisive datapoint for Q1

The `GROUPING SETS` column is **0 for every probed MDX** under the
default HSQLDB configuration. This directly confirms the code-inspection
conclusion: `GroupingSetsList` arrives at
`SegmentLoader.createExecuteSql` with size 1 every time. Nothing in the
out-of-the-box fixture exercises the batched path.

### 3.2 Decisive datapoint for Q2

In the five probed queries the cohort count **equals** the segment-load
count — no MDX emits two segment loads with the same
fact/WHERE/measures signature but different group-bys. On this fixture
the `GROUPING SETS` batching optimisation would save **zero**
statements. The remaining 2–11 statements per MDX are overwhelmingly
dimension scans and per-hierarchy cardinality probes
(`COUNT(DISTINCT "the_year") AS "c" FROM "time_by_day"` and siblings),
which are not segment loads at all and are out of scope for the current
lever.

Cohort dumps (one row per cohort; `n` is the cohort population):

```
crossjoin
    n=1  fact="sales_fact_1997"  where="time_by_day"."the_year" = 1997
         measures=[sum("sales_fact_1997"."unit_sales") as "m0"]

topcount
    n=1  fact="sales_fact_1997"  where=<none>
         measures=[sum("sales_fact_1997"."store_sales") as "m0"]
    n=1  fact=(subq of sales_fact_1997)  where="product_department" in (...)
         measures=[sum("store_sales") as "m0"]

aggregate-measure
    n=1  fact=(subq of sales_fact_1997)  where="month_of_year" in (...)
         measures=[sum("store_sales") as "m0"]

agg-distinct-count-customers-levels
    n=1  fact=(subq)  where="country" = '<v>'
         measures=[count(distinct "customer_id") as "m0"]
    n=1  fact=(subq)  where="country" = '<v>' AND "state_province" in(...)
         measures=[count(distinct "customer_id") as "m0"]

non-empty-rows
    n=1  fact="sales_fact_1997"  where="time_by_day"."the_year" = 1997
         measures=[sum("sales_fact_1997"."unit_sales") as "m0"]
```

### 3.3 Scope caveat — where cohorts *would* fire

The absence of n>1 cohorts is a **property of the fixture/toggle**, not
the pipeline. `CompositeBatch` accretion inside
`FastBatchingCellReader.groupBatches` fires when a run produces a
detailed batch plus one or more summary batches that share its
aggregation key (same fact, same predicates, same measures, strictly
coarser group-by) — this is common when:

  * `UseAggregates=true` with agg tables present (summary batches roll
    up to different grains of the same fact);
  * MDX mixes multiple hierarchy levels from the same dimension in
    parallel axes (e.g. Product Family + Product Department on rows
    against the same slicer);
  * Drill-down UIs reuse sibling segments inside one execution.

The probed corpus does not stress these patterns much, and the
default `UseAggregates=false` path in `executeCold` suppresses them
further. **This is a gap in the spike corpus, not a reason to drop
strategy (c).**

## 4. Strategy decision — (c), with two work items

Chosen: **(c) — already-batched-in-GroupingSetsList**. The segment-load
primitive already accepts a multi-grouping-set input, and
`FastBatchingCellReader.groupBatches` already detects and accretes
cohorts into `CompositeBatch`/`GroupingSetsCollector(true)`. Going with
(a) submitter-side or (b) detector-side would duplicate logic that has
lived in Mondrian since 2007.

The two work items are on either side of the existing primitive and
neither of them is "add a batcher":

  * **Gate**: `shouldUseGroupingFunction()` demands that both the
    property and the dialect opt in. For Task 6 we need to flip the
    gate on — at minimum by overriding
    `supportsGroupingSets()` in `PostgreSqlDialect` (Postgres 9.5+ has
    native support) and opt-in-ing `EnableGroupingSets=true` for the
    Calcite backend. HSQLDB can keep returning false; the spike fixture
    will continue to exercise the single-grouping-set path.
  * **Translator**: `CalcitePlannerAdapters.fromSegmentLoad` currently
    throws `UnsupportedTranslation` for any multi-member
    `GroupingSetsList`. Task 7 needs to teach it to emit
    `GROUP BY GROUPING SETS ((...), (...), ...)` plus the
    `GROUPING(col)` expressions that `SegmentLoader.processData`
    relies on to demux rows back to per-grouping-set caches. Calcite's
    `RelBuilder#groupKey(ImmutableList<ImmutableBitSet>)` /
    `LogicalAggregate` with multiple group sets is the natural target.

Risk: the legacy emitter's shape for the `GROUPING(c)` bit ordering
is consumed verbatim by `SegmentLoader.processData` via
`GroupingSetsList.getGroupingBitKeyIndex()`. Task 7 must reproduce the
same column ordering so the demux still works. The existing Mondrian
SQL-builder at `src/main/java/mondrian/rolap/sql/SqlQueryBuilder.java`
is the reference.

## 5. Concrete next-step plan (informs Tasks 6 + 7)

**Task 6 — gate flip + extended cohort probe**

  1. Override `supportsGroupingSets()` in `PostgreSqlDialect` to
     return true (and note the 9.5+ minimum, which we already require
     for `DISTINCT ON` / filter aggregate patterns on the Calcite
     branch).
  2. Add a probe mode to `GroupingSetBatchProbeTest` that flips
     `EnableGroupingSets=true` when the running dialect supports it,
     and re-runs the 5 MDX under `UseAggregates=true` and a wider
     corpus slice (at minimum the 7 MvHit queries plus a
     two-level-same-dimension query) — to produce a non-zero cohort
     population and verify that `CompositeBatch.load` actually submits
     one multi-GS segment load.
  3. Extend the report with the with-gate numbers. Expect some
     queries to collapse 2-3 segment loads into 1.

**Task 7 — Calcite translator support for `GROUPING SETS`**

  1. Remove the `useGroupingSets()` early-throw at
     `CalcitePlannerAdapters.java:1701`.
  2. Build a `GroupingSetsList`-aware `aggregate(groupKey(List))`
     variant. Column ordering must match the legacy emitter so
     `SegmentLoader.processData` can read the `GROUPING(col)` bit
     indices without change.
  3. Add a targeted `SegmentLoadGroupingSetsTranslationTest` that
     pins the emitted SQL shape.
  4. Run the Task 6 probe again under `-Dmondrian.sqlemitter=calcite`;
     segment-load counts should match the legacy-with-gate numbers.

## 6. Blockers and surprises

  * **The brief's statement counts don't match the probe.** Brief
    said 11/5/8/7/6 SQL statements per MDX; we measured
    11/5/8/12/4. Two explanations:
      - `non-empty-rows` in our corpus is a small store-name-only
        variant (4 stmts). If the brief counted the full non-empty
        crossjoin variant, that would be in the 6+ range.
      - `agg-distinct-count-customers-levels` counts higher (12 vs
        7) because cold-cache via `executeCold` reflects every
        dimension-cardinality probe; the brief may have excluded
        these. This doesn't move the conclusion.
  * **Postgres dialect doesn't advertise `supportsGroupingSets`**,
    despite Postgres 9.5 supporting the construct natively since
    2016. This is a straightforward miss in the Mondrian base
    codebase that Task 6 must fix. (If Y.3's Postgres benchmark was
    with `EnableGroupingSets=true`, it would have silently been a
    no-op.)
  * **The reference corpus doesn't produce cohort populations.** Our
    five probed queries never trigger `CompositeBatch` even in the
    ideal world, because none of them request multiple grain levels
    of the same dimension in one shot. Tasks 6 and 7 need to probe a
    broader corpus to characterise the real-world cohort distribution
    before we bet on statement-count savings driving the perf delta.
    If the distribution is sparse, Phase 2 may save very few
    statements and the perf work should redirect to cutting
    per-statement Java overhead directly.
  * The Calcite translator has an explicit `UnsupportedTranslation`
    for multi-GS at
    `CalcitePlannerAdapters.java:1701`. That confirms Task 7 is
    blocking: we cannot turn on the gate for the Calcite backend
    until the translator learns to handle the multi-member input.
