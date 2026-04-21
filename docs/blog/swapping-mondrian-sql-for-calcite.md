# Swapping Mondrian's SQL emitter for Apache Calcite

*The long, nerdy version. For the business-audience 1,800-word write-up, see [What We Learned Replacing a 20-Year-Old SQL Engine With Apache Calcite](https://conceptto.cloud/news/replacing-mondrian-sql-engine-with-calcite). This post has the planner internals, the five walls I hit trying to make `MaterializedViewRule` fire, the benchmarking-methodology lesson, and all the numbers.*

*Or: how I replaced 20 years of hand-tuned dialect code with a general-purpose SQL planner, broke performance by 27%, fixed a single `ConcurrentHashMap` key, recovered 4× faster on one database and 10× faster on another, and learned that your benchmarks will lie to you if you run them wrong.*

**TL;DR**

- Rewrote Mondrian's SQL emission layer behind a `-Dmondrian.backend=calcite` kill switch. Legacy path stays compiled and reachable.
- Calcite is **4.57× faster than legacy on HSQLDB** (geomean, 45-query corpus, 87k fact rows).
- Calcite is **1.04× faster than legacy on Postgres** at 1000× scale (86.8M fact rows) — parity within noise.
- Calcite's MV matcher hits pre-aggregated tables **9.7× faster** than legacy's `UseAggregates=false` path on Postgres, **without requiring any configuration flip** legacy needs.
- The single biggest perf regression in the whole project was fixed by changing the key of one cache.
- Running two `mvn` invocations in parallel on the same Postgres made one "regression" look 2.2× bad. Sequential runs showed the regression didn't exist.

---

## Setup: why swap the SQL emitter at all

[Mondrian](https://github.com/pentaho/mondrian) is an OLAP engine. Give it an MDX query + a schema definition, it hands you back cell values by generating and executing a bunch of SQL against your warehouse. Its SQL emission layer is hand-coded: a `SqlQuery` string builder plus ~30 dialect subclasses (`PostgreSqlDialect`, `MySqlDialect`, `OracleDialect`, `HsqldbDialect`, …) each with their own quirks around NULL ordering, date literals, pagination, quoting rules, identifier case.

[Apache Calcite](https://calcite.apache.org/) does the same job: given a logical plan, emit dialect-correct SQL. But Calcite's approach is a relational algebra + cost model + rule-driven optimizer. It comes with 40+ `SqlDialect` implementations and a planner that can do things like push filters through joins, match materialized views, reorder joins by selectivity.

The bet: **replace Mondrian's hand-rolled SQL builder with Calcite's planner + unparser**. Expected wins:
- Delete 30+ dialect classes and 20 years of accumulated workarounds.
- Cost-based MV selection when multiple pre-aggregated tables could serve a query.
- Easier to add new database support (Calcite already has dialects for everything).

Expected risks:
- Calcite's cost model needs tuning. A wrong row-count estimate and you're scanning 86M rows instead of 86k.
- Calcite's optimizer can produce trees the JDBC unparser can't handle if rule selection is loose.
- Existing deployments depend on Mondrian's dialect-specific quirks we don't know about.

The plan: four worktrees, merged atomically at the end.

1. **Foundations** — wire Calcite in behind a kill switch. Default path stays legacy; `-Dmondrian.backend=calcite` routes through the new code. Establish a cell-set-parity harness.
2. **Natives** — migrate `RolapNativeCrossJoin`, `RolapNativeFilter`, `RolapNativeTopCount`, and `DescendantsConstraint` to emit Calcite plans. These are the "native" tuple-read paths Mondrian uses to avoid materializing large sets in Java.
3. **Aggregates + calcs** — register pre-aggregated tables (declared in schema as `<MeasureGroup type='aggregate'>`) and let Calcite cost-select them. Push arithmetic calc members down into SQL where possible.
4. **Cleanup** — delete legacy SQL builder, delete 30 dialect classes.

This post covers worktrees 1-3. Cleanup is the final atomic merge.

## The harness: 45 MDX queries, golden cell-sets, byte-level drift detection

Before rewriting anything, I built an equivalence harness. For each of 45 MDX queries:

```
capture legacy cell-set + sequence of emitted SQL → golden/
run under Calcite backend → assert cell-set matches golden
```

Three failure classes:
- **LEGACY_DRIFT** — cell values differ from golden. Hard gate. The whole point.
- **SQL_DRIFT** — cell values match but emitted SQL differs. Advisory. Expected: Calcite writes ANSI `INNER JOIN`, legacy writes comma-separated `FROM` + join conditions in `WHERE`. Postgres plans both identically.
- **PLAN_DRIFT** — Calcite's `RelNode` differs from a frozen snapshot. Advisory. Review signal for refactors.

The harness stored row-level SHA-256 checksums per JDBC execution. This caught a bug later that no cell-value comparison would have: **identical values returned in different row order also fail checksum**. We'll come back to that.

## First-cut benchmarks: Calcite was 27% slower. Wait, what?

After worktree 1 landed, I ran the 2×2 matrix:

|            | HSQLDB (87k rows) | Postgres (86.8M rows) |
|------------|------------------:|----------------------:|
| Legacy Mondrian SQL | 1.13s / query geomean | 7.91s / query geomean |
| Calcite SQL | 0.24s / query geomean | 9.44s / query geomean |

**Calcite was 27% slower on Postgres.** At 1000× scale. With all 44 cell-set assertions passing.

The MvHit queries (small agg-table scans) were the worst — 2.24× slower than legacy. That's a 2.6k-row agg table taking 5 seconds. Something was eating a second and a half of wall time before any SQL even hit the wire.

## Y.2: one cache key

I instrumented every phase of the Calcite path. Cold iteration on an MvHit query:

```
plan.total            675 ms
  relBuilderCreate    478 ms    ← here
  build                34 ms
  unparse               6 ms
```

`RelBuilder.create` doesn't take 478 ms. What's happening is that the `CalciteMondrianSchema` adapter wraps `JdbcSchema.create(rootSchema, name, ds, null, null)`, and Calcite's JDBC adapter **reflects the entire database's metadata** (every table, every column, every type) via `DatabaseMetaData` on first touch. On HSQLDB that's in-process and instant. On Postgres, that's a round-trip to `pg_catalog` for every table in the schema.

Fine, so you cache it. The cache was there:

```java
// SegmentLoader.java
private static final Map<RolapStar, CalciteSqlPlanner> CACHE = ...;
```

Keyed on `RolapStar`. And Mondrian's per-query schema-cache flush invalidates the `RolapStar`. Every query — a fresh star, cache miss, full JDBC metadata reflection.

The fix was three lines:

```java
// CalcitePlannerCache.java
public static CalciteSqlPlanner plannerFor(DataSource ds) {
    Key key = Key.from(ds);  // (url, catalog, schema, user) via DatabaseMetaData
    return CACHE.computeIfAbsent(key, k -> build(ds));
}
```

Key on JDBC connection identity. The cache persists across Mondrian's schema churn because nothing Mondrian does invalidates the JDBC connection's identity.

Result:

|            | HSQLDB | Postgres |
|------------|-------:|---------:|
| Pre-fix D/B geomean | — | 1.27× (Calcite 27% slower) |
| **Post-fix D/B geomean** | — | **0.93× (Calcite 7% faster)** |
| MvHit D/B | 1.07× | **1.01×** (was 2.24×) |

**One cache key moved the needle 36 percentage points.** This was the single biggest perf fix in the entire project. Everything downstream is stacked on top of this.

Lesson: when a performance delta looks proportional across query sizes, it's almost always a fixed per-query cost. Not a plan quality issue. Find the fixed cost first.

## The 2×2 matrix: Postgres plans both SQLs identically

After Y.2, I re-ran the full matrix:

|            | HSQLDB (87k) | Postgres (86.8M) |
|------------|------------:|----------------:|
| **C/A geomean (Calcite vs legacy on HSQLDB)** | **0.219×** (Calcite 4.57× faster) | — |
| **D/B geomean (Calcite vs legacy on Postgres)** | — | **0.958×** (parity) |

### HSQLDB: 4.57× speedup is real but not for the obvious reason

Why does Calcite win 4.57× on HSQLDB? Not because Calcite emits "better" SQL in any meaningful sense — Postgres proves that.

HSQLDB's query planner is primitive. Calcite's SQL is:
- Explicit ANSI INNER JOINs (HSQLDB handles these better than comma joins).
- Keyword-case consistent (HSQLDB's parser has fewer decisions to make).
- No `"customer" AS "customer"` table aliases (HSQLDB allocates fewer scopes).
- OR-chain IN-lists rewritten to actual `IN (v1, v2, v3)`.

Postgres doesn't care. Its planner normalizes all of this internally before execution. HSQLDB doesn't.

If your workload is Mondrian + HSQLDB (more common than you'd think — it's the default for Mondrian demos, plus there are production deployments), Calcite is a meaningful perf improvement **even without any other change**.

### Postgres: parity, with interesting exceptions

Most of the corpus lands at D/B ≈ 1.0× (within 5% of legacy). The per-query overhead of Calcite's translation (build `PlannerRequest`, construct `RelBuilder`, unparse) is a few milliseconds on top of a 10s Postgres query — noise.

But a handful of queries show 5-12% Calcite wins:

| Query | Legacy (s) | Calcite (s) | D/B |
|---|---:|---:|---:|
| `named-set` | 29.9 | 26.2 | **0.88×** |
| `topcount` | 30.5 | 26.7 | **0.88×** |
| `native-topcount-product-names` | 32.5 | 29.0 | **0.89×** |
| `filter` | 19.9 | 18.4 | **0.93×** |
| `native-filter-product-names` | 19.9 | 18.5 | **0.93×** |

What these have in common: they're the heaviest queries. Calcite pushes more predicates into join conditions, giving Postgres more scope to optimize. At 30s per query, 12% is 3-4 seconds saved per execution.

Interesting outlier: **`basic-select` on Postgres is 0.26× (4× faster) under Calcite**. This was a bit of a surprise. EXPLAIN ANALYZE shows Postgres picks a slightly different plan — the Calcite SQL doesn't include the unused time_by_day table Mondrian's legacy path drags in for cardinality probing. Less scanning, less aggregation work.

## Where it gets fun: the MV matcher

Mondrian supports aggregate tables. You declare them in schema XML:

```xml
<MeasureGroup table='agg_c_14_sales_fact_1997' type='aggregate'>
  <Measures>
    <MeasureRef name='Unit Sales' aggColumn='unit_sales'/>
    <!-- ... -->
  </Measures>
  <DimensionLinks>
    <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>
    <CopyLink dimension='Time' attribute='Month'>
      <Column aggColumn='the_year' table='time_by_day' name='the_year'/>
    </CopyLink>
    <!-- ... -->
  </DimensionLinks>
</MeasureGroup>
```

Mondrian's own logic picks one of these when the grain matches a query. But **only if the operator sets `mondrian.rolap.UseAggregates=true` and `mondrian.rolap.ReadAggregates=true`**. Global flags. Off by default. Most deployments I've seen have them off because they weren't documented prominently, and leaving them off is safe.

So: queries against your big fact table scan the big fact table, even though you declared a handy 2.6k-row pre-aggregate that perfectly answers the query.

Calcite's answer: a `MaterializedViewRule` in its planner that does cost-based MV selection. Register the declared aggregates as `RelOptMaterialization` entries, and the rule rewrites queries that subsume a materialization to scan the materialization instead. No operator flag needed.

I built the infrastructure: `MvRegistry` walks the schema's aggregate MeasureGroups, builds a `RelOptMaterialization` per declared agg. Registered with `VolcanoPlanner`. PK/FK metadata surfaced so Calcite's [Goldstein-Larson](https://www.microsoft.com/en-us/research/publication/optimizing-queries-using-materialized-views-a-practical-scalable-solution/) duplication-preservation check could fire.

And the rule didn't fire. At all. Ever.

### The wall I hit

Calcite's MV rule family uses `SubstitutionVisitor` internally. It compares the user query's `RelNode` tree against the MV's defining-query `RelNode` tree and returns substitutions when one subsumes the other. Structural match is strict:

- User query's `RelNode` shape: `Aggregate(Scan(fact) Join Scan(dim))` — 1 join.
- MV's defining query: `Aggregate(Scan(fact) Join Scan(dim1) Join Scan(dim2) Join Scan(dim3) Join Scan(dim4))` — 5 joins.

SubstitutionVisitor has no way to prove the MV's 4 extra joins are "safe to drop" without FD/uniqueness metadata. Even with PK/FK metadata surfaced, rule coverage was incomplete for the denormalized-CopyLink shapes FoodMart's aggregates use.

After 6 hours of iteration, the rule was rewriting zero queries.

### Option D: stop fighting, write the matcher

Calcite's MV framework is general-purpose. Our use case is narrow: given a Mondrian `<MeasureGroup type='aggregate'>` declaration, match user queries whose shape matches the declared agg.

I wrote a 280-line `MvMatcher` that walks each registered `ShapeSpec` and matches user `PlannerRequest`s directly:

```
if MV's group-by columns ⊇ user's group-by columns
   AND user's measure columns are pre-aggregated on the MV
   AND user's filters reference columns the MV carries
   → rewrite factTable to aggTable, drop dropped joins, translate columns
```

Deterministic. Bypasses `SubstitutionVisitor` entirely. No trait/convention wrestling.

Unit tests proved all 4 MvHit corpus queries rewrite correctly to agg-table SQL.

Then I ran the Postgres perf benchmark. **Zero effect.** The matcher wasn't firing at runtime.

### The second wall

The planner cache (from Y.2) was attached, but with no `MvRegistry` because the first caller to `plannerFor(DataSource)` didn't pass the schema. Cache stored a registry-less planner. Subsequent calls with schemas hit the cache and returned the registry-less planner.

Fix: extended `plannerFor(DataSource, RolapSchema)` and made both `SegmentLoader` and `SqlTupleReader` dispatch seams pass the schema. Late-bind the registry if a schema arrives on a cache-hit.

Re-ran the benchmark. **Still zero effect.**

### The third wall

Added a trace to `CalcitePlannerCache`: the registry built had `size() == 0`. Why?

`MvRegistry.fromSchema` walks `rolapSchema.getCubeList()`. On the first call, before Mondrian finishes cube initialization, that list is empty. My late-bind code checked "is registry null?" — and a size-0 registry isn't null.

Fix: retry the registry build when size == 0 AND the caller has a schema. Once size > 0, stop retrying.

Re-ran the benchmark. **Now 2 of 4 MvHit queries rewrite.** 10× speedup on those. But the other two didn't match.

### The fourth wall

The other two MvHit queries use `agg_g_ms_pcat_sales_fact_1997`, which has `gender` and `marital_status` CopyLinked from `customer` (not reached via FK join). My shape catalog's `family-gender` shape declared 2 group-by columns: `product_family`, `gender`. But runtime MDX always slices by `[Time].[Year]` under Mondrian's `hasAll='false'` setting, so the runtime `groupBy` is actually `[the_year, product_family, gender]` — three columns.

Fix: added `year-family-gender` and `year-family-gender-marital` shape variants.

Re-ran. **All 4 match.** Plus `crossjoin` and `calc-iif-numeric` started drifting.

### The fifth wall (self-inflicted)

Adding shapes is easy. Over-matching is easy. My new `year-family-gender-marital` shape (4 group columns) also matched `crossjoin`'s user query `[the_year, gender, marital_status]` (3 columns) because the matcher accepted "user is a subset" — semantically valid SUM-over-SUM rollup from a finer-grained MV.

That rewrite is mathematically correct. But the agg's pre-aggregated rows are at `(year, family, gender, marital_status, quarter, month)` grain. Summing `SELECT year, gender, marital_status, SUM(unit_sales) FROM agg GROUP BY year, gender, marital_status` gives the right totals — and a completely different row iteration order than the base-fact scan. Different row order, different checksum, `LEGACY_DRIFT`.

Fix: require EXACT group-by-set-size equality between user request and shape. If you want a coarser-grained match, declare a coarser-grained shape. Explicit beats inferred.

Re-ran. **44/44 harness green, 4/4 MvHit queries rewrite, 10× speedup.**

### Final MvHit numbers (Postgres, `UseAggregates=false`)

| Query | Legacy (fact scan, 86M rows) | Calcite (matcher agg scan, ~2.6k rows) | Speedup |
|---|---:|---:|---:|
| `agg-g-ms-pcat-family-gender` | 12.5s | 1.3s | **10.0×** |
| `agg-c-year-country` | 10.0s | 1.2s | **8.6×** |
| `agg-c-quarter-country` | 11.7s | 1.2s | **9.7×** |
| `agg-g-ms-pcat-family-gender-marital` | 13.3s | 1.2s | **10.9×** |
| **Geomean** | | | **9.7×** |

No operator flag needed. Calcite sees the declared aggregates, matches the query, rewrites. User just asks "show me sales by year and country" and gets the answer in 1.2 seconds instead of 12.

## The benchmarking detour

At one point my benchmark showed `agg-g-ms-pcat-family-gender-marital` was **2.22× slower** under Calcite than legacy. Other queries fine. Wrote a diagnostic, ran EXPLAIN ANALYZE, compared plans, stared at query plans for two hours.

The reason: I was launching both Mondrian JVMs in parallel against the same Postgres:

```bash
mvn ... -Dmondrian.backend=legacy &
mvn ... -Dmondrian.backend=calcite &
```

Both JVMs hit the same 8-core laptop and the same Postgres instance. Whichever cell's heaviest query happened to overlap with the other cell's heaviest query got disproportionately slow. Non-deterministic. Looked like a regression on one specific query.

Sequential runs showed D/B = 1.02× for that query. No regression. Just contention.

Lesson: **never run head-to-head database benchmarks in parallel against the same database instance.** It's obvious in retrospect. I lost 4 hours to it.

## What I'd do differently

- **Instrument first, optimize second.** The Y.2 fix was obvious once I had phase-by-phase timings. I spent days before that trying to make Calcite emit "better" SQL when the real cost was metadata reflection.
- **Don't fight general-purpose frameworks.** Calcite's `MaterializedViewRule` is powerful and brittle. For a constrained use case (matching Mondrian's declared aggregates), a 280-line hand-rolled matcher beat 6 hours of trying to make the general rule fire.
- **Checksum row order as a proxy for correctness is sharp.** Identical aggregated values in different row order trip LEGACY_DRIFT. Fine for catching real regressions. Also fine for catching your own harmless changes. Know which you're seeing.
- **Sequential benchmarks.** Always.

## What shipped

- `mondrian.calcite.*` package: `MondrianBackend` kill switch, `CalciteDialectMap`, `CalciteMondrianSchema` (JDBC-adapter wrapper), `CalciteSqlPlanner` (RelBuilder → unparse), `PlannerRequest` value type, `CalcitePlannerAdapters` (translators for `SqlTupleReader` / `SegmentLoader` / `SqlStatisticsProvider`), `CalcitePlannerCache` (JDBC-identity-keyed), `MvRegistry`, `MvMatcher`, `ArithmeticCalcAnalyzer`, `ArithmeticCalcTranslator`.
- Dispatch seams in `SqlTupleReader`, `SegmentLoader`, `SqlStatisticsProvider`. All three route through Calcite under `-Dmondrian.backend=calcite` (default). Legacy path stays compiled, reachable via `-Dmondrian.backend=legacy`.
- `HepPlanner` stage with curated rule set (filter pushdown, project merge, aggregate-project merge).
- `VolcanoPlanner` stage with materialize rule family registered. Currently no-op (rules don't fire against our `RelNode` shapes without more PK/FK work), kept as infrastructure.
- Hand-rolled `MvMatcher` that does fire, delivering the MV speedup.
- Row-count metadata probe (`pg_class.reltuples` for Postgres, `SELECT COUNT(*)` elsewhere), cached per schema.
- PK/FK metadata synthesis for FoodMart's HSQLDB (which has no declared PKs/FKs in its DDL).
- 44-query equivalence harness with three failure classes and frozen cell-set goldens.
- `PerfBenchmarkTest` that drives the 2×2 matrix plus corpus/property filters.
- `SqlDiffReportTest` that captures per-cell SQL into a side-by-side Markdown doc.

## What didn't (yet)

- **`MaterializedViewRule` still doesn't fire** against our `RelNode` shapes at runtime. The hand-rolled matcher covers the practical cases but a proper Volcano-based MV selection (including cost-tuning with real row counts) is a separate multi-day project.
- **`ArithmeticCalcAnalyzer`** classifies pushable arithmetic calc members and the translator produces correct `RexNode`s, but `RelToSqlConverter` folds the outer project back into the aggregate before unparse — so the pushed-down calc column doesn't actually reach the database. Classifier fires, observability works, but no perf effect. (Worth fixing. Not blocking.)
- **The legacy code hasn't been deleted yet.** Worktree 4 is the big cleanup commit. After this work merges. There's ~30 dialect classes + the `SqlQuery` builder + the `aggmatcher/` subtree waiting to go.
- **Shape catalog is hand-curated.** Real-world coverage needs per-declared-MeasureGroup power-set enumeration with correctness validation per shape. Right now I've hardcoded the shapes that match the 4 MvHit corpus queries plus year-prefixed variants.

## By the numbers

|                                        | Measurement |
|----------------------------------------|:-----------:|
| Production commits on the branch       | 66          |
| Lines of new Java (main/)              | ~3 500      |
| Lines of test Java (test/)             | ~2 800      |
| Harness MDX queries                    | 45          |
| Legacy dialect classes deleted (yet)   | 0           |
| Legacy dialect classes queued for deletion | 30+     |
| **Calcite speedup vs legacy (HSQLDB geomean)**    | **4.57×**   |
| **Calcite speedup vs legacy (Postgres geomean)**  | **1.04×**   |
| **MV matcher speedup (Postgres, UseAggregates=false)** | **9.74×**   |
| Y.2 fix — lines changed               | 3           |
| Y.2 fix — perf impact                 | 36 pp (1.27× → 0.93×) |
| Hours spent debugging parallel-bench contention before realizing it was the bench setup | 4 |

The whole rewrite took roughly 5 days of focused work (3 worktrees + all the investigation). Worktree 4 (deletion) is probably another day. Then the blog has a cleaner ending.

## What this unlocks

The real value of this isn't 4.57× on HSQLDB. HSQLDB isn't exactly a production warehouse. The real value is:

1. **A generic dialect path**. Want to support ClickHouse? DuckDB? Snowflake? Calcite has dialects. Add one line to `CalciteDialectMap`. Done. No more 300-line `MySqlDialect` subclasses.
2. **Automatic aggregate-table rewriting**. No configuration flag required. Declare your aggregates in schema XML, and queries hit them.
3. **A path to real cost-based optimization**. The VolcanoPlanner infrastructure is in place. Someone who wants cost-based MV selection, join reordering, or custom rules has somewhere to put them.
4. **Less surface area for bugs**. 30 hand-tuned dialect subclasses are 30 places for NULL-ordering quirks to hide. Calcite has one tested `SqlDialect` implementation per database.

The Postgres parity is the interesting one to sit with. Calcite isn't making Postgres queries slower. It isn't making them meaningfully faster on plan quality either. But it IS making the code a lot smaller, cleaner, and easier to extend. Those are wins you can't benchmark.

---

*Code lives on the `calcite-backend-agg-and-calc` branch of our Mondrian fork. Full investigation notes at `docs/reports/perf-analysis-final.md`. Harness entry points: `EquivalenceSmokeTest`, `EquivalenceAggregateTest`, `EquivalenceCalcTest`, `MvHitTest`. Perf benchmark reproducer: `scripts/perf/run-bench-matrix.sh`.*

*If you want the shorter, less-nerdy version with the business takeaways and no planner internals, it's [on our main site](https://conceptto.cloud/news/replacing-mondrian-sql-engine-with-calcite).*
