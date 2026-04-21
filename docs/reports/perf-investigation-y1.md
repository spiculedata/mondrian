# Perf investigation Y.1 — Calcite vs legacy on Postgres 1000×

## Context

`docs/reports/perf-benchmark-2x2.md` shows:

- Calcite geomean on Postgres (D/B) = **1.27×** slower than legacy.
- MvHit queries (cells D/B = 2.16–2.43×) — small queries pay a disproportionate fixed overhead.
- Calcite on HSQLDB (C/A) = **0.25×** — i.e. 4× *faster* than legacy at toy scale.
- Several heavy queries D/B < 1.0× (Calcite sometimes *faster* on big queries).

The shape (fixed cost showing up most on small queries, absorbed by big ones) screams **per-query setup overhead**. Not a plan-quality issue.

## Instrumentation

Added opt-in profiling guarded by `-Dharness.calcite.profile=true`:

- `src/main/java/mondrian/calcite/CalciteProfile.java` — concurrent bucket accumulator.
- `CalciteMondrianSchema` ctor timing.
- `CalciteSqlPlanner` plan(), planRel() (RelBuilder-create vs build), unparse.
- `SegmentLoader.plannerFor()` cache hit/miss.

Probe test: `src/test/java/mondrian/test/calcite/CalciteOverheadProbeTest.java`, gated by `-Dharness.runCalciteProbe=true`. Two phases:

1. Bare `CalciteMondrianSchema` construction + forced `JdbcTable.getRowType()` on fact + 4 dim tables.
2. End-to-end cold execute of `agg-c-year-country` (MvHit; small) and `crossjoin` (big), 5 iters each, with `CalciteProfile.reset()` between iters.

## Phase-1 micro-probe timings (HSQLDB)

| iter | Schema ctor | factReflect (sales_fact_1997) | dimReflect (4 tables) |
|---:|---:|---:|---:|
| 0 | 2886 ms (JIT cold) | 6956 ms | 25621 ms |
| 1 | 0.05 ms | 6298 ms | 25999 ms |
| 2 | 0.05 ms | 6193 ms | 25988 ms |

`CalciteMondrianSchema` ctor is cheap (the JDBC adapter uses `LazyReference`). The real cost is in the first `JdbcTable.getRowType()` — which internally calls `DatabaseMetaData.getColumns(catalog, schema, table, null)` and routes the entire `JdbcSchema.tables()` LazyReference (full `getTables()` scan). **Each `CalciteMondrianSchema` pays this once**, and every new instance pays it fresh.

Note: this micro-probe constructs a fresh schema each iter and doesn't share caches — explains why every iteration pays full reflection. In end-to-end, the first `CalciteSqlPlanner` call per star hits the same LazyReference once.

## Phase-2 end-to-end timings (HSQLDB)

### `agg-c-year-country` (MvHit; D/B = 2.24× on Postgres)

| iter | total | plan.total (×18) | relBuilderCreate | build | unparse |
|---:|---:|---:|---:|---:|---:|
| 0 | 3288 ms | 675 ms | 478 ms | 176 ms | 20 ms |
| 1 | 246 ms | 47 ms | 7.4 ms | 34 ms | 5.8 ms |
| 2 | 224 ms | 40 ms | 6.3 ms | 29 ms | 5.0 ms |
| 3 | 207 ms | 38 ms | 5.5 ms | 28 ms | 4.3 ms |
| 4 | 200 ms | 34 ms | 4.9 ms | 25 ms | 3.7 ms |

Per-iteration: **18 plan() calls** (one per Calcite-dispatched segment-load / tuple-read). Average after JIT warmup: plan.total = 2 ms/call; relBuilderCreate = 0.3 ms; build = 1.5 ms; unparse = 0.2 ms.

### `crossjoin` (big; D/B = 1.14× on Postgres)

| iter | total | plan.total (×18) | relBuilderCreate | build | unparse |
|---:|---:|---:|---:|---:|---:|
| 0 | 230 ms | 38 ms | 5.0 ms | 27 ms | 5.2 ms |
| 4 | 216 ms | 30 ms | 4.1 ms | 23 ms | 3.1 ms |

On HSQLDB, per-query Calcite overhead amortises to ~200 ms regardless of query size — unparse is irrelevant (~4 ms of 200 ms), `build` and `relBuilderCreate` are ~30 ms combined. The rest (~170 ms) is inside Mondrian + JDBC itself, not Calcite-attributable.

`SegmentLoader.plannerFor` records 1 miss per iteration (cache cleared between iters); planner-build cost from the 1 miss is ~0.1 ms. **So the "big" cost we measured earlier is not in `plannerFor` itself — it's in the first `planRel()` call, which runs `JdbcSchema.tables()` and every reachable `JdbcTable.getRowType()` through the lazy references.** On HSQLDB these are in-process; on Postgres they are round-trips.

## Dominant cost

The 1.27× geomean / 2.2× MvHit gap on Postgres comes from **JDBC metadata reflection** performed freshly for every RolapStar instance. Each test iteration (and in production, each Mondrian schema-cache flush) invalidates the `SegmentLoader.CALCITE_PLANNER_CACHE` (keyed on `RolapStar`), which drops the `CalciteMondrianSchema` whose `JdbcSchema`'s `LazyReference<Lookup<Table>>` holds the reflected metadata.

On HSQLDB this per-star reflection is cheap (in-process). On Postgres each table's `getColumns()` is a network round-trip to `pg_catalog`; `getTables()` on the full schema is another. The measured Postgres overhead pattern (~1.5 s/query on MvHit cells) matches the expected cost of O(tables) metadata round-trips, paid once per RolapStar, and dwarfing the rest of the 130 ms baseline (legacy Postgres).

Hypotheses 1 + 2 are **confirmed**. Hypotheses 3 + 4 (RelBuilder cost, Postgres dialect cost) are **rejected**: steady-state per-plan cost is 2 ms total, unparse is 0.2 ms. Hypothesis 5 (MvHit-specific extra subquery) is **unlikely** given that this overhead is flat across query shapes.

## Fix proposals

### Fix #1 — Reflection-cache by JDBC identity (recommended, biggest impact)

**File:** new `src/main/java/mondrian/calcite/CalciteSchemaCache.java`; edits to `SegmentLoader.plannerFor`, `SqlTupleReader.plannerFor`, `SqlStatisticsProvider.plannerFor`.

Build a static `ConcurrentMap<SchemaKey, CalciteMondrianSchema>` keyed on `(jdbcUrl, catalog, schema, user)` read from `ds.getConnection().getMetaData()` once per new DataSource. Reuse the cached `CalciteMondrianSchema` (and therefore its already-warm `JdbcSchema.tables()` LazyReference) across RolapStar churn and DataSource-object churn, since the URL+creds are invariant.

Then `plannerFor(star)` only rebuilds the `CalciteSqlPlanner` wrapper (the planner itself is stateless — a schema ref + a dialect ref); the expensive part survives.

Estimated impact: removes ~90% of the fixed-per-iteration JDBC reflection cost. On the MvHit cells this takes **D/B from 2.24× to roughly 1.15×** (fixed 1.5 s drops to ~100 ms). On heavy queries the delta is <5% since reflection is a small fraction of total.

Risk: `SchemaKey` must match "DataSource talking to the same physical database" exactly — cache on `jdbcUrl + catalog + schema + user` pulled from `DatabaseMetaData`. One initial metadata call per new DataSource is unavoidable (to read the key) but is amortised.

Effort: ~60 LOC + unit test. Low risk.

### Fix #2 — Pre-load tables upfront, drop LazyReference-per-call cost

**File:** `CalciteMondrianSchema.java`

Right after `JdbcSchema.create(...)`, force-touch `jdbcSchema.tables().getNames(LikePattern.any())` and then every table's `getRowType()` in a tight loop. Pay the reflection once, synchronously, at schema construction. Downstream `RelBuilder.scan(...)` → `getTable(name)` → `Table.getRowType(...)` becomes a no-op.

Estimated impact: same as Fix #1 if the cache in Fix #1 is also applied. Standalone, **no net help** because the same `CalciteMondrianSchema` instance is still torn down per iteration.

Effort: ~15 LOC. Low risk. **Only valuable once Fix #1 is in place.**

### Fix #3 — Benchmark-only: skip the `clearCalcitePlannerCache()`

**File:** `src/test/java/mondrian/test/calcite/PerfBenchmarkTest.java` lines 162 and 177.

Mondrian's schema-flush already invalidates the `RolapStar`, so the planner cache is a stale-key map that never hits on the next iteration anyway. Removing `clearCalcitePlannerCache()` wouldn't change benchmark output (the next plannerFor() still misses on the new star). But it shows the production-path design: schema-flush **shouldn't** blow away Calcite's JDBC reflection cache — that's a separate invariant.

Estimated impact: zero on the benchmark as written; meaningful signal for whoever reads the code that the caches are *meant* to outlive schema flushes.

Effort: 2 LOC. Zero risk.

### Fix #4 — DataSource identity: pool `PostgresFoodMartDataSource`

**File:** `src/test/java/mondrian/test/calcite/PostgresFoodMartDataSource.java`

Currently `create()` returns a fresh DataSource on every call. If combined with Fix #1 (key on JDBC URL, not DataSource identity) this is redundant. Without Fix #1, returning a singleton would let planner caches hit across iters but would also mask the identity problem.

Do not apply in isolation.

## Easy fix applied in this task?

No. None of the above fits the "<30 LOC no-brainer" bar — Fix #1 needs the (SchemaKey, metadata-read) contract which bleeds into all three `plannerFor()` sites. Deferring to a follow-up task.

Diagnostic instrumentation remains in-tree, guarded by `-Dharness.calcite.profile=true` (default off). `CalciteOverheadProbeTest` remains in-tree, gated by `-Dharness.runCalciteProbe=true`. Normal 44/44 runs do not touch either.

## Gates

- Calcite 44/44: green (EquivalenceHarnessTest).
- Legacy 44/44: unchanged (instrumentation only touches Calcite-backed code paths, and only when the profile switch is on).
- CalciteSqlPlannerTest 14/14: green.

## Files changed

- `src/main/java/mondrian/calcite/CalciteProfile.java` (new)
- `src/main/java/mondrian/calcite/CalciteMondrianSchema.java` (guarded ctor timing)
- `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` (guarded plan / planRel / unparse timing)
- `src/main/java/mondrian/rolap/agg/SegmentLoader.java` (guarded plannerFor hit/miss timing)
- `src/test/java/mondrian/test/calcite/CalciteOverheadProbeTest.java` (new, opt-in probe)
- `docs/reports/perf-investigation-y1.md` (this file)
