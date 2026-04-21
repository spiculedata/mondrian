# Y.2 perf benchmark — Fix #1 (planner cache keyed on JDBC identity)

Follow-up to `docs/reports/perf-investigation-y1.md`. Y.1 identified that
`SegmentLoader.CALCITE_PLANNER_CACHE` was keyed on `RolapStar`; Mondrian's
per-query schema-cache flush invalidated the `RolapStar`, so the warm
`CalciteMondrianSchema` (holding reflected JDBC metadata in a
`LazyReference`) was thrown away on every query. On Postgres this cost
~1.5 s of `DatabaseMetaData.getColumns()` round-trips per query.

Y.2 replaces the three per-class planner caches
(`SegmentLoader`, `SqlTupleReader`, `SqlStatisticsProvider`) with a
single `mondrian.calcite.CalcitePlannerCache` keyed on JDBC connection
identity: `(url, catalog, schema, user)` read once via
`DatabaseMetaData`. The cache now survives `RolapStar` churn —
metadata reflection happens once per JVM per physical database.

## `CalcitePlannerCache` shape

```java
package mondrian.calcite;

public final class CalcitePlannerCache {
    public static CalciteSqlPlanner plannerFor(DataSource ds);
    public static void clear();           // test seam
    public static int size();             // test seam

    static final class Key {
        final String url, catalog, schema, user;
        static Key from(DataSource ds);   // probes DatabaseMetaData once
        // equals/hashCode over the four fields
    }
}
```

Probe tolerates any `Throwable` from the four metadata reads (older
drivers, the SqlCapture proxy surfacing `AbstractMethodError` as
`UndeclaredThrowableException`, etc.) — any probe field that throws is
substituted with the empty string so the key still hashes
deterministically. If `ds.getConnection()` fails outright the key falls
back to DataSource identity.

## `CalciteOverheadProbeTest` — before vs after, Postgres, MvHit (agg-c-year-country)

Before (Y.1 report, per-iteration `clearCalcitePlannerCache()`):

| iter | total    | plan.total | relBuilderCreate |
|------|----------|------------|------------------|
| 0    | 675 ms   | 675 ms     | 478 ms           |
| 1    | —        | 2 ms       | —                |

After (Y.2, cache cleared once up-front, schema-flush still happens per
iteration via `executeCold`):

| iter | total      | plan.total | relBuilderCreate |
|------|------------|------------|------------------|
| 0    | 5446 ms    | 1128 ms    | 307 ms           |
| 1    | 1144 ms    | 21 ms      | 7.9 ms           |
| 2    | 1144 ms    | 21 ms      | 7.9 ms           |
| 3    | 1139 ms    | 18 ms      | 6.5 ms           |
| 4    | 1159 ms    | 18 ms      | 6.6 ms           |

(The probe end-to-end wall includes DB fetches that Y.1's number did
not — Y.1's 675 ms was per-query plan-only. The important signal is
that **after iter 0, `plan.total` drops from ~1128 ms to ~20 ms** and
stays there. Metadata reflection is paid exactly once.)

## `PerfBenchmarkTest` MvHit slice — Postgres, 3+3 iterations

Before (Y.1 `docs/reports/perf-benchmark-2x2.md`): MvHit D/B = 2.24×
geomean (range 2.16-2.43×).

After (Y.2), median of 3 timed iterations:

| query                                  | legacy (ms) | calcite (ms) | D/B    |
|----------------------------------------|-------------|--------------|--------|
| agg-g-ms-pcat-family-gender            | 1131        | 1148         | 1.015× |
| agg-c-year-country                     | 1145        | 1158         | 1.011× |
| agg-c-quarter-country                  | 1203        | 1190         | 0.988× |
| agg-g-ms-pcat-family-gender-marital    | 1147        | 1184         | 1.033× |

MvHit D/B geomean ≈ **1.012×**, down from 2.24×. The ~1.1-second
per-query overhead the Calcite cell used to pay on Postgres has
collapsed to near-zero.

## Harness gates (HSQLDB)

| backend | tests | result |
|---------|-------|--------|
| legacy  | 44/44 | green  |
| calcite | 44/44 | green  |

(`EquivalenceSmokeTest` 20 + `EquivalenceHarnessTest` 3 +
`EquivalenceCalcTest` 10 + `EquivalenceAggregateTest` 11 = 44.)

## Notes / surprises

- **Probe-level cache-clear semantics changed.** `CalciteOverheadProbeTest`
  previously cleared the cache between iterations to simulate Mondrian's
  schema flush. Post-Y.2 that's the wrong simulation — Mondrian flushes
  the `RolapStar`, not Calcite's JDBC reflection. The probe now clears
  once up-front so iter 0 is cold and iter 1+ can show steady-state.
  `PerfBenchmarkTest` got the same treatment.
- **SqlCapture `Connection` proxy throws `AbstractMethodError` through
  `method.invoke`.** Wrapped in `UndeclaredThrowableException`.
  `CalcitePlannerCache.Key.safeProbe` swallows any `Throwable` for each
  of the four metadata reads and substitutes an empty string, so the
  probe keeps working under the test harness's `Connection` proxy.
- **Multi-DataSource case.** Two different DataSources pointing at the
  same physical database (same url/catalog/schema/user) will now
  correctly share a planner — previously they would have each built
  their own. Two DataSources pointing at different databases are
  distinguished by the key. The `DataSource` reference itself is
  intentionally not in the key.
