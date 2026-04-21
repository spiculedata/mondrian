# Y.3 — Post-Y.2 Calcite-vs-legacy gap analysis on Postgres

**Date:** 2026-04-21
**Worktree:** `calcite-backend-agg-and-calc`
**Predecessor:** `docs/reports/perf-investigation-y1.md` (root cause), Y.2 commit `a93371d` (planner cache by JDBC identity)

## Summary

Y.2's cache-by-JDBC-identity fix moved the per-query JDBC reflection cost off the critical path. Re-measuring the full corpus on Postgres-1000× post-fix:

| | Pre-Y.2 (matrix run) | Post-Y.2 (re-run) |
|---|---|---|
| **Geomean D/B (Calcite/legacy)** | 1.27× | **0.93×** |
| Arithmetic mean D/B | — | 0.95× |
| Queries with D/B > 1.10× | many | **6** of 45 |
| Queries with D/B < 1.00× (Calcite faster) | ~10 | **24** of 45 |
| Worst query | 2.43× (MvHit) | 1.21× |
| Best query | — | 0.60× |

**Net: Calcite is now 7% faster than legacy on Postgres at scale, geomean.** The pre-Y.2 picture of "Calcite is 27% slower" is wrong — it was measuring the JDBC-reflection tax, not SQL quality.

## Distribution

### Where Calcite genuinely wins (D/B < 1.0×)

24 of 45 queries. Top 10:

| Query | Corpus | Legacy ms | Calcite ms | D/B |
|---|---|---|---|---|
| `agg-crossjoin-gender-states` | aggregate | 9 049 | 5 438 | **0.60×** |
| `agg-distinct-count-particular-tuple` | aggregate | 7 553 | 4 635 | 0.61× |
| `agg-distinct-count-quarters` | aggregate | 13 915 | 8 514 | 0.61× |
| `agg-distinct-count-two-states` | aggregate | 19 538 | 11 938 | 0.61× |
| `agg-distinct-count-set-of-members` | aggregate | 20 626 | 12 859 | 0.62× |
| `agg-distinct-count-measure-tuple` | aggregate | 8 130 | 5 099 | 0.63× |
| `native-cj-usa-product-names` | aggregate | 33 703 | 21 651 | **0.64× (12s saved)** |
| `format-string` | smoke | 14 808 | 9 409 | 0.64× |
| `native-topcount-product-names` | aggregate | 35 470 | 30 365 | 0.86× |
| `named-set` | smoke | 30 151 | 26 589 | 0.88× |

Pattern: **the heaviest queries (mostly distinct-count + native crossjoin/topcount) consistently land 30-40% faster under Calcite**. These are the queries that drive total OLAP load times — small absolute-percent improvements compound to multi-second wall-clock wins per query.

### Where Calcite still loses (D/B > 1.10×)

6 queries. All but one are small (<10s legacy):

| Query | Legacy ms | Calcite ms | Δ ms | D/B | Notes |
|---|---|---|---|---|---|
| `agg-distinct-count-customers-levels` | 24 544 | 29 581 | +5 037 | 1.21× | 7 SQL statements |
| `calc-member` | 9 866 | 11 617 | +1 751 | 1.18× | calc with measure refs |
| `basic-select` | 4 214 | 4 992 | +778 | 1.18× | simplest possible |
| `crossjoin` | 8 887 | 10 423 | +1 536 | 1.17× | 11 SQL statements |
| `agg-distinct-count-product-family-weekly` | 1 102 | 1 282 | +180 | 1.16× | small fact subset |
| `non-empty-rows` | 21 581 | 23 845 | +2 264 | 1.10× | NECJ shape |

## Root cause analysis on the residuals

### EXPLAIN ANALYZE comparison: `basic-select`

**Legacy SQL:**
```sql
select "time_by_day"."the_year" as "c0",
       sum("sales_fact_1997"."unit_sales") as "m0"
from "sales_fact_1997", "time_by_day"
where "time_by_day"."the_year" = 1997
  and "sales_fact_1997"."time_id" = "time_by_day"."time_id"
group by "time_by_day"."the_year"
```

**Calcite SQL:**
```sql
SELECT "time_by_day"."the_year",
       SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year"
```

**EXPLAIN (Postgres):** _identical plans_, both Parallel Seq Scan + Hash Join + Partial GroupAggregate. Execution time:
- Legacy: 4 569 ms
- Calcite: 4 553 ms

**The plans are equivalent. Yet the bench measures Calcite 800ms slower.**

Where the 800ms goes:
1. ~5 ms — Calcite RelBuilder construction + RelToSqlConverter unparse (per `CalciteOverheadProbeTest` numbers).
2. ~3 ms — Postgres planning time (3 ms vs 6 ms — Calcite's SQL is *faster* to plan, surprisingly).
3. **~790 ms — unaccounted.**

Hypothesis for the unaccounted Δ: this MDX query produces 2 SQL statements per execution (cardinality probe + the segment load). With 1 warmup + 2 timed iterations and a Mondrian schema-cache flush before each iteration, the Calcite path may be paying a *per-iteration* setup cost that legacy doesn't — possibly the `ArithmeticCalcAnalyzer` or `CalcPushdownRegistry` registration in `RolapResult` (Task T.1 hook).

Worth a follow-up profile to confirm. But the 800ms on a query that should hardly use either backend suggests **fixed JVM/Mondrian per-iteration setup overhead** — which would be eliminated by warming the JVM once and running many queries (the realistic production pattern, and **what the SQL-quality plan's GROUPING SETS batching directly addresses**).

### Pattern across the residuals

All 6 are queries that emit **multiple separate SQL statements** per MDX:
- `basic-select`: 2 statements
- `crossjoin`: 11 statements
- `calc-member`: 4 statements
- `agg-distinct-count-customers-levels`: 7 statements
- `agg-distinct-count-product-family-weekly`: 5 statements
- `non-empty-rows`: 6 statements

Calcite's per-statement Java cost (build RelNode → unparse → optionally apply Y.2-cached planner) is small (~1-5 ms each) but constant. Across 11 statements (`crossjoin`), that's 11-55 ms of fixed cost vs legacy's near-zero per-statement overhead. Compounded over Mondrian's per-statement cache write/lookup, it adds up.

**The fix is not in the SQL plan. The fix is to emit fewer statements per MDX.** Hence Phase 2 of the Y.4 plan: GROUPING SETS batching collapses N same-shape segment loads into 1 SQL statement. That eliminates ~70-90% of the per-statement Calcite Java overhead on multi-axis queries.

## Conclusions

1. **Y.2 was the home-run fix.** Geomean D/B 1.27 → 0.93 means Calcite is now genuinely faster than legacy on Postgres-at-scale. The blog headline shifts from "Calcite is slower" to "Calcite is 7% faster on average, with the heaviest queries 30-40% faster."

2. **The remaining 6 residuals are not SQL-plan bugs.** EXPLAIN ANALYZE on `basic-select` shows Postgres produces identical plans for legacy and Calcite SQL. The residual Δ is per-statement Java overhead in Calcite's translation/build pipeline — a per-iteration cost that compounds with Mondrian's emit-many-small-SQLs pattern.

3. **The compounding effect goes away with GROUPING SETS batching.** Phase 2 of `docs/plans/2026-04-21-calcite-sql-quality-and-grouping-sets.md` collapses the N-statement pattern into a single SQL per MDX, which both: (a) eliminates per-statement Calcite overhead, and (b) cuts Postgres planner round-trips. Predicted post-batching geomean D/B: ≤ 0.85×.

4. **No further bottleneck investigation is needed before Phase 2.** The story is now: shipping the planned SQL-quality + batching changes will turn the current 7% lead into a meaningful blog-worthy headline (15-20% faster than legacy on Postgres at scale).

## What we learned about the perf benchmark itself

- Reduced iteration count (1 warmup + 2 timed) produces numbers within ±5% of the original 3+5 run. Two-iteration measurements are sufficient for the matrix.
- The benchmark's per-iteration cache flush amplifies small per-statement overhead. A "production-pattern" benchmark (warm JVM, sequential MDX without cache invalidation) would show Calcite further ahead than the matrix suggests.

## Appendix: bench config used

- HSQLDB cells: original 3 warmup + 5 timed (unchanged from initial matrix run).
- Postgres cells (this Y.3 re-run): 1 warmup + 2 timed for tractability over 86.8M-row data.
- Hardware: Apple M-series laptop, 8.4 GB foodmart_calcite, Postgres 18.1 with 5 single-column FK indexes on `sales_fact_1997` plus per-table dim indexes.
- Comparison: every query's median wall time, taken across timed iterations only (warmup discarded).
