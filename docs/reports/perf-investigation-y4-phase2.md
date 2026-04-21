# Y.4 Phase 2 — outcome report

**Date:** 2026-04-21
**Worktree:** `calcite-backend-agg-and-calc`
**Predecessor:** `docs/reports/perf-investigation-y3.md`

## Summary

Phase 2 as planned (GROUPING SETS batching) was **not implementable** in a reasonable scope. Phase 2 pivot (SQL template cache) was **attempted and reverted** — the implementation regressed performance instead of improving it.

**Net: Phase 2 ships no perf change. Y.3's geomean D/B = 0.93× stands as the final Phase-2-era result.**

## What happened

### GROUPING SETS spike (Task 5 + Task 5b)

Two spike phases:
- Initial probe across 5 default-corpus queries → **zero batchable cohorts** (every segment load unique in shape).
- Extended probe with 4 engineered-to-cohort MDX + `UseAggregates=true` + `EnableGroupingSets=true` → **13.3% statement reduction ceiling** across an engineered-to-cohort corpus, with only one query (`cohort-hierarchical-sum`) hitting the 20% threshold the plan set as the "worth it" bar.

Compounding findings from the spike:
- Postgres dialect in Mondrian's `JdbcDialectImpl` defaults `supportsGroupingSets = false` despite Postgres 9.5+ native support. Would need upstream dialect rewrite.
- Calcite translator explicitly rejects multi-GS input (`CalcitePlannerAdapters.fromSegmentLoad:1701`). Would need translator extension.

Total effort to unlock GROUPING SETS: multi-week (dialect rewrites + translator extension). Expected payoff: ≤13% on statement count on a subset of queries — and zero on the default corpus. **Rejected.**

### SQL template cache pivot (Task 6)

Pivoted Phase 2 to per-statement Java cost reduction: cache the unparsed SQL string as a parameterized template, keyed on `PlannerRequest` structural hash. Implementation landed at commit `edec802`:
- `CalciteSqlTemplateCache` + `Template` class (literal detection via word-boundary matching).
- `PlannerRequest.structuralHash()` + `structurallyEqual()` + `literals()` helpers.
- Wired around `CalciteSqlPlanner.plan(req)`, with `planRel(req)` and capture mode bypassed.
- 9 unit tests all green. HSQLDB harness 44/44 both backends.

### Regression & revert

Post-cache perf re-run on Postgres (2026-04-21 00:24 + 00:57):

| metric | Y.3 (no cache) | Post-cache |
|---|---|---|
| geomean D/B | **0.93×** | **1.38×** |
| arith mean D/B | 0.95× | 1.41× |
| max D/B | 1.21× | 1.72× |
| queries < 1.00× | 24 / 45 | 7 / 45 |
| queries > 1.10× | 6 / 45 | 36 / 45 |

Calcite became **38% slower on average** with the cache enabled — a massive regression across almost every query. Top regressions included queries that had been near-parity or Calcite-wins in Y.3:
- `calc-non-pushable-ytd`: 1.72× (previously 1.31×)
- `calc-non-pushable-parent`: 1.70× (previously 1.31×)
- `agg-g-ms-pcat-family-gender-marital`: 1.66× (previously 2.30× → 1.01× after Y.2, now 1.66×)

Root-cause investigation was attempted but deferred. Likely culprits based on the data:
1. **Cache misses dominated.** `structuralHash` probably includes state that changes per Mondrian-query-iteration (e.g. `RolapStar`-derived column references), meaning every iteration saw a miss AND paid hash-compute overhead on top of the uncached planner cost.
2. **Template build overhead.** Building the parameterised template required a second RelBuilder+unparse pass with sentinel literals to locate them in the SQL. On miss, we're paying 2× the original work.
3. **Literal-substitution fallback rate unknown.** Ambiguous-literal-position fallback (the safety net) makes the cache store a no-op entry; subsequent calls re-run the planner AND pay cache lookup.

Without telemetry to distinguish these, the cleanest action was revert. Commit `b7a88c2` (amended to drop diagnostic instrumentation) removes the cache entirely. Harness stays 44/44 both backends. D/B returns to Y.3's 0.93× baseline.

## Conclusion

**Phase 2 was a wash.** Y.3's 0.93× geomean is the final perf result at the end of Phase 2. No code changes shipped.

Lessons:
- The GROUPING SETS design premise in the plan assumed Mondrian's segment-load flow naturally batches. It doesn't — or at least, the default property configuration kills the path. Fixing it upstream is multi-week work.
- Structural hash + literal substitution is harder than it looks. The stabilisation work (normalising instance identity out of the hash, telemetry to prove hit rate) needed more up-front design than a single-task subagent run.
- Y.2's JDBC-identity planner cache was the correct-shaped fix and it delivered. Chasing further perf within the same backend at the per-statement level hits diminishing returns.

The Y.4 plan's Phase 3 (VolcanoPlanner + MV registration) remains the next natural lever, but should be motivated by **feature-unlock** (cost-based MV selection, custom rules) rather than chased as a perf win. The current 0.93× geomean is already a blog-worthy headline.

## Files touched this phase

- `src/test/java/mondrian/test/calcite/GroupingSetBatchProbeTest.java` — diagnostic probe, kept.
- `docs/reports/perf-investigation-y4-phase2-spike.md` — spike findings, kept.
- `docs/reports/perf-investigation-y4-phase2.md` — this report.

Reverted:
- `CalciteSqlTemplateCache` + tests + `PlannerRequest` structural helpers + `CalciteSqlPlanner` cache wiring (commit `edec802` → reverted in `b7a88c2`).

## Harness gates

- HSQLDB legacy: 44/44 green.
- HSQLDB Calcite: 44/44 green.
- Postgres connectivity: green.
- Full Postgres perf re-run post-revert: not executed (would be identical to Y.3's measurements since revert brings us back to that state).
