# Calcite Equivalence Harness — Design

**Date:** 2026-04-19
**Branch:** `calcite-equivalence-harness`
**Status:** Design approved, ready for implementation

## Goal

Build a test harness that lets us safely evaluate integrating Apache Calcite into Mondrian by detecting any drift in query results or emitted SQL when Calcite is inserted into the pipeline. The harness is the prerequisite for every subsequent Calcite integration decision (SQL post-processing, RelBuilder rewrite, aggregate-matching, shared plan cache).

## Non-goals (this worktree)

- No actual Calcite optimization.
- No multi-backend matrix — H2 FoodMart only.
- No production-scope Calcite dependency (test scope only).
- No changes to Mondrian's SQL builder beyond a single `SqlInterceptor` SPI seam.

## Equivalence levels

Layered gates (recommendation from brainstorming): **MDX cell-set diff (outer) + SQL rowset diff (inner) + baseline-golden gate**. Each failure class has a distinct error so we know which layer drifted.

## Module layout

All harness code lives under test sources:

```
src/test/java/mondrian/test/calcite/
  EquivalenceHarness.java          # core runner
  SqlCapture.java                  # JDBC DataSource wrapper
  CalcitePassThrough.java          # no-op Calcite interceptor
  BaselineRecorder.java            # golden generator
  corpus/
    SmokeCorpus.java               # tier-1 MDX list
    AggregateCorpus.java           # tier-3 MDX list
  EquivalenceSmokeTest.java        # parameterized @Test per smoke MDX
  EquivalenceAggregateTest.java    # parameterized @Test per agg MDX
  BaselineRegenerationTest.java    # @Ignore by default; manual refresh
src/test/resources/calcite-harness/golden/<hash>.json
```

## Dependencies

Added to `pom.xml`, **test scope only**:

- `org.apache.calcite:calcite-core:1.41.0` (latest stable as of 2026-04-19)
- `org.apache.calcite:calcite-linq4j:1.41.0` (pinned for reproducibility)

Test-scope means the harness observes but never replaces the production SQL path.

## Production-code touchpoint (the single seam)

`mondrian.rolap.RolapUtil.executeQuery` — the central JDBC execution point. Add one SPI:

```java
public interface SqlInterceptor {
    String onSqlEmitted(String sql, Dialect dialect);  // default: identity
}
```

- Default: identity. Zero behavioural change when unregistered.
- Registered via system property `mondrian.sqlInterceptor` — opt-in, production untouched.

## Baseline (characterization) phase

`BaselineRecorder` runs against **unmodified Mondrian** (no Calcite in loop) on H2 FoodMart and writes golden artefacts per query:

```json
{
  "mdx": "...",
  "cellSet": "<TestContext.toString(result)>",
  "sqlExecutions": [
    { "seq": 0, "sql": "...", "rowset": [...], "rowCount": N, "checksum": "sha256:..." }
  ]
}
```

`SqlCapture` wraps the JDBC `DataSource` and intercepts every `PreparedStatement.executeQuery`, materializing and checksumming the rowset. This is richer than Mondrian's existing test assertions (which check cell-set text only) and catches drift in multi-phase queries (agg-table probes, native subqueries, drillthrough).

**Refresh policy:** `-Dharness.rebaseline=true` regenerates goldens; CI runs without it and fails on drift. Matches the existing `DiffRepository` idiom.

## MDX corpus — three tiers

1. **Smoke (~20 queries):** one per major MDX construct — basic select, calc members, named sets, NON EMPTY, crossjoin, time functions, drillthrough.
2. **Mondrian's own test queries:** scrape MDX strings from `BasicQueryTest`, `CompoundSlicerTest`, etc. — free coverage.
3. **Aggregate-table queries:** target `AggregationManager` and native evaluators — most likely drift points under Calcite.

## Calcite wiring — `CalcitePassThrough`

The no-op spike:

1. Parse emitted SQL with Calcite `SqlParser` using the matching dialect (map from Mondrian `Dialect` → Calcite dialect).
2. Validate against a Calcite schema built lazily from Mondrian's `DataSource` metadata.
3. Re-emit via `SqlPrettyWriter` with the same dialect.
4. Return the re-emitted SQL.

On parse failure → log + return original SQL (fail-open). The harness records both the original and round-tripped SQL for diffing.

## Diff runner — `EquivalenceHarness`

Per MDX in the corpus:

1. **Run A (classic):** no interceptor, capture cell set + SQL executions.
2. **Run B (Calcite round-trip):** interceptor = `CalcitePassThrough`, capture same.
3. **Outer gate:** diff cell sets. Identical → pass.
4. **Inner gate:** on cell-set drift, diff SQL executions pairwise (same count? same rowsets?). Report first mismatch with both SQLs + rowset diff.
5. **Baseline gate:** diff Run A against recorded golden. Run-A drift means the *baseline* is broken (dirty H2, env issue), **not** a Calcite bug — distinct error class.

## Failure classes

| Class | Meaning | Where to look |
|---|---|---|
| `BASELINE_DRIFT` | Run A differs from golden | Environment / Mondrian change unrelated to Calcite |
| `CELL_SET_DRIFT` | Run B cell set differs from Run A | MDX-level semantic divergence |
| `SQL_ROWSET_DRIFT` | Run B SQL rowset differs from Run A | Dialect / pushdown bug in Calcite round-trip |

## CI wiring

Added to `mvn test` behind profile `-Pcalcite-harness` (**default off** for this worktree). Flips to default-on after 2 weeks green.

## Observability

Each run writes `target/calcite-harness-report.html` — per-query pass/fail, cell-set diff, SQL side-by-side, rowset delta. Enables eyeball review when a drift is legitimate (e.g. Calcite reordering an unordered `ORDER BY`).

## "Done" criteria

1. Baseline recorded for smoke + aggregate corpus; `BaselineRecorder` deterministic (identical checksums across runs).
2. `CalcitePassThrough` round-trips every corpus SQL without parse failures. Failures triaged → fixed dialect mapping, or documented ticket.
3. `EquivalenceHarness` reports **zero drift** across the corpus. Any drift at this stage is a harness or dialect-fidelity bug — we want it now, not during real optimization.
4. **Mutation test:** temporarily modify `CalcitePassThrough` to swap `=` for `<>` in one WHERE clause. Harness must catch it and report `SQL_ROWSET_DRIFT` (or `CELL_SET_DRIFT`). Revert before merge. Proves the gates have teeth.
5. This design doc committed.

## Open items for next worktree (not this one)

- Pick the first real integration point: (1) SQL post-process, (2) RelBuilder rewrite, (3) agg/MV matching, (4) shared plan cache. Recommendation from brainstorming: start with (3).
- Promote Calcite from test-scope to compile-scope.
- Expand harness to multi-backend matrix (DuckDB, Postgres via Testcontainers).
- Run Sonatype audit on Calcite 1.41.0 before promoting scope.
