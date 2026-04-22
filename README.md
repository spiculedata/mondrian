# Mondrian — Spicule fork with Calcite SQL backend

Mondrian is the open-source OLAP engine that powers analytics dashboards by translating MDX queries into SQL against a relational warehouse. This Spicule fork keeps the original Mondrian semantics intact and adds an **Apache Calcite-based SQL generation layer** alongside the legacy hand-rolled SQL builder.

Forked from [OSBI/mondrian](https://github.com/OSBI/mondrian) (itself a fork of [pentaho/mondrian](https://github.com/pentaho/mondrian)). Full pre-fork history lives upstream.

License: Eclipse Public License v1.0 — see `LICENSE.html`.

## Why Calcite?

Legacy Mondrian hand-codes SQL across ~30 dialect subclasses. Apache Calcite is a general-purpose SQL planner with 40+ dialects and modern optimiser machinery. Swapping Mondrian's emitter for Calcite unlocks:

- **Automatic aggregate-table rewriting.** Declared `<MeasureGroup type="aggregate">` rollups get used **without** flipping Mondrian's `UseAggregates` / `ReadAggregates` globals. ~10× wins on queries that match a declared aggregate.
- **Cleaner SQL on less-clever planners.** HSQLDB benchmarks ~5× faster because Calcite emits ANSI joins + IN-lists that HSQLDB plans better than Mondrian's comma-join SQL.
- **Parity on modern warehouses.** On Postgres at 86.8M-row scale, Calcite-generated SQL and legacy SQL produce equivalent plans — same execution time, same results. No regression to ship this.
- **Single-line dialect adds.** New database backends (DuckDB, ClickHouse, Snowflake) already live in Calcite — wiring them is one dialect-mapping line, not a 300-line dialect subclass.

## Measured performance (2026-04-22 benchmark)

45-query MDX corpus, 1 warmup + 3 timed iterations, median reported.

| Workload | Calcite vs Legacy |
|---|---|
| HSQLDB (~87k rows) | **4.80× faster** |
| Postgres (86.8M rows, 1000×) | **1.01× (parity)** |
| Declared aggregate hits (Postgres, `UseAggregates=false`) | **9.6× faster** |

See [`docs/reports/perf-analysis-2026-04-22-postmerge.md`](docs/reports/perf-analysis-2026-04-22-postmerge.md) for per-query numbers and methodology.

## Runtime flags

All flags are `-D` system properties; defaults are production-safe.

| Flag | Default | What it does |
|---|---|---|
| `mondrian.backend` | `calcite` | Pick the SQL emitter. Set to `legacy` to route through the original Mondrian SQL builder — useful as a kill switch while shadow-evaluating the new path. |
| `mondrian.calcite.mvMatch` | `true` | Hand-rolled MV matcher that rewrites segment-load requests onto declared aggregate tables when the query shape matches. Runs even when Mondrian's `UseAggregates` is off — this is the main "aggregates just work" win. |
| `mondrian.calcite.volcano` | `true` | Use Calcite's `VolcanoPlanner` to apply a curated rule set during SQL generation. When false, falls back to the `HepPlanner`-only path. |
| `mondrian.calcite.calcConsume` | `false` | **Experimental.** When on, arithmetic calc members (e.g. `SUM(x) / SUM(y)`) are emitted as SQL expressions in the SELECT list instead of being recomputed in Java. Off by default because the SegmentLoader path that consumes the computed column is still behind a profiler-required investigation — see "Known issues" below. |
| `mondrian.calcite.mvMaxSubsetSize` | `4` | Size cap on power-set enumeration of aggregate-table shapes. Each declared aggregate generates candidate shapes for the MV matcher; this bounds combinatorial blowup. |
| `mondrian.calcite.trace` | `false` | Dumps per-request matcher + SQL capture to stderr. Use for debugging MV rewrites. |

## Quick start

```bash
# Default — Calcite backend, matcher on:
mvn test -Dmondrian.backend=calcite

# Compare against legacy:
mvn test -Dmondrian.backend=legacy

# Profile with matcher tracing:
mvn test -Dmondrian.backend=calcite -Dmondrian.calcite.trace=true
```

## How the aggregate-matching works

Mondrian schemas declare aggregate tables via `<MeasureGroup type="aggregate">`. Each such MG is described by:

- Its **base fact table** (e.g. `sales_fact_1997`)
- A list of **copy-linked columns** (denormalised columns copied into the agg table, e.g. `the_year`, `product_family`)
- A list of **foreign-key-linked dimensions** reachable via `ForeignKeyLink`

On schema load, `MvRegistry.fromSchema` enumerates power-set subsets of the copy-linked columns (up to `mvMaxSubsetSize`), adds hand-curated FK-reachable shapes, dedupes overlapping shapes (smaller row count wins), and builds a shape catalog.

At query time, `MvMatcher.match` compares every `PlannerRequest` (a segment-load's {groupBy, measures}) against the catalog via `GroupColKey` O(1) lookup. On match, the request is rewritten to scan the aggregate instead of the fact table — typically a 1000× row reduction on 1000×-scale FoodMart.

This runs **independently of** Mondrian's `RolapGalaxy.findAgg` code path — no configuration flip required.

## Building

```bash
mvn clean package -DskipTests                    # build the jar
mvn test                                         # run unit tests (excludes slow harnesses)
mvn -Pcalcite-harness test                       # run the Calcite equivalence harness
```

Custom dependencies (eigenbase-*, olap4j-spicule-*, mondrian-data-foodmart-hsql) that aren't on Maven Central are vendored in-tree at `lib/repo/` and resolved via a `file://` repository defined in `pom.xml`. No external repo configuration needed.

CI: GitHub Actions builds on every push to `main` and publishes a JAR to GitHub Packages. See `.github/workflows/build.yml`.

## Known issues

**Calc pushdown SegmentLoader consume path (commit `439438c`).** The SegmentLoader widening that would let the SQL-computed calc value replace the Java-side calc-member evaluation caused an unexplained 2× regression across non-calc Postgres queries (`slicer-where`, `time-fn`, `topcount`). Empirical bisect isolated it to that one commit; static code reading doesn't explain it. The commit lives on branch `calcite-calc-pushdown-sql` pending a profiler session. `mondrian.calcite.calcConsume` stays off by default until this is resolved.

**Calcite's own `MaterializedViewRule` doesn't fire on Mondrian-generated plans.** The cost-based MV selection infrastructure is wired but the rule engine's internal substitution logic requires exact structural matching that Mondrian's generated plans don't satisfy. The hand-rolled `MvMatcher` covers the practical cases; Volcano-driven MV selection is future work (see `docs/plans/2026-04-21-volcano-mv-selection.md`).

## Further reading

- **Blog (short):** ["What We Learned Replacing a 20-Year-Old SQL Engine With Apache Calcite"](docs/blog/swapping-mondrian-sql-for-calcite.md)
- **Engineering report:** [`docs/reports/perf-analysis-2026-04-22-postmerge.md`](docs/reports/perf-analysis-2026-04-22-postmerge.md)
- **Design docs:** [`docs/plans/`](docs/plans/) (calcite-backend-rewrite-design, MV matcher, shape catalog, calc pushdown)
- **Upstream Mondrian docs:** [`doc/install.html`](doc/install.html), [mondrian.pentaho.com](http://mondrian.pentaho.com) (legacy)
