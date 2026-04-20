# Calcite Backend Rewrite — Design

**Date:** 2026-04-19
**Status:** Design validated, ready for implementation
**Predecessor:** [Calcite Equivalence Harness](2026-04-19-calcite-equivalence-harness-design.md) (merged into `main`)

## Premise

No external users depend on this Mondrian version. The harness is in place. The pre-requisite fixture work is merged. Go all-in: replace Mondrian's SQL-generation layer with Apache Calcite, keep the MDX engine intact, push arithmetic calc members down to SQL.

## Scope (locked)

- **Replace** all SQL generation with Calcite `RelBuilder`-driven planning.
- **Replace** hand-coded `mondrian.spi.impl.*Dialect` subclasses with Calcite `SqlDialect` catalogue entries behind a thin mapper.
- **Replace** `mondrian.rolap.aggmatcher.DefaultRules` + `*Recognizer` chain with Calcite `RelOptMaterialization` registration (`MaterializedViewRule`).
- **Push down** arithmetic-only calc members (`+ - * /`, numeric `IIf`, `CoalesceEmpty`, numeric literals, base-measure refs) to SQL at plan time.
- **Keep** MDX parser, Fun* function library, `RolapEvaluator`, `RolapCube`/`RolapSchema` metadata, cache infrastructure, XMLA, olap4j — everything above the SQL emission layer.

## Non-goals

- Full MDX → SQL translation (non-arithmetic calcs stay in Java).
- Multi-backend matrix (HSQLDB only for this work; Postgres/DuckDB etc. later).
- Fixing the 111 failures / 58 errors in pre-existing Mondrian tests unrelated to SQL generation.
- Backward compatibility with a `classic-Mondrian` runtime mode post-merge.

## Execution strategy

Big-bang rewrite on a long-lived branch. Four worktrees over the rewrite; one atomic merge at the end. Harness checkpoints within the branch give incremental signal — we don't wait until month 3 to find out the planner can't express something.

**Worktrees (in order):**

1. `calcite-backend-foundations` — `CalciteSqlPlanner`, `CalciteMondrianSchema`, `CalciteDialectMap`, `SqlTupleReader`, `RolapAggregationManager`. Ends with checkpoint #4 (first end-to-end MDX runs on Calcite).
2. `calcite-backend-natives` — `RolapNative*` migrations. Ends with smoke corpus executing (~20/31).
3. `calcite-backend-agg-and-calc` — `MvRegistry`, agg-table → MV translation; `ArithmeticCalcAnalyzer` + pushdown; drillthrough. Ends with 31/31 corpus + calc-equivalence corpus green.
4. `calcite-backend-cleanup` — delete legacy SQL builder, dialect classes, aggmatcher. Final commit. Verify 87 legacy test classes still green.

Rebase each worktree weekly onto `main` to avoid > 1 month of drift.

## Done criteria (locked)

1. All 31 harness corpus queries (20 smoke + 11 aggregate) pass `EquivalenceHarness` against archived legacy goldens.
2. New calc-equivalence corpus (6–10 queries, added in worktree #3) 100% green.
3. All 87 currently-green Mondrian test classes (per `/tmp/foodmart-fixture-verify.log`) remain green.
4. Pre-existing 111 failures / 58 errors catalogued. Not regressed; not fixed as part of this work.
5. Legacy code deleted. No references to `SqlQuery`, `SqlQueryBuilder`, `DefaultRecognizer`, `ExplicitRecognizer`, any `*Dialect` class.

## Architecture

### Pipeline

```
MDX text
  → Mondrian parser (UNCHANGED)
  → RolapConnection / RolapResult (UNCHANGED — top half)
  → RolapEvaluator / Fun* calcs (UNCHANGED except for arithmetic push-down hook)
  → [NEW] CalciteSqlPlanner
      - builds RelNode tree from cube request + constraints + pushable calc members
      - runs Calcite planner (RBO + CBO, MaterializedViewRule included)
      - emits SQL via Calcite's JDBC adapter + matched SqlDialect
  → JDBC (via existing RolapUtil.executeQuery + SqlInterceptor seam)
```

### Package layout

**New production:**
```
mondrian/calcite/
  CalciteSqlPlanner.java          entry: request → SQL
  CalciteMondrianSchema.java      RolapCube → RelOptSchema adapter
  CalciteDialectMap.java          Mondrian Dialect → Calcite SqlDialect
  ArithmeticCalcAnalyzer.java     pushable-vs-not classifier
  ArithmeticCalcTranslator.java   calc → RexNode
  MvRegistry.java                 agg tables → RelOptMaterialization
  rel/                            Mondrian-specific RelNode subclasses if needed
```

**Repurposed (same FQN, new internals):**
```
mondrian/rolap/
  SqlTupleReader.java             API preserved; calls planner
  RolapAggregationManager.java    API preserved; probes via planner
  RolapNativeCrossJoin / Filter / TopCount / Set
  RolapUtil.java                  executeQuery + SqlInterceptor seam unchanged
```

**Deleted (in the final cleanup commit):**
```
mondrian/rolap/sql/                 except SqlInterceptor.java
mondrian/rolap/aggmatcher/          entire subtree
mondrian/spi/impl/*Dialect.java     all ~30 dialect classes
mondrian/rolap/RolapNativeSql.java
```

### Dependency change

`pom.xml`: Calcite moves **test scope → compile scope**. Artifacts: `calcite-core`, `calcite-linq4j`, `avatica-core` (transitive). No `calcite-server`, no `calcite-plus`, no per-adapter modules yet.

## Cutover checkpoints

| # | Subsystem | Cumulative harness coverage |
|---|---|---|
| 1 | `CalciteSqlPlanner` + `CalciteMondrianSchema` core | 0/31 (unit tests only) |
| 2 | `CalciteDialectMap` | 0/31 |
| 3 | `SqlTupleReader` → Calcite | 0/31 |
| 4 | `RolapAggregationManager` → Calcite | **~5/31 — first end-to-end** |
| 5 | `RolapNative*` → Calcite | ~20/31 |
| 6 | Agg-table → `MvRegistry` | ~25/31 |
| 7 | `ArithmeticCalcAnalyzer` + pushdown | 31/31 + calc-equivalence corpus |
| 8 | Drillthrough | 31/31 + legacy drill tests still green |
| 9 | Delete legacy code | 31/31, no refs |
| 10 | Done-criterion verification | merge gate |

Checkpoint #4 is the decisive milestone. Before it: writing code on faith. After: the harness is the co-pilot.

## Arithmetic calc-member push-down

**Pushable** iff every node in the expression tree is one of:
- Measure ref to a base measure present in (or addable to) the query's SELECT list.
- Arithmetic `+ - * /`, unary minus.
- Numeric literal.
- `IIf(cond, then, else)` with numeric comparison and pushable branches.
- `CoalesceEmpty(a, b)` with both sides pushable.
- Parens / associativity.

**Not pushable** (stays in Java evaluator): anything referencing `Member` / `Tuple` / `Set` / `Hierarchy`, any dimensional navigation (`.Parent`, `Ancestor`, `Descendants`, `ParallelPeriod`, `YTD`, `Rank`, `Order`, `TopCount`), UDFs, or calcs transitively depending on non-pushable calcs.

**Flow:**
1. `RolapEvaluator` collects active calc members.
2. `ArithmeticCalcAnalyzer.classify(calc) -> {PUSHABLE, NOT_PUSHABLE}` (transitive).
3. Pushable calcs become extra columns on a `LogicalProject` above the cell-reading aggregation. `ArithmeticCalcTranslator` walks the MDX expression tree, emits `RexNode`.
4. Non-pushable calcs flow through the evaluator as today — now reading a SQL-materialized row instead of computing Java-side.
5. Mondrian's segment cache keys pushed calcs alongside base measures.

**Edge cases:**
- Divide-by-zero → `CASE WHEN b = 0 THEN NULL ELSE a/b END` at translate time. Preserves Mondrian's "empty cell on x/0" behaviour.
- Decimal precision → coerce SQL-returned DECIMAL back to Java `double` so cell-set assertions against legacy goldens match.

## Agg-table → materialized-view translation

**Schema XML stays identical.** `<AggName>`, `<AggPattern>`, `<AggFactCount>`, `<AggMeasure>`, `<AggLevel>` — unchanged. Only the thing built from them changes: `AggStar` becomes `RelOptMaterialization`.

**Schema-load flow:**
1. `RolapSchema` parses XML as today.
2. `MvRegistry.fromSchema(schema)` walks agg-table list, builds one `RelOptMaterialization` per agg table (explicit + pattern-matched).
3. Registry attached to per-schema `CalciteSqlPlanner`.
4. At plan time, `MaterializedViewRule` rewrites the query to scan the MV when it subsumes — no special code path.

**Gains over today:**
- Cost-based MV selection across multiple candidates (today: first-match).
- Subsumption matching — a finer-grain MV can answer coarser-grain queries via rollup.
- `EXPLAIN` surfaces why an MV didn't match.

## Harness evolution

**Legacy goldens archived.** Before worktree #1 starts, re-record the full corpus against current Mondrian one last time, commit to `src/test/resources/calcite-harness/golden-legacy/`. Frozen — never regenerated. Represents "known-correct pre-rewrite truth."

**Gate renames:**
- `BASELINE_DRIFT` → `LEGACY_DRIFT`. Same semantics, clearer name post-rewrite.

**New gates added:**
1. **Calc-equivalence corpus** (`CalcCorpus.java`). 6–10 MDX queries exercising arithmetic calcs. Cell-set parity against legacy goldens.
2. **Plan-snapshot gate.** `RelOptUtil.toString` per corpus query → `golden-plans/<name>.plan`. New `PLAN_DRIFT` failure class. Rebaselined with `-Dharness.replan=true`. Drift is a **review signal**, not necessarily a failure — the reviewer decides if the plan change was intended.
3. **MV-hit assertions.** Targeted `MvHitTest` — for each known-covering agg table, assert the plan scans the MV not the base fact. 3–4 assertions. Catches silent MV-miss regressions — Mondrian's historical weak spot.

**Harness stays HSQLDB-only for this work.** Multi-backend matrix is a future worktree.

## Risk & rollback

### Anticipated hazards

| Risk | Mitigation |
|---|---|
| Calcite can't express role-based row-level security predicates | Extend planner with a custom rule, or emit raw `SqlNode`. Never fall back to string concat. |
| Outer joins on degenerate dimensions lose semantics | Custom `MondrianJoin` RelNode subclass if needed. |
| `<InlineTable>` schema elements | Translate at schema-load to `LogicalValues`. |
| Calcite reorders SQL into something HSQLDB 1.8 rejects | Plan-snapshot gate surfaces early; `CalciteDialectMap` can tune writer config per dialect. |
| Performance regression at high cardinality | `HarnessPerfTest`: fail if any corpus query runs > 3× slower than legacy. Advisory, not correctness. |
| Branch diverges from `main` | Rebase weekly; never let drift exceed a month. |

### The `-Dmondrian.backend=legacy\|calcite` kill switch

Added in worktree #1. Default `calcite`. Flipping to `legacy` routes through still-present `SqlQuery` code. Lets reviewers bisect during code review. Deleted with the rest of the legacy code in the final cleanup commit.

### Deletion discipline

**No legacy file gets deleted until every checkpoint passes.** Legacy code stays compiled, unreferenced, for the entire cutover. Final commit in worktree #4: `chore: remove legacy SQL builder, dialects, and aggmatcher` — single commit, cleanly revertable.

### Rollback paths

| Scenario | Action |
|---|---|
| Single corpus query drifts | Debug translator; exclude query with ticket if genuinely intentional behaviour change. |
| Subsystem stalls | Keep that subsystem on legacy `SqlQuery`; merge the rest. Scope reduction allowed. |
| Calcite fundamentally unsuitable | Branch abandoned. Harness + legacy goldens still on `main`. Time lost, nothing else. |

## Success definition

Single atomic merge of ~20 commits from four worktrees + the deletion commit + a design-doc appendix capturing real pitfalls hit. Harness green (31 corpus + calc-equivalence corpus + MV hits). 87 legacy test classes green. That's done.
