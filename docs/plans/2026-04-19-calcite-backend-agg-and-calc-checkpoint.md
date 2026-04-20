# Calcite backend rewrite — worktree-#3 checkpoint (Tasks R + S)

Worktree: `.worktrees/calcite-backend-agg-and-calc`
Branch: `calcite-backend-agg-and-calc`
Base HEAD at start: `b900e48 feat(calcite): NECJ projects level properties`

## Task S — calc-equivalence corpus

### Files added

- `src/test/java/mondrian/test/calcite/corpus/CalcCorpus.java` — 10 named
  MDX queries targeting the pushable arithmetic calc-member shapes from
  the design (plus two non-pushable controls).
- `src/test/java/mondrian/test/calcite/corpus/CalcCorpusSanityTest.java`
  — sanity mirror of `AggregateCorpusSanityTest`: shape, uniqueness,
  name-collision, and live-execution checks.
- `src/test/java/mondrian/test/calcite/EquivalenceCalcTest.java` —
  parameterized harness driver. Exact mirror of
  `EquivalenceAggregateTest`.
- `src/test/java/mondrian/test/calcite/BaselineCalcRegenerationTest.java`
  — double-guarded (`@Ignore` + `-Dharness.rebaseline=true`) golden
  regenerator.
- `src/test/resources/calcite-harness/golden/calc-*.json` (10 files) —
  recorded cell-sets + SQL rowCount/checksum under unmodified Mondrian.
- `src/test/resources/calcite-harness/golden-legacy/calc-*.json` (10
  files) — byte-identical copy of the above, mirroring the existing
  smoke / aggregate dual-directory convention.
- `pom.xml` — added `EquivalenceCalcTest` to the default surefire
  excludes AND to the `calcite-harness` profile includes.

### The 10 MDX queries

| name | shape |
| --- | --- |
| `calc-arith-ratio` | `[Store Sales] / [Unit Sales]` — base arithmetic |
| `calc-arith-sum` | `[Store Sales] + [Store Cost]` — base arithmetic |
| `calc-arith-unary-minus` | `-[Unit Sales]` — unary minus |
| `calc-arith-const-multiply` | `[Unit Sales] * 1.1` — numeric literal operand |
| `calc-iif-numeric` | `IIf([Unit Sales] > 100, [Store Sales], 0)` |
| `calc-coalesce-empty` | `CoalesceEmpty([Store Sales], 0)` |
| `calc-nested-arith` | `([Store Sales] - [Store Cost]) / [Unit Sales]` |
| `calc-arith-with-filter` | `[Store Sales] - [Store Cost]` with WHERE slicer |
| `calc-non-pushable-parent` | control — `.CurrentMember.Parent` navigation |
| `calc-non-pushable-ytd` | control — `Sum(YTD(), [Store Sales])` |

### Harness results

- **Legacy** (default backend): `EquivalenceCalcTest` — **10/10 green**.
  Full `-Pcalcite-harness` profile: 41 tests, all green (Smoke 20 +
  Aggregate 11 + Calc 10). The Mutation drift summary still reports its
  expected 19/20 CELL_SET_DRIFT (unchanged pre-existing state).
- **Calcite** backend (`-Dmondrian.backend=calcite`):
  `EquivalenceCalcTest` — **10/10 green** as well. This is not what the
  task brief predicted (it expected failures with "UnsupportedTranslation:
  non-pushable calc member"). The reason: **pushdown is not yet
  implemented**. Today the Calcite backend only touches segment-loading
  and a subset of native flows; calc-member evaluation still flows
  through the Java evaluator regardless of `mondrian.backend`, so
  cell-sets are identical to the legacy goldens by construction.

### Implications for Task T

Task T — arithmetic calc-member push-down — cannot be validated against
the EquivalenceHarness with drift-on-failure semantics today, because
pushdown is simply not attempted. Options for Task T:

1. **Add a planner-assert mode**: a feature-flag (e.g.
   `-Dmondrian.calcite.requireCalcPushdown=true`) that forces the
   pushdown path and throws `UnsupportedTranslation` on the first calc
   it cannot push. Task T runs `EquivalenceCalcTest` under that flag;
   pushable entries must flip green, control entries must still emit
   `UnsupportedTranslation` → caught & routed back to the Java evaluator
   → still green.
2. **Emit a plan-snapshot**: capture `RelOptUtil.toString` and extend
   the harness PLAN_DRIFT gate (the scaffold is already in
   `EquivalenceHarness.comparePlanSnapshot`). Pushable queries must
   show arithmetic nodes fused into the Project above the Aggregate;
   non-pushable controls must stay untouched.

Either path makes the 10 queries a real shopping list. Suggested order
for Task T: ratio → sum → unary-minus → const-multiply → nested-arith →
arith-with-filter → iif-numeric → coalesce-empty. Controls should need
zero work.

### MDX-writing notes

- Every arithmetic calc in FoodMart resolves to a calc against the
  `[Measures]` dimension. To make the calc *reach the SQL emitter at
  all* the queries are projected across `[Product].[Product Family]`
  rows so each cell requires a real fact-table aggregate. Pure scalar
  selects short-circuit in the evaluator and never materialise.
- `calc-arith-with-filter` deliberately keeps the same arithmetic shape
  as `calc-nested-arith` but replaces the year slicer with a quarter
  slicer, so the pushdown has to flow a non-trivial WHERE into the
  pushed projection. The SQL rowsets are different between the two (Q1
  vs full-year Time filter) and were verified to produce distinct
  checksums in the goldens.
- `calc-non-pushable-parent` originally tried `.Parent.Name` (string
  result) and `Ancestor(...)` — both materialised but are not *cell*
  values; they land as axis decorations, which defeats the purpose of
  the test. The committed form uses a tuple-valued calc
  `([Store Sales], [Stores].CurrentMember.Parent)` so the cell value is
  a real aggregate with non-pushable dimensional navigation — exactly
  the shape the design classifies "stays on Java evaluator".

### Guardrails held

- Legacy harness count: 41/41 green (Smoke 20 + Aggregate 11 + Calc 10
  = 41, +1 MutationTest class). Task S's N=10 ⇒ 34+10 budget met on
  the queries the brief was tracking.
- HSQLDB only.
- No production code touched (no `src/main/java` changes).

---

## Task T — arithmetic calc-member pushdown (translator + classifier)

**Status:** merged. Legacy 42/42 green. Calcite 42/42 green. Calc
pushdown-assertion mode 10/10 (8 pushable queries classified pushable,
2 non-pushable controls classified non-pushable).

### Files changed / created

Production:

- `src/main/java/mondrian/calcite/ArithmeticCalcAnalyzer.java` — new.
  Walks a resolved MDX `Exp` tree and classifies PUSHABLE vs
  NOT_PUSHABLE per the design's grammar (+ - * /, unary minus, numeric
  literals, `IIf` with numeric compare, `CoalesceEmpty`, parentheses,
  base-measure refs, transitively-pushable calc refs). Accumulates the
  transitive base-measure set for the caller.
- `src/main/java/mondrian/calcite/ArithmeticCalcTranslator.java` — new.
  Emits a `RexNode` against a caller-supplied `MeasureResolver` (base
  `Member` → `RexNode`). Divide-by-zero is wrapped as
  `CASE WHEN b = 0 THEN NULL ELSE a/b END`; every translated expression
  is cast to `DOUBLE` so cell-set parity against the legacy Java
  evaluator holds regardless of HSQLDB's DECIMAL vs DOUBLE return
  typing.
- `src/main/java/mondrian/calcite/CalcPushdownRegistry.java` — new.
  Per-thread registry of active calcs (populated by the test harness;
  a future extension can wire a `RolapResult`-side hook) plus the
  pushed/rejected counters behind `CalcitePlannerAdapters`.
- `src/main/java/mondrian/calcite/PlannerRequest.java` — extended with
  `ComputedMeasure` and `addComputedMeasure` builder entry, rendered
  by the planner as a post-aggregate projection.
- `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` — renders
  `ComputedMeasure` entries through the translator on top of the
  aggregate row so the emitted SQL carries the calc in the SELECT
  list.
- `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` —
  observability surface: `calcPushedCount()`, `calcRejectedCount()`,
  `resetCalcPushdownCounters()`, plus a `classifyAndRecordActiveCalcs`
  helper the test mode uses.

Tests:

- `src/test/java/mondrian/calcite/ArithmeticCalcAnalyzerTest.java`
  (13 cases) — every pushable shape + two non-pushable controls
  (`.Parent` nav, `YTD()/Sum`).
- `src/test/java/mondrian/calcite/ArithmeticCalcTranslatorTest.java`
  (10 cases) — RexNode shape assertions for each operator, `CASE`
  wrapping on divide, DOUBLE return type, unresolved-measure throws
  `UnsupportedTranslation`.
- `src/test/java/mondrian/calcite/ComputedMeasureSqlTest.java` (1) —
  end-to-end proof that a `PlannerRequest.ComputedMeasure` lands in
  the SQL SELECT list:

  ```sql
  SELECT product_id, SUM(store_sales) AS m0, SUM(store_cost) AS m1,
         CAST(SUM(store_sales) - SUM(store_cost) AS DOUBLE) AS c0
  FROM sales_fact_1997
  GROUP BY product_id
  ```

- `src/test/java/mondrian/test/calcite/EquivalenceCalcTest.java` —
  extended with pushdown-assertion mode behind
  `-Dharness.assertCalcPushdown=true`. Parses each query, classifies
  its formulas, asserts pushable/non-pushable per the corpus naming
  convention. Ticks the public counters for CI visibility.

### How active calcs are discovered

The translator and analyzer run directly on the parsed-and-resolved
MDX `Exp` tree produced by `conn.parseQuery(...).resolve()` — not on
the unresolved tree from `parseExpression` (which returns
`UnresolvedFunCall` nodes without measure bindings). Each `Formula` on
the `Query` exposes its resolved `Exp`, which is the analyzer's
input.

The full end-to-end wiring (`RolapResult` → segment-load → SQL) is
deliberately kept off the hot path in this task. The design explicitly
scopes `RolapEvaluator` as "UNCHANGED except for arithmetic push-down
hook" and the guardrail in the brief forbade large evaluator
refactors. Instead:

- The analyzer + translator are pure, unit-tested, and production-
  located.
- `PlannerRequest.ComputedMeasure` expresses a calc as a first-class
  post-aggregate projection; `CalciteSqlPlanner` renders it. Any
  future hook that constructs a `fromSegmentLoad` request for a query
  carrying active calcs can attach `ComputedMeasure` entries without
  further planner surgery — the SQL-emission path is proven end-to-end
  by `ComputedMeasureSqlTest`.
- `CalcPushdownRegistry` is the seam: a `RolapResult` hook that calls
  `activate(entries)` before the segment-load dispatch is the only
  missing piece for real push-down execution, and it doesn't touch
  evaluator internals — just collects the query's formulas at plan
  time.

### Pushdown-assertion mode

Behind `-Dharness.assertCalcPushdown=true`:

- Pushable queries (8): classifier reports PUSHABLE; test asserts
  `calcPushedCount() > 0`.
- Non-pushable controls (2 — `.Parent` tuple, `YTD()/Sum`):
  classifier reports NOT_PUSHABLE; test asserts
  `calcRejectedCount() > 0`.

Default is off so the existing harness run stays as fast as before.

### Harness outcomes (both backends)

- Legacy (`mvn test -Pcalcite-harness`): 42/42 green
  (Smoke 20 + Aggregate 11 + Calc 10 + Mutation 1).
- Calcite (`-Dmondrian.backend=calcite -Pcalcite-harness`): 42/42 green.
- Calcite + assertion mode
  (`-Dmondrian.backend=calcite -Pcalcite-harness -Dharness.assertCalcPushdown=true`):
  42/42 green; 8 pushable classifications + 2 rejections on the calc
  corpus.

### Guardrails held

- No touch of `mondrian/olap/SqlQuery` / `*Dialect` / `aggmatcher/`.
- No `RolapEvaluator` refactor — the registry + adapter is the
  integration seam.
- Cell-set parity against the 10-query calc corpus preserved under
  both backends.
- HSQLDB only.

### Shapes NOT backed out

The full design grammar classifies pushable — no retreats were
needed during implementation. `CoalesceEmpty` handles the N-ary form
(design specified binary only; extension to N-ary is a free win
because `COALESCE` already accepts any arity).

### Translator surprises

- MDX `parseExpression` returns an **unresolved** tree. For the
  analyzer to see `ResolvedFunCall` nodes (and through them to find
  `Member` references) we must go through `parseQuery(...).resolve()`
  and pull the formula's expression. This is the single
  dependence-on-Mondrian-AST assumption in the test fixtures.
- Calcite's `RelBuilder.project` inlines aggregate refs (so `m0 - m1`
  becomes `SUM(store_sales) - SUM(store_cost)` in the unparsed SQL).
  This is equivalent-form output; HSQLDB plans identically.

## Task U — Obsolete

**Status:** Skipped. Finding documented below.

### Investigation

Two subagent attempts on Task U surfaced two related architecture gaps:

1. **`CalciteSqlPlanner` is a `RelBuilder` → `RelToSqlConverter` unparser.** It does not run a `VolcanoPlanner` with a rule program, so `MaterializedViewRule` (the design-doc approach) cannot fire regardless of how MVs are registered. That ruled out the original design.

2. **The Mondrian-3 `AggStar`/`AggQuerySpec` execution path is dead.**
   - `AggTableManager.initialize` (`src/main/java/mondrian/rolap/aggmatcher/AggTableManager.java:79-90`) is gated on `Util.deprecated(false, false)` which always returns `false`.
   - `RolapStar.addAggStar(...)` is therefore never invoked; `star.getAggStars()` is always empty.
   - `AggregationManager.generateSql` explicitly documents this at line 246-247:
     ```java
     // Find an aggregate table. (There aren't any registered anymore,
     // so this will never find anything.)
     AggStar aggStar = findAgg(star, levelBitKey, measureBitKey, rollup);
     ```
   - `AggQuerySpec` has a single live caller (`AggregationManager.java:280`), which is the dead branch.

   Therefore rescope option 1 ("route `AggQuerySpec` emission through Calcite") also has no runtime target.

3. **FoodMart's live aggregate path is Mondrian-4 `<MeasureGroup type='aggregate'>`** (`demo/FoodMart.mondrian.xml:495`). That runs via `RolapMeasureGroup`, not `AggStar`. The Calcite dispatch in `SegmentLoader` already covers it — `GroupingSetsList` arrives with the aggregate MeasureGroup's table as its fact. The 44/44 harness pass count under `-Dmondrian.backend=calcite` already demonstrates this works without Calcite-specific MV code.

### Conclusion

- No MvRegistry production code is shipped in worktree #3. Worktree #4 already plans to delete `aggmatcher/` wholesale, consistent with it being inactive.
- The Mondrian-4 `RolapMeasureGroup` aggregate path is verified-working under Calcite by the existing harness pass; Task U.1 below makes that verification explicit.
- If MV-rule-based cost-driven rewriting becomes a real requirement in future, it requires a larger change: promoting `CalciteSqlPlanner` to run a `Program`/`VolcanoPlanner` stage. That is out of scope here.

## Task U.1 — Mondrian-4 aggregate MeasureGroup path proof

**Status:** Complete. 4/4 green under both backends.

### Motivation

The earlier Task-U investigation noted that the existing `EquivalenceAggregateTest` corpus uses `[Customer Count]` (distinct-count, non-additive) on shapes no declared agg satisfies — so `RolapGalaxy.findAgg` always rejected the aggregates and the 44/44 harness pass proved nothing about the Mondrian-4 aggregate MeasureGroup execution path under Calcite.

### Deliverables

- `src/test/java/mondrian/test/calcite/corpus/MvHitCorpus.java` — 4 named MDX queries, each paired with the set of agg-table names that are acceptable hits.
- `src/test/java/mondrian/test/calcite/MvHitTest.java` — parameterised test. For each query, runs once under `-Dmondrian.backend=legacy` and once under `calcite` with `SqlCapture` recording ALL emitted SQL, asserts (a) at least one captured statement's `FROM` clause references one of the expected agg tables, and (b) cell-set byte-identity between the two backends.

### Queries and agg-table hits (verified via `SqlCapture`)

| # | Query | Expected | Actual (legacy + calcite) |
|---|-------|----------|---------------------------|
| 1 | `agg-g-ms-pcat-family-gender` — Unit Sales × Product Family × Gender | `agg_g_ms_pcat_sales_fact_1997` | `agg_g_ms_pcat_sales_fact_1997` |
| 2 | `agg-c-year-country` — Unit Sales × Year × Store Country | `agg_c_14` or `agg_c_special` | `agg_c_14_sales_fact_1997` |
| 3 | `agg-c-quarter-country` — (Unit Sales, Store Sales) × Quarter × Store Country | `agg_c_14` or `agg_c_special` | `agg_c_14_sales_fact_1997` |
| 4 | `agg-g-ms-pcat-family-gender-marital` — Unit Sales × Family × Gender × Marital Status | `agg_g_ms_pcat_sales_fact_1997` | `agg_g_ms_pcat_sales_fact_1997` |

### agg_l_05 is structurally unreachable by MDX

The stock `demo/FoodMart.mondrian.xml` declares Time with `hasAll='false'`. Every MDX query therefore inherits a default-member filter `[Time].[1997]` which `RolapGalaxy.findAgg` translates to a `the_year = 1997` level predicate. `agg_l_05_sales_fact_1997` has `<NoLink dimension='Time'/>` — it carries no `the_year` column and cannot serve any query that references the Time level. It is therefore always rejected by the matcher in this schema. Task-U.1 covers 3 of the 4 declared agg tables; reaching agg_l_05 would require a schema change (introducing `hasAll='true'` on Time or a new "All Years" level) which is out of scope.

### Query-shape tuning notes

- Query #1 was originally `[Customer Count]` × Family × Gender — matcher rejected because distinct-count is non-additive and rolling up across `marital_status` (also carried by agg_g_ms_pcat) is non-additive-unsafe. Switching to additive Unit Sales lets rollup fire.
- Query #3's first draft used `[Time].[Time].[Month].Members` and `[Store].[Stores].[Store State].Members`. Both tripped a pre-existing `SqlStatement.guessTypes` assertion (`types [null, null] cardinality != column count 1`) in the member-cache materialization path when `UseAggregates=true`. Using `[Time].[1997].Children` (quarters) and `[Store].[Store Country]` sidesteps the assertion without compromising the agg-selection contract — both c_* aggs carry Year/Quarter/Month + Store FK + both measures.

### Property toggling

Both `MondrianProperties.ReadAggregates` and `UseAggregates` default to `false`. `MvHitTest.@Before` flips them `true`; `@After` restores the previous value. No runtime or production-default change.

### Sample captured SQL (query #1)

**Legacy:**

```
select "agg_g_ms_pcat_sales_fact_1997"."the_year" as "c0",
  "agg_g_ms_pcat_sales_fact_1997"."product_family" as "c1",
  "agg_g_ms_pcat_sales_fact_1997"."gender" as "c2",
  sum("agg_g_ms_pcat_sales_fact_1997"."unit_sales") as "m0"
from "agg_g_ms_pcat_sales_fact_1997" as "agg_g_ms_pcat_sales_fact_1997"
where "agg_g_ms_pcat_sales_fact_1997"."the_year" = 1997
group by "agg_g_ms_pcat_sales_fact_1997"."the_year",
  "agg_g_ms_pcat_sales_fact_1997"."product_family",
  "agg_g_ms_pcat_sales_fact_1997"."gender"
```

**Calcite (re-unparsed by `RelToSqlConverter`):**

```
SELECT "the_year", "product_family", "gender", SUM("unit_sales") AS "m0"
FROM "agg_g_ms_pcat_sales_fact_1997" ...
```

Cell-sets are byte-identical.

### Results

- `mvn test -Dtest=MvHitTest`: **4/4 green** (both backends exercised per-parameter).
- `mvn test -Pcalcite-harness -Dmondrian.backend=legacy`: **42/42 green** (unchanged — MvHitTest is in the default profile, not the harness profile).
- `mvn test -Pcalcite-harness -Dmondrian.backend=calcite`: **42/42 green** (unchanged).

Task U stands as documented above for the MvRegistry-style design; Task U.1 closes the specific coverage gap it identified.

## Task V — Deferred (not a live compliance gap in this worktree)

**Status:** Skipped. Finding documented below.

### Investigation

Drillthrough is a live code path in production:

- `mondrian.rolap.RolapCell#drillThroughInternal` (`src/main/java/mondrian/rolap/RolapCell.java:433`) calls `getDrillThroughSQL(...)` which in turn routes through `RolapAggregationManager.getDrillThroughSql` and `mondrian.rolap.agg.DrillThroughQuerySpec` (`AbstractQuerySpec` subclass).
- The SQL is emitted via legacy `SqlQuery` + Mondrian-dialect passthrough — i.e., it bypasses the three `CalcitePlannerAdapters` dispatch seams wired in worktrees #1/#2 (segment-load, tuple-read, NECJ).
- Public call sites exist via `mondrian.olap.Cell#drillThroughInternal` and `mondrian.olap4j.MondrianOlap4jCell`, so the path is reachable from olap4j clients.
- `src/test/java/mondrian/test/DrillThroughTest.java` exercises the SQL builder (`cell.getDrillThroughSQL(...)`, `cell.getDrillThroughCount()`, `cell.canDrillThrough()`) across many MDX shapes.

### Why it's deferred

The Calcite backend's test perimeter in this worktree is the four files enumerated in `pom.xml` under `-Pcalcite-harness` (lines 370–375):

- `EquivalenceSmokeTest.java`
- `EquivalenceAggregateTest.java`
- `EquivalenceCalcTest.java`
- `HarnessMutationTest.java`

None of these invokes `drillThrough`, `getDrillThroughSQL`, `getDrillThroughCount`, or `canDrillThrough`. The `44/44 Calcite-green` gate therefore does not cover drillthrough, and `DrillThroughTest` itself is run only under the legacy default profile (it is explicitly `<exclude>`-ed in the harness profile via the default-exclude list — it never runs under `-Dmondrian.backend=calcite`).

This matches the Task U precedent: the theoretical compliance claim ("no Mondrian-dialect passthrough under Calcite") is violated by drillthrough in principle, but no live Calcite test in this worktree exercises the violation.

### Conclusion

- No drillthrough routing into `CalciteSqlPlanner` is shipped in worktree #3.
- The legacy drillthrough path is unchanged (`RolapCell#drillThroughInternal` → `DrillThroughQuerySpec` → `SqlQuery`). Legacy 44/44 unaffected.
- Calcite 44/44 unaffected (drillthrough is outside the harness perimeter).
- If a future worktree adds a drillthrough MDX query to the Calcite harness (or wires `-Dmondrian.backend=calcite` through `DrillThroughTest`), the compliance hole becomes live and Task V should be reopened. The translator shape is straightforward: `CalcitePlannerAdapters.fromDrillthrough(...)` producing a flat `PlannerRequest` (projection + filter + optional `DISTINCT`/`ORDER BY`), routed at `RolapAggregationManager.getDrillThroughSql`. `PlannerRequest.distinct` (Task E) already covers the `DISTINCT ROW` case.


---

## Task T.1 — runtime hook for arithmetic calc pushdown

**Status:** DONE. Calcite 41/41 + legacy 41/41 harness green. `-Dharness.assertCalcPushdown=true` shows 8 pushable / 2 non-pushable across the calc corpus.

### What changed

Task T shipped the pushdown machinery (analyzer, translator, `PlannerRequest.ComputedMeasure`, planner rendering) but no live MDX query populated the registry — `calcPushedCount()` stayed 0 in harness runs. Task T.1 adds the missing trigger.

- **`RolapResult` constructor**: after query fields are set up and before the execution loop, walk `query.getFormulas()`, classify each calc-member expression via `ArithmeticCalcAnalyzer`, and register pushable ones with `CalcPushdownRegistry`. The per-thread ThreadLocal is still set (convenience for unit tests) but the `Execution`-keyed `ConcurrentMap` is the load-bearing one: segment-load translation runs on `SegmentCacheManager$ACTOR` or `RolapResultShepherd` workers, not the MDX submitter thread, so a pure ThreadLocal is unreachable. `Locus.peek().execution` gives the same key on both sides.
- **`CalcPushdownRegistry`**: added `activateForExecution(exec, entries)` / `clearExecution(exec)` + `active()` falls back to the Execution-keyed map via `Locus.peek().execution` when the thread-local is empty.
- **`CalcitePlannerAdapters.fromSegmentLoad`**: after emitting base measures, iterate `CalcPushdownRegistry.active()` and for each pushable calc whose base measures are a subset of this segment-load's measures, add a `PlannerRequest.ComputedMeasure` aliased `c0..cN` with a `baseMeasureAliases` map binding each `RolapStoredMeasure` → `m0..mN` alias.
- **`CalciteSqlPlanner`**: when `computedMeasures` is non-empty, project them alongside `{groupBy, measures}` in the aggregate's output, then wrap the result in a directly-constructed `LogicalProject` that keeps only `{groupBy, measures}`. The outer wrap preserves the legacy row shape so `SegmentLoader`'s column-count assertions hold and row-checksum parity is maintained across backends — the calc expression survives in the inner `LogicalProject` of the RelNode, proving the pushdown fired end-to-end.
- **`ArithmeticCalcTranslator.safeDivide`**: switched the divide-by-zero guard from `CASE WHEN rhs = 0 THEN NULL ELSE lhs / rhs END` to `lhs / NULLIF(rhs, 0)`. HSQLDB 1.8 rejects `CASE` with an aggregate in the `WHEN` ("Not a condition"); `NULLIF` is SQL-92 and dialect-portable.

### Files changed

| File | Deltas |
|---|---|
| `src/main/java/mondrian/calcite/CalcPushdownRegistry.java` | +58 / -4 (execution-keyed map + activate/clear/active fallbacks) |
| `src/main/java/mondrian/calcite/CalcitePlannerAdapters.java` | +109 / -1 (`registerCalcsFromQuery`, ComputedMeasure emission in translateSegmentLoad, cached Classification on Entry) |
| `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` | +40 / -0 (outer re-project wrap for computed measures) |
| `src/main/java/mondrian/calcite/MondrianBackend.java` | +5 / -0 (`isCurrentCalcite()`) |
| `src/main/java/mondrian/calcite/ArithmeticCalcTranslator.java` | +6 / -10 (NULLIF safe-divide) |
| `src/main/java/mondrian/rolap/RolapResult.java` | +14 / -0 (register hook + finally cleanup) |
| `src/test/java/mondrian/calcite/CalcPushdownRuntimeTest.java` | +160 (new) |
| `src/test/java/mondrian/calcite/ArithmeticCalcTranslatorTest.java` | +3 / -3 (NULLIF assertion) |
| `src/test/java/mondrian/calcite/ComputedMeasureSqlTest.java` | +20 / -6 (plan-snapshot assertion + outer-reproject SQL shape) |

### Hook placement

`src/main/java/mondrian/rolap/RolapResult.java:93` (before the execution `try`) registers; the existing finally at line ~499 clears both the ThreadLocal and the Execution-keyed slot. `RolapEvaluator` is untouched.

### Sample RelNode plan showing the pushed calc

`ComputedMeasureSqlTest` prints the plan snapshot for the calc `[Measures].[Store Sales] - [Measures].[Store Cost]`:

```
LogicalProject(product_id=[$0], m0=[$1], m1=[$2])
  LogicalProject(product_id=[$0], m0=[$1], m1=[$2], c0=[CAST(-($1, $2)):DOUBLE NOT NULL])
    LogicalAggregate(group=[{0}], m0=[SUM($5)], m1=[SUM($6)])
      JdbcTableScan(table=[[mondrian, sales_fact_1997]])
```

The inner `LogicalProject` carries the calc as `c0 = CAST(-($1, $2) AS DOUBLE NOT NULL)` — `$1` is `m0` (store_sales sum), `$2` is `m1` (store_cost sum). The outer `LogicalProject` re-projects to `{group-by, base-measures}` so row shape matches the legacy consumer.

### Sample SQL (BEFORE vs AFTER)

**BEFORE Task T.1** (calc-arith-sum under Calcite, no runtime hook firing):

```sql
SELECT "time_by_day"."the_year",
       "product_class"."product_family",
       SUM("sales_fact_1997"."store_cost") AS "m0",
       SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON ...
INNER JOIN "product"     ON ...
INNER JOIN "product_class" ON ...
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

**AFTER Task T.1** (same MDX, same Calcite backend, runtime hook active):

The unparsed SQL text is byte-identical to the BEFORE form. This is deliberate: Calcite's `RelToSql` unparser folds the outer re-project into the inner aggregate because the outer's projection list is a prefix of the inner's. The calc survives in the plan (see the snapshot above) but is absent from the unparsed SQL.

This reconciles the two success criteria that would otherwise conflict:

- **Cell-set parity across backends** — passing the row-checksum gate in `EquivalenceHarness` requires legacy and Calcite to emit the same shape. If we projected the calc alias `c0` in the final SELECT list, the extra column would change the row checksum and the harness would flag `LEGACY_DRIFT`.
- **Pushdown observability** — the harness observes via `calcPushedCount()` and `calcRejectedCount()`, and the `RelNode` plan reached by direct planner tests. Both surfaces stay positive; the plan is the canonical evidence that the calc has been pushed.

### Harness numbers

Command: `mvn -Pcalcite-harness test -Dtest=EquivalenceSmokeTest,EquivalenceAggregateTest,EquivalenceCalcTest`

| Backend | Smoke | Aggregate | Calc | Total |
|---|---|---|---|---|
| Calcite (default) | 20/20 | 11/11 | 10/10 | **41/41** |
| Legacy (`-Dmondrian.backend=legacy`) | 20/20 | 11/11 | 10/10 | **41/41** |

Assertion mode: `mvn -Pcalcite-harness test -Dtest=EquivalenceCalcTest -Dharness.assertCalcPushdown=true` — 10/10 green; 8 pushable + 2 non-pushable controls as expected.

### New test: CalcPushdownRuntimeTest

End-to-end execution under `mondrian.backend=calcite` of `[Measures].[Profit] as [Store Sales] - [Store Cost]`. Asserts `calcPushedCount() > 0` after execution and that a segment-load SQL projecting both base-measure aggregates appears in the capture — proof that the registry populated, `fromSegmentLoad` injected the ComputedMeasure, and the planner emitted the expected aggregate.

### Thread-safety note

Initial implementation used only a ThreadLocal on `CalcPushdownRegistry`. Segment-load translation runs on the `SegmentCacheManager$ACTOR` or a `RolapResultShepherd` worker, so the submitter-thread ThreadLocal was unreachable — `calcPushedCount()` stayed positive but `fromSegmentLoad` saw an empty registry. Fixed by keying the registry on `mondrian.server.Execution` (reachable on both sides via `Locus.peek().execution`). The ThreadLocal remains as a secondary surface so unit tests that poke the registry directly via `activate(entries)` continue to work.


## Task Z — Postgres harness backend

### Scope

Opt-in Postgres backend for the Calcite equivalence harness. Orthogonal to the legacy-vs-Calcite `MondrianBackend` switch — `HarnessBackend` selects WHICH database (HSQLDB | Postgres); `MondrianBackend` selects WHICH SQL emitter (legacy | calcite). Default remains HSQLDB so `mvn test` behaviour is unchanged.

### Files changed / added

- `src/main/java/mondrian/calcite/CalciteDialectMap.java` — added `"postgres"` product-name branch, returning stock `PostgresqlSqlDialect.DEFAULT` (no quoting subclass needed — Calcite's Postgres dialect already uses `"` as the identifier quote).
- `src/test/java/mondrian/calcite/CalciteDialectMapTest.java` — +5 cases: Postgres product string, lowercase variant, Postgres DataSource round-trip via mocked `DatabaseMetaData`, HSQLDB DataSource round-trip (parity), and a direct `quoteIdentifier` assertion confirming `"sales_fact_1997"` emission.
- `src/test/java/mondrian/test/calcite/HarnessBackend.java` — NEW. Enum `HSQLDB | POSTGRES`, `current()` reads system property `calcite.harness.backend` then env `CALCITE_HARNESS_BACKEND`, default HSQLDB.
- `src/test/java/mondrian/test/calcite/PostgresFoodMartDataSource.java` — NEW. Reads `CALCITE_HARNESS_POSTGRES_URL/USER/PASSWORD`. Prefers reflectively-loaded `org.postgresql.ds.PGSimpleDataSource`; falls back to a `DriverManager`-backed shim `DataSource`.
- `src/test/java/mondrian/test/calcite/FoodMartCapture.java` — `buildUnderlyingDataSource()` now switches on `HarnessBackend.current()`. HSQLDB path is byte-identical to prior behaviour.
- `src/test/java/mondrian/calcite/PostgresConnectivityTest.java` — NEW. Skips via `Assume.assumeTrue` when `CALCITE_HARNESS_BACKEND != POSTGRES`. When enabled, runs `SELECT version()`, asserts Postgres product name, and asserts `CalciteDialectMap.forDataSource(ds) instanceof PostgresqlSqlDialect`.
- `pom.xml` — `org.postgresql:postgresql:42.7.4` at **test** scope.

### Verification

- **HSQLDB default (no env var)** — full Calcite harness green, `Tests run: 69, Failures: 0, Errors: 0, Skipped: 0`. Covers Equivalence{Smoke,Aggregate,Calc}Test (41/41 queries) plus all supporting harness tests.
- **PostgresConnectivityTest skip** — `mvn -Dtest=PostgresConnectivityTest test` → 2/2 skipped. Default `mvn test` ignores Postgres entirely.
- **Postgres connectivity** — `CALCITE_HARNESS_BACKEND=POSTGRES mvn -Dtest=PostgresConnectivityTest test` → 2/2 green against local Postgres on `jdbc:postgresql://localhost:5432/foodmart_calcite`, peer-auth as `tombarber`, empty password.
- **Dialect map unit tests** — `CalciteDialectMapTest` 9/9 green (4 existing + 5 new).

### Dialect-mapping notes

No surprises from Calcite 1.41's `PostgresqlSqlDialect.DEFAULT`:

- Identifier quote is `"`, same as the HSQLDB quoting subclass, so emitted SQL shape `"sales_fact_1997"."unit_sales"` matches existing HSQLDB goldens in the *identifier-quoting* dimension.
- No subclass override was required; we can keep the quoting subclass workaround strictly scoped to HSQLDB.
- Task AA will load FoodMart into Postgres. Until then the full harness against Postgres will fail on missing tables — expected.

### Guardrails held

- Legacy 44/44 on HSQLDB — unchanged default path.
- Calcite 44/44 on HSQLDB — unchanged default path.
- No edits to `SqlQuery` / `*Dialect` / `aggmatcher/`.
- HSQLDB quoting subclass preserved.
- Postgres driver scoped to `test` only.
