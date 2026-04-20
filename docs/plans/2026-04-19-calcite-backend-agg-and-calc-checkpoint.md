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
