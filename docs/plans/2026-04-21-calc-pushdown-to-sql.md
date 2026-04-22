# Calc Pushdown Lands in SQL Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make arithmetic calc-member pushdown (`SUM(x)/SUM(y)`, `SUM(x)-SUM(y)`, etc.) actually emit the derived expression in the generated SQL, instead of being folded away by `RelToSqlConverter`.

**Architecture:** Today the CalcClassifier correctly marks pushdown-eligible calcs and the planner builds an outer `Project` with the arithmetic on top of an `Aggregate`. Calcite's unparser folds the outer `Project` back into the `Aggregate`'s projection, losing the derived column. The fix: emit the calc as a `Project` that references the `Aggregate` output by alias through a tiny materialization boundary (a `TableFunctionScan` or a sentinel `Hint`) that Calcite's `ProjectMergeRule` won't cross.

**Tech Stack:** Calcite RelToSqlConverter, RelBuilder, CoreRules.PROJECT_MERGE, ProjectAggregateMergeRule.

---

## Revision 2026-04-21b — re-revision after empirical check

The subagent ran `ComputedMeasureSqlTest` and captured actual SQL:

```
SELECT product_id, SUM(store_sales) AS m0, SUM(store_cost) AS m1
FROM sales_fact_1997 GROUP BY product_id
```

**The arithmetic is not in the SQL at all.** The `force=true` + `LogicalProject.create` boundary at `CalciteSqlPlanner.java:742-751` does not survive `RelToSqlConverter` unparse. `CalcPushdownRuntimeTest` only asserts the two `SUM()` base calls are present — the test's own comments acknowledge (lines 145-147) that "the calc alias itself may be folded out of the final SQL text."

**So Revision 2026-04-21 below is wrong on its load-bearing premise.** The original plan's Tasks 3 and 4 (prevent fold / add alias boundary) are the actual blocker. Sequence:

1. **Original Task 3** — diagnose which rule/unparse step drops the outer Project; exclude `ProjectAggregateMergeRule` / `AggregateProjectMergeRule` from the Hep program when the `mondrian.calc.pushdown` hint is present. Keep `force=true` but don't rely on it alone.
2. **Original Task 4 (if Task 3 insufficient)** — wrap the `Aggregate` in an aliased `LogicalProject` (forming a subquery boundary in `RelToSqlConverter`). Verify the outer SELECT in the emitted SQL now contains the arithmetic.
3. **New Task 3' (was skipped below, now reinstated)** — once arithmetic lands in SQL, teach `SegmentLoader` to read the SQL-computed column as the calc-member's cell value, behind a `-Dmondrian.calcite.calcConsume=true` opt-in flag.
4. **New Task 4' (reinstated)** — harness cell-parity check for SQL-side CASE-guarded divide vs Java-side empty-on-/0 arithmetic.

**Therefore: ignore Revision 2026-04-21's "skip Task 3/skip Task 4" guidance.** Execute the original Task 3 and Task 4 as written, using the real class names from the table below. The earlier revision's class-name mapping is still correct; only its "skip" directive was wrong.

Class-name mapping remains:

## Revision 2026-04-21 — codebase alignment

The original plan was drafted from an earlier-session summary and uses placeholder class names. Real-code mapping:

| Plan reference | Actual class / hook |
|---|---|
| `CalcClassifier` | `mondrian.calcite.ArithmeticCalcAnalyzer` |
| `CalcPushdownRewriter.rewrite()` | `ArithmeticCalcTranslator` + `CalcPushdownRegistry` + inner/outer Project construction in `CalciteSqlPlanner.java` ~lines 680–740 |
| `CalciteSqlEmitter` | Doesn't exist — unparse happens in-line via `RelToSqlConverter` from `CalciteSqlPlanner` |
| `CalciteSqlPlanner.lastSql` | Use the existing `mondrian.test.calcite.SqlCapture` + `CapturedExecution` harness — see `CalcPushdownRuntimeTest` for the pattern |
| `EquivalenceCalcTest` | Not yet created; add it or extend `EquivalenceAggregateTest` with a calc-corpus parameterised axis |

**The real symptom is different from the original plan's premise.** `CalciteSqlPlanner` already emits inner (with calc) + outer (without calc) Projects using `force=true` so the inner SQL carries the arithmetic for observability — `CalcPushdownRuntimeTest` already asserts this. The outer Project *deliberately* drops the calc column so `SegmentLoader`'s output row shape matches legacy for row-checksum parity.

**So Task B isn't "prevent a Hep fold" — it's "let the SQL-computed calc value actually reach the calc-member evaluator instead of being recomputed in Java."** Concretely:
1. Mark `PlannerRequest.ComputedMeasure` as consumable (`consumeInSegmentLoad`).
2. Teach `SegmentLoader` to read the SQL-computed column into the calc member's cell, bypassing Java arithmetic when the flag is on.
3. Normalise SQL div-by-zero (`CASE WHEN b=0 THEN NULL` — already in `ArithmeticCalcTranslator`) vs Java empty-on-/0 so equivalence cells match.
4. Opt-in flag `-Dmondrian.calcite.calcConsume=true` keeps row-checksum parity green by default.

**Task mapping (implementer guidance):**
- Tasks 1–2: keep the spirit (SQL capture + diagnosis) but use `SqlCapture`. Rename to "confirm SQL arithmetic lands in inner projection, trace why `SegmentLoader` still recomputes Java-side."
- Task 3 (remove fold-inducing rules): **skip** — not the blocker.
- Task 4 (alias boundary): **skip** — `force=true` already does it.
- **New Task 3'**: route consumed calc through `SegmentLoader` behind `calcConsume` flag.
- **New Task 4'**: harness cell-parity assertion on div-by-zero between SQL and Java paths.
- Task 5 (equivalence): keep; axis becomes `calcConsume={false,true}`.
- Task 6 (observability): `CalcitePlannerAdapters.calcPushedCount()` exists. Add `calcConsumedCount()` for "SQL value actually used."

If the short-circuit diagnosis reveals a different blocker than described above, **stop and report** — don't push through. The source summary may be one revision stale.

---

### Task 1: Reproduce the fold — capture before/after SQL

**Files:**
- Create: `src/test/java/mondrian/calcite/CalcPushdownSqlTest.java`

**Step 1:** Write failing test `calcIifNumericEmitsArithmeticInSql()`:
- MDX: `WITH MEMBER [Measures].[Ratio] AS '[Measures].[Unit Sales] / [Measures].[Store Sales]' ...`
- Capture generated SQL via `CalciteSqlPlanner.lastSql`
- Assert SQL contains `SUM("unit_sales") / SUM("store_sales")` (or equivalent divide) in the SELECT list

**Step 2:** Run: `mvn test -Dtest=CalcPushdownSqlTest#calcIifNumericEmitsArithmeticInSql`
Expected: FAIL — SQL has `SUM("unit_sales") AS m0, SUM("store_sales") AS m1` with the division happening in Java.

**Step 3:** Commit the failing test.

```bash
git commit -m "test: failing baseline for calc pushdown SQL emission"
```

---

### Task 2: Audit why the outer Project folds

**Files:**
- Read: `src/main/java/mondrian/calcite/CalcPushdownRewriter.java` (or equivalent)
- Read: Calcite source `org.apache.calcite.rel.rel2sql.RelToSqlConverter.visit(Project)`

**Step 1:** Dump the RelNode tree at three points:
  - After planner builds it (should be `Project[ratio=/($0,$1)](Aggregate(...))`)
  - After `HepPlanner` runs (check if `ProjectAggregateMergeRule` or `AggregateProjectMergeRule` is collapsing them)
  - What `RelToSqlConverter` receives

**Step 2:** Identify the exact rule/converter step that eliminates the outer Project. Document in `docs/reports/calc-pushdown-fold-analysis.md`.

**Step 3:** Commit the analysis doc.

```bash
git commit -m "docs: analyze calc pushdown fold path"
```

---

### Task 3: Remove fold-inducing rules from planner program

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java` (Hep program builder)

**Step 1:** When the input RelNode has a calc-pushdown marker (add `Hint` "mondrian.calc.pushdown" on the outer Project in the rewriter), exclude `ProjectAggregateMergeRule` and `AggregateProjectMergeRule` from the Hep program.

**Step 2:** Run Task 1's test. If the outer Project survives Hep but RelToSqlConverter still folds it, proceed to Task 4.

**Step 3:** If the test passes here, commit and skip to Task 5.

```bash
git commit -m "fix(calcite): preserve calc-pushdown Project through Hep"
```

---

### Task 4: Unfold-safe Project via explicit alias boundary

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalcPushdownRewriter.java`

**Why Task 3 may not suffice:** RelToSqlConverter inlines trivial Projects during unparse even without rules firing. The robust fix is to ensure the outer Project references the Aggregate by an alias that the unparser treats as a boundary.

**Step 1:** In `CalcPushdownRewriter.rewrite()`:
- Wrap the `Aggregate` in a `LogicalProject` that re-projects every output column by its named alias (`m0 AS m0, m1 AS m1, ...`)
- Put the arithmetic calc in a second `LogicalProject` on top referencing those aliases
- Mark the outer Project's hint `mondrian.calc.pushdown=true`

**Step 2:** In `CalciteSqlEmitter` (wherever the SqlImplementor is configured), override `visit(Project)` to emit a subquery boundary when the hint is present: `SELECT outer.* FROM (SELECT inner.m0, inner.m1 FROM ... GROUP BY ...) inner`.

**Step 3:** Run Task 1's test. Expected: PASS — SQL contains the arithmetic in outer SELECT.

**Step 4:** Commit.

```bash
git commit -m "feat(calcite): emit calc pushdown as SQL derived expression"
```

---

### Task 5: Equivalence + perf on calc corpus

**Files:**
- Modify: `src/test/java/mondrian/calcite/EquivalenceCalcTest.java` (enable full corpus)

**Step 1:** Run the 10 calc-corpus queries through the equivalence harness with pushdown on. Assert cell parity.

**Step 2:** Run perf benchmark on Postgres 1000× for the calc corpus:
- Before (Java-side division): recorded in `docs/reports/perf-analysis-final.md`
- After (SQL-side division): expect ≥ 1 network round-trip saved per query and reduced rowset size

**Step 3:** Record results in `docs/reports/calc-pushdown-perf.md`. If any query regresses > 10%, add a heuristic: only push down when the aggregate has ≥ N groups (threshold TBD from data).

**Step 4:** Commit.

```bash
git commit -m "test: calc pushdown passes equivalence + perf"
```

---

### Task 6: Observability

**Files:**
- Modify: `src/main/java/mondrian/calcite/CalcClassifier.java`

**Step 1:** Emit a `mondrian.calcite.calcPushdown.emitted` counter (increment when the SQL actually contains the arithmetic) vs `mondrian.calcite.calcPushdown.classified` (already exists).

**Step 2:** Log at DEBUG when classified but not emitted — indicates a fold regression.

**Step 3:** Commit.

```bash
git commit -m "feat(calcite): observability for calc pushdown SQL emission"
```

---

## Risks

- **Subquery boundary cost:** Some databases (old Postgres, MySQL) don't flatten derived tables perfectly. Benchmark Task 5 catches this.
- **Alias collisions:** If the inner Project's aliases collide with outer scope, `SqlValidator` complains. Generate unique aliases.
- **Nullability propagation:** Division by zero currently throws Java `ArithmeticException`; SQL returns NULL or errors depending on dialect. Harness's cell-level equality may flag this. Normalize both paths to NULL on div-by-zero in the equivalence harness.

## Done When

- `calc-iif-numeric` SQL contains the division in SELECT
- 10/10 calc corpus queries pass equivalence with pushdown enabled
- Perf neutral-or-better on Postgres 1000×
- Observability counter shows emitted > 0 on real workloads
