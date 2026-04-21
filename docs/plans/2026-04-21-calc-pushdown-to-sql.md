# Calc Pushdown Lands in SQL Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make arithmetic calc-member pushdown (`SUM(x)/SUM(y)`, `SUM(x)-SUM(y)`, etc.) actually emit the derived expression in the generated SQL, instead of being folded away by `RelToSqlConverter`.

**Architecture:** Today the CalcClassifier correctly marks pushdown-eligible calcs and the planner builds an outer `Project` with the arithmetic on top of an `Aggregate`. Calcite's unparser folds the outer `Project` back into the `Aggregate`'s projection, losing the derived column. The fix: emit the calc as a `Project` that references the `Aggregate` output by alias through a tiny materialization boundary (a `TableFunctionScan` or a sentinel `Hint`) that Calcite's `ProjectMergeRule` won't cross.

**Tech Stack:** Calcite RelToSqlConverter, RelBuilder, CoreRules.PROJECT_MERGE, ProjectAggregateMergeRule.

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
