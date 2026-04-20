# Calcite Backend Foundations (Worktree #1) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or superpowers:subagent-driven-development) to implement this plan task-by-task.

**Goal:** Land the first four cutover checkpoints from the Calcite rewrite design (`docs/plans/2026-04-19-calcite-backend-rewrite-design.md`): introduce the `mondrian/calcite/` planner package, a backend kill switch, and route `SqlTupleReader` + `RolapAggregationManager` through Calcite — ending with the first end-to-end MDX query served by Calcite-generated SQL.

**Architecture:** A new `mondrian.calcite` package owns all Calcite plumbing: `CalciteSqlPlanner` (entry point), `CalciteMondrianSchema` (RolapCube → `RelOptSchema`), `CalciteDialectMap` (Mondrian `Dialect` → Calcite `SqlDialect`). The legacy `SqlQueryBuilder` path stays compiled and reachable via `-Dmondrian.backend=legacy`; `calcite` is the new default. `SqlTupleReader` and the segment-loading side of `RolapAggregationManager` gain a dispatch that chooses the backend at call time. No legacy file is deleted in this worktree.

**Tech Stack:** Apache Calcite 1.41.0 (`calcite-core`, `calcite-linq4j`, transitive Avatica), JUnit 4, Maven (`calcite-harness` profile), HSQLDB FoodMart fixture.

**Harness as co-pilot:** The equivalence harness (32/32 green on legacy; 34 tests total incl. plumbing) asserts cell-set parity against frozen legacy goldens in `src/test/resources/calcite-harness/golden-legacy/` (archived in the first commit on this branch). Every checkpoint below ends with a harness run.

**Out of scope for this worktree:**
- `RolapNative*` migrations (worktree #2).
- `MvRegistry` / agg-table → materialized view (worktree #3).
- `ArithmeticCalcAnalyzer` / calc-member push-down (worktree #3).
- Drillthrough migration (worktree #3).
- Deleting any legacy SQL/dialect/aggmatcher code (worktree #4).

---

## Ground rules

- **TDD everywhere.** Every new class lands with a failing unit test first, then the minimal code, then green. Every routing change lands with a harness run before and after.
- **Commit per task.** One task = one logical commit. Messages follow repo style: `feat:`, `refactor:`, `test:`, `build:`, `docs:`.
- **Legacy stays compiled.** Nothing in `mondrian/rolap/sql/`, `mondrian/rolap/aggmatcher/`, or `mondrian/spi/impl/*Dialect.java` is deleted.
- **Default backend flips to `calcite` only in Task 9.** Until then, the calcite path is opt-in via `-Dmondrian.backend=calcite`, which keeps CI green while foundations go in.
- **Harness is authoritative.** `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test` must stay green on `backend=legacy` throughout. The new `backend=calcite` mode is allowed to fail until Task 10.

---

## Task 1: Promote Calcite to compile scope

**Why first:** Every subsequent task imports `org.apache.calcite.*`. Doing this change in isolation keeps the diff trivial and reviewable.

**Files:**
- Modify: `pom.xml:88-103` (Calcite dependency block)

**Step 1: Write the failing test**

Create `src/test/java/mondrian/calcite/CalciteClasspathTest.java`:

```java
package mondrian.calcite;

import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class CalciteClasspathTest {
    @Test public void calciteIsOnCompileClasspath() {
        assertNotNull(Frameworks.newConfigBuilder());
    }
}
```

**Step 2: Verify it fails to compile**

Run: `mvn -q -DskipTests compile test-compile 2>&1 | tail -20`
Expected: compilation error on `CalciteClasspathTest` because `main` sources can't see test-scope Calcite (test sources can, but `CalciteClasspathTest` is intended to also prove it's available from `src/main`; move it later). Alternative: temporarily reference `Frameworks` from a throwaway class under `src/main/java/mondrian/calcite/ClasspathProbe.java` to force compile failure on test-scope.

**Step 3: Change scope**

In `pom.xml` at the `calcite-core` and `calcite-linq4j` dependency entries, remove `<scope>test</scope>`.

**Step 4: Verify compile + test pass**

Run: `mvn -q -DskipTests compile test-compile && mvn -q -Dtest=CalciteClasspathTest test`
Expected: BUILD SUCCESS, 1 test passed.

**Step 5: Harness baseline**

Run: `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test 2>&1 | grep -E "Tests run:|BUILD"`
Expected: 34 tests, 0 failures, BUILD SUCCESS.

**Step 6: Commit**

```
build: promote Calcite to compile scope

Worktree #1 Task 1. Calcite is about to become a production dependency
of the new mondrian/calcite/ package, so it can no longer be test-only.
```

---

## Task 2: Backend selector + kill switch

**Why:** Every routing change downstream needs a way to choose between legacy and Calcite paths at call time. Landing the selector first means later tasks only add `if (backend.isCalcite()) { ... }` branches.

**Files:**
- Create: `src/main/java/mondrian/calcite/MondrianBackend.java`
- Create: `src/test/java/mondrian/calcite/MondrianBackendTest.java`

**Step 1: Write the failing test**

```java
package mondrian.calcite;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class MondrianBackendTest {
    @After public void clear() { System.clearProperty("mondrian.backend"); }

    @Test public void defaultsToLegacy() {
        assertSame(MondrianBackend.LEGACY, MondrianBackend.current());
    }
    @Test public void calcitePropertyPicksCalcite() {
        System.setProperty("mondrian.backend", "calcite");
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
    }
    @Test public void unknownFallsBackToLegacyWithWarning() {
        System.setProperty("mondrian.backend", "bogus");
        assertSame(MondrianBackend.LEGACY, MondrianBackend.current());
    }
    @Test public void caseInsensitive() {
        System.setProperty("mondrian.backend", "CALCITE");
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
    }
}
```

**Step 2: Run — must fail**

Run: `mvn -q -Dtest=MondrianBackendTest test`
Expected: compile error.

**Step 3: Implement**

```java
package mondrian.calcite;

public enum MondrianBackend {
    LEGACY, CALCITE;

    public static final String PROPERTY = "mondrian.backend";

    public static MondrianBackend current() {
        String raw = System.getProperty(PROPERTY, "legacy");
        try { return MondrianBackend.valueOf(raw.toUpperCase(java.util.Locale.ROOT)); }
        catch (IllegalArgumentException ex) { return LEGACY; }
    }

    public boolean isCalcite() { return this == CALCITE; }
}
```

**Step 4: Pass**

Run: `mvn -q -Dtest=MondrianBackendTest test`
Expected: 4 tests pass.

**Step 5: Commit**

```
feat(calcite): add MondrianBackend kill switch

-Dmondrian.backend=calcite|legacy, default legacy. Unknown values
fall back to legacy. Foundation for routing decisions in
SqlTupleReader and RolapAggregationManager.
```

---

## Task 3: `CalciteDialectMap` (HSQLDB only)

**Why:** The planner needs a `SqlDialect` to emit SQL. HSQLDB is the only backend the harness hits; shipping only `HSQLDB_1_8_X` keeps the surface area honest. Real Mondrian `Dialect` detection logic is unchanged — we only map *from* it.

**Files:**
- Create: `src/main/java/mondrian/calcite/CalciteDialectMap.java`
- Create: `src/test/java/mondrian/calcite/CalciteDialectMapTest.java`

**Step 1: Test**

```java
package mondrian.calcite;

import mondrian.spi.Dialect;
import mondrian.spi.impl.JdbcDialectImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.junit.Test;
import static org.junit.Assert.*;

public class CalciteDialectMapTest {
    @Test public void hsqldbDialectMapsToHsqldb() {
        Dialect mondrian = JdbcDialectImpl.class.cast(null); // placeholder — real impl: fake with HSQLDB product name
        SqlDialect sd = CalciteDialectMap.forProductName("HSQL Database Engine");
        assertTrue(sd instanceof HsqldbSqlDialect);
    }
    @Test public void unknownProductThrows() {
        try { CalciteDialectMap.forProductName("Frobnitz 9.9"); fail(); }
        catch (IllegalArgumentException expected) {}
    }
}
```

(Editor: refine the first test — pass a real product-name string since worktree #1 only supports HSQLDB.)

**Step 2: Verify fail.** `mvn -q -Dtest=CalciteDialectMapTest test` → compile error.

**Step 3: Implement**

```java
package mondrian.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

public final class CalciteDialectMap {
    private CalciteDialectMap() {}

    public static SqlDialect forProductName(String product) {
        if (product == null) throw new IllegalArgumentException("null product name");
        String p = product.toLowerCase(java.util.Locale.ROOT);
        if (p.contains("hsql")) return HsqldbSqlDialect.DEFAULT;
        throw new IllegalArgumentException(
            "No Calcite SqlDialect mapping for JDBC product '" + product + "'. "
            + "Worktree #1 supports HSQLDB only; extend CalciteDialectMap to add more.");
    }
}
```

**Step 4: Pass.** Expected: 2 tests pass.

**Step 5: Commit.**

```
feat(calcite): CalciteDialectMap, HSQLDB-only for worktree #1

Design doc locks multi-backend matrix as a later worktree. Any
unmapped product name fails loud rather than silently falling back.
```

---

## Task 4: `CalciteMondrianSchema` adapter (star-schema only)

**Why:** Planner needs a `RelOptSchema` that exposes fact + dimension tables. For worktree #1 we only need enough adapter to serve the ~5 corpus queries that hit checkpoint #4 — i.e. ones without degenerate dimensions, inline tables, or snowflakes. This is an explicit scope limit; harness will surface queries we can't handle.

**Files:**
- Create: `src/main/java/mondrian/calcite/CalciteMondrianSchema.java`
- Create: `src/test/java/mondrian/calcite/CalciteMondrianSchemaTest.java`

**Step 1: Test** — construct an adapter for the Sales cube (via existing test fixture `FoodMartCapture`), request a table by qualified name, assert it returns a `RelOptTable` whose row type matches the fact-table column set.

**Step 2–4:** Red → minimal impl (extends `AbstractSchema`, enumerates fact + dimension `RolapStar.Table`s, converts each to Calcite `Table` with JDBC column metadata) → green.

**Step 5: Harness baseline.** `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test` — must stay green (this task does not touch routing).

**Step 6: Commit.**

```
feat(calcite): CalciteMondrianSchema — RolapCube -> RelOptSchema

Exposes fact + dimension tables of a Mondrian star schema as a
Calcite RelOptSchema. Star-schema only; snowflakes and <InlineTable>
land in later worktrees.
```

---

## Task 5: `CalciteSqlPlanner` skeleton (request → SQL)

**Why:** The critical class. Takes a `SqlTupleReader` / `AggregationManager`-style *request* (constrained by levels/members, fact table, measures), returns a SQL string + dialect-aware parameter types. Worktree #1 scope: grouping + filter + projection. No joins across multiple hierarchies yet beyond what FoodMart's Sales cube forces on the ~5 green queries.

**Files:**
- Create: `src/main/java/mondrian/calcite/CalciteSqlPlanner.java`
- Create: `src/main/java/mondrian/calcite/PlannerRequest.java` (value type: fact table, columns, filters, group-by, order-by)
- Create: `src/test/java/mondrian/calcite/CalciteSqlPlannerTest.java`

**Step 1: Tests (three, red)**

1. Simple `SELECT unit_sales FROM sales_fact_1997` → planner emits exactly that SQL against HSQLDB dialect.
2. Grouping: `SELECT time_by_day.the_year, sum(unit_sales) FROM sales_fact_1997 JOIN time_by_day ... GROUP BY the_year` — snapshot-compare emitted SQL against a golden string.
3. Dialect-awareness: same request with an override to a DIFFERENT dialect (e.g. `SqlDialect.DatabaseProduct.CALCITE.getDialect()`) produces detectably different SQL (e.g. quoting).

**Step 2–4:** Red → minimal `RelBuilder` pipeline (scan, filter, aggregate, project) → emit via `RelToSqlConverter` with dialect from `CalciteDialectMap` → green.

**Step 5: Harness baseline.** Still green — no routing yet.

**Step 6: Commit.**

```
feat(calcite): CalciteSqlPlanner + PlannerRequest

Request-to-SQL entry point. RelBuilder -> RelToSqlConverter with a
dialect supplied by CalciteDialectMap. Feature coverage matches the
~5 corpus queries targeted for checkpoint #4.
```

---

## Task 6: Route `SqlTupleReader` through planner when `backend=calcite`

**Why:** First real routing change. Tuple reads are the dominant SQL shape in the smoke corpus, so if planner can satisfy them we unlock most of checkpoint #4.

**Files:**
- Modify: `src/main/java/mondrian/rolap/SqlTupleReader.java` around line 498 (the `RolapUtil.executeQuery` call site).
- Test: existing `EquivalenceSmokeTest` (via harness, run under `-Dmondrian.backend=calcite`).
- Create: `src/test/java/mondrian/calcite/SqlTupleReaderCalciteBackendTest.java` — a single targeted test that invokes a tuple read under `backend=calcite` and asserts no exception + row count matches legacy.

**Step 1: Write the failing targeted test.**

**Step 2: Run — must fail** (NPE or routing not-wired, as expected).

**Step 3: Implement routing.**

Inside `SqlTupleReader`, locate the point where `SqlQuery` is assembled and `RolapUtil.executeQuery` is invoked. Introduce:

```java
final String sql;
if (MondrianBackend.current().isCalcite()) {
    PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(/* existing locals */);
    sql = new CalciteSqlPlanner(schema, dialectMap).plan(req);
} else {
    sql = legacySqlQuery.toSqlAndTypes().left;
}
```

Keep the `RolapUtil.executeQuery` call site identical regardless of backend — both paths feed the same JDBC seam, preserving `SqlInterceptor`.

**Step 4: Targeted test passes under `-Dmondrian.backend=calcite`.**

**Step 5: Harness — two runs.**

- `mvn -Pcalcite-harness -Dtest='Equivalence*Test' test` (legacy, default) → still 34/34.
- `mvn -Pcalcite-harness -Dmondrian.backend=calcite -Dtest=EquivalenceSmokeTest test 2>&1 | tee /tmp/calcite-smoke.log` → record count. It's ALLOWED to be partially red at this point (probably ~3–5/20 pass). Save the log for the checkpoint-4 review.

**Step 6: Commit.**

```
refactor(calcite): route SqlTupleReader through CalciteSqlPlanner

Dispatch on MondrianBackend.current(). Legacy path unchanged (still
the default). SqlInterceptor seam preserved — both backends share the
same RolapUtil.executeQuery invocation.
```

---

## Task 7: Route segment loading through planner when `backend=calcite`

**Why:** Aggregation / segment loading is the other dominant SQL shape. With both routing points wired, checkpoint #4 becomes reachable.

**Files:**
- Modify: `src/main/java/mondrian/rolap/SegmentLoader.java` around line 633 (the `RolapUtil.executeQuery` call site).
- Modify (if needed): `src/main/java/mondrian/rolap/RolapAggregationManager.java` — preserve public API; delegate via `SegmentLoader`.
- Create: `src/test/java/mondrian/calcite/SegmentLoaderCalciteBackendTest.java`.

**Steps 1–6:** identical shape to Task 6. Pattern, test, route, harness twice, commit.

```
refactor(calcite): route SegmentLoader through CalciteSqlPlanner

RolapAggregationManager's public API unchanged; the SQL emission
point in SegmentLoader now dispatches on MondrianBackend. SqlQuery
path stays compiled for -Dmondrian.backend=legacy.
```

---

## Task 8: Plan-snapshot golden support (scaffolding only)

**Why:** The design specifies a `PLAN_DRIFT` gate (`RelOptUtil.toString` per corpus query → `golden-plans/<name>.plan`). Landing the scaffolding — directory, harness capture point, failure class — now lets worktree #2+ start populating goldens without another plumbing commit.

**Files:**
- Create: `src/test/resources/calcite-harness/golden-plans/.gitkeep`.
- Modify: `src/test/java/mondrian/test/calcite/FailureClass.java` — add `PLAN_DRIFT`.
- Modify: `src/test/java/mondrian/test/calcite/EquivalenceHarness.java` — capture `RelOptUtil.toString(rel)` when running under Calcite backend; compare against `golden-plans/<name>.plan` if file exists; ignore if absent.
- Modify: `src/test/java/mondrian/test/calcite/EquivalenceHarness.java` — rename `BASELINE_DRIFT` → `LEGACY_DRIFT` (design §Harness evolution).

**Step 1:** add a tiny unit test (`HarnessPlanSnapshotTest`) that feeds a manufactured plan string, verifies drift is reported when file contents differ.

**Step 2:** Implement.

**Step 3:** Harness run under both backends — still green (no golden-plans files committed yet, so the gate is a no-op for the 34 existing tests).

**Step 4: Commit.**

```
test: plan-snapshot + LEGACY_DRIFT rename

Adds the scaffolding for the PLAN_DRIFT gate from the rewrite design.
No plan goldens committed yet — worktree #2 starts populating them.
```

---

## Task 9: Flip default backend to `calcite`

**Why:** Now that legacy + calcite routing both exist, flip the default. This is a one-line change but psychologically significant — it forces the harness to run the new path on every `mvn test` going forward.

**Files:**
- Modify: `src/main/java/mondrian/calcite/MondrianBackend.java` — change default from `"legacy"` to `"calcite"`.
- Modify: `src/test/java/mondrian/calcite/MondrianBackendTest.java` — update `defaultsToLegacy` to `defaultsToCalcite`.

**Step 1: Flip + test update.**

**Step 2: Harness — two runs.**

- Default (now Calcite): expect partial red. Record in `docs/plans/2026-04-19-calcite-backend-foundations-checkpoint4.md` the exact pass/fail distribution.
- `-Dmondrian.backend=legacy`: expect 34/34 green.

**Step 3: Commit.**

```
feat(calcite): flip default backend to calcite

-Dmondrian.backend=legacy still works and stays green. New default
Calcite path is allowed to be partially red until worktree #2 lands
the RolapNative* migrations. Current pass rate captured in
docs/plans/2026-04-19-calcite-backend-foundations-checkpoint4.md.
```

---

## Task 10: Checkpoint #4 — first end-to-end MDX on Calcite

**Why:** Design-doc success criterion for worktree #1: "~5/31 — first end-to-end." The smoke corpus includes `basic-select`, `aggregate-measure`, `slicer-where`, `calc-member` (passing pushdown isn't here yet, but the *cell values* for simple calcs match because legacy evaluator still handles calcs above a Calcite-generated base query), `format-string`. Goal: these (or any ~5) pass under `backend=calcite` against the archived legacy goldens.

**Files:**
- Create: `docs/plans/2026-04-19-calcite-backend-foundations-checkpoint4.md` — checkpoint report.
- Whatever small translator gaps the harness surfaces in Tasks 6–9 that block specifically 5 queries.

**Step 1: Run smoke under Calcite.**

`mvn -Pcalcite-harness -Dmondrian.backend=calcite -Dtest=EquivalenceSmokeTest test 2>&1 | tee /tmp/checkpoint4.log`

**Step 2: For each of 5 target queries that fail, triage.**

One of:
- Translator bug → fix in `CalciteSqlPlanner` / `PlannerRequest`, one commit per fix.
- Schema-adapter bug → fix in `CalciteMondrianSchema`.
- Legitimate scope gap (e.g. uses a hierarchy form we deferred) → replace target with another corpus query.

**Step 3: Re-run smoke.** 5/20 passing under Calcite is the bar. Document the 15 that aren't (with failure class from the harness) in the checkpoint report.

**Step 4: Legacy sanity.** `mvn -Pcalcite-harness -Dmondrian.backend=legacy test` → 34/34.

**Step 5: Commit the checkpoint report.**

```
docs: checkpoint #4 — first end-to-end MDX on Calcite

5/20 smoke queries green under -Dmondrian.backend=calcite against
archived legacy goldens. Remaining 15 bucketed by failure class for
worktree #2 planning. Legacy path still 34/34.
```

---

## Task 11: Rebase onto `main` (safety valve)

**Why:** Design doc §Execution strategy: "Rebase each worktree weekly." Worktree #1 is the longest-running; doing a rebase dry-run at the end gives us a clean merge-readiness signal for the other worktrees to branch from.

**Step 1:** `git fetch origin && git log --oneline main..HEAD` — inspect the worktree's own commits.

**Step 2:** `git rebase origin/main` — resolve any conflicts (unlikely this early, but flush now).

**Step 3:** Re-run harness (both backends). Nothing should have moved, but confirm.

**Step 4:** Push if remote exists: `git push -u origin calcite-backend-foundations` (ASK THE USER before pushing — this is a shared-state action).

---

## Execution handoff

**Plan saved to `docs/plans/2026-04-19-calcite-backend-foundations.md`.** Two execution options:

1. **Subagent-Driven (this session)** — dispatch a fresh subagent per task, review between tasks.
2. **Parallel Session (separate)** — new session in `.worktrees/calcite-backend-foundations` runs executing-plans.

You asked for subagent-driven-development, so defaulting to option 1 unless you say otherwise.
