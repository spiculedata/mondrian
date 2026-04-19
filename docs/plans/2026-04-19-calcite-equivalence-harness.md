# Calcite Equivalence Harness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a test harness that detects any drift in MDX cell sets or emitted SQL rowsets when Apache Calcite is inserted into Mondrian's query pipeline, using unmodified Mondrian + H2 FoodMart as the golden baseline.

**Architecture:** One production-code seam (`SqlInterceptor` SPI in `RolapUtil.executeQuery`, default identity, opt-in via system property). All other code lives under `src/test/java/mondrian/test/calcite/`. Calcite 1.41.0 added as test-scope only. A `BaselineRecorder` captures cell set + every JDBC execution + rowset checksum against unmodified Mondrian into JSON goldens. An `EquivalenceHarness` re-runs each MDX with a `CalcitePassThrough` interceptor (parse + re-emit via Calcite, no-op in intent) and applies three layered gates: baseline-golden, MDX cell-set diff, SQL rowset diff — each with a distinct failure class.

**Tech Stack:** Java 11+, Maven, JUnit 4 (Mondrian's existing test framework), Apache Calcite 1.41.0 (`calcite-core`, `calcite-linq4j`), H2 FoodMart (existing Mondrian test fixture), Jackson (already transitively available via Mondrian) for golden JSON.

**Reference design doc:** `docs/plans/2026-04-19-calcite-equivalence-harness-design.md`

---

## Prerequisites

- Worktree already created at `.worktrees/calcite-equivalence-harness` on branch `calcite-equivalence-harness`.
- Design doc committed (`c545692`).
- Baseline `mvn test` on unmodified Mondrian must be green before Task 1. If not, triage is a blocker — the harness cannot distinguish "Calcite broke it" from "already broken".

---

## Task 1: Add Calcite as test-scope dependency

**Files:**
- Modify: `pom.xml` (dependencies section)

**Step 1: Locate the dependencies section**

Run: `grep -n "<dependencies>" pom.xml | head -3`
Expected: line number of the top-level `<dependencies>` block.

**Step 2: Add Calcite entries before `</dependencies>`**

```xml
<dependency>
  <groupId>org.apache.calcite</groupId>
  <artifactId>calcite-core</artifactId>
  <version>1.41.0</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.calcite</groupId>
  <artifactId>calcite-linq4j</artifactId>
  <version>1.41.0</version>
  <scope>test</scope>
</dependency>
```

**Step 3: Verify resolution**

Run: `mvn -DskipTests=true dependency:resolve -Dverbose | grep calcite`
Expected: `calcite-core:jar:1.41.0:test` resolves; no conflicts.

**Step 4: Verify existing tests still compile**

Run: `mvn test-compile`
Expected: `BUILD SUCCESS`.

**Step 5: Commit**

```bash
git add pom.xml
git commit -m "build: add apache calcite 1.41.0 (test scope)"
```

---

## Task 2: Production seam — `SqlInterceptor` SPI

**Files:**
- Create: `src/main/java/mondrian/rolap/sql/SqlInterceptor.java`
- Modify: `src/main/java/mondrian/rolap/RolapUtil.java` (the `executeQuery` method — find via `grep -n "executeQuery" src/main/java/mondrian/rolap/RolapUtil.java`)
- Test: `src/test/java/mondrian/test/calcite/SqlInterceptorTest.java`

**Step 1: Write the failing test**

```java
package mondrian.test.calcite;

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SqlInterceptorTest {
    @Test
    public void defaultInterceptorIsIdentity() {
        SqlInterceptor i = SqlInterceptor.IDENTITY;
        assertEquals("select 1", i.onSqlEmitted("select 1", null));
    }

    @Test
    public void systemPropertySelectsInterceptor() throws Exception {
        String prop = "mondrian.sqlInterceptor";
        String prev = System.getProperty(prop);
        try {
            System.setProperty(prop, RecordingInterceptor.class.getName());
            SqlInterceptor i = SqlInterceptor.loadFromSystemProperty();
            assertEquals("RECORDED:select 2", i.onSqlEmitted("select 2", null));
        } finally {
            if (prev == null) System.clearProperty(prop); else System.setProperty(prop, prev);
        }
    }

    public static class RecordingInterceptor implements SqlInterceptor {
        @Override public String onSqlEmitted(String sql, Dialect dialect) { return "RECORDED:" + sql; }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SqlInterceptorTest`
Expected: FAIL — `SqlInterceptor` does not exist.

**Step 3: Create the SPI**

```java
// src/main/java/mondrian/rolap/sql/SqlInterceptor.java
package mondrian.rolap.sql;

import mondrian.spi.Dialect;

public interface SqlInterceptor {
    String onSqlEmitted(String sql, Dialect dialect);

    SqlInterceptor IDENTITY = (sql, dialect) -> sql;

    static SqlInterceptor loadFromSystemProperty() {
        String cls = System.getProperty("mondrian.sqlInterceptor");
        if (cls == null || cls.isEmpty()) return IDENTITY;
        try {
            return (SqlInterceptor) Class.forName(cls).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load SqlInterceptor: " + cls, e);
        }
    }
}
```

**Step 4: Wire into `RolapUtil.executeQuery`**

Find the method that prepares SQL before JDBC execution (typically near `statement.executeQuery`). Add at the start of SQL preparation:

```java
// existing: String sql = ...;
sql = mondrian.rolap.sql.SqlInterceptor.loadFromSystemProperty()
    .onSqlEmitted(sql, dialect);
```

If the interceptor is loaded per-call it's fine for tests. If a perf regression is visible in the baseline suite (Task 9), cache it in a static volatile field keyed by system-property value.

**Step 5: Run tests to verify pass**

Run: `mvn test -Dtest=SqlInterceptorTest`
Expected: both tests PASS.

**Step 6: Run full Mondrian suite to verify no regression**

Run: `mvn test`
Expected: same pass/fail counts as the pre-Task-1 baseline captured in prerequisites.

**Step 7: Commit**

```bash
git add src/main/java/mondrian/rolap/sql/SqlInterceptor.java \
        src/main/java/mondrian/rolap/RolapUtil.java \
        src/test/java/mondrian/test/calcite/SqlInterceptorTest.java
git commit -m "feat(rolap): add SqlInterceptor SPI seam (default identity)"
```

---

## Task 3: `SqlCapture` — JDBC DataSource wrapper

**Files:**
- Create: `src/test/java/mondrian/test/calcite/SqlCapture.java`
- Create: `src/test/java/mondrian/test/calcite/CapturedExecution.java`
- Test: `src/test/java/mondrian/test/calcite/SqlCaptureTest.java`

**Step 1: Write the failing test**

```java
@Test
public void capturesSqlAndRowset() throws Exception {
    DataSource underlying = h2FoodMartDataSource();  // helper: reuses Mondrian's test DS
    SqlCapture capture = new SqlCapture(underlying);
    try (Connection c = capture.getConnection();
         PreparedStatement ps = c.prepareStatement("select 1 as x, 'a' as y")) {
        ps.executeQuery().close();
    }
    List<CapturedExecution> execs = capture.drain();
    assertEquals(1, execs.size());
    assertEquals("select 1 as x, 'a' as y", execs.get(0).sql);
    assertEquals(1, execs.get(0).rowCount);
    assertNotNull(execs.get(0).checksum);
}
```

**Step 2: Run to verify fail**

Run: `mvn test -Dtest=SqlCaptureTest`
Expected: FAIL — classes don't exist.

**Step 3: Implement `CapturedExecution` (data record)**

```java
public final class CapturedExecution {
    public final int seq;
    public final String sql;
    public final List<List<Object>> rowset;
    public final int rowCount;
    public final String checksum;  // sha256 of canonical rowset bytes
    // constructor + getters
}
```

**Step 4: Implement `SqlCapture`**

- Wrap `DataSource`, return a `Connection` proxy (JDK dynamic proxy or explicit wrapper).
- Wrap every `PreparedStatement`; on `executeQuery`, buffer the `ResultSet` into a `List<List<Object>>`, compute SHA-256 over a canonical encoding (column count, per-row `String.valueOf` of each cell joined by `\u001F`, rows joined by `\u001E`), then return a replaying `ResultSet` so the caller still works.
- Thread-safe append into a `CopyOnWriteArrayList<CapturedExecution>`; `drain()` snapshots + clears.

**Step 5: Run test**

Run: `mvn test -Dtest=SqlCaptureTest`
Expected: PASS.

**Step 6: Commit**

```bash
git add src/test/java/mondrian/test/calcite/SqlCapture.java \
        src/test/java/mondrian/test/calcite/CapturedExecution.java \
        src/test/java/mondrian/test/calcite/SqlCaptureTest.java
git commit -m "test: add SqlCapture DataSource wrapper with rowset checksums"
```

---

## Task 4: Smoke MDX corpus

**Files:**
- Create: `src/test/java/mondrian/test/calcite/corpus/SmokeCorpus.java`

**Step 1: Write the corpus**

```java
public final class SmokeCorpus {
    public static List<NamedMdx> queries() {
        return Arrays.asList(
            new NamedMdx("basic-select",
                "select [Measures].[Unit Sales] on columns from [Sales]"),
            new NamedMdx("crossjoin",
                "select NON EMPTY CrossJoin([Product].[Product Family].Members, " +
                "[Measures].[Unit Sales]) on columns from [Sales]"),
            new NamedMdx("non-empty",
                "select NON EMPTY [Product].[Product Family].Members on rows, " +
                "[Measures].[Unit Sales] on columns from [Sales]"),
            // ~20 total, one per major MDX construct
            // full list: calc members, named sets, time fns, slicers, drillthrough, etc.
            new NamedMdx("calc-member",
                "with member [Measures].[Double Sales] as [Measures].[Unit Sales] * 2 " +
                "select [Measures].[Double Sales] on columns from [Sales]")
        );
    }

    public static final class NamedMdx {
        public final String name;
        public final String mdx;
        public NamedMdx(String name, String mdx) { this.name = name; this.mdx = mdx; }
    }
}
```

**Step 2: Verify each MDX parses against FoodMart**

Quick smoke — extend `FoodMartTestCase` in a temporary `@Test` that runs each MDX via `TestContext.instance().executeQuery()` and asserts no exception. Remove the temp test after verifying; it'll be replaced by the real equivalence tests.

Run: `mvn test -Dtest=SmokeCorpusSanityTest`
Expected: all MDX strings execute.

**Step 3: Commit**

```bash
git add src/test/java/mondrian/test/calcite/corpus/SmokeCorpus.java
git commit -m "test: add smoke MDX corpus for calcite harness"
```

---

## Task 5: `BaselineRecorder` — golden generator

**Files:**
- Create: `src/test/java/mondrian/test/calcite/BaselineRecorder.java`
- Create: `src/test/resources/calcite-harness/golden/` (directory)
- Test: `src/test/java/mondrian/test/calcite/BaselineRecorderTest.java`

**Step 1: Write the failing test**

```java
@Test
public void recordsAndReRecordsDeterministically() throws Exception {
    Path goldenDir = tempGoldenDir();
    BaselineRecorder rec = new BaselineRecorder(goldenDir, TestContext.instance());
    rec.record(SmokeCorpus.queries());

    // Capture hashes of all golden files
    Map<String,String> firstRun = checksumAll(goldenDir);

    rec.record(SmokeCorpus.queries());
    Map<String,String> secondRun = checksumAll(goldenDir);

    assertEquals("rebaseline must be deterministic", firstRun, secondRun);
}
```

**Step 2: Run to verify fail**

**Step 3: Implement `BaselineRecorder`**

- Takes a `TestContext` (Mondrian's existing test fixture with FoodMart DS).
- For each `NamedMdx`: install `SqlCapture` on the `TestContext`'s DataSource (reflective setter — see how `DialectTest` does this; fall back to decorating the DS returned from `getConnection()`), run MDX via `context.executeQuery(mdx)`, capture the cell set via `TestContext.toString(result)`, drain `SqlCapture`.
- Canonicalize: sort column names in the captured rowset? **No** — column order matters for correctness. But **do** sort nothing — preserve natural order from the SQL. Determinism comes from H2 being single-threaded + identical FoodMart fixture.
- Write `<goldenDir>/<name>.json` via Jackson. Pretty-print so diffs are reviewable.

**Step 4: Run test**

Expected: PASS.

**Step 5: Add `BaselineRegenerationTest`**

```java
@Ignore("run manually with -Dharness.rebaseline=true")
public class BaselineRegenerationTest {
    @Test public void regenerate() {
        Assume.assumeTrue(Boolean.getBoolean("harness.rebaseline"));
        Path dir = Paths.get("src/test/resources/calcite-harness/golden");
        new BaselineRecorder(dir, TestContext.instance()).record(SmokeCorpus.queries());
    }
}
```

**Step 6: Generate initial goldens**

Run: `mvn test -Dtest=BaselineRegenerationTest -Dharness.rebaseline=true`
Expected: N json files written under `src/test/resources/calcite-harness/golden/`.

**Step 7: Commit**

```bash
git add src/test/java/mondrian/test/calcite/BaselineRecorder.java \
        src/test/java/mondrian/test/calcite/BaselineRecorderTest.java \
        src/test/java/mondrian/test/calcite/BaselineRegenerationTest.java \
        src/test/resources/calcite-harness/golden/
git commit -m "test: add BaselineRecorder and initial smoke goldens"
```

---

## Task 6: `CalcitePassThrough` — no-op Calcite interceptor

**Files:**
- Create: `src/test/java/mondrian/test/calcite/CalcitePassThrough.java`
- Test: `src/test/java/mondrian/test/calcite/CalcitePassThroughTest.java`

**Step 1: Write the failing test**

```java
@Test
public void roundTripsSimpleSelect() {
    CalcitePassThrough pt = new CalcitePassThrough();
    String roundTripped = pt.onSqlEmitted(
        "SELECT \"product_id\" FROM \"product\"",
        new H2Dialect());
    // Not asserting string equality — Calcite may re-format.
    // Assert it still parses back, and that it mentions the same columns/tables.
    assertTrue(roundTripped.toLowerCase().contains("product_id"));
    assertTrue(roundTripped.toLowerCase().contains("product"));
}

@Test
public void failOpensOnParseError() {
    CalcitePassThrough pt = new CalcitePassThrough();
    String garbage = "this is not sql ¯\\_(ツ)_/¯";
    assertEquals(garbage, pt.onSqlEmitted(garbage, new H2Dialect()));
}
```

**Step 2: Run to verify fail**

**Step 3: Implement**

```java
public class CalcitePassThrough implements SqlInterceptor {
    private final Logger log = Logger.getLogger(CalcitePassThrough.class.getName());

    @Override public String onSqlEmitted(String sql, Dialect dialect) {
        try {
            SqlParser.Config cfg = SqlParser.config()
                .withLex(mapLex(dialect));
            SqlNode node = SqlParser.create(sql, cfg).parseStmt();
            SqlDialect out = mapDialect(dialect);
            return node.toSqlString(out).getSql();
        } catch (Exception e) {
            log.warning("CalcitePassThrough parse failed; returning original SQL: " + e.getMessage());
            return sql;
        }
    }

    private Lex mapLex(Dialect d) {
        // map Mondrian dialect family → Calcite Lex
        // H2/Postgres → Lex.JAVA or Lex.MYSQL depending on quoting
        return Lex.JAVA;  // refine per dialect as Task 8 uncovers issues
    }

    private SqlDialect mapDialect(Dialect d) {
        // Mondrian's H2 dialect → Calcite H2Dialect. Start with H2 only.
        return H2SqlDialect.DEFAULT;
    }
}
```

**Step 4: Run test**

Expected: PASS.

**Step 5: Commit**

```bash
git add src/test/java/mondrian/test/calcite/CalcitePassThrough.java \
        src/test/java/mondrian/test/calcite/CalcitePassThroughTest.java
git commit -m "test: add CalcitePassThrough interceptor (H2 round-trip)"
```

---

## Task 7: `EquivalenceHarness` — the diff runner

**Files:**
- Create: `src/test/java/mondrian/test/calcite/EquivalenceHarness.java`
- Create: `src/test/java/mondrian/test/calcite/HarnessResult.java`
- Create: `src/test/java/mondrian/test/calcite/FailureClass.java` (enum: BASELINE_DRIFT, CELL_SET_DRIFT, SQL_ROWSET_DRIFT, PASS)
- Test: `src/test/java/mondrian/test/calcite/EquivalenceHarnessTest.java`

**Step 1: Write the failing test**

```java
@Test
public void passesWhenCalciteIsNoOp() {
    EquivalenceHarness h = new EquivalenceHarness(goldenDir(), TestContext.instance());
    HarnessResult r = h.run(SmokeCorpus.queries().get(0), new CalcitePassThrough());
    assertEquals(FailureClass.PASS, r.failureClass);
}

@Test
public void detectsBaselineDrift() {
    // Corrupt a golden file, run, assert BASELINE_DRIFT
    Path badDir = copyGoldensAndMutate();
    EquivalenceHarness h = new EquivalenceHarness(badDir, TestContext.instance());
    HarnessResult r = h.run(SmokeCorpus.queries().get(0), SqlInterceptor.IDENTITY);
    assertEquals(FailureClass.BASELINE_DRIFT, r.failureClass);
}
```

**Step 2: Run to verify fail**

**Step 3: Implement the three-gate pipeline**

1. Run classic (no interceptor) → capture cell set + execs.
2. Load golden for this MDX name.
3. If classic cell set or exec rowsets differ from golden → `BASELINE_DRIFT`.
4. Run with interceptor → capture cell set + execs.
5. If interceptor cell set differs from classic → `CELL_SET_DRIFT`.
6. For each exec pair, if rowset checksums differ → `SQL_ROWSET_DRIFT`.
7. Else → `PASS`.

`HarnessResult` carries both runs' artefacts so the report in Task 9 can render diffs.

**Step 4: Run tests**

Expected: PASS.

**Step 5: Commit**

```bash
git add src/test/java/mondrian/test/calcite/EquivalenceHarness.java \
        src/test/java/mondrian/test/calcite/HarnessResult.java \
        src/test/java/mondrian/test/calcite/FailureClass.java \
        src/test/java/mondrian/test/calcite/EquivalenceHarnessTest.java
git commit -m "test: add EquivalenceHarness with three-gate pipeline"
```

---

## Task 8: Parameterized smoke equivalence test

**Files:**
- Create: `src/test/java/mondrian/test/calcite/EquivalenceSmokeTest.java`

**Step 1: Write the test**

```java
@RunWith(Parameterized.class)
public class EquivalenceSmokeTest {
    @Parameters(name = "{0}") public static Collection<Object[]> data() {
        return SmokeCorpus.queries().stream()
            .map(q -> new Object[]{q.name, q.mdx}).collect(Collectors.toList());
    }

    private final String name, mdx;
    public EquivalenceSmokeTest(String name, String mdx) { this.name = name; this.mdx = mdx; }

    @Test public void equivalent() {
        Path golden = Paths.get("src/test/resources/calcite-harness/golden");
        EquivalenceHarness h = new EquivalenceHarness(golden, TestContext.instance());
        HarnessResult r = h.run(new SmokeCorpus.NamedMdx(name, mdx), new CalcitePassThrough());
        assertEquals("drift detected: " + r.detail, FailureClass.PASS, r.failureClass);
    }
}
```

**Step 2: Run**

Run: `mvn test -Dtest=EquivalenceSmokeTest`
Expected: all smoke queries PASS. Any that fail → triage:
  - Parse failure in `CalcitePassThrough` → refine `mapLex`/`mapDialect` for the offending construct.
  - Rowset drift → Calcite's re-emission is semantically different for that SQL shape → either (a) teach Calcite to preserve it, or (b) document as a known dialect-fidelity issue.

**Step 3: Commit (after all triaging resolved)**

```bash
git add src/test/java/mondrian/test/calcite/EquivalenceSmokeTest.java
git commit -m "test: parameterized smoke equivalence test (20 MDX)"
```

---

## Task 9: Maven profile + HTML report

**Files:**
- Modify: `pom.xml` (profiles)
- Create: `src/test/java/mondrian/test/calcite/HarnessReporter.java`

**Step 1: Add profile to `pom.xml`**

```xml
<profiles>
  <profile>
    <id>calcite-harness</id>
    <build>
      <plugins>
        <plugin>
          <artifactId>maven-surefire-plugin</artifactId>
          <configuration>
            <includes>
              <include>**/mondrian/test/calcite/Equivalence*Test.java</include>
            </includes>
          </configuration>
        </plugin>
      </plugins>
    </build>
  </profile>
</profiles>
```

**Step 2: Default Surefire must exclude `calcite-harness` tests**

Add an `<excludes>` in the main `maven-surefire-plugin` config excluding the `calcite` package. Harness only runs under `-Pcalcite-harness`. This keeps mainline `mvn test` untouched.

**Step 3: Implement `HarnessReporter`**

- `@AfterClass` hook on `EquivalenceSmokeTest` that writes `target/calcite-harness-report.html`.
- Per-query row: name, status, failure class, side-by-side SQL (if `SQL_ROWSET_DRIFT`), rowset diff snippet.

**Step 4: Verify**

Run: `mvn test -Pcalcite-harness`
Expected: harness tests run, report generated. Default `mvn test` still excludes harness.

**Step 5: Commit**

```bash
git add pom.xml src/test/java/mondrian/test/calcite/HarnessReporter.java
git commit -m "build: calcite-harness maven profile + html reporter"
```

---

## Task 10: Mutation test — prove the gates have teeth

**Files:**
- Temporary edit to `CalcitePassThrough.java` (must be reverted before merge).

**Step 1: Introduce a deliberate bug**

In `CalcitePassThrough.onSqlEmitted`, after parsing, walk the tree and replace any `=` operator with `<>` using a `SqlShuttle`.

**Step 2: Run harness**

Run: `mvn test -Pcalcite-harness -Dtest=EquivalenceSmokeTest`
Expected: FAIL with `CELL_SET_DRIFT` or `SQL_ROWSET_DRIFT` on queries that have `=` in their emitted SQL. If **zero** tests fail, the harness is toothless — stop and fix it before proceeding.

**Step 3: Revert the mutation**

```bash
git checkout -- src/test/java/mondrian/test/calcite/CalcitePassThrough.java
```

**Step 4: Re-run to confirm green**

Run: `mvn test -Pcalcite-harness`
Expected: all PASS.

**Step 5: Record mutation-test outcome in design doc appendix**

Append to `docs/plans/2026-04-19-calcite-equivalence-harness-design.md`:

> ## Mutation test result (YYYY-MM-DD)
> Replaced `=` with `<>` in `CalcitePassThrough`. N of M queries reported `<FAILURE_CLASS>`. Harness verified to have teeth.

**Step 6: Commit**

```bash
git add docs/plans/2026-04-19-calcite-equivalence-harness-design.md
git commit -m "docs: record harness mutation-test outcome"
```

---

## Task 11: Aggregate-query corpus (tier 3)

**Files:**
- Create: `src/test/java/mondrian/test/calcite/corpus/AggregateCorpus.java`
- Create: `src/test/java/mondrian/test/calcite/EquivalenceAggregateTest.java`

**Step 1: Scrape MDX queries from Mondrian's existing aggregate tests**

Source: `AggregationOnDistinctCountMeasuresTest`, `AggregationManagerTest`, native-evaluator tests. Extract ~10 MDX strings that hit the agg-table path.

**Step 2: Add corpus + parameterized test mirroring Task 8**

**Step 3: Generate goldens, run equivalence, triage**

Run: `mvn test -Dtest=BaselineRegenerationTest -Dharness.rebaseline=true`
Run: `mvn test -Pcalcite-harness -Dtest=EquivalenceAggregateTest`
Expected: PASS. Any drift → triage same as Task 8.

**Step 4: Commit**

```bash
git add src/test/java/mondrian/test/calcite/corpus/AggregateCorpus.java \
        src/test/java/mondrian/test/calcite/EquivalenceAggregateTest.java \
        src/test/resources/calcite-harness/golden/
git commit -m "test: aggregate-query corpus and equivalence test"
```

---

## Task 12: Done criteria verification + merge prep

**Step 1: Verify all done criteria from design doc**

1. Baseline recorder deterministic → re-run `BaselineRegenerationTest` twice, `git diff src/test/resources/calcite-harness/golden/` must be empty.
2. `CalcitePassThrough` round-trips whole corpus without parse failures → check logs.
3. `EquivalenceHarness` reports zero drift → `mvn test -Pcalcite-harness` green.
4. Mutation test documented (Task 10).
5. Design doc present (pre-existing, `c545692`).

**Step 2: Run the existing Mondrian suite one more time**

Run: `mvn test`
Expected: same pass/fail counts as the pre-Task-1 baseline. The `SqlInterceptor` seam must not regress anything.

**Step 3: Squash-review history (optional but recommended)**

```bash
git log --oneline calcite-equivalence-harness ^jakarta-servlet
```

**Step 4: Finish the branch**

Use superpowers:finishing-a-development-branch to decide merge vs PR vs further work.

---

## Skills to use during execution

- `@superpowers:test-driven-development` for every task (test first, verify fail, implement, verify pass).
- `@superpowers:verification-before-completion` before claiming any task done — run the command, read the output.
- `@superpowers:systematic-debugging` when triaging harness failures in Tasks 8 and 11 — do not guess at dialect mappings.
- `@superpowers:receiving-code-review` if reviews come back on this plan.

## Non-goals / guardrails during execution

- Do not promote Calcite to compile scope.
- Do not change `SqlQuery.java` or any other SQL-builder class.
- Do not add a second production-code touchpoint — if you think you need one, stop and escalate.
- Do not skip the mutation test (Task 10) — without it, the harness is unproven.
