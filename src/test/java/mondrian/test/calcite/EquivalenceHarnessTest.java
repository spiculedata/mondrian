/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.test.calcite;

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.SmokeCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies the three-gate pipeline of {@link EquivalenceHarness}:
 * <ul>
 *   <li>{@link #passesWhenInterceptorIsIdentityAndGoldenMatches()} — all gates
 *       pass with an identity interceptor and the committed goldens.</li>
 *   <li>{@link #detectsBaselineDriftWhenGoldenIsCorrupt()} — Gate 1 fires
 *       when the golden is tampered with.</li>
 *   <li>{@link #detectsSqlRowsetDriftUnderMutatingInterceptor()} — Gate 2 or
 *       3 fires when the interceptor deliberately changes SQL semantics.
 *       Doubles as the mutation-test dress rehearsal: if neither post-gate-1
 *       failure class is raised, the harness has no teeth.</li>
 * </ul>
 */
public class EquivalenceHarnessTest {

    private static final Path GOLDEN_DIR =
        Paths.get("src/test/resources/calcite-harness/golden");

    private String priorSysProp;

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Before
    public void captureSysProp() {
        priorSysProp = System.getProperty(EquivalenceHarness.SYS_PROP);
        // Start each test clean — no leakage from prior runs.
        System.clearProperty(EquivalenceHarness.SYS_PROP);
    }

    @After
    public void restoreSysProp() {
        String now = System.getProperty(EquivalenceHarness.SYS_PROP);
        // The harness must restore the sys-prop on the way out. If a test
        // leaked it, we still clean up here — but we assert the contract.
        assertEquals(
            "harness leaked mondrian.sqlInterceptor system property",
            priorSysProp, now);
        if (priorSysProp == null) {
            System.clearProperty(EquivalenceHarness.SYS_PROP);
        } else {
            System.setProperty(EquivalenceHarness.SYS_PROP, priorSysProp);
        }
    }

    @Test
    public void passesWhenInterceptorIsIdentityAndGoldenMatches()
        throws Exception
    {
        EquivalenceHarness h = new EquivalenceHarness(GOLDEN_DIR);
        NamedMdx q = SmokeCorpus.queries().get(0); // basic-select
        HarnessResult r = h.run(q, IdentityInterceptor.class);
        assertEquals(
            "expected PASS; detail was: " + r.detail,
            FailureClass.PASS, r.failureClass);
    }

    @Test
    public void detectsBaselineDriftWhenGoldenIsCorrupt() throws Exception {
        Path tempGolden = Files.createTempDirectory("eh-bad-golden");
        // Copy real goldens over, then mutate basic-select.json's cellSet.
        try (Stream<Path> paths = Files.list(GOLDEN_DIR)) {
            paths.forEach(p -> {
                try {
                    Files.copy(
                        p,
                        tempGolden.resolve(p.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        NamedMdx basicSelect = SmokeCorpus.queries().get(0);
        Path target = tempGolden.resolve(basicSelect.name + ".json");
        String content =
            new String(Files.readAllBytes(target), StandardCharsets.UTF_8);
        // Flip a character inside cellSet so baseline compare fails.
        String mutated =
            content.replace("Unit Sales", "UNIT SALES TAMPERED");
        assertNotEquals(
            "test-setup guard: mutation did not change file",
            content, mutated);
        Files.write(target, mutated.getBytes(StandardCharsets.UTF_8));

        EquivalenceHarness h = new EquivalenceHarness(tempGolden);
        HarnessResult r = h.run(basicSelect, IdentityInterceptor.class);
        assertEquals(
            "expected BASELINE_DRIFT; detail=" + r.detail,
            FailureClass.BASELINE_DRIFT, r.failureClass);
        assertNull(
            "Run B must not have been executed once Gate 1 tripped",
            r.runBCellSet);
    }

    @Test
    public void detectsSqlRowsetDriftUnderMutatingInterceptor()
        throws Exception
    {
        EquivalenceHarness h = new EquivalenceHarness(GOLDEN_DIR);
        // basic-select's captured SQL contains `"the_year" = 1997`.
        // The mutating interceptor rewrites 1997 → 1998 so the rowset
        // changes (empty), but the SQL still parses cleanly.
        NamedMdx q = SmokeCorpus.queries().get(0);
        HarnessResult r = h.run(q, YearMutatingInterceptor.class);
        // Surface which failure class got raised — helpful when the mutation
        // test is used as a dress rehearsal for downstream report writers.
        System.out.println(
            "[mutation-test] YearMutatingInterceptor -> "
            + r.failureClass + " :: "
            + r.detail.replace('\n', ' '));
        assertNotEquals(
            "harness flagged no drift — mutation test has no teeth; detail=" + r.detail,
            FailureClass.PASS, r.failureClass);
        assertNotEquals(
            "Gate 1 should not fire; baseline run used identity interceptor",
            FailureClass.BASELINE_DRIFT, r.failureClass);
    }

    /** Identity: returns SQL unchanged. */
    public static class IdentityInterceptor implements SqlInterceptor {
        @Override
        public String onSqlEmitted(String sql, Dialect dialect) {
            return sql;
        }
    }

    /**
     * Rewrites literal predicates like {@code = 1997} to {@code = 1998}.
     * Cheap, parseable mutation that forces FoodMart's single-year dataset
     * to return zero rows for predicates on {@code the_year}, triggering
     * SQL_ROWSET_DRIFT (or CELL_SET_DRIFT further upstream).
     *
     * <p>Narrow pattern on purpose — a bare textual replace of {@code 1997}
     * would corrupt the FoodMart agg-table names (e.g.
     * {@code agg_c_special_sales_fact_1997}) and crash schema load.
     */
    public static class YearMutatingInterceptor implements SqlInterceptor {
        @Override
        public String onSqlEmitted(String sql, Dialect dialect) {
            return sql.replace("= 1997", "= 1998");
        }
    }
}
