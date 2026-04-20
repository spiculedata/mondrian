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

import mondrian.calcite.ArithmeticCalcAnalyzer;
import mondrian.calcite.CalcitePlannerAdapters;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.Formula;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;
import mondrian.test.calcite.corpus.CalcCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Parameterized equivalence test for the tier-4 calc-member corpus.
 * Exact mirror of {@link EquivalenceAggregateTest} but iterating over
 * {@link CalcCorpus#queries()}. Shares the single {@link HarnessReporter}
 * sink so the emitted HTML covers all suites when they are executed
 * together (e.g. via the {@code calcite-harness} profile).
 *
 * <p>Task S of the Calcite backend rewrite plan. Under legacy this suite
 * is expected green once goldens are recorded. Under Calcite (pre Task T)
 * the pushable arithmetic entries are expected to fail with an
 * unsupported-translation surface until Task T implements the pushdown.
 * The two {@code calc-non-pushable-*} control entries should stay green
 * on both backends because dimensional navigation remains on the Java
 * evaluator.
 */
@RunWith(Parameterized.class)
public class EquivalenceCalcTest {

    private static final Path GOLDEN_DIR =
        Paths.get("src/test/resources/calcite-harness/golden");

    /** System-property flag: when {@code true}, classify each query's
     *  calc members via {@link ArithmeticCalcAnalyzer} and assert the
     *  expected pushable/non-pushable verdict. Default false so the
     *  existing harness run stays fast. */
    private static final String ASSERT_PUSHDOWN_PROP =
        "harness.assertCalcPushdown";

    /** Names prefixed by this string are non-pushable controls. */
    private static final String NON_PUSHABLE_PREFIX = "calc-non-pushable-";

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return CalcCorpus.queries().stream()
            .map(q -> new Object[]{q.name, q})
            .collect(Collectors.toList());
    }

    private final String name;
    private final NamedMdx mdx;

    public EquivalenceCalcTest(String name, NamedMdx mdx) {
        this.name = name;
        this.mdx = mdx;
    }

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @AfterClass
    public static void writeReport() throws Exception {
        HarnessReporter.writeHtml(
            Paths.get("target/calcite-harness-report.html"));
    }

    @Test
    public void equivalent() throws Exception {
        EquivalenceHarness h = new EquivalenceHarness(GOLDEN_DIR);
        HarnessResult r = h.run(mdx, CalcitePassThrough.class);
        HarnessReporter.record(mdx.name, r);
        assertEquals(
            "drift: " + r.failureClass + " - " + r.detail,
            FailureClass.PASS, r.failureClass);

        if (Boolean.getBoolean(ASSERT_PUSHDOWN_PROP)) {
            assertPushdownVerdict();
        }
    }

    /** Parse the MDX, extract formulas, classify each and verify
     *  pushable/non-pushable per the corpus convention: names starting
     *  with {@link #NON_PUSHABLE_PREFIX} must classify as non-pushable
     *  (rejectedCount bumped); all others must classify as pushable
     *  (pushedCount bumped). Uses the module-internal
     *  {@link ArithmeticCalcAnalyzer} and ticks the test-visible
     *  {@link CalcitePlannerAdapters} counters. */
    private void assertPushdownVerdict() {
        CalcitePlannerAdapters.resetCalcPushdownCounters();

        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        Connection conn = DriverManager.getConnection(props, null);
        try {
            Query q = conn.parseQuery(mdx.mdx);
            q.resolve();
            int pushed = 0;
            int rejected = 0;
            for (Formula f : q.getFormulas()) {
                if (f.getMdxMember() == null) continue;
                ArithmeticCalcAnalyzer.Classification c =
                    ArithmeticCalcAnalyzer.classify(
                        f.getExpression(),
                        java.util.Collections.emptySet());
                if (c.isPushable()) {
                    pushed++;
                } else {
                    rejected++;
                }
            }
            // Tick the public-facing counters so external observers
            // (CI, report) see the same numbers.
            for (int i = 0; i < pushed; i++) {
                mondrian.calcite.CalcPushdownRegistry.onPushed();
            }
            for (int i = 0; i < rejected; i++) {
                mondrian.calcite.CalcPushdownRegistry.onRejected();
            }

            boolean isControl = mdx.name.startsWith(NON_PUSHABLE_PREFIX);
            if (isControl) {
                assertTrue(
                    "control query " + mdx.name
                    + " must classify as non-pushable; pushed="
                    + pushed + " rejected=" + rejected,
                    CalcitePlannerAdapters.calcRejectedCount() > 0);
            } else {
                assertTrue(
                    "pushable query " + mdx.name
                    + " must classify as pushable; pushed="
                    + pushed + " rejected=" + rejected,
                    CalcitePlannerAdapters.calcPushedCount() > 0);
            }
        } finally {
            conn.close();
        }
    }
}

// End EquivalenceCalcTest.java
