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

import mondrian.test.FoodMartHsqldbBootstrap;
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
    }
}

// End EquivalenceCalcTest.java
