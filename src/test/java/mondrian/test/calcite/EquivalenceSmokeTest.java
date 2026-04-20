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
import mondrian.test.calcite.corpus.SmokeCorpus;
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
 * Parameterized smoke equivalence test that runs {@link EquivalenceHarness}
 * against every MDX in {@link SmokeCorpus} with {@link CalcitePassThrough} as
 * the interceptor, asserting {@link FailureClass#PASS} for each.
 *
 * <p>Task 8 of the Calcite Equivalence Harness plan.
 */
@RunWith(Parameterized.class)
public class EquivalenceSmokeTest {

    private static final Path GOLDEN_DIR =
        Paths.get("src/test/resources/calcite-harness/golden");

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return SmokeCorpus.queries().stream()
            .map(q -> new Object[]{q.name, q})
            .collect(Collectors.toList());
    }

    private final String name;
    private final NamedMdx mdx;

    public EquivalenceSmokeTest(String name, NamedMdx mdx) {
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

// End EquivalenceSmokeTest.java
