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

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Corpus-wide mutation witness: runs the smoke corpus against the
 * {@link EquivalenceHarness} with {@link MutatingCalcitePassThrough} — which
 * rewrites {@code =} to {@code &lt;&gt;} in predicate clauses — and asserts
 * the harness flags drift for at least a handful of queries.
 *
 * <p>Load-bearing: if some future refactor makes the harness blind to a
 * deliberate semantic mutation, this test breaks loudly. It is the
 * permanent dress-rehearsal complement to
 * {@link EquivalenceHarnessTest#detectsSqlRowsetDriftUnderMutatingInterceptor}.
 *
 * <p>Runs the full 20-query corpus (~30-60s), so it is gated behind the
 * {@code calcite-harness} Maven profile alongside
 * {@link EquivalenceSmokeTest}.
 */
public class HarnessMutationTest {

    private static final Path GOLDEN_DIR =
        Paths.get("src/test/resources/calcite-harness/golden");

    /**
     * Minimum number of queries (out of 20) that must register a
     * non-PASS / non-LEGACY_DRIFT failure class for the harness to be
     * considered to "have teeth". Three is robust against single-query
     * coincidences while still tolerating queries that have no predicate
     * {@code =} to rewrite.
     */
    private static final int MIN_DRIFT_COUNT = 3;

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Test
    public void harnessCatchesEqualsToNotEqualsMutation() throws Exception {
        EquivalenceHarness h = new EquivalenceHarness(GOLDEN_DIR);
        int drifted = 0;
        int passed = 0;
        int legacyDrift = 0;
        Map<FailureClass, Integer> classCounts = new LinkedHashMap<>();
        List<String> driftDetails = new ArrayList<>();

        for (NamedMdx q : SmokeCorpus.queries()) {
            HarnessResult r = h.run(q, MutatingCalcitePassThrough.class);
            classCounts.merge(r.failureClass, 1, Integer::sum);
            switch (r.failureClass) {
            case PASS:
                passed++;
                break;
            case LEGACY_DRIFT:
                legacyDrift++;
                // Do not count legacy drift as a caught mutation —
                // it would mean the goldens are wrong, not that the
                // harness spotted our injected semantic change.
                break;
            case CELL_SET_DRIFT:
            case SQL_ROWSET_DRIFT:
            case PLAN_DRIFT:
            default:
                drifted++;
                driftDetails.add(q.name + " -> " + r.failureClass);
                break;
            }
        }

        System.out.println(
            "[HarnessMutationTest] drift summary: " + classCounts
            + " total=" + SmokeCorpus.queries().size()
            + " drifted=" + drifted
            + " passed=" + passed
            + " legacyDrift=" + legacyDrift);
        for (String d : driftDetails) {
            System.out.println("  " + d);
        }

        assertTrue(
            "Harness failed to catch deliberate = -> <> mutation across corpus."
            + " drifted=" + drifted
            + " (required >= " + MIN_DRIFT_COUNT + ")"
            + " classes=" + classCounts
            + " details=" + driftDetails,
            drifted >= MIN_DRIFT_COUNT);
    }
}

// End HarnessMutationTest.java
