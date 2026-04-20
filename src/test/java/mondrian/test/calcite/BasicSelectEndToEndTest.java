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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import mondrian.calcite.CalcitePlannerAdapters;
import mondrian.calcite.UnsupportedTranslation;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.SmokeCorpus;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end verification of the {@code basic-select} MDX under the Calcite
 * backend. Under the no-fallback policy (see
 * {@code 2026-04-19-calcite-backend-foundations-checkpoint4.md § Policy
 * change: no fallback}), schema initialization for the Sales cube calls
 * {@code SqlTupleReader.readMembers} to resolve the default member on each
 * hierarchy, which routes through
 * {@code CalcitePlannerAdapters.fromTupleRead} — and tuple-read translation
 * is deferred to worktree #2. So the query currently fails loud with
 * {@link UnsupportedTranslation}.
 *
 * <p>Prior to the policy change this test asserted cell-set parity with
 * the archived golden because the translation gap fell back to legacy SQL.
 * With fallback removed the assertion flips: this test now documents the
 * hard-fail contract. When worktree #2 lands tuple-read translation, this
 * test flips back to asserting cell-set parity.
 *
 * <p>Lives under {@code mondrian.test.calcite} (not {@code mondrian.calcite})
 * so it shares the harness's package-private {@code FoodMartCapture} plumbing
 * and isn't mixed in with tests that open FoodMart in read-only mode.
 */
public class BasicSelectEndToEndTest {

    private static final Path GOLDEN =
        Paths.get(
            "src/test/resources/calcite-harness/"
            + "golden-legacy/basic-select.json");

    @BeforeClass public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @AfterClass public static void clearBackend() {
        System.clearProperty("mondrian.backend");
        SegmentLoader.clearCalcitePlannerCache();
    }

    @Before public void reset() {
        CalcitePlannerAdapters.resetUnsupportedCount();
        SegmentLoader.clearCalcitePlannerCache();
        System.setProperty("mondrian.backend", "calcite");
    }

    @Test public void basicSelectFailsLoudOnUnsupportedTupleRead()
        throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode golden = mapper.readTree(GOLDEN.toFile());
        String mdx = golden.path("mdx").asText();
        assertTrue(
            "golden must carry basic-select MDX",
            mdx.toLowerCase().contains("unit sales"));

        long tupleReadUnsupportedBefore =
            CalcitePlannerAdapters.tupleReadUnsupportedCount();

        SmokeCorpus.NamedMdx named =
            new SmokeCorpus.NamedMdx("basic-select", mdx);

        try {
            FoodMartCapture.executeCold(named, null);
            fail(
                "expected UnsupportedTranslation under no-fallback policy"
                + " (tuple-read translation is deferred to worktree #2)");
        } catch (Exception e) {
            Throwable t = e;
            while (t != null && !(t instanceof UnsupportedTranslation)) {
                t = t.getCause();
            }
            assertTrue(
                "expected UnsupportedTranslation somewhere in the cause"
                + " chain; actual: " + e,
                t instanceof UnsupportedTranslation);
        }

        long tupleReadUnsupportedAfter =
            CalcitePlannerAdapters.tupleReadUnsupportedCount();

        // Observability: the dispatch seam must have been exercised at
        // least once (schema load calls readMembers for default-member
        // resolution on cube init).
        assertTrue(
            "tuple-read dispatch must have fired at least once; delta="
                + (tupleReadUnsupportedAfter - tupleReadUnsupportedBefore),
            tupleReadUnsupportedAfter - tupleReadUnsupportedBefore > 0);
    }
}

// End BasicSelectEndToEndTest.java
