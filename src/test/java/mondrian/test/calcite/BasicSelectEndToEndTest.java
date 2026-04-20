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
import mondrian.rolap.agg.SegmentLoader;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.SmokeCorpus;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end verification that the {@code basic-select} MDX runs under the
 * Calcite backend with SQL emitted by
 * {@link mondrian.calcite.CalciteSqlPlanner} (not the legacy fallback) and
 * produces a cell-set identical to the archived legacy golden.
 *
 * <p>This is the first genuine end-to-end translation in the rewrite: the
 * segment-load SQL actually goes through Calcite's RelBuilder +
 * RelToSqlConverter. Cell-set parity is the hard gate; SQL-string drift is
 * advisory (by the Task A split).
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
        CalcitePlannerAdapters.resetFallbackCount();
        SegmentLoader.clearCalcitePlannerCache();
        System.setProperty("mondrian.backend", "calcite");
    }

    @Test public void basicSelectCellSetMatchesLegacyGolden()
        throws Exception
    {
        // Load the archived legacy golden so we know what cell-set to
        // expect — it was recorded against the legacy backend and is the
        // contract Calcite must match.
        ObjectMapper mapper = new ObjectMapper();
        JsonNode golden = mapper.readTree(GOLDEN.toFile());
        String mdx = golden.path("mdx").asText();
        String expectedCellSet = golden.path("cellSet").asText();
        assertTrue(
            "golden must carry basic-select MDX",
            mdx.toLowerCase().contains("unit sales"));

        long segmentFallbacksBefore =
            CalcitePlannerAdapters.segmentLoadFallbackCount();

        // Run with the Calcite backend.
        SmokeCorpus.NamedMdx named =
            new SmokeCorpus.NamedMdx("basic-select", mdx);
        FoodMartCapture.CapturedRun run =
            FoodMartCapture.executeCold(named, null);

        assertEquals(
            "basic-select cell-set must match archived legacy golden",
            expectedCellSet, run.cellSet);

        long segmentFallbacksAfter =
            CalcitePlannerAdapters.segmentLoadFallbackCount();

        // The hard gate for this task: segment-load translation must have
        // succeeded every time during this query. Tuple-read fallbacks are
        // still expected (they cover cardinality probes / member reads
        // that land in later worktrees).
        assertEquals(
            "basic-select must not fall back to legacy SQL at segment-load;"
                + " delta=" + (segmentFallbacksAfter - segmentFallbacksBefore),
            0L, segmentFallbacksAfter - segmentFallbacksBefore);
    }
}

// End BasicSelectEndToEndTest.java
