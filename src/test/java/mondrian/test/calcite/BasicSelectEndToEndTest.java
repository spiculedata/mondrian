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
 * End-to-end cell-set parity check for {@code basic-select} under the
 * Calcite backend.
 *
 * <p>Task E unblocked Sales-cube schema initialization by teaching
 * {@code CalcitePlannerAdapters.fromTupleRead} the single-level
 * member-list shape. With segment-load already translated (Task B),
 * basic-select now runs end-to-end through the Calcite path — this test
 * locks the cell-set to the archived legacy golden so we notice any
 * regression. The captured SQL text will look different from the legacy
 * dialect (Calcite renders identifiers / joins in its own idiom), but
 * the numeric result is the contract.
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

    @Test public void basicSelectMatchesGoldenCellSet() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode golden = mapper.readTree(GOLDEN.toFile());
        String mdx = golden.path("mdx").asText();
        String goldenCellSet = golden.path("cellSet").asText();
        assertTrue(
            "golden must carry basic-select MDX",
            mdx.toLowerCase().contains("unit sales"));
        assertTrue(
            "golden must carry a serialized cell-set",
            goldenCellSet != null && !goldenCellSet.isEmpty());

        SmokeCorpus.NamedMdx named =
            new SmokeCorpus.NamedMdx("basic-select", mdx);
        FoodMartCapture.CapturedRun run =
            FoodMartCapture.executeCold(named, null);

        assertEquals(
            "cell-set must match legacy golden under calcite backend",
            goldenCellSet, run.cellSet);
    }
}

// End BasicSelectEndToEndTest.java
