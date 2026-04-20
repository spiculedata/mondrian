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

import mondrian.calcite.MondrianBackend;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.MvHitCorpus;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Task-U.1: prove the Mondrian-4 aggregate {@code MeasureGroup} path works
 * under both the legacy and Calcite backends.
 *
 * <p>For each entry in {@link MvHitCorpus}, runs the MDX twice (once under
 * {@code -Dmondrian.backend=legacy}, once under {@code calcite}), captures
 * ALL emitted SQL via {@link SqlCapture}, and asserts:
 * <ol>
 *   <li>At least one captured SQL statement's {@code FROM} clause names
 *       one of the entry's {@link MvHitCorpus.Entry#acceptableAggTables}
 *       (hard gate — proves {@code RolapGalaxy.findAgg} selected the agg
 *       MeasureGroup, not the base fact table).</li>
 *   <li>Cell-set byte-identity between the two backends (parity gate —
 *       proves Calcite's SQL against the aggregate table yields the same
 *       numerical result as the legacy SQL).</li>
 * </ol>
 *
 * <p><b>Property toggling:</b> {@code MondrianProperties.ReadAggregates}
 * and {@code UseAggregates} default to {@code false}. This test flips both
 * to {@code true} for the duration of each parameterised run (via
 * {@link Before}/{@link After}), which is the minimum needed to activate
 * {@code RolapGalaxy.findAgg}. No runtime or production-default change.
 *
 * <p>Closes the coverage gap left by the original Task U: the existing
 * {@code AggregateCorpus} uses {@code [Customer Count]} (distinct-count,
 * non-additive) on shapes no agg satisfies, so the matcher always falls
 * back to the base fact. The queries here are Unit-Sales-on-agg-native
 * grain, letting rollup fire through the additive path.
 */
@RunWith(Parameterized.class)
public class MvHitTest {

    /** All four declared agg tables, for diagnostic SQL-scraping (so a
     *  wrong-agg hit is distinguishable from a base-fact fallback). */
    private static final List<String> ALL_DECLARED_AGGS = Arrays.asList(
        "agg_c_special_sales_fact_1997",
        "agg_l_05_sales_fact_1997",
        "agg_c_14_sales_fact_1997",
        "agg_g_ms_pcat_sales_fact_1997");

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return MvHitCorpus.entries().stream()
            .map(e -> new Object[]{e.mdx.name, e})
            .collect(Collectors.toList());
    }

    private final String name;
    private final MvHitCorpus.Entry entry;

    private boolean prevReadAgg;
    private boolean prevUseAgg;
    private String prevBackend;

    public MvHitTest(String name, MvHitCorpus.Entry entry) {
        this.name = name;
        this.entry = entry;
    }

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Before
    public void enableAggregates() {
        MondrianProperties p = MondrianProperties.instance();
        prevReadAgg = p.ReadAggregates.get();
        prevUseAgg = p.UseAggregates.get();
        p.ReadAggregates.set(true);
        p.UseAggregates.set(true);
        prevBackend = System.getProperty(MondrianBackend.PROPERTY);
        SegmentLoader.clearCalcitePlannerCache();
    }

    @After
    public void restoreState() {
        MondrianProperties p = MondrianProperties.instance();
        p.ReadAggregates.set(prevReadAgg);
        p.UseAggregates.set(prevUseAgg);
        if (prevBackend == null) {
            System.clearProperty(MondrianBackend.PROPERTY);
        } else {
            System.setProperty(MondrianBackend.PROPERTY, prevBackend);
        }
        SegmentLoader.clearCalcitePlannerCache();
    }

    @Test
    public void aggregateTableFiresUnderBothBackends() {
        // --- Run under legacy backend ---
        System.setProperty(MondrianBackend.PROPERTY, "legacy");
        SegmentLoader.clearCalcitePlannerCache();
        FoodMartCapture.CapturedRun legacy =
            FoodMartCapture.executeCold(entry.mdx, null);

        String legacyHit = findAggHit(legacy.executions);
        assertTrue(
            "legacy backend did not reference any declared agg table for "
                + name + ";\n"
                + " expected one of " + entry.acceptableAggTables
                + "\n captured SQL:\n" + dumpSql(legacy.executions),
            legacyHit != null
                && entry.acceptableAggTables.contains(legacyHit));

        // --- Run under calcite backend ---
        System.setProperty(MondrianBackend.PROPERTY, "calcite");
        SegmentLoader.clearCalcitePlannerCache();
        FoodMartCapture.CapturedRun calcite =
            FoodMartCapture.executeCold(entry.mdx, null);

        String calciteHit = findAggHit(calcite.executions);
        assertTrue(
            "calcite backend did not reference any declared agg table for "
                + name + ";\n"
                + " expected one of " + entry.acceptableAggTables
                + "\n captured SQL:\n" + dumpSql(calcite.executions),
            calciteHit != null
                && entry.acceptableAggTables.contains(calciteHit));

        // --- Cell-set parity ---
        assertEquals(
            "cell-set must match between legacy and calcite backends for "
                + name,
            legacy.cellSet, calcite.cellSet);
    }

    /**
     * Returns the first declared-agg-table name found in any captured
     * SQL statement, scanning across ALL four declared aggs so the
     * failure diagnostic can distinguish "wrong agg" from "base fact
     * fallback". Comparison is case-insensitive on raw SQL text — the
     * legacy and Calcite backends both quote identifiers consistently
     * lowercase in HSQLDB.
     */
    private static String findAggHit(List<CapturedExecution> executions) {
        for (CapturedExecution ex : executions) {
            String sql = ex.sql == null ? "" : ex.sql.toLowerCase();
            for (String agg : ALL_DECLARED_AGGS) {
                if (sql.contains(agg)) {
                    return agg;
                }
            }
        }
        return null;
    }

    private static String dumpSql(List<CapturedExecution> executions) {
        List<String> lines = new ArrayList<>();
        int i = 0;
        for (CapturedExecution ex : executions) {
            lines.add("[" + (i++) + "] " + ex.sql);
        }
        return String.join("\n", lines);
    }
}

// End MvHitTest.java
