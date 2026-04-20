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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit-tests the SQL-drift split in {@link EquivalenceHarness}: cell-set and
 * rowCount/checksum mismatches remain hard-gate {@link FailureClass#LEGACY_DRIFT}
 * failures, while SQL-string mismatches become {@link FailureClass#SQL_DRIFT}
 * observations whose hardness is governed by the
 * {@code -Dharness.sqlCompare} system property.
 *
 * <p>Three modes under test:
 * <ol>
 *   <li>{@code strict}  — SQL mismatch trips the gate (legacy behaviour).</li>
 *   <li>{@code advisory} (default) — SQL mismatch is recorded but not a
 *       failure; cell-set mismatch still fails hard.</li>
 *   <li>{@code off}     — SQL comparison is skipped entirely.</li>
 * </ol>
 *
 * <p>The tests feed {@link EquivalenceHarness#compareAgainstGoldenForTest}
 * synthetic goldens and captured runs rather than exercising the full MDX
 * round-trip — cheap and deterministic.
 */
public class HarnessSqlDriftTest {

    private static final String SYS_PROP =
        EquivalenceHarness.SQL_COMPARE_SYS_PROP;

    private String prior;

    @Before
    public void save() {
        prior = System.getProperty(SYS_PROP);
        System.clearProperty(SYS_PROP);
    }

    @After
    public void restore() {
        if (prior == null) {
            System.clearProperty(SYS_PROP);
        } else {
            System.setProperty(SYS_PROP, prior);
        }
    }

    // ------------------------------------------------------------------
    // Synthetic fixtures
    // ------------------------------------------------------------------

    /** Golden JSON with cellSet="CS" and one sqlExecution seq=0. */
    private static JsonNode goldenFixture(String sql,
                                          int rowCount,
                                          String checksum)
        throws Exception
    {
        String json =
            "{"
            + "\"cellSet\":\"CS\","
            + "\"sqlExecutions\":["
            + "  {"
            + "    \"seq\":0,"
            + "    \"sql\":\"" + sql + "\","
            + "    \"rowCount\":" + rowCount + ","
            + "    \"checksum\":\"" + checksum + "\""
            + "  }"
            + "]"
            + "}";
        return new ObjectMapper().readTree(json);
    }

    private static FoodMartCapture.CapturedRun runFixture(String cellSet,
                                                          String sql,
                                                          int rowCount,
                                                          String checksum)
    {
        CapturedExecution ce = new CapturedExecution(
            0, sql, Collections.<java.util.List<Object>>emptyList(),
            rowCount, checksum);
        return new FoodMartCapture.CapturedRun(
            cellSet, Collections.singletonList(ce));
    }

    // ------------------------------------------------------------------
    // Cell-set parity: hard gate regardless of mode
    // ------------------------------------------------------------------

    @Test
    public void cellSetMismatchIsLegacyDriftEvenInOffMode() throws Exception {
        System.setProperty(SYS_PROP, "off");
        JsonNode golden = goldenFixture("SELECT *", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS-DIFFERENT", "SELECT *", 5, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertEquals(FailureClass.LEGACY_DRIFT, c.failureClass);
        assertNotNull(c.detail);
        assertNull(c.sqlDriftDetail);
    }

    @Test
    public void rowCountMismatchIsLegacyDriftEvenInOffMode() throws Exception {
        System.setProperty(SYS_PROP, "off");
        JsonNode golden = goldenFixture("SELECT *", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT *", 9, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertEquals(FailureClass.LEGACY_DRIFT, c.failureClass);
    }

    @Test
    public void checksumMismatchIsLegacyDriftEvenInOffMode() throws Exception {
        System.setProperty(SYS_PROP, "off");
        JsonNode golden = goldenFixture("SELECT *", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT *", 5, "xyz");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertEquals(FailureClass.LEGACY_DRIFT, c.failureClass);
    }

    // ------------------------------------------------------------------
    // STRICT
    // ------------------------------------------------------------------

    @Test
    public void strictFailsOnSqlOnlyMismatch() throws Exception {
        System.setProperty(SYS_PROP, "strict");
        JsonNode golden = goldenFixture("SELECT a FROM t", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT a FROM t JOIN u ON ...", 5, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertEquals(FailureClass.SQL_DRIFT, c.failureClass);
        assertNotNull(c.detail);
    }

    @Test
    public void strictPassesWhenSqlMatches() throws Exception {
        System.setProperty(SYS_PROP, "strict");
        JsonNode golden = goldenFixture("SELECT a", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT a", 5, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertNull(c.failureClass);
        assertNull(c.sqlDriftDetail);
    }

    // ------------------------------------------------------------------
    // ADVISORY (default)
    // ------------------------------------------------------------------

    @Test
    public void advisoryRecordsSqlDriftButDoesNotFail() throws Exception {
        System.setProperty(SYS_PROP, "advisory");
        JsonNode golden = goldenFixture("SELECT a FROM t", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT a FROM t JOIN u ON ...", 5, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertNull(
            "advisory mode must not fail the test on SQL drift; detail="
                + c.sqlDriftDetail,
            c.failureClass);
        assertNotNull(
            "advisory mode must surface the drift as sqlDriftDetail",
            c.sqlDriftDetail);
    }

    @Test
    public void advisoryIsTheDefaultWhenSysPropUnset() throws Exception {
        // prior cleared in @Before
        assertNull(System.getProperty(SYS_PROP));
        assertEquals(
            EquivalenceHarness.SqlCompareMode.ADVISORY,
            EquivalenceHarness.sqlCompareMode());
    }

    // ------------------------------------------------------------------
    // OFF
    // ------------------------------------------------------------------

    @Test
    public void offSkipsSqlComparison() throws Exception {
        System.setProperty(SYS_PROP, "off");
        JsonNode golden = goldenFixture("SELECT a FROM t", 5, "abc");
        FoodMartCapture.CapturedRun run =
            runFixture("CS", "SELECT a FROM t JOIN u ON ...", 5, "abc");
        HarnessResult.Comparison c =
            EquivalenceHarness.compareAgainstGoldenForTest(golden, run);
        assertNull(c.failureClass);
        assertNull(
            "off mode must not populate sqlDriftDetail",
            c.sqlDriftDetail);
    }
}

// End HarnessSqlDriftTest.java
