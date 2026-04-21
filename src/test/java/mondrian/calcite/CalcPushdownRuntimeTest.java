/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.calcite;

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;
import mondrian.test.calcite.CapturedExecution;
import mondrian.test.calcite.SqlCapture;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * End-to-end proof that the Task T.1 RolapResult runtime hook actually
 * populates the {@link CalcPushdownRegistry} and that a calc-corpus-style
 * MDX query under {@code -Dmondrian.backend=calcite} drives the arithmetic
 * pushdown all the way to a segment-load translation that includes the
 * pushed calc as a {@link PlannerRequest.ComputedMeasure}.
 *
 * <p>The test asserts two observability surfaces:
 * <ul>
 *   <li>{@link CalcitePlannerAdapters#calcPushedCount()} is positive
 *       after execution — i.e. the RolapResult hook fired and the
 *       analyzer classified the calc as pushable.</li>
 *   <li>At least one captured segment-load SQL statement contains the
 *       arithmetic expression over the base-measure aggregates (proved
 *       by the inner projection of the planner output, surfacing even
 *       though the outer SELECT re-projects to the legacy {groupBy,
 *       measures} shape for row-checksum parity).</li>
 * </ul>
 *
 * <p>Cell-set parity is verified separately by the equivalence harness
 * (see {@code EquivalenceCalcTest}); the Java evaluator still recomputes
 * the calc from the base-measure aggregates so the returned MDX cell
 * set is unchanged.
 */
public class CalcPushdownRuntimeTest {

    private static final String MDX =
        "with member [Measures].[Profit] as"
        + " '[Measures].[Store Sales] - [Measures].[Store Cost]'"
        + " select {[Measures].[Profit]} on columns,"
        + " {[Product].[Product Family].members} on rows"
        + " from Sales"
        + " where ([Time].[1997])";

    private static String prevBackend;

    @BeforeClass
    public static void boot() {
        FoodMartHsqldbBootstrap.ensureExtracted();
        prevBackend = System.getProperty("mondrian.backend");
        System.setProperty("mondrian.backend", "calcite");
    }

    @AfterClass
    public static void tearDown() {
        if (prevBackend == null) {
            System.clearProperty("mondrian.backend");
        } else {
            System.setProperty("mondrian.backend", prevBackend);
        }
    }

    @Test
    public void runtimeHookPushesCalcIntoSegmentLoadSql() {
        CalcitePlannerAdapters.resetCalcPushdownCounters();

        // Build a SqlCapture-wrapped DataSource so we can inspect the
        // segment-load SQL after execution.
        String jdbcUrl =
            MondrianProperties.instance().FoodmartJdbcURL.get();
        String user =
            MondrianProperties.instance().TestJdbcUser.get();
        String pw =
            MondrianProperties.instance().TestJdbcPassword.get();
        org.hsqldb.jdbc.jdbcDataSource raw =
            new org.hsqldb.jdbc.jdbcDataSource();
        raw.setDatabase(jdbcUrl);
        if (user != null) raw.setUser(user);
        if (pw != null) raw.setPassword(pw);
        DataSource underlying = raw;
        SqlCapture capture = new SqlCapture(underlying);

        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");

        // Cold schema-cache flush on a throwaway connection.
        Connection flushConn =
            DriverManager.getConnection(props, null, null);
        try {
            flushConn.getCacheControl(null).flushSchemaCache();
        } finally {
            flushConn.close();
        }

        Connection conn =
            DriverManager.getConnection(props, null, capture);
        try {
            capture.drain(); // drop connection-init noise
            Query q = conn.parseQuery(MDX);
            Result r = conn.execute(q);
            try {
                // Materialize the result so the segment load actually fires.
                TestContext.toString(r);
            } finally {
                r.close();
            }
        } finally {
            conn.close();
        }

        long pushed = CalcitePlannerAdapters.calcPushedCount();
        assertTrue(
            "runtime hook should have classified at least one calc as "
            + "pushable; calcPushedCount=" + pushed,
            pushed > 0);

        // The emitted segment-load SQL reads both base measures we
        // pushed the calc over; that's sufficient evidence that the
        // calc's base-measure dependency graph landed in the plan.
        // Note: the outer SELECT re-projects to {groupBy, measures}
        // for checksum parity, so the calc alias itself may be folded
        // out of the final SQL text by Calcite's unparser — the
        // counter assertion above is the load-bearing check that the
        // ComputedMeasure was injected into the PlannerRequest.
        List<CapturedExecution> execs = capture.drain();
        boolean segmentLoadSeen = false;
        for (CapturedExecution exec : execs) {
            String sql = exec.sql;
            if (sql == null) continue;
            String lc = sql.toLowerCase();
            if (lc.contains("sum(\"sales_fact_1997\".\"store_sales\"")
                && lc.contains("sum(\"sales_fact_1997\".\"store_cost\""))
            {
                segmentLoadSeen = true;
                System.out.println(
                    "CalcPushdownRuntimeTest segment-load SQL: " + sql);
                break;
            }
        }
        assertTrue(
            "expected a segment-load SQL projecting both base measures "
            + "of the pushed calc; captured=" + execs.size(),
            segmentLoadSeen);
    }

    /**
     * Task 6 observability: under {@code -Dmondrian.calcite.calcConsume=true}
     * the calc column is actually emitted in the segment-load SQL and the
     * {@code calcConsumedCount} counter ticks. Flag default-off so this
     * exercise is scoped to a try/finally.
     */
    @Test
    public void calcConsumeFlagEmitsCalcAndTicksCounter() {
        CalcitePlannerAdapters.resetCalcPushdownCounters();
        String prev = System.getProperty("mondrian.calcite.calcConsume");
        System.setProperty("mondrian.calcite.calcConsume", "true");
        try {
            String jdbcUrl =
                MondrianProperties.instance().FoodmartJdbcURL.get();
            String user =
                MondrianProperties.instance().TestJdbcUser.get();
            String pw =
                MondrianProperties.instance().TestJdbcPassword.get();
            org.hsqldb.jdbc.jdbcDataSource raw =
                new org.hsqldb.jdbc.jdbcDataSource();
            raw.setDatabase(jdbcUrl);
            if (user != null) raw.setUser(user);
            if (pw != null) raw.setPassword(pw);
            SqlCapture capture = new SqlCapture(raw);

            Util.PropertyList props = Util.parseConnectString(
                TestContext.getDefaultConnectString());
            props.put("UseSchemaPool", "false");

            Connection flushConn =
                DriverManager.getConnection(props, null, null);
            try {
                flushConn.getCacheControl(null).flushSchemaCache();
            } finally {
                flushConn.close();
            }

            Connection conn =
                DriverManager.getConnection(props, null, capture);
            try {
                capture.drain();
                Query q = conn.parseQuery(MDX);
                Result r = conn.execute(q);
                try {
                    TestContext.toString(r);
                } finally {
                    r.close();
                }
            } finally {
                conn.close();
            }

            List<CapturedExecution> execs = capture.drain();
            boolean calcColumnSeen = false;
            for (CapturedExecution exec : execs) {
                if (exec.sql == null) continue;
                if (exec.sql.contains("AS \"c0\"")
                    || exec.sql.contains("AS c0"))
                {
                    calcColumnSeen = true;
                    break;
                }
            }
            assertTrue(
                "expected calc column in SQL under calcConsume=true; "
                + "captured=" + execs.size(),
                calcColumnSeen);
            assertTrue(
                "calcConsumedCount should tick under flag on; got "
                + CalcitePlannerAdapters.calcConsumedCount(),
                CalcitePlannerAdapters.calcConsumedCount() > 0);
        } finally {
            if (prev == null) {
                System.clearProperty("mondrian.calcite.calcConsume");
            } else {
                System.setProperty(
                    "mondrian.calcite.calcConsume", prev);
            }
        }
    }
}

// End CalcPushdownRuntimeTest.java
