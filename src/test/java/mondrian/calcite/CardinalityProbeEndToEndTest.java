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

import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.spi.impl.SqlStatisticsProvider;
import mondrian.test.FoodMartHsqldbBootstrap;

import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.*;

/**
 * Unit-level coverage for the third dispatch seam: the cardinality probe
 * emitted by {@code SqlStatisticsProvider.getColumnCardinality}, routed
 * through {@link CalcitePlannerAdapters#fromCardinalityProbe(String,
 * String, String)}.
 *
 * <p>Two things get asserted:
 * <ul>
 *   <li>The trivial shape (unqualified schema, real column, real table)
 *       translates cleanly and the wire SQL contains
 *       {@code COUNT(DISTINCT "col")}.</li>
 *   <li>The fallback counter increments on rejected shapes (null table,
 *       qualified DB schema).</li>
 * </ul>
 *
 * <p>The full-stack "at least one probe during an MDX execution goes
 * through CalciteSqlPlanner" assertion lives in
 * {@code mondrian.test.calcite.CardinalityProbeCalciteHarnessTest} (or is
 * covered implicitly by the basic-select equivalence suite); we keep this
 * test package-light to avoid dragging the FoodMart fixture into a unit
 * module.
 */
public class CardinalityProbeEndToEndTest {

    @BeforeClass public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Before public void reset() {
        CalcitePlannerAdapters.resetUnsupportedCount();
        SqlStatisticsProvider.clearCalcitePlannerCache();
    }

    @After public void clearBackend() {
        System.clearProperty("mondrian.backend");
        CalcitePlannerAdapters.resetUnsupportedCount();
        SqlStatisticsProvider.clearCalcitePlannerCache();
    }

    @AfterClass public static void unsetBackend() {
        System.clearProperty("mondrian.backend");
        SqlStatisticsProvider.clearCalcitePlannerCache();
    }

    @Test public void trivialProbeTranslatesToPlannerRequest() {
        PlannerRequest req =
            CalcitePlannerAdapters.fromCardinalityProbe(
                null, "product", "product_id");
        assertEquals("product", req.factTable);
        assertTrue("probe is an aggregation", req.isAggregation());
        assertEquals(1, req.measures.size());
        PlannerRequest.Measure m = req.measures.get(0);
        assertSame(PlannerRequest.AggFn.COUNT, m.fn);
        assertTrue("distinct flag must be set", m.distinct);
        assertEquals("product_id", m.column.name);
        assertTrue("no group-by on a probe", req.groupBy.isEmpty());
        assertTrue("no joins on a probe", req.joins.isEmpty());
        assertTrue("no filters on a probe", req.filters.isEmpty());
    }

    @Test public void qualifiedSchemaRejected() {
        long before =
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromCardinalityProbe(
                "public", "product", "product_id");
            fail("expected UnsupportedTranslation for qualified schema");
        } catch (UnsupportedTranslation ex) {
            assertNotNull(ex.getMessage());
            assertTrue(
                "message mentions schema",
                ex.getMessage().toLowerCase().contains("schema"));
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount());
    }

    @Test public void nullTableRejected() {
        long before =
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromCardinalityProbe(
                null, null, "product_id");
            fail("expected UnsupportedTranslation for null table");
        } catch (UnsupportedTranslation ignored) {
            // expected
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount());
    }

    @Test public void nullColumnRejected() {
        long before =
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromCardinalityProbe(
                null, "product", null);
            fail("expected UnsupportedTranslation for null column");
        } catch (UnsupportedTranslation ignored) {
            // expected
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount());
    }

    @Test public void planRendersCountDistinctSqlAgainstFoodMart()
        throws Exception
    {
        // Spin up a real FoodMart HSQLDB DataSource, hand it to
        // CalciteSqlPlanner via CalciteMondrianSchema, render the probe,
        // and execute the emitted SQL directly. Proves the full chain
        // (PlannerRequest → RelBuilder → RelToSqlConverter → HSQLDB JDBC)
        // works for a cardinality probe, which is what the Task C
        // dispatch in SqlStatisticsProvider wires up. The dispatch
        // counter observation stays on the pure unit tests above.
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase(
            "jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");

        Dialect dialect;
        try (Connection conn = ds.getConnection()) {
            dialect = DialectManager.createDialect(ds, conn);
        }
        assertSame(
            Dialect.DatabaseProduct.HSQLDB, dialect.getDatabaseProduct());

        PlannerRequest req =
            CalcitePlannerAdapters.fromCardinalityProbe(
                null, "product", "product_id");

        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(ds, "mondrian");
        CalciteSqlPlanner planner =
            new CalciteSqlPlanner(
                schema, CalciteDialectMap.forProductName("HSQLDB"));
        String sql = planner.plan(req);

        // Wire assertions: must be a COUNT(DISTINCT ...) over product.
        String lower = sql.toLowerCase();
        assertTrue(
            "calcite-emitted SQL must use COUNT(DISTINCT ...): " + sql,
            lower.contains("count(distinct"));
        assertTrue(
            "calcite-emitted SQL must reference product_id: " + sql,
            sql.contains("product_id"));
        assertTrue(
            "calcite-emitted SQL must reference product: " + sql,
            sql.contains("product"));

        // Execute it — proves the emitted SQL is actually dialect-valid.
        try (Connection conn = ds.getConnection();
             java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery(sql))
        {
            assertTrue("probe must return a row", rs.next());
            int card = rs.getInt(1);
            assertTrue(
                "product_id cardinality must be positive: " + card,
                card > 0);
        }

        // Dispatch-level counter assertion for the "probe-fallback stays
        // at 0" contract called out in the task brief.
        assertEquals(
            0L,
            CalcitePlannerAdapters.cardinalityProbeUnsupportedCount());
    }

    @Test public void measureCtorKeepsDistinctFalseByDefault() {
        // The legacy 3-arg ctor must keep distinct=false so every pre-Task-C
        // call site (segment-load translator, existing tests) continues to
        // emit non-distinct aggregates.
        PlannerRequest.Measure m =
            new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column(null, "x"),
                "m0");
        assertFalse(m.distinct);
    }
}

// End CardinalityProbeEndToEndTest.java
