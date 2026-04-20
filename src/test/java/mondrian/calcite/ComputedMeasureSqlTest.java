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
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * End-to-end proof that a {@link PlannerRequest.ComputedMeasure} lands in
 * the emitted SQL as a post-aggregate projection via
 * {@link ArithmeticCalcTranslator}. This verifies the rendering wiring
 * without requiring a RolapEvaluator refactor: the calc's MDX expression
 * is translated inside {@link CalciteSqlPlanner#planRel}, and the
 * resulting SQL contains the arithmetic operator in the SELECT list.
 */
public class ComputedMeasureSqlTest {

    private static Connection conn;
    private static CalciteMondrianSchema schema;
    private static SqlDialect hsqldb;

    @BeforeClass
    public static void boot() throws Exception {
        FoodMartHsqldbBootstrap.ensureExtracted();
        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        conn = DriverManager.getConnection(props, null);

        // Build a CalciteMondrianSchema for the HSQLDB FoodMart JDBC.
        org.hsqldb.jdbc.jdbcDataSource ds =
            new org.hsqldb.jdbc.jdbcDataSource();
        ds.setDatabase(
            mondrian.olap.MondrianProperties.instance()
                .FoodmartJdbcURL.get());
        ds.setUser(mondrian.olap.MondrianProperties.instance()
            .TestJdbcUser.get());
        ds.setPassword(mondrian.olap.MondrianProperties.instance()
            .TestJdbcPassword.get());
        schema = new CalciteMondrianSchema(ds, null);
        hsqldb = HsqldbSqlDialect.DEFAULT;
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) { conn.close(); conn = null; }
    }

    @Test
    public void computedMeasureAppearsInSelectList() {
        // Parse a calc-member MDX, resolve, pull out the base-measure
        // expression.
        String mdx =
            "with member [Measures].[Profit] as"
            + " '[Measures].[Store Sales] - [Measures].[Store Cost]'"
            + " select {[Measures].[Profit]} on columns from Sales";
        Query q = conn.parseQuery(mdx);
        q.resolve();
        Formula profit = null;
        for (Formula f : q.getFormulas()) {
            if (f.getMdxMember() != null
                && "Profit".equals(f.getMdxMember().getName()))
            {
                profit = f;
                break;
            }
        }
        Exp expr = profit.getExpression();
        ArithmeticCalcAnalyzer.Classification cls =
            ArithmeticCalcAnalyzer.classify(
                expr, java.util.Collections.emptySet());
        assertTrue("calc should push", cls.isPushable());

        // Build a PlannerRequest that mimics what a future fromSegmentLoad
        // would produce: one group-by key + two base measures + the calc.
        PlannerRequest.Builder b =
            PlannerRequest.builder("sales_fact_1997");
        b.addGroupBy(new PlannerRequest.Column(null, "product_id"));
        b.addMeasure(new PlannerRequest.Measure(
            PlannerRequest.AggFn.SUM,
            new PlannerRequest.Column(null, "store_sales"),
            "m0"));
        b.addMeasure(new PlannerRequest.Measure(
            PlannerRequest.AggFn.SUM,
            new PlannerRequest.Column(null, "store_cost"),
            "m1"));
        // Map the calc's base-measure refs onto m0/m1. Order in the
        // analyzer's baseMeasures set mirrors the walk order
        // (Store Sales first, then Store Cost).
        Map<Object, String> refs = new LinkedHashMap<>();
        String[] aliases = {"m0", "m1"};
        int i = 0;
        for (Member m : cls.baseMeasures) {
            refs.put(m, aliases[i++]);
        }
        b.addComputedMeasure(new PlannerRequest.ComputedMeasure(
            "c0", expr, refs));
        PlannerRequest req = b.build();

        String sql = new CalciteSqlPlanner(schema, hsqldb).plan(req);

        // The SELECT list should include the aggregate aliases AND the
        // computed calc (rendered as m0 - m1 cast to DOUBLE).
        assertTrue("expected m0 in sql: " + sql, sql.contains("m0"));
        assertTrue("expected m1 in sql: " + sql, sql.contains("m1"));
        assertTrue("expected c0 alias for calc: " + sql,
            sql.contains("c0"));
        assertTrue("expected - in projection: " + sql,
            sql.contains("-"));

        System.out.println("ComputedMeasureSqlTest SQL: " + sql);
    }
}

// End ComputedMeasureSqlTest.java
