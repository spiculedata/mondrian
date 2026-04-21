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

import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Failing baseline for Task 1 of docs/plans/2026-04-21-calc-pushdown-to-sql.md:
 * asserts that a pushable calc ({@code SUM(a) / SUM(b)}) emits the arithmetic
 * operator in the planner's rendered SQL. Today {@link CalciteSqlPlanner} wraps
 * the inner projection (base measures + calc) with an outer projection that
 * drops the calc alias; {@code RelToSqlConverter} then folds both projects
 * into the Aggregate, erasing the arithmetic from the SQL text entirely.
 *
 * <p>This test fails until Tasks 3 and 4 land: the goal is for the emitted
 * SQL's SELECT list to contain {@code /} (or {@code CASE WHEN ... THEN NULL
 * ELSE ... / ... END} — {@link ArithmeticCalcTranslator} guards div-by-zero)
 * applied to the two SUM aggregates.
 */
public class CalcPushdownSqlTest {

    private static Connection conn;
    private static CalciteMondrianSchema schema;

    @BeforeClass
    public static void boot() {
        FoodMartHsqldbBootstrap.ensureExtracted();
        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        conn = DriverManager.getConnection(props, null);

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
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) { conn.close(); conn = null; }
    }

    @Test
    public void calcRatioEmitsArithmeticInSql() {
        String prev = System.getProperty("mondrian.calcite.calcConsume");
        System.setProperty("mondrian.calcite.calcConsume", "true");
        try {
            runCalcRatio();
        } finally {
            if (prev == null) {
                System.clearProperty("mondrian.calcite.calcConsume");
            } else {
                System.setProperty("mondrian.calcite.calcConsume", prev);
            }
        }
    }

    private void runCalcRatio() {
        String mdx =
            "with member [Measures].[Ratio] as"
            + " '[Measures].[Unit Sales] / [Measures].[Store Sales]'"
            + " select {[Measures].[Ratio]} on columns from Sales";
        Query q = conn.parseQuery(mdx);
        q.resolve();
        Formula ratio = null;
        for (Formula f : q.getFormulas()) {
            if (f.getMdxMember() != null
                && "Ratio".equals(f.getMdxMember().getName()))
            {
                ratio = f;
                break;
            }
        }
        Exp expr = ratio.getExpression();
        ArithmeticCalcAnalyzer.Classification cls =
            ArithmeticCalcAnalyzer.classify(
                expr, java.util.Collections.emptySet());
        assertTrue("ratio calc should be pushable", cls.isPushable());

        PlannerRequest.Builder b =
            PlannerRequest.builder("sales_fact_1997");
        b.addGroupBy(new PlannerRequest.Column(null, "product_id"));
        b.addMeasure(new PlannerRequest.Measure(
            PlannerRequest.AggFn.SUM,
            new PlannerRequest.Column(null, "unit_sales"),
            "m0"));
        b.addMeasure(new PlannerRequest.Measure(
            PlannerRequest.AggFn.SUM,
            new PlannerRequest.Column(null, "store_sales"),
            "m1"));
        Map<Object, String> refs = new LinkedHashMap<>();
        String[] aliases = {"m0", "m1"};
        int i = 0;
        for (Member m : cls.baseMeasures) {
            refs.put(m, aliases[i++]);
        }
        b.addComputedMeasure(new PlannerRequest.ComputedMeasure(
            "ratio", expr, refs));
        PlannerRequest req = b.build();

        CalciteSqlPlanner planner =
            new CalciteSqlPlanner(schema, HsqldbSqlDialect.DEFAULT);
        String sql = planner.plan(req);
        System.out.println("CalcPushdownSqlTest SQL: " + sql);

        // Calc uses /, translator wraps it as CASE WHEN b=0 THEN NULL ELSE a/b.
        // Either form of division operator surviving to the SQL text proves
        // the inner projection did not get folded into the Aggregate.
        assertTrue(
            "expected division arithmetic in emitted SQL; got: " + sql,
            sql.contains("/"));
    }
}

// End CalcPushdownSqlTest.java
