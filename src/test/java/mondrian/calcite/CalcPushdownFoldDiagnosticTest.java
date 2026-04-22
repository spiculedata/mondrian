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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Diagnostic-only (no assertions): dumps the RelNode tree at three points
 * — raw planner output, post-Hep, and the SQL RelToSqlConverter produces —
 * so we can pin down which stage drops the outer Project that carries the
 * calc arithmetic. Output goes to stdout; the test always passes.
 *
 * <p>Feeds the write-up in {@code docs/reports/calc-pushdown-fold-analysis.md}.
 */
public class CalcPushdownFoldDiagnosticTest {

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
    public void dumpFoldTransition() {
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
        RelNode raw = planner.planRel(req);
        System.out.println(
            "\n==== RAW (pre-Hep) ====\n" + RelOptUtil.toString(raw));
        RelNode post = planner.optimize(raw);
        System.out.println(
            "\n==== POST-HEP ====\n" + RelOptUtil.toString(post));

        String sql = planner.plan(req);
        System.out.println("\n==== FINAL SQL ====\n" + sql + "\n");
    }
}

// End CalcPushdownFoldDiagnosticTest.java
