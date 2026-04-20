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

import mondrian.test.FoodMartHsqldbBootstrap;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CalciteSqlPlanner}: PlannerRequest -> dialect-rendered SQL.
 */
public class CalciteSqlPlannerTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    private static DataSource foodmartDs() {
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase("jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private static CalciteSqlPlanner plannerFor(SqlDialect dialect) {
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        return new CalciteSqlPlanner(schema, dialect);
    }

    @Test
    public void simpleScanEmitsSelect() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addProjection(new PlannerRequest.Column(null, "unit_sales"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected SELECT in: " + sql, lower.contains("select"));
        assertTrue("expected unit_sales in: " + sql,
            lower.contains("unit_sales"));
        assertTrue("expected sales_fact_1997 in: " + sql,
            lower.contains("sales_fact_1997"));
    }

    @Test
    public void groupedAggregateEmitsGroupBy() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "unit_sales"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected GROUP BY in: " + sql, lower.contains("group by"));
        assertTrue("expected the_year in: " + sql, lower.contains("the_year"));
        assertTrue("expected SUM in: " + sql, lower.contains("sum("));
    }

    @Test
    public void distinctProjectionEmitsSelectDistinct() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("product_class")
            .addProjection(
                new PlannerRequest.Column(null, "product_family"))
            .addOrderBy(new PlannerRequest.OrderBy(
                new PlannerRequest.Column("product_class", "product_family"),
                PlannerRequest.Order.ASC))
            .distinct(true)
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue(
            "expected SELECT DISTINCT (or equivalent) in: " + sql,
            lower.contains("distinct") || lower.contains("group by"));
        assertTrue("expected product_family in: " + sql,
            lower.contains("product_family"));
        assertTrue("expected ORDER BY in: " + sql,
            lower.contains("order by"));
    }

    @Test(expected = IllegalStateException.class)
    public void distinctWithAggregationRejected() {
        PlannerRequest.builder("sales_fact_1997")
            .addGroupBy(new PlannerRequest.Column(null, "time_id"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column(null, "unit_sales"),
                "m"))
            .distinct(true)
            .build();
    }

    @Test
    public void dialectAwareness() {
        // Baseline HSQLDB dialect uses double-quoted identifiers; build a
        // custom-context variant using backtick identifier quoting so the
        // dialect parameter visibly affects the rendered SQL even on the
        // simple, non-keyword identifiers used by this corpus query.
        SqlDialect backtickDialect = new SqlDialect(
            HsqldbSqlDialect.DEFAULT_CONTEXT
                .withIdentifierQuoteString("`")
                .withNullCollation(NullCollation.HIGH)) {};
        CalciteSqlPlanner hsql = plannerFor(HsqldbSqlDialect.DEFAULT);
        CalciteSqlPlanner alt = plannerFor(backtickDialect);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "unit_sales"))
            .addOrderBy(new PlannerRequest.OrderBy(
                new PlannerRequest.Column("time_by_day", "the_year"),
                PlannerRequest.Order.ASC))
            .build();
        String hsqlSql = hsql.plan(req);
        String altSql = alt.plan(req);
        assertNotNull(hsqlSql);
        assertNotNull(altSql);
        assertNotEquals(
            "expected dialect parameter to affect emitted SQL; identical:\n"
                + hsqlSql,
            hsqlSql, altSql);
    }
}

// End CalciteSqlPlannerTest.java
