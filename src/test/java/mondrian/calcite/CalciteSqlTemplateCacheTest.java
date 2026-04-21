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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CalciteSqlTemplateCache}: structural-hash
 * keying, template build/render, and fallback behaviour.
 */
public class CalciteSqlTemplateCacheTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Before @After
    public void resetCache() {
        CalciteSqlTemplateCache.clear();
    }

    private static DataSource foodmartDs() {
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase(
            "jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private static CalciteSqlPlanner planner() {
        SqlDialect dialect = HsqldbSqlDialect.DEFAULT;
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        return new CalciteSqlPlanner(schema, dialect);
    }

    private static PlannerRequest yearEqRequest(int year) {
        return PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                year))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
    }

    @Test
    public void structuralHashIgnoresLiteralValues() {
        PlannerRequest a = yearEqRequest(1997);
        PlannerRequest b = yearEqRequest(1998);
        assertEquals(a.structuralHash(), b.structuralHash());
        assertTrue(a.structurallyEqual(b));
    }

    @Test
    public void structuralHashDiffersOnJoins() {
        PlannerRequest a = yearEqRequest(1997);
        PlannerRequest b = PlannerRequest.builder("sales_fact_1997")
            // no join → different shape
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column(null, "time_id"), 1997))
            .addGroupBy(new PlannerRequest.Column(null, "time_id"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        assertNotEquals(a.structuralHash(), b.structuralHash());
        assertTrue(!a.structurallyEqual(b));
    }

    @Test
    public void literalsReturnedInTraversalOrder() {
        PlannerRequest r = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                PlannerRequest.Operator.IN,
                java.util.Arrays.<Object>asList(1997, 1998)))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        assertEquals(
            java.util.Arrays.<Object>asList(1997, 1998),
            r.literals());
    }

@Test
    public void cacheHitRendersIdenticalSql() {
        CalciteSqlPlanner p = planner();
        PlannerRequest r = yearEqRequest(1997);
        String first = p.plan(r);
        long missesAfterFirst = CalciteSqlTemplateCache.misses();
        long hitsAfterFirst = CalciteSqlTemplateCache.hits();
        String second = p.plan(r);
        assertEquals(first, second);
        assertEquals(missesAfterFirst, CalciteSqlTemplateCache.misses());
        assertEquals(hitsAfterFirst + 1, CalciteSqlTemplateCache.hits());
    }

    @Test
    public void cacheHitWithDifferentLiteralsSubstitutes() {
        CalciteSqlPlanner p = planner();
        String sql1997 = p.plan(yearEqRequest(1997));
        long hitsBefore = CalciteSqlTemplateCache.hits();
        String sql1998 = p.plan(yearEqRequest(1998));
        // Second call hits the cache (same structural shape).
        assertEquals(hitsBefore + 1, CalciteSqlTemplateCache.hits());
        // SQL must differ only in the year literal.
        assertNotEquals(sql1997, sql1998);
        assertTrue(
            "expected 1998 in rendered: " + sql1998,
            sql1998.contains("1998"));
        assertTrue(
            "expected 1997 not substituted out of alias/column context: "
            + sql1998,
            // the_year column reference should remain; verify by
            // reconstructing the 1997 version via reverse swap.
            sql1998.replace("1998", "1997").equals(sql1997));
    }

    @Test
    public void cacheMissOnDifferentShape() {
        CalciteSqlPlanner p = planner();
        p.plan(yearEqRequest(1997));
        long missesBefore = CalciteSqlTemplateCache.misses();
        // Different shape: adds a second filter.
        PlannerRequest r2 = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                1997))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "quarter"),
                "Q1"))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        p.plan(r2);
        assertEquals(missesBefore + 1, CalciteSqlTemplateCache.misses());
    }

    @Test
    public void cacheClearResetsState() {
        CalciteSqlPlanner p = planner();
        p.plan(yearEqRequest(1997));
        assertTrue(CalciteSqlTemplateCache.size() >= 1);
        CalciteSqlTemplateCache.clear();
        assertEquals(0, CalciteSqlTemplateCache.size());
        assertEquals(0, CalciteSqlTemplateCache.hits());
        assertEquals(0, CalciteSqlTemplateCache.misses());
    }

    @Test
    public void templateBuildFallsBackOnAmbiguousLiteral() {
        // Synthetic: two literals with the same formatted value.
        // The second literal's first word-boundary occurrence at or
        // past the cursor IS the same position already consumed by
        // the first literal's match, so the second lookup fails —
        // tryBuild returns null (noop fallback).
        //
        // We exercise this via an IN-filter where the same value
        // appears twice (col IN (7, 7)): after consuming the first
        // "7", the search for another "7" still finds the duplicate
        // in the SQL; if rendering would emit both as the same
        // literal position, the second indexOf finds only one
        // unique-boundary match remaining — wait, both emit. Each
        // branch renders a distinct "= 7" pair. That's fine. Use a
        // direct Template.tryBuild test instead — build a synthetic
        // request whose literals collide with a columnar token.
        //
        // Here we verify tryBuild() returns null when the formatted
        // literal can't be uniquely bounded. Construct SQL manually:
        String contrivedSql =
            "SELECT x FROM t WHERE a = 42 AND b = 42";
        // Manufacture a request whose literals() returns [42] but
        // the SQL contains two word-bounded "42"s.
        PlannerRequest r = PlannerRequest.builder("t")
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column(null, "a"), 42))
            .addProjection(new PlannerRequest.Column(null, "x"))
            .build();
        CalciteSqlTemplateCache.Template t =
            CalciteSqlTemplateCache.Template.tryBuild(r, contrivedSql);
        org.junit.Assert.assertNull(
            "ambiguous literal should cause tryBuild() to return null",
            t);
    }

    @Test
    public void formatLiteralHandlesBasicTypes() {
        assertEquals("1997",
            CalciteSqlTemplateCache.formatLiteral(Integer.valueOf(1997)));
        assertEquals("'foo'",
            CalciteSqlTemplateCache.formatLiteral("foo"));
        assertEquals("'it''s'",
            CalciteSqlTemplateCache.formatLiteral("it's"));
        assertEquals("TRUE",
            CalciteSqlTemplateCache.formatLiteral(Boolean.TRUE));
    }
}

// End CalciteSqlTemplateCacheTest.java
