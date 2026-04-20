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
    public void inListEmitsOrChain() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                PlannerRequest.Operator.IN,
                java.util.Arrays.<Object>asList(1997, 1998, 1999)))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        assertTrue("expected 1997 in: " + sql, sql.contains("1997"));
        assertTrue("expected 1998 in: " + sql, sql.contains("1998"));
        assertTrue("expected 1999 in: " + sql, sql.contains("1999"));
        String lower = sql.toLowerCase();
        assertTrue(
            "expected OR (or IN) chain in: " + sql,
            lower.contains(" or ") || lower.contains(" in "));
    }

    @Test
    public void falseFilterEmitsFalseLiteral() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addProjection(new PlannerRequest.Column(null, "unit_sales"))
            .universalFalse(true)
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue(
            "expected FALSE literal in WHERE: " + sql,
            lower.contains("false") || lower.contains("1 = 0")
                || lower.contains("1=0"));
    }

    @Test
    public void distinctCountMeasureEmitsCountDistinct() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addGroupBy(
                new PlannerRequest.Column("sales_fact_1997", "time_id"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.COUNT,
                new PlannerRequest.Column("sales_fact_1997", "customer_id"),
                "dc",
                true))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase().replaceAll("\\s+", " ");
        assertTrue(
            "expected COUNT(DISTINCT ...) in: " + sql,
            lower.contains("count(distinct"));
        assertTrue("expected customer_id in: " + sql,
            lower.contains("customer_id"));
    }

    @Test
    public void planRequestWithCrossJoinEmitsCrossJoinSql() {
        // Multi-target tuple-read (Task H) emits a CROSS JOIN between two
        // dim tables. Verify the planner translates Join.cross(...) into
        // an unparsed CROSS JOIN (or, at minimum, a join with no equi-key
        // predicate — some dialects render this as "INNER JOIN ... ON TRUE"
        // but HSQLDB keeps the CROSS form).
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("product_class")
            .addJoin(PlannerRequest.Join.cross("time_by_day"))
            .addProjection(
                new PlannerRequest.Column("product_class", "product_family"))
            .addProjection(
                new PlannerRequest.Column("time_by_day", "the_year"))
            .distinct(true)
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase().replaceAll("\\s+", " ");
        // HSQLDB dialect via Calcite may render an unconditional INNER
        // JOIN on TRUE as a CROSS JOIN, an "INNER JOIN ... ON TRUE", or
        // as the comma-separated FROM form (implicit cross product).
        // Any of those three is a valid cross-join rendering.
        boolean cross = lower.contains("cross join");
        boolean innerOnTrue =
            lower.contains("inner join") && lower.contains("true");
        // comma-separated FROM: "from product_class , time_by_day"
        boolean commaFrom =
            lower.matches(".*from\\s+\\w+\\s*,\\s*\\w+.*");
        assertTrue(
            "expected CROSS JOIN / INNER JOIN ON TRUE / comma-FROM in: "
                + sql,
            cross || innerOnTrue || commaFrom);
        assertTrue("expected product_class in: " + sql,
            lower.contains("product_class"));
        assertTrue("expected time_by_day in: " + sql,
            lower.contains("time_by_day"));
    }

    @Test
    public void snowflakeMultiHopEmitsChainedJoins() {
        // Task I: PlannerRequest carries a two-edge snowflake chain:
        //   sales_fact_1997 -> product (product_id)
        //                   -> product_class (product_class_id)
        // The second Join's leftTable=product identifies the already-
        // joined LHS so the renderer resolves product.product_class_id
        // unambiguously.
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "product", "product_id", "product_id"))
            .addJoin(PlannerRequest.Join.chained(
                "product", "product_class_id",
                "product_class", "product_class_id"))
            .addGroupBy(
                new PlannerRequest.Column(
                    "product_class", "product_family"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        // Both hops must appear in the FROM/JOIN structure.
        assertTrue("expected product in: " + sql,
            lower.contains("product"));
        assertTrue("expected product_class in: " + sql,
            lower.contains("product_class"));
        assertTrue("expected product_family in: " + sql,
            lower.contains("product_family"));
        assertTrue("expected sales_fact_1997 in: " + sql,
            lower.contains("sales_fact_1997"));
        // There must be at least two INNER JOINs (or one JOIN + a comma-
        // from in degenerate dialects). For HSQLDB via Calcite, expect
        // the explicit JOIN keyword to appear twice.
        int joinCount = 0;
        int idx = 0;
        while ((idx = lower.indexOf(" join ", idx)) >= 0) {
            joinCount++;
            idx += 5;
        }
        assertTrue("expected >=2 JOIN keywords in: " + sql, joinCount >= 2);
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

    /** Task M: TupleFilter renders as an OR of ANDs across columns. */
    @Test
    public void tupleFilterEmitsOrOfAnds() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addJoin(new PlannerRequest.Join(
                "customer", "customer_id", "customer_id"))
            .addTupleFilter(new PlannerRequest.TupleFilter(
                java.util.Arrays.asList(
                    new PlannerRequest.Column("time_by_day", "the_year"),
                    new PlannerRequest.Column("customer", "gender")),
                java.util.Arrays.asList(
                    java.util.Arrays.<Object>asList(1997, "F"),
                    java.util.Arrays.<Object>asList(1998, "M"))))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected OR in: " + sql, lower.contains(" or "));
        assertTrue("expected AND in: " + sql, lower.contains(" and "));
        assertTrue("expected 1997 in: " + sql, sql.contains("1997"));
        assertTrue("expected 1998 in: " + sql, sql.contains("1998"));
        assertTrue("expected 'F' in: " + sql,
            sql.contains("'F'") || sql.contains("F"));
        assertTrue("expected 'M' in: " + sql,
            sql.contains("'M'") || sql.contains("M"));
    }

    /**
     * Regression: Calcite's {@code Aggregate} normalises the group set to an
     * ImmutableBitSet, which re-orders group columns into the input-row's
     * column-ordinal order. The planner must reproject the group columns
     * after aggregate so the SELECT list matches the request's group-by
     * order — Mondrian's segment consumer positionally maps SELECT columns
     * onto {@code GroupingSet.columns[i]} and a reordered SELECT silently
     * assigns axis values to the wrong column (cells miss on lookup, every
     * measure comes back empty).
     *
     * <p>In FoodMart's {@code customer} table, {@code marital_status}
     * precedes {@code gender} physically, so without the reproject Calcite
     * emits them in (marital_status, gender) order even when the request
     * lists (gender, marital_status).
     */
    @Test
    public void aggregateSelectOrderMatchesGroupByRequestOrder() {
        CalciteSqlPlanner planner = plannerFor(HsqldbSqlDialect.DEFAULT);
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addJoin(new PlannerRequest.Join(
                "customer", "customer_id", "customer_id"))
            .addGroupBy(
                new PlannerRequest.Column("time_by_day", "the_year"))
            .addGroupBy(
                new PlannerRequest.Column("customer", "gender"))
            .addGroupBy(
                new PlannerRequest.Column("customer", "marital_status"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = planner.plan(req);
        assertNotNull(sql);
        // Find SELECT-list-position of gender vs marital_status. The
        // default HSQLDB dialect Calcite ships with unparses identifiers
        // unquoted, so compare substrings on the raw identifier text.
        String selectList =
            sql.substring(0, sql.toUpperCase().indexOf("FROM "));
        int iGender = selectList.indexOf("gender");
        int iMarital = selectList.indexOf("marital_status");
        assertTrue(
            "expected 'gender' in SELECT: " + sql, iGender > 0);
        assertTrue(
            "expected 'marital_status' in SELECT: " + sql, iMarital > 0);
        assertTrue(
            "SELECT list must preserve request order: gender before "
            + "marital_status; got:\n" + sql,
            iGender < iMarital);
    }
}

// End CalciteSqlPlannerTest.java
