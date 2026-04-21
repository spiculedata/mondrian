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
import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapSchema;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Exercises the hand-rolled MV matcher ({@link MvMatcher}) that operates
 * at the {@link PlannerRequest} level (Option D, design in
 * {@code docs/reports/perf-investigation-volcano-mv-win.md}).
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@link #matchesMvHitShape} — the year×country MvHit shape gets
 *       rewritten onto {@code agg_c_14_sales_fact_1997}.</li>
 *   <li>{@link #matchesWithDimJoinRetained} — a shape whose group col
 *       lives on a dim NOT denormalized on the agg (store_country on
 *       agg_c_14) keeps that join, but still rewrites factTable.</li>
 *   <li>{@link #noMatchWhenMeasureUnsupported} — DistinctCount aggregator
 *       blocks rewrite (no lossless rollup).</li>
 *   <li>{@link #noMatchWhenFilterUnsupported} — a filter on a column
 *       neither denormalized on the agg nor on a dim the agg joins to
 *       leaves the request untouched.</li>
 * </ul>
 */
public class MvMatcherTest {

    private static Connection mondrianConn;
    private static RolapSchema rolapSchema;
    private static DataSource ds;
    private static MvRegistry registry;

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        mondrianConn = DriverManager.getConnection(props, null, null);
        RolapConnection rc = (RolapConnection) mondrianConn;
        rolapSchema = rc.getSchema();
        ds = rc.getDataSource();
        CalciteMondrianSchema cms = new CalciteMondrianSchema(ds, "foodmart");
        registry = MvRegistry.fromSchema(rolapSchema, cms);
    }

    @AfterClass
    public static void close() {
        if (mondrianConn != null) {
            mondrianConn.close();
            mondrianConn = null;
        }
    }

    /** MvHit year×country shape: rewrite onto agg_c_14, drop the
     *  time_by_day join (year denormalized), keep store join
     *  (store_country not denormalized). */
    @Test
    public void matchesMvHitShape() {
        PlannerRequest yc = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        PlannerRequest out = MvMatcher.tryRewrite(yc, registry);
        assertNotSame("year-country should rewrite", yc, out);
        assertEquals(
            "factTable should point at agg_c_14",
            "agg_c_14_sales_fact_1997",
            out.factTable);
        boolean hasTime = false;
        boolean hasStore = false;
        for (PlannerRequest.Join j : out.joins) {
            if ("time_by_day".equals(j.dimTable)) hasTime = true;
            if ("store".equals(j.dimTable)) hasStore = true;
        }
        assertTrue("year is denormalized — time join should be dropped",
            !hasTime);
        assertTrue("store_country not denormalized — store join retained",
            hasStore);
        assertEquals(1, out.measures.size());
        assertEquals(
            "agg_c_14_sales_fact_1997", out.measures.get(0).column.table);
        assertEquals("unit_sales", out.measures.get(0).column.name);
    }

    /** MvHit #2 — store_country is NOT denormalized on agg_c_14;
     *  the store join is retained but factTable switches. */
    @Test
    public void matchesWithDimJoinRetained() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        PlannerRequest out = MvMatcher.tryRewrite(req, registry);
        assertNotSame(req, out);
        assertEquals("agg_c_14_sales_fact_1997", out.factTable);
        assertEquals(1, out.joins.size());
        assertEquals("store", out.joins.get(0).dimTable);
    }

    /** A DistinctCount aggregator (e.g. Customer Count) can't roll up
     *  over a pre-aggregated table; leave the request alone. */
    @Test
    public void noMatchForDistinctCount() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.COUNT,
                new PlannerRequest.Column(
                    "sales_fact_1997", "customer_id"),
                "m0",
                true /* distinct */))
            .build();
        assertSame(req, MvMatcher.tryRewrite(req, registry));
    }

    /** Filter on a column the agg doesn't carry and can't reach via
     *  its retained dim joins — reject. */
    @Test
    public void noMatchWhenFilterUnsupported() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addFilter(new PlannerRequest.Filter(
                // warehouse table — not present on any agg shape
                new PlannerRequest.Column("warehouse", "warehouse_id"),
                42))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        assertSame(req, MvMatcher.tryRewrite(req, registry));
    }

    /** MvHit #1 — product_family × gender. Columns live on
     *  {@code product_class} (snowflake-chained off {@code product})
     *  and {@code customer}, both CopyLink-denormalized on the agg.
     *  Matcher should rewrite onto {@code agg_g_ms_pcat_sales_fact_1997}
     *  and drop all three joins (product, product_class, customer). */
    @Test
    public void matchesCopyLinkShape_familyGender() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "product", "product_id", "product_id"))
            .addJoin(PlannerRequest.Join.chained(
                "product", "product_class_id",
                "product_class", "product_class_id"))
            .addJoin(new PlannerRequest.Join(
                "customer", "customer_id", "customer_id"))
            .addGroupBy(new PlannerRequest.Column(
                "product_class", "product_family"))
            .addGroupBy(new PlannerRequest.Column(
                "customer", "gender"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        PlannerRequest out = MvMatcher.tryRewrite(req, registry);
        assertNotSame("family-gender should rewrite", req, out);
        assertEquals(
            "factTable should point at agg_g_ms_pcat_sales_fact_1997",
            "agg_g_ms_pcat_sales_fact_1997",
            out.factTable);
        assertTrue(
            "All dim joins CopyLinked on agg; none should be retained",
            out.joins.isEmpty());
        // Group-by columns are rewritten to the agg-side denorm columns.
        for (PlannerRequest.Column gc : out.groupBy) {
            assertEquals(
                "groupBy column should reference the agg table",
                "agg_g_ms_pcat_sales_fact_1997",
                gc.table);
        }
        assertEquals(1, out.measures.size());
        assertEquals(
            "agg_g_ms_pcat_sales_fact_1997",
            out.measures.get(0).column.table);
        assertEquals("unit_sales", out.measures.get(0).column.name);
    }

    /** MvHit #4 — product_family × gender × marital_status. Three-col
     *  CopyLinked group-by: product_family (product_class), gender and
     *  marital_status (customer). All three should come off the agg. */
    @Test
    public void matchesCopyLinkShape_familyGenderMarital() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "product", "product_id", "product_id"))
            .addJoin(PlannerRequest.Join.chained(
                "product", "product_class_id",
                "product_class", "product_class_id"))
            .addJoin(new PlannerRequest.Join(
                "customer", "customer_id", "customer_id"))
            .addGroupBy(new PlannerRequest.Column(
                "product_class", "product_family"))
            .addGroupBy(new PlannerRequest.Column(
                "customer", "gender"))
            .addGroupBy(new PlannerRequest.Column(
                "customer", "marital_status"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        PlannerRequest out = MvMatcher.tryRewrite(req, registry);
        assertNotSame(
            "family-gender-marital should rewrite", req, out);
        assertEquals(
            "agg_g_ms_pcat_sales_fact_1997", out.factTable);
        assertTrue(
            "All dim joins CopyLinked; none retained",
            out.joins.isEmpty());
        assertEquals(3, out.groupBy.size());
        for (PlannerRequest.Column gc : out.groupBy) {
            assertEquals(
                "agg_g_ms_pcat_sales_fact_1997", gc.table);
        }
    }

    /** Reject when the user references a dim column that's neither
     *  denormalized/CopyLinked on the agg nor reachable via a retained
     *  join. {@code customer.yearly_income} is not on
     *  {@code agg_g_ms_pcat_sales_fact_1997}, and no shape declares a
     *  customer FK join → match must fail, request untouched. */
    @Test
    public void rejectWhenDimColumnNotCopyLinked() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "product", "product_id", "product_id"))
            .addJoin(PlannerRequest.Join.chained(
                "product", "product_class_id",
                "product_class", "product_class_id"))
            .addJoin(new PlannerRequest.Join(
                "customer", "customer_id", "customer_id"))
            .addGroupBy(new PlannerRequest.Column(
                "product_class", "product_family"))
            .addGroupBy(new PlannerRequest.Column(
                "customer", "gender"))
            // yearly_income is NOT a CopyLink column on the agg and no
            // shape keeps the customer dim as a retained-join
            // resolution path.
            .addGroupBy(new PlannerRequest.Column(
                "customer", "yearly_income"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        assertSame(
            "unresolvable dim col must block rewrite",
            req,
            MvMatcher.tryRewrite(req, registry));
    }

    /** Empty registry is a no-op. */
    @Test
    public void emptyRegistryIsNoOp() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addGroupBy(new PlannerRequest.Column(
                "sales_fact_1997", "product_id"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        assertSame(req, MvMatcher.tryRewrite(req, new MvRegistry() {}));
    }

    /** Plan-level sanity: plan() with registry attached emits SQL
     *  referencing the agg table for the year-country shape. */
    @Test
    public void planEmitsAggTableScan() {
        CalciteMondrianSchema cms =
            new CalciteMondrianSchema(ds, "foodmart");
        CalciteSqlPlanner p = new CalciteSqlPlanner(
            cms,
            org.apache.calcite.sql.dialect.HsqldbSqlDialect.DEFAULT);
        p.attachMvRegistry(MvRegistry.fromSchema(rolapSchema, cms));

        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = p.plan(req);
        assertNotNull(sql);
        assertTrue(
            "plan() SQL should reference agg_c_14 after rewrite; got: "
                + sql,
            sql.contains("agg_c_14_sales_fact_1997"));
        // And should NOT reference sales_fact_1997 anymore.
        assertTrue(
            "plan() SQL should not scan sales_fact_1997 after rewrite; got: "
                + sql,
            !sql.contains("sales_fact_1997 ")
                && !sql.contains("sales_fact_1997\""));
    }
}

// End MvMatcherTest.java
