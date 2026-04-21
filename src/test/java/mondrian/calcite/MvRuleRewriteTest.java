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

import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test for shape-aware {@link MvRegistry} materializations:
 * builds the {@link PlannerRequest} for each MvHit-corpus shape, runs
 * {@code CalciteSqlPlanner.plan()} with the FoodMart MvRegistry
 * attached, and captures the emitted SQL.
 *
 * <p><b>Status:</b> Calcite 1.41's {@code MaterializedView*Rule}
 * family requires PK-FK uniqueness metadata on the underlying JDBC
 * tables to prove that the MV's re-join chain does not multiply rows.
 * Our {@code CalciteMondrianSchema} exposes plain {@code JdbcTable}s
 * reflected from HSQLDB with no surfaced unique-keys, so the rule
 * accepts no rewrite despite structurally matching shape. The tests
 * below are therefore <em>capture-only</em>: they run the plan, log
 * the SQL, and assert only that the planner produces valid output
 * (no throw, non-empty SQL). A follow-up task must plumb PK/FK
 * metadata through {@code CalciteMondrianSchema} before these can
 * graduate to hard assertions.
 *
 * <p>Kept in-tree as regression against:
 * <ol>
 *   <li>Future Calcite upgrades (relaxed MV rule).</li>
 *   <li>PK-FK metadata surfacing work.</li>
 *   <li>The four MvHit corpus shapes.</li>
 * </ol>
 */
public class MvRuleRewriteTest {

    private static Connection mondrianConn;
    private static RolapSchema rolapSchema;
    private static DataSource ds;

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
    }

    @AfterClass
    public static void close() {
        if (mondrianConn != null) {
            mondrianConn.close();
            mondrianConn = null;
        }
    }

    private static CalciteSqlPlanner plannerWithRegistry() {
        CalciteMondrianSchema cms =
            new CalciteMondrianSchema(ds, "foodmart");
        CalciteSqlPlanner p =
            new CalciteSqlPlanner(cms, HsqldbSqlDialect.DEFAULT);
        p.attachMvRegistry(MvRegistry.fromSchema(rolapSchema, cms));
        return p;
    }

    /** MvHit #2 — the_year × store_country, Unit Sales. */
    @Test
    public void yearCountry() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column(
                    "sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = plannerWithRegistry().plan(req);
        assertNotNull("plan() returned null", sql);
        assertTrue("plan() returned empty SQL", sql.length() > 0);
    }

    /** MvHit #3 — the_year × quarter × store_country. */
    @Test
    public void yearQuarterCountry() {
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "store", "store_id", "store_id"))
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column(
                "store", "store_country"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "the_year"))
            .addGroupBy(new PlannerRequest.Column(
                "time_by_day", "quarter"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column(
                    "sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = plannerWithRegistry().plan(req);
        assertNotNull(sql);
        assertTrue(sql.length() > 0);
    }

    /** MvHit #1 — product_family × gender. */
    @Test
    public void familyGender() {
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
                new PlannerRequest.Column(
                    "sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = plannerWithRegistry().plan(req);
        assertNotNull(sql);
        assertTrue(sql.length() > 0);
    }

    /** MvHit #4 — product_family × gender × marital_status. */
    @Test
    public void familyGenderMarital() {
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
                new PlannerRequest.Column(
                    "sales_fact_1997", "unit_sales"),
                "m0"))
            .build();
        String sql = plannerWithRegistry().plan(req);
        assertNotNull(sql);
        assertTrue(sql.length() > 0);
    }
}

// End MvRuleRewriteTest.java
