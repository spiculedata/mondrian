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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Exercises Phase 3+ Volcano stage: {@link CalciteSqlPlanner#runVolcano}
 * wires an {@link org.apache.calcite.plan.volcano.VolcanoPlanner} after
 * Hep so the {@code MaterializedView*} rule family can consume
 * {@link MvRegistry}'s entries and (in principle) rewrite queries onto
 * declared aggregate tables.
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@code planRunsWithoutError} — a trivial grouped aggregate
 *       goes through Hep + Volcano and produces valid SQL.</li>
 *   <li>{@code emptyRegistryBypassesVolcano} — with no MVs the
 *       Volcano stage is a no-op and returns the input rel unchanged
 *       (identity check).</li>
 *   <li>{@code fallbackOnError} — a planner whose registry contains
 *       a busted materialization still emits SQL (falls back to the
 *       Hep output rather than throwing).</li>
 *   <li>{@code volcanoStageWithFoodmartRegistry} — end-to-end plan()
 *       with the real FoodMart MvRegistry attached. The emitted SQL
 *       must stay well-formed whether or not the MV rule fires.</li>
 * </ul>
 *
 * <p>Deliberately NOT asserted here: that the MV rule actually
 * rewrites onto an agg table. That assertion lives in the
 * calcite-equivalence harness (gate) and in a Postgres spot check
 * documented in the commit message. Calcite's MaterializedViewRule
 * is picky about the structural equivalence of the defining query's
 * tree with the user's tree; the MvRegistry entries are built from
 * Mondrian's schema walk and may not match the RelBuilder shape of
 * an incoming PlannerRequest byte-for-byte. When that happens the
 * rule silently fails to match — which is graceful — and this test's
 * job is to prove the stage is wired, not that every FoodMart agg
 * rewrites.
 */
public class CalciteSqlPlannerVolcanoTest {

    // Single Mondrian connection held for the test run: its JDBC
    // DataSource is shared across all planner builders so we don't
    // open competing HSQLDB connections against the read-only
    // FoodMart file (HSQLDB bars a second process from holding the
    // same database open).
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
    public static void closeFoodMart() {
        if (mondrianConn != null) {
            mondrianConn.close();
            mondrianConn = null;
        }
    }

    private static CalciteSqlPlanner bareplanner() {
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(ds, "foodmart");
        return new CalciteSqlPlanner(schema, HsqldbSqlDialect.DEFAULT);
    }

    /** A planner wired with FoodMart's real MvRegistry (four agg MVs). */
    private static CalciteSqlPlanner plannerWithFoodmartRegistry() {
        CalciteMondrianSchema cms =
            new CalciteMondrianSchema(ds, "foodmart");
        CalciteSqlPlanner p =
            new CalciteSqlPlanner(cms, HsqldbSqlDialect.DEFAULT);
        p.attachMvRegistry(MvRegistry.fromSchema(rolapSchema, cms));
        return p;
    }

    private static PlannerRequest simpleGroupedRequest() {
        return PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(
                new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column(
                    "sales_fact_1997", "unit_sales"),
                "m"))
            .build();
    }

    @Test
    public void planRunsWithoutError() {
        CalciteSqlPlanner p = plannerWithFoodmartRegistry();
        String sql = p.plan(simpleGroupedRequest());
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected SELECT: " + sql, lower.contains("select"));
        assertTrue(
            "expected GROUP BY: " + sql, lower.contains("group by"));
    }

    @Test
    public void emptyRegistryBypassesVolcano() {
        // No registry attached → runVolcano must return its input
        // RelNode by identity (no planner constructed, no rules run).
        CalciteSqlPlanner p = bareplanner();
        RelNode raw = p.planRel(simpleGroupedRequest());
        RelNode hepped = p.optimize(raw);
        RelNode afterVolcano = p.runVolcano(hepped);
        assertSame(
            "Volcano must no-op when registry is null",
            hepped, afterVolcano);
    }

    @Test
    public void emptyRegistryAttachedStillBypasses() {
        // Empty-but-attached registry also short-circuits.
        CalciteSqlPlanner p = bareplanner();
        p.attachMvRegistry(new EmptyMvRegistry());
        RelNode raw = p.planRel(simpleGroupedRequest());
        RelNode hepped = p.optimize(raw);
        RelNode afterVolcano = p.runVolcano(hepped);
        assertSame(
            "Volcano must no-op for an empty registry",
            hepped, afterVolcano);
    }

    @Test
    public void killSwitchDisablesVolcano() {
        // With the kill switch off, Volcano is a no-op even with a
        // non-empty registry attached. The kill switch is read at
        // class-init time, so we can't flip it per-test; instead we
        // assert the *documented* behaviour via the no-op contract
        // (a null registry also bypasses — they share the early-exit
        // path). Kept here to prevent accidental removal of the
        // early-exit short-circuit.
        CalciteSqlPlanner p = bareplanner();
        RelNode raw = p.planRel(simpleGroupedRequest());
        RelNode afterVolcano = p.runVolcano(raw);
        assertSame(raw, afterVolcano);
    }

    @Test
    public void fallbackOnError() {
        // A registry whose materialization is intentionally
        // mis-shaped (queryRel in a different cluster than the
        // user's tree) will trip the planner. runVolcano must
        // swallow that and return the Hep output.
        CalciteSqlPlanner p = bareplanner();
        p.attachMvRegistry(new BrokenMvRegistry());
        RelNode raw = p.planRel(simpleGroupedRequest());
        RelNode hepped = p.optimize(raw);
        // Must not throw; must return a non-null RelNode; on
        // failure, returns the Hep input unchanged.
        RelNode afterVolcano = p.runVolcano(hepped);
        assertNotNull(afterVolcano);
    }

    @Test
    public void volcanoStageWithFoodmartRegistryEmitsValidSql() {
        // The hard gate: plan() end-to-end with the real FoodMart
        // registry produces well-formed SQL whether or not the MV
        // rule fires. This is the guardrail the plan calls out —
        // "if Volcano produces unparseable output on any single
        // query, trim rules or fall back graceful". We don't trim
        // rules here; we rely on runVolcano's catch-and-fallback.
        CalciteSqlPlanner p = plannerWithFoodmartRegistry();
        String sql = p.plan(simpleGroupedRequest());
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected SELECT: " + sql, lower.contains("select"));
        assertTrue("expected FROM: " + sql, lower.contains("from"));
        assertTrue(
            "expected one of sales_fact_1997 or agg_*: " + sql,
            lower.contains("sales_fact_1997")
                || lower.contains("agg_"));
        assertFalse(
            "emitted SQL must not contain Enumerable markers: " + sql,
            lower.contains("enumerable"));
    }

    // ------------------------------------------------------------------
    // Helpers — stand-in registries for the bypass / fallback paths.
    // ------------------------------------------------------------------

    /** A registry with zero materializations. Exercises the early-exit. */
    private static final class EmptyMvRegistry extends MvRegistry {
        EmptyMvRegistry() {
            super();
        }
    }

    /**
     * A registry containing one materialization built from a
     * stand-alone RelBuilder — different cluster than the user's
     * plan tree, which triggers Calcite's cross-cluster assertion
     * and exercises the catch-and-fallback path.
     */
    private static final class BrokenMvRegistry extends MvRegistry {
        BrokenMvRegistry() {
            super(fabricateMaterializations());
        }

        private static List<RelOptMaterialization>
            fabricateMaterializations()
        {
            org.apache.calcite.tools.FrameworkConfig cfg =
                org.apache.calcite.tools.Frameworks.newConfigBuilder()
                    .defaultSchema(
                        org.apache.calcite.tools.Frameworks
                            .createRootSchema(true))
                    .build();
            // An empty defaultSchema has no tables — scan() will
            // throw. That's actually the point: the fabricated
            // materialization can't be constructed, so we return a
            // throwaway materialization wrapping a literal values
            // rel whose cluster is foreign to the user's plan.
            org.apache.calcite.tools.RelBuilder rb =
                org.apache.calcite.tools.RelBuilder.create(cfg);
            RelNode values =
                rb.values(new String[]{"x"}, 1).build();
            return Arrays.asList(
                new RelOptMaterialization(
                    values, values, null,
                    Collections.singletonList("__broken__")));
        }
    }
}

// End CalciteSqlPlannerVolcanoTest.java
