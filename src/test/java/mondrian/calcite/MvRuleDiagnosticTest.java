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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.List;

/**
 * Diagnostic-only test for Task U's follow-up: pinpoint WHY Calcite
 * 1.41's {@code MaterializedViewRule} family does not rewrite the
 * {@code agg-c-year-country} MvHit query onto
 * {@code agg_c_14_sales_fact_1997}, even though both the MvRegistry
 * and the VolcanoPlanner stage are wired.
 *
 * <p>This test is intentionally asserting almost nothing — its value
 * is the captured output. The output is used to write the diagnosis
 * report; the test remains in-tree as regression against future
 * Calcite version upgrades (a change in how {@code SubstitutionVisitor}
 * reports its state will be visible here first).
 *
 * <p>What it does:
 * <ol>
 *   <li>Builds a {@link PlannerRequest} that mirrors the SQL Mondrian
 *       emits for {@code agg-c-year-country}:
 *       {@code Aggregate[the_year, store_country, SUM(unit_sales)](
 *         Join(sales_fact_1997, store) Join(time_by_day))}.</li>
 *   <li>Builds the raw RelNode via {@code CalciteSqlPlanner.planRel}.</li>
 *   <li>Runs the Hep stage to produce the tree that Volcano would see.</li>
 *   <li>Builds the full FoodMart MvRegistry.</li>
 *   <li>Prints, for every registered MV:
 *       target row type + defining-query row type + full RelNode dumps.</li>
 *   <li>Runs {@link VolcanoPlanner} with MV rules registered, captures
 *       the plan before/after, and prints whether the root changed.</li>
 * </ol>
 *
 * <p>Output goes to {@code System.out} during test execution.
 */
public class MvRuleDiagnosticTest {

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

    /**
     * Build a PlannerRequest modelling the SQL shape Mondrian emits
     * for the {@code agg-c-year-country} MvHit query:
     *
     * <pre>
     *   SELECT store.store_country, time_by_day.the_year,
     *          SUM(sales_fact_1997.unit_sales)
     *   FROM sales_fact_1997, store, time_by_day
     *   WHERE sales_fact_1997.store_id = store.store_id
     *     AND sales_fact_1997.time_id = time_by_day.time_id
     *   GROUP BY store.store_country, time_by_day.the_year
     * </pre>
     */
    private static PlannerRequest yearCountryRequest() {
        return PlannerRequest.builder("sales_fact_1997")
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
    }

    @Test
    public void dumpMvRuleDiagnosis() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintWriter w = new PrintWriter(buf, true);

        CalciteMondrianSchema cms =
            new CalciteMondrianSchema(ds, "foodmart");
        CalciteSqlPlanner planner =
            new CalciteSqlPlanner(cms, HsqldbSqlDialect.DEFAULT);
        MvRegistry reg = MvRegistry.fromSchema(rolapSchema, cms);
        planner.attachMvRegistry(reg);

        w.println("=== MvRegistry contents (" + reg.size()
            + " MVs, skipped=" + reg.skippedAggregates() + ") ===");
        w.println();

        RelOptMaterialization aggC14 = null;
        for (RelOptMaterialization m : reg.materializations()) {
            String name =
                m.qualifiedTableName.get(
                    m.qualifiedTableName.size() - 1);
            w.println("--- MV: " + name + " ---");
            w.println("target.rowType = "
                + m.tableRel.getRowType().getFullTypeString());
            w.println("queryRel.rowType = "
                + m.queryRel.getRowType().getFullTypeString());
            w.println("target:");
            w.println(RelOptUtil.toString(m.tableRel));
            w.println("queryRel:");
            w.println(RelOptUtil.toString(m.queryRel));
            if ("agg_c_14_sales_fact_1997".equals(name)) {
                aggC14 = m;
            }
        }

        // --- user query: raw + hep ---
        PlannerRequest req = yearCountryRequest();
        RelNode raw = planner.planRel(req);
        w.println("=== user query: raw RelNode (post-RelBuilder) ===");
        w.println("rowType = " + raw.getRowType().getFullTypeString());
        w.println(RelOptUtil.toString(raw));

        RelNode hepped = planner.optimize(raw);
        w.println("=== user query: post-Hep RelNode ===");
        w.println("rowType = " + hepped.getRowType().getFullTypeString());
        w.println(RelOptUtil.toString(hepped));

        // --- Volcano with FoodMart MvRegistry ---
        w.println("=== Volcano stage ===");
        // Print row-count estimates we surface to Volcano — sanity
        // check that the MV table is cheaper than the fact join.
        w.println("rowCount(sales_fact_1997) = "
            + cms.rowCount("sales_fact_1997"));
        w.println("rowCount(agg_c_14_sales_fact_1997) = "
            + cms.rowCount("agg_c_14_sales_fact_1997"));
        w.println("rowCount(store) = " + cms.rowCount("store"));
        w.println("rowCount(time_by_day) = " + cms.rowCount("time_by_day"));
        RelNode afterVolcano;
        try {
            afterVolcano = planner.runVolcano(hepped);
        } catch (Throwable t) {
            w.println("runVolcano threw: " + t);
            t.printStackTrace(w);
            afterVolcano = hepped;
        }
        w.println("rowType = "
            + afterVolcano.getRowType().getFullTypeString());
        w.println(RelOptUtil.toString(afterVolcano));
        w.println("identity-changed? "
            + (afterVolcano != hepped));
        w.println("text-equal-to-hep? "
            + RelOptUtil.toString(afterVolcano)
                .equals(RelOptUtil.toString(hepped)));
        // Contains agg_c_14_sales_fact_1997 anywhere?
        w.println("afterVolcano contains agg_c_14? "
            + RelOptUtil.toString(afterVolcano)
                .contains("agg_c_14_sales_fact_1997"));

        // --- Manual Volcano run with verbose trait reporting ---
        if (aggC14 != null) {
            w.println();
            w.println("=== Focused Volcano run vs agg_c_14 only ===");
            try {
                VolcanoPlanner vp = new VolcanoPlanner();
                vp.addRelTraitDef(ConventionTraitDef.INSTANCE);
                vp.registerAbstractRelationalRules();
                // Register the same MV-rule family the production
                // planner uses (reflect via the field we can reach).
                for (RelOptRule r : volcanoRulesFromProduction()) {
                    vp.addRule(r);
                }
                vp.addMaterialization(aggC14);
                RelTraitSet target = hepped.getTraitSet();
                RelNode rooted = hepped.getTraitSet().equals(target)
                    ? hepped
                    : vp.changeTraits(hepped, target);
                vp.setRoot(rooted);
                RelNode best = vp.findBestExp();
                w.println("focused-best rowType = "
                    + best.getRowType().getFullTypeString());
                w.println(RelOptUtil.toString(best));
                w.println("agg_c_14 present in best plan? "
                    + RelOptUtil.toString(best)
                        .contains("agg_c_14_sales_fact_1997"));
            } catch (Throwable t) {
                w.println("focused Volcano threw: " + t);
                t.printStackTrace(w);
            }
        }

        // --- Diagnosis via SubstitutionVisitor direct call ---
        // Probe EVERY registered MV, not just the last one named
        // agg_c_14 — multiple shapes are registered under the same name.
        w.println();
        w.println("=== SubstitutionVisitor direct probe (all MVs) ===");
        int totalHits = 0;
        int idx = 0;
        for (RelOptMaterialization m : reg.materializations()) {
            w.println(" -- MV["
                + (idx++) + "] target="
                + m.qualifiedTableName
                + " queryRowType="
                + m.queryRel.getRowType().getFieldCount()
                + "cols --");
            totalHits += probeSubstitution(m, hepped, w);
        }
        w.println("Total SubstitutionVisitor hits: " + totalHits);

        // --- Probe table statistics on every TableScan ---
        w.println();
        w.println("=== Statistic probe (getKeys / getReferentialConstraints) ===");
        if (aggC14 != null) {
            w.println("MV queryRel statistics:");
            dumpStatistics(aggC14.queryRel, w);
            w.println("MV tableRel statistics:");
            dumpStatistics(aggC14.tableRel, w);
        }
        w.println("User query (hepped) statistics:");
        dumpStatistics(hepped, w);

        w.println();
        w.println("=== CalciteMondrianSchema constraint snapshot ===");
        for (java.util.Map.Entry<String, java.util.List<java.util.List<String>>> e
            : cms.primaryKeysSnapshot().entrySet())
        {
            w.println("  PK " + e.getKey() + " = " + e.getValue());
        }
        for (java.util.Map.Entry<String,
                    java.util.List<CalciteMondrianSchema.ForeignKey>> e
            : cms.foreignKeysSnapshot().entrySet())
        {
            StringBuilder sb = new StringBuilder();
            for (CalciteMondrianSchema.ForeignKey fk : e.getValue()) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(fk.childColumn).append(" -> ")
                    .append(fk.parentTable).append(".")
                    .append(fk.parentColumn);
            }
            w.println("  FK " + e.getKey() + " : " + sb);
        }

        System.out.println(buf.toString());

        // Regression gate: PK/FK metadata must let at least one MV
        // produce a non-empty substitution list. Before the PK/FK
        // plumbing this was zero for every shape; keeping the assert
        // surfaces any future regression (Calcite upgrade, MvRegistry
        // shape change, schema decorator bug).
        org.junit.Assert.assertTrue(
            "Expected >=1 SubstitutionVisitor hit across registered MVs; "
                + "PK/FK metadata regression likely",
            totalHits >= 1);
    }

    /** Walk a RelNode tree and report getKeys / getReferentialConstraints
     *  on every TableScan encountered. */
    private static void dumpStatistics(RelNode rel, PrintWriter w) {
        rel.accept(new org.apache.calcite.rel.RelShuttleImpl() {
            @Override public RelNode visit(
                org.apache.calcite.rel.core.TableScan scan)
            {
                org.apache.calcite.plan.RelOptTable t = scan.getTable();
                w.println("  scan " + t.getQualifiedName()
                    + " keys=" + t.getKeys()
                    + " refs=" + t.getReferentialConstraints());
                return super.visit(scan);
            }
        });
    }

    /** Reflectively grab the production VOLCANO_RULES so the
     *  diagnostic stays in sync with what the planner actually
     *  registers. Falls back to an empty list if reflection fails. */
    private static RelOptRule[] volcanoRulesFromProduction()
        throws Exception
    {
        java.lang.reflect.Field f =
            CalciteSqlPlanner.class.getDeclaredField("VOLCANO_RULES");
        f.setAccessible(true);
        return (RelOptRule[]) f.get(null);
    }

    /**
     * Directly invoke {@code SubstitutionVisitor.go(target)} with the
     * MV's defining query as the "target" and the user's rel as the
     * "query" — mirroring the call the MaterializedView rule makes
     * inside Volcano. Captures any exception and whether a non-empty
     * substitution list came back.
     */
    private static int probeSubstitution(
        RelOptMaterialization mv,
        RelNode userRel,
        PrintWriter w)
    {
        try {
            // SubstitutionVisitor(target, query) — "target" is the
            // MV's defining query (what we want to match against),
            // "query" is the user's rel (what the planner has).
            // Reflective because the public signature varies
            // slightly across Calcite versions; this pins us to
            // 1.41 without hard-coding the class.
            Class<?> svClass =
                Class.forName(
                    "org.apache.calcite.plan.SubstitutionVisitor");
            java.lang.reflect.Constructor<?> ctor =
                svClass.getConstructor(RelNode.class, RelNode.class);
            Object sv = ctor.newInstance(mv.queryRel, userRel);
            java.lang.reflect.Method go =
                svClass.getMethod("go", RelNode.class);
            @SuppressWarnings("unchecked")
            List<RelNode> subs =
                (List<RelNode>) go.invoke(sv, mv.tableRel);
            w.println("substitutions returned: "
                + (subs == null ? "null" : subs.size()));
            if (subs != null) {
                int i = 0;
                for (RelNode s : subs) {
                    w.println("  [" + (i++) + "]:");
                    w.println(RelOptUtil.toString(s));
                }
                return subs.size();
            }
            return 0;
        } catch (java.lang.reflect.InvocationTargetException ite) {
            w.println("SubstitutionVisitor threw: " + ite.getCause());
            ite.getCause().printStackTrace(w);
            return 0;
        } catch (Throwable t) {
            w.println("SubstitutionVisitor probe failed: " + t);
            t.printStackTrace(w);
            return 0;
        }
    }
}

// End MvRuleDiagnosticTest.java
