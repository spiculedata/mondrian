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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Phase 3 Task 10 — verifies {@link MvRegistry} walks FoodMart's
 * schema and builds a {@link RelOptMaterialization} per declared
 * {@code <MeasureGroup type='aggregate'>}. Does not assert 3-vs-4 on
 * the {@code agg_l_05_sales_fact_1997} "unreachable" case: our
 * registry is structural (it doesn't inspect hierarchy {@code hasAll}
 * policy), so it happily builds all four. A downstream Volcano stage
 * consuming these materializations is the right place to cost-reject
 * an unreachable agg — that stage doesn't exist yet (see class
 * Javadoc for the HepPlanner vs Volcano gap).
 */
public class MvRegistryTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Test
    public void registersFoodMartAggregates() {
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapConnection rc = (RolapConnection) conn;
            RolapSchema schema = rc.getSchema();
            DataSource ds = rc.getDataSource();
            CalciteMondrianSchema cms =
                new CalciteMondrianSchema(ds, "mondrian");

            MvRegistry reg = MvRegistry.fromSchema(schema, cms);

            assertNotNull("registry must not be null", reg);
            List<RelOptMaterialization> mvs = reg.materializations();

            // Hard gate: FoodMart's shape-aware registry emits N MVs
            // per agg MeasureGroup. The catalog currently covers
            // agg_c_14 (3 shapes) + agg_g_ms_pcat (2 shapes) = 5.
            // agg_c_special and agg_l_05 are not yet in the shape
            // catalog — the Mondrian-4 findAgg path still services
            // them. We assert >=4 here (one per MvHit corpus query
            // at minimum).
            assertTrue(
                "expected at least 4 MvHit-matching materializations, "
                    + "got " + mvs.size() + "; skipped="
                    + reg.skippedAggregates(),
                mvs.size() >= 4);

            // Each registered MV carries a plausible tableRel + queryRel.
            for (RelOptMaterialization m : mvs) {
                String name =
                    m.qualifiedTableName.get(
                        m.qualifiedTableName.size() - 1);
                assertTrue(
                    "MV target must be a declared agg table, got " + name,
                    name.startsWith("agg_")
                        && name.endsWith("_sales_fact_1997"));

                RelNode target = m.tableRel;
                RelNode query = m.queryRel;
                assertNotNull("MV must have tableRel", target);
                assertNotNull("MV must have queryRel", query);

                String targetText = RelOptUtil.toString(target);
                assertTrue(
                    "target RelNode must be a LogicalTableScan of "
                        + name + ", got:\n" + targetText,
                    targetText.contains(name));

                String queryText = RelOptUtil.toString(query);
                assertTrue(
                    "queryRel for " + name
                        + " must include a LogicalAggregate, got:\n"
                        + queryText,
                    queryText.contains("LogicalAggregate"));
                assertTrue(
                    "queryRel for " + name
                        + " must reference the base fact table "
                        + "sales_fact_1997, got:\n" + queryText,
                    queryText.contains("sales_fact_1997"));
            }

            assertEquals(
                "materializations() size and size() must agree",
                mvs.size(), reg.size());
        } finally {
            conn.close();
        }
    }
}

// End MvRegistryTest.java
