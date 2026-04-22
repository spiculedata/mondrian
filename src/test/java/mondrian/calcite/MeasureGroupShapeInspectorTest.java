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
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapMeasureGroup;
import mondrian.rolap.RolapSchema;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that {@link MeasureGroupShapeInspector#copyLinkedColumns}
 * surfaces the copy-linked columns declared on FoodMart's
 * {@code agg_c_14_sales_fact_1997}.
 *
 * <p>FoodMart schema declares CopyLinks for {@code the_year},
 * {@code quarter}, and {@code month_of_year} from {@code time_by_day}
 * onto {@code agg_c_14}. {@code store_country} is NOT copy-linked —
 * it's FK-reachable through the {@code store_id} join, and will be
 * surfaced by the Phase-2 FK-dim enumerator.
 */
public class MeasureGroupShapeInspectorTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Test
    public void agg_c_14_copyLinks_year_quarter_month() {
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapSchema schema = ((RolapConnection) conn).getSchema();
            RolapMeasureGroup mg = findAggMeasureGroup(
                schema, "agg_c_14_sales_fact_1997");
            assertNotNull(
                "agg_c_14_sales_fact_1997 MeasureGroup not found", mg);

            List<MvRegistry.GroupCol> cols =
                MeasureGroupShapeInspector.copyLinkedColumns(mg);
            assertTrue(
                "expected at least 3 copy-linked columns, got " + cols,
                cols.size() >= 3);
            assertTrue(
                "expected time_by_day.the_year in " + cols,
                contains(cols, "time_by_day", "the_year"));
            assertTrue(
                "expected time_by_day.quarter in " + cols,
                contains(cols, "time_by_day", "quarter"));
            assertTrue(
                "expected time_by_day.month_of_year in " + cols,
                contains(cols, "time_by_day", "month_of_year"));

            assertTrue(
                "hasCopyLinkedYear must be true for agg_c_14",
                MeasureGroupShapeInspector.hasCopyLinkedYear(mg));
        } finally {
            conn.close();
        }
    }

    @Test
    public void agg_g_ms_pcat_copyLinks_include_year_and_family() {
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapSchema schema = ((RolapConnection) conn).getSchema();
            RolapMeasureGroup mg = findAggMeasureGroup(
                schema, "agg_g_ms_pcat_sales_fact_1997");
            assertNotNull(
                "agg_g_ms_pcat_sales_fact_1997 MeasureGroup not found", mg);

            List<MvRegistry.GroupCol> cols =
                MeasureGroupShapeInspector.copyLinkedColumns(mg);
            assertTrue(
                "expected the_year copy-link on agg_g_ms_pcat, got " + cols,
                contains(cols, "time_by_day", "the_year"));
            assertTrue(
                "expected product_class.product_family copy-link, got "
                    + cols,
                contains(cols, "product_class", "product_family"));
            assertTrue(
                "expected customer.gender copy-link, got " + cols,
                contains(cols, "customer", "gender"));
        } finally {
            conn.close();
        }
    }

    private static RolapMeasureGroup findAggMeasureGroup(
        RolapSchema schema, String aggTableName)
    {
        for (RolapCube cube : schema.getCubeList()) {
            for (RolapMeasureGroup mg : cube.getMeasureGroups()) {
                if (!mg.isAggregate()) {
                    continue;
                }
                RolapSchema.PhysRelation rel = mg.getFactRelation();
                if (rel instanceof RolapSchema.PhysTable
                    && aggTableName.equals(
                        ((RolapSchema.PhysTable) rel).getName()))
                {
                    return mg;
                }
            }
        }
        return null;
    }

    private static boolean contains(
        List<MvRegistry.GroupCol> cols, String table, String column)
    {
        for (MvRegistry.GroupCol g : cols) {
            if (table.equals(g.table) && column.equals(g.column)) {
                return true;
            }
        }
        return false;
    }
}

// End MeasureGroupShapeInspectorTest.java
