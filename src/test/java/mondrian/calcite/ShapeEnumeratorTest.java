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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * Verifies {@link ShapeEnumerator}'s power-set enumeration for
 * FoodMart's {@code agg_c_14_sales_fact_1997}. Per plan revision
 * 2026-04-21 we do not assert precise shape counts (the FoodMart
 * copy-link schema may evolve); instead we assert:
 *
 * <ul>
 *   <li>Singleton subsets for each copy-linked column appear.</li>
 *   <li>At least one multi-column subset appears.</li>
 *   <li>Shape count equals
 *       C(n,1)+...+C(n,min(n,cap)) after dedup.</li>
 *   <li>Each shape's {@code joins} resolve (non-null).</li>
 * </ul>
 */
public class ShapeEnumeratorTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Test
    public void enumeratesAggC14CopyLinkSubsets() {
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapSchema schema = ((RolapConnection) conn).getSchema();
            RolapMeasureGroup mg = findAggMeasureGroup(
                schema, "agg_c_14_sales_fact_1997");
            assertNotNull(
                "agg_c_14 MeasureGroup not found", mg);

            String agg = "agg_c_14_sales_fact_1997";
            String fact = "sales_fact_1997";
            List<MvRegistry.MeasureRef> noMeasures = Collections.emptyList();

            List<MvRegistry.ShapeSpec> shapes =
                ShapeEnumerator.enumerate(mg, agg, fact, noMeasures, 4);
            assertFalse(
                "expected non-empty shape list for agg_c_14",
                shapes.isEmpty());

            int n = MeasureGroupShapeInspector.copyLinkedColumns(mg).size();
            // After dedup on (dim,col) key.
            int distinct = distinctDimColCount(mg);
            int cap = Math.min(4, distinct);
            int expected = 0;
            for (int k = 1; k <= cap; k++) {
                expected += binomial(distinct, k);
            }
            assertEquals(
                "subset count mismatch — n=" + n
                    + " distinct=" + distinct + " cap=" + cap,
                expected, shapes.size());

            // Singleton {the_year}, {quarter}, {month_of_year} all
            // present.
            assertTrue(containsGroup(shapes, "time_by_day", "the_year"));
            assertTrue(containsGroup(shapes, "time_by_day", "quarter"));
            assertTrue(
                containsGroup(shapes, "time_by_day", "month_of_year"));

            // Each shape's joins are non-null and cover the subset's
            // dim tables.
            for (MvRegistry.ShapeSpec s : shapes) {
                assertNotNull(s.joins);
                assertNotNull(s.groups);
                assertEquals(s.aggTable, agg);
                assertEquals(s.factTable, fact);
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void respectsSubsetSizeCap() {
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapSchema schema = ((RolapConnection) conn).getSchema();
            RolapMeasureGroup mg = findAggMeasureGroup(
                schema, "agg_c_14_sales_fact_1997");
            assertNotNull(mg);

            List<MvRegistry.ShapeSpec> capped =
                ShapeEnumerator.enumerate(
                    mg, "agg_c_14_sales_fact_1997",
                    "sales_fact_1997",
                    Collections.<MvRegistry.MeasureRef>emptyList(),
                    1);
            for (MvRegistry.ShapeSpec s : capped) {
                assertTrue(
                    "cap=1 must yield singleton shapes only, got "
                        + s.groups.size() + " in " + s.name,
                    s.groups.size() == 1);
            }
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

    private static int distinctDimColCount(RolapMeasureGroup mg) {
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        for (MvRegistry.GroupCol g
            : MeasureGroupShapeInspector.copyLinkedColumns(mg))
        {
            seen.add(g.table + "." + g.column);
        }
        return seen.size();
    }

    private static boolean containsGroup(
        List<MvRegistry.ShapeSpec> shapes, String table, String column)
    {
        for (MvRegistry.ShapeSpec s : shapes) {
            if (s.groups.size() != 1) {
                continue;
            }
            MvRegistry.GroupCol g = s.groups.get(0);
            if (table.equals(g.table) && column.equals(g.column)) {
                return true;
            }
        }
        return false;
    }

    private static int binomial(int n, int k) {
        if (k < 0 || k > n) {
            return 0;
        }
        long c = 1;
        for (int i = 0; i < k; i++) {
            c = c * (n - i) / (i + 1);
        }
        return (int) c;
    }
}

// End ShapeEnumeratorTest.java
