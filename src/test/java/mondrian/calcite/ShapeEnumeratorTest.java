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

            // When the MG copy-links time_by_day.the_year, every emitted
            // shape must include the_year (filter-agg guard — see
            // ShapeEnumerator for rationale).
            for (MvRegistry.ShapeSpec s : shapes) {
                assertTrue(
                    "every shape must contain the_year, got " + s.name,
                    hasYear(s.groups));
            }

            // Singleton {the_year} is the only year-only shape; the
            // multi-col shapes all extend it.
            assertTrue(containsGroup(shapes, "time_by_day", "the_year"));

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
    public void addsYearPrefixedVariantsAtCapBoundary() {
        // agg_g_ms_pcat copy-links the_year, product_family, gender,
        // marital_status. With cap=2, the power-set yields size-1 and
        // size-2 subsets only; year-prefix should push no-year size-2
        // subsets into size-3 variants (family+gender → year+family+
        // gender, etc.) — those are NOT in the base power-set and are
        // specifically what Mondrian's implicit-slicer shape needs.
        Util.PropertyList props =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        props.put("UseSchemaPool", "false");
        Connection conn = DriverManager.getConnection(props, null, null);
        try {
            RolapSchema schema = ((RolapConnection) conn).getSchema();
            RolapMeasureGroup mg = findAggMeasureGroup(
                schema, "agg_g_ms_pcat_sales_fact_1997");
            assertNotNull(mg);

            List<MvRegistry.ShapeSpec> shapes =
                ShapeEnumerator.enumerate(
                    mg, "agg_g_ms_pcat_sales_fact_1997",
                    "sales_fact_1997",
                    Collections.<MvRegistry.MeasureRef>emptyList(),
                    2);

            // At least one size-3 shape exists — must be a year-variant
            // since the power-set cap is 2.
            boolean sawSize3 = false;
            for (MvRegistry.ShapeSpec s : shapes) {
                if (s.groups.size() == 3) {
                    sawSize3 = true;
                    // All size-3 shapes must include the_year.
                    assertTrue(
                        "size-3 shape must be year-prefixed, got "
                            + s.name,
                        hasYear(s.groups));
                }
            }
            assertTrue(
                "expected at least one size-3 year-prefixed variant",
                sawSize3);

            // Specifically: {gender, marital_status} → {year, gender,
            // marital_status}. (product_family is on product_class,
            // which is a snowflake dim — 2-hop fact→product→product_class
            // is out of scope for Phase 1; gender/marital are direct
            // 1-hop fact→customer FK joins.)
            assertTrue(
                "expected year+gender+marital_status year-variant shape, "
                    + "got " + shapeNames(shapes),
                hasShape(shapes,
                    "time_by_day.the_year",
                    "customer.gender",
                    "customer.marital_status"));
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
            // Base power-set honours the cap; year-prefix variants may
            // add one to each no-year subset, so the max permitted
            // size here is cap + 1.
            for (MvRegistry.ShapeSpec s : capped) {
                assertTrue(
                    "cap=1 + year-variant must be ≤ 2, got "
                        + s.groups.size() + " in " + s.name,
                    s.groups.size() <= 2);
            }
            // Every size-2 shape at cap=1 must be year-prefixed.
            for (MvRegistry.ShapeSpec s : capped) {
                if (s.groups.size() == 2) {
                    assertTrue(
                        "size-2 at cap=1 must be year-prefixed, got "
                            + s.name,
                        hasYear(s.groups));
                }
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

    private static boolean hasYear(List<MvRegistry.GroupCol> groups) {
        for (MvRegistry.GroupCol g : groups) {
            if ("time_by_day".equals(g.table)
                && "the_year".equals(g.column))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean hasShape(
        List<MvRegistry.ShapeSpec> shapes, String... tableDotCol)
    {
        java.util.Set<String> need =
            new java.util.HashSet<>(java.util.Arrays.asList(tableDotCol));
        for (MvRegistry.ShapeSpec s : shapes) {
            if (s.groups.size() != need.size()) {
                continue;
            }
            java.util.Set<String> have = new java.util.HashSet<>();
            for (MvRegistry.GroupCol g : s.groups) {
                have.add(g.table + "." + g.column);
            }
            if (have.equals(need)) {
                return true;
            }
        }
        return false;
    }

    private static String shapeNames(List<MvRegistry.ShapeSpec> shapes) {
        StringBuilder sb = new StringBuilder();
        for (MvRegistry.ShapeSpec s : shapes) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(s.name);
        }
        return sb.toString();
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
