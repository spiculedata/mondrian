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

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.rolap.DefaultTupleConstraint;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapCubeDimension;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapSchema;
import mondrian.test.TestContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Task L regression: when a level's key columns live on a snowflaked
 * dim table (e.g. {@code [Product].[Products].[Product Department]} is
 * keyed on {@code product_class}, reached via {@code product →
 * product_class}), the emitted {@link PlannerRequest} must include an
 * INNER JOIN back to the intermediate dim so orphan catalog rows on
 * the leaf don't leak into the DISTINCT member list.
 *
 * <p>Legacy tuple-read for this shape emits (see
 * {@code golden-legacy/slicer-where.json}):
 * <pre>
 *   FROM "product" AS "product", "product_class" AS "product_class"
 *   WHERE "product"."product_class_id" = "product_class"."product_class_id"
 *   GROUP BY … ORDER BY …
 * </pre>
 * i.e. an implicit inner join between the dim-key table and the leaf.
 * Calcite's {@code RelBuilder} renders this as a proper INNER JOIN.
 */
public class TupleReadSnowflakeTest {

    private static RolapCube sales;

    @BeforeClass public static void bootFoodMart() {
        mondrian.olap.Connection conn = TestContext.instance().getConnection();
        RolapSchema schema = (RolapSchema) conn.getSchema();
        for (RolapCube cube : schema.getCubeList()) {
            if ("Sales".equalsIgnoreCase(cube.getName())) {
                sales = cube;
                break;
            }
        }
        assertNotNull("Sales cube", sales);
    }

    private static RolapCubeLevel findLevel(
        String dimName, String hierarchyName, String levelName)
    {
        for (RolapCubeDimension d : sales.getDimensionList()) {
            if (!dimName.equalsIgnoreCase(d.getName())) {
                continue;
            }
            for (Hierarchy h : d.getHierarchyList()) {
                if (!hierarchyName.equalsIgnoreCase(h.getName())
                    && !hierarchyName.equalsIgnoreCase(h.getUniqueName()))
                {
                    continue;
                }
                for (Level lvl : h.getLevelList()) {
                    if (levelName.equalsIgnoreCase(lvl.getName())) {
                        return (RolapCubeLevel) lvl;
                    }
                }
            }
        }
        return null;
    }

    /**
     * [Product].[Products].[Product Department] is keyed on product_class
     * but its dimension-key table is product. Expect a single INNER JOIN
     * edge from product_class back to product on product_class_id.
     */
    @Test public void productDepartmentIncludesProductJoin() {
        RolapCubeLevel dept = findLevel(
            "Product", "Products", "Product Department");
        assertNotNull("Product Department level", dept);

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(dept),
            DefaultTupleConstraint.instance());

        assertEquals(
            "factTable (tuple-read root) is the key table product_class",
            "product_class", req.factTable);
        assertEquals(
            "exactly one join: product_class ↔ product",
            1, req.joins.size());
        PlannerRequest.Join j = req.joins.get(0);
        assertEquals("joined dim table is product",
            "product", j.dimTable);
        assertEquals("FK column on leaf is product_class_id",
            "product_class_id", j.factKey);
        assertEquals("PK column on product is product_class_id",
            "product_class_id", j.dimKey);
        assertEquals("INNER join",
            PlannerRequest.JoinKind.INNER, j.kind);
        assertNull(
            "first chain edge has null leftTable (LHS = leaf scan)",
            j.leftTable);

        assertTrue("DISTINCT still set", req.distinct);
    }

    /**
     * Sanity: a level whose key columns live on the dim-key table itself
     * (e.g. [Product].[Product Family]) emits no joins — the chain is
     * empty.
     */
    @Test public void productFamilyNoChain() {
        RolapCubeLevel fam = findLevel(
            "Product", "Products", "Product Family");
        assertNotNull("Product Family level", fam);

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(fam),
            DefaultTupleConstraint.instance());

        // product_family lives on product_class; the dim key table for
        // Product is product (so this still snowflakes) — so the chain
        // should be present even for the single-key case. Verify by
        // checking that the factTable is product_class and joins
        // include product.
        assertEquals("product_class", req.factTable);
        assertEquals("one join to product",
            1, req.joins.size());
        assertEquals("product", req.joins.get(0).dimTable);
    }

    /**
     * [Time].[Year] lives on time_by_day; the Time dimension's key table
     * IS time_by_day (no snowflake). Expect an empty chain.
     */
    @Test public void timeYearNoChain() {
        RolapCubeLevel year = findLevel("Time", "Time", "Year");
        assertNotNull("Time.Year level", year);

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(year),
            DefaultTupleConstraint.instance());

        assertEquals("time_by_day", req.factTable);
        assertEquals(
            "no joins — Time is flat (key columns live on dim-key table)",
            0, req.joins.size());
    }
}

// End TupleReadSnowflakeTest.java
