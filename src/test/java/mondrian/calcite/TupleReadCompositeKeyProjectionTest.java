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
import mondrian.rolap.RolapCubeHierarchy;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapSchema;
import mondrian.test.TestContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Task J regression: pins the column order emitted by
 * {@link CalcitePlannerAdapters#fromTupleRead} for a composite-key level.
 *
 * <p>The {@code [Product].[Product Department]} attribute in FoodMart is
 * keyed on {@code (product_family, product_department)} — parent-most
 * first. Legacy {@code SqlTupleReader.addLevelMemberSql} emits the SELECT
 * list in that order, and the reader's {@code LevelColumnLayout} records
 * ordinals pointing into that shape. The Calcite translator must emit the
 * same columns in the same order; earlier (pre-Task-J) the order was
 * inverted because the attribute's {@code orderByList} — which for this
 * attribute is the name column {@code product_department} — was emitted
 * <em>before</em> the key list, taking the leaf column's SELECT slot and
 * bumping the parent key to ordinal 1. On the axis this manifested as
 * swapped parent/leaf member labels (e.g.
 * {@code [Product].[Alcoholic Beverages].[Drink]} instead of
 * {@code [Product].[Drink].[Alcoholic Beverages]}).
 */
public class TupleReadCompositeKeyProjectionTest {

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
     * [Product].[Products].[Product Department] has composite key
     * (product_family, product_department). The emitted PlannerRequest
     * projection list must be exactly that, in that order — the reader's
     * column-ordinal layout (built by legacy addLevelMemberSql) depends
     * on it.
     */
    @Test public void productDepartmentProjectionIsParentKeyFirst() {
        RolapCubeLevel dept = findLevel(
            "Product", "Products", "Product Department");
        assertNotNull(
            "Product Department level in Products hierarchy", dept);

        // Pre-flight: confirm the schema still wires the attribute as a
        // composite key with product_family ahead of product_department.
        List<RolapSchema.PhysColumn> keyList =
            dept.getAttribute().getKeyList();
        assertEquals(
            "Product Department is a 2-column composite key",
            2, keyList.size());
        assertTrue(
            "parent-most key column is product_family (got "
                + keyList.get(0) + ")",
            keyList.get(0).toString().toLowerCase()
                .contains("product_family"));
        assertTrue(
            "leaf-most key column is product_department (got "
                + keyList.get(1) + ")",
            keyList.get(1).toString().toLowerCase()
                .contains("product_department"));

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(dept),
            DefaultTupleConstraint.instance());

        // First two projections must be the keys in parent-to-leaf order.
        assertTrue(
            "need at least 2 projections, got " + req.projections,
            req.projections.size() >= 2);
        assertEquals(
            "projection[0] must be the parent key (product_family)",
            "product_family", req.projections.get(0).name);
        assertEquals(
            "projection[1] must be the leaf key (product_department)",
            "product_department", req.projections.get(1).name);

        // DISTINCT is on, and every projection hangs off product_class.
        assertTrue(
            "DISTINCT projection for member-list shape",
            req.distinct);
        for (PlannerRequest.Column c : req.projections) {
            assertEquals(
                "projection " + c + " must bind to product_class",
                "product_class", c.table);
        }

        // ORDER BY is parent-to-leaf — each key column contributes, in
        // that order, matching legacy's addLevelMemberSql outer-loop
        // emission order (family's SELECT_ORDER add runs before
        // department's, so ORDER BY product_family precedes
        // product_department).
        assertTrue(
            "ORDER BY must have at least the two keys, got "
                + req.orderBy,
            req.orderBy.size() >= 2);
        assertEquals(
            "ORDER BY[0] is product_family",
            "product_family", req.orderBy.get(0).column.name);
        assertEquals(
            "ORDER BY[1] is product_department",
            "product_department", req.orderBy.get(1).column.name);
    }

    /**
     * [Product].[Products].[Product Category] has a 3-column composite
     * key. Order must remain parent-to-leaf through 3 levels deep so
     * deeper hierarchies don't regress.
     */
    @Test public void productCategoryProjectionIsParentKeyFirst() {
        RolapCubeLevel cat = findLevel(
            "Product", "Products", "Product Category");
        assertNotNull("Product Category level", cat);
        assertEquals(
            "3-column composite key",
            3, cat.getAttribute().getKeyList().size());

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(cat),
            DefaultTupleConstraint.instance());

        assertEquals(
            "projection[0] = product_family",
            "product_family", req.projections.get(0).name);
        assertEquals(
            "projection[1] = product_department",
            "product_department", req.projections.get(1).name);
        assertEquals(
            "projection[2] = product_category",
            "product_category", req.projections.get(2).name);

        assertEquals(
            "ORDER BY[0] = product_family",
            "product_family", req.orderBy.get(0).column.name);
        assertEquals(
            "ORDER BY[1] = product_department",
            "product_department", req.orderBy.get(1).column.name);
        assertEquals(
            "ORDER BY[2] = product_category",
            "product_category", req.orderBy.get(2).column.name);
    }

    /**
     * Single-column-key sanity: [Product].[Products].[Product Family]
     * still emits one key column and doesn't regress.
     */
    @Test public void productFamilySingleKeyStillWorks() {
        RolapCubeLevel fam = findLevel(
            "Product", "Products", "Product Family");
        assertNotNull("Product Family level", fam);
        assertEquals(
            "single-column key",
            1, fam.getAttribute().getKeyList().size());

        PlannerRequest req = CalcitePlannerAdapters.fromTupleRead(
            Collections.singletonList(fam),
            DefaultTupleConstraint.instance());

        assertEquals(
            "projection[0] = product_family",
            "product_family", req.projections.get(0).name);
        assertEquals(
            "ORDER BY[0] = product_family",
            "product_family", req.orderBy.get(0).column.name);
    }
}

// End TupleReadCompositeKeyProjectionTest.java
