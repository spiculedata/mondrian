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

import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.test.TestContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the Task-I snowflake multi-hop walker used by
 * {@link CalcitePlannerAdapters#fromSegmentLoad}.
 *
 * <p>Drives the package-private {@code ensureJoinedChain} helper against a
 * live FoodMart {@link RolapStar}. The canonical snowflake is
 * {@code sales_fact_1997 → product → product_class} (path length 3) —
 * every Product-family grouping query exercises this chain.
 */
public class SegmentLoadSnowflakeTranslationTest {

    private static RolapStar sales;

    @BeforeClass public static void bootFoodMart() {
        mondrian.olap.Connection conn = TestContext.instance().getConnection();
        RolapSchema schema = (RolapSchema) conn.getSchema();
        sales = schema.getStar("sales_fact_1997");
        assertNotNull("sales star", sales);
    }

    private static void invokeEnsureChain(
        PlannerRequest.Builder b,
        RolapStar.Table fact,
        RolapStar.Table leaf,
        Set<String> joined)
        throws Exception
    {
        Method m = CalcitePlannerAdapters.class.getDeclaredMethod(
            "ensureJoinedChain",
            PlannerRequest.Builder.class,
            RolapStar.Table.class,
            RolapStar.Table.class,
            Set.class);
        m.setAccessible(true);
        m.invoke(null, b, fact, leaf, joined);
    }

    /** Locate a descendant Table by alias (the fact tree is keyed by
     *  alias, not name). Walks children recursively. */
    private static RolapStar.Table findByAlias(
        RolapStar.Table root, String alias)
    {
        if (alias.equals(root.getAlias())) {
            return root;
        }
        for (RolapStar.Table child : root.getChildren()) {
            RolapStar.Table hit = findByAlias(child, alias);
            if (hit != null) {
                return hit;
            }
        }
        return null;
    }

    @Test public void productClassChainEmitsTwoJoinsInFactLeafOrder()
        throws Exception
    {
        RolapStar.Table fact = sales.getFactTable();
        RolapStar.Table productClass = findByAlias(fact, "product_class");
        assertNotNull(
            "product_class table must hang off sales fact", productClass);
        // Sanity: path from fact to product_class has length 3 (fact,
        // product, product_class) — exactly the snowflake that was
        // previously blocked.
        assertEquals(
            "expected path length 3 for product_class",
            3, productClass.getPath().hopList.size());

        PlannerRequest.Builder b =
            PlannerRequest.builder("sales_fact_1997")
                .addGroupBy(new PlannerRequest.Column(
                    "product_class", "product_family"))
                .addMeasure(new PlannerRequest.Measure(
                    PlannerRequest.AggFn.SUM,
                    new PlannerRequest.Column(
                        "sales_fact_1997", "unit_sales"),
                    "m"));
        Set<String> joined = new LinkedHashSet<>();
        joined.add(fact.getAlias());
        invokeEnsureChain(b, fact, productClass, joined);

        PlannerRequest req = b.build();
        List<PlannerRequest.Join> joins = req.joins;
        assertEquals(
            "expected two join edges (fact→product, product→product_class)",
            2, joins.size());

        PlannerRequest.Join edge1 = joins.get(0);
        PlannerRequest.Join edge2 = joins.get(1);
        assertEquals("first edge attaches product",
            "product", edge1.dimTable);
        // Fact-rooted edge: leftTable stays null for back-compat.
        assertEquals(
            "first edge LHS must be fact (leftTable=null)",
            null, edge1.leftTable);

        assertEquals("second edge attaches product_class",
            "product_class", edge2.dimTable);
        assertEquals(
            "second edge must name its LHS explicitly (product)",
            "product", edge2.leftTable);
        // INNER kind on both.
        assertEquals(
            PlannerRequest.JoinKind.INNER, edge1.kind);
        assertEquals(
            PlannerRequest.JoinKind.INNER, edge2.kind);
    }

    @Test public void prefixSharingDoesNotDuplicateFactProductEdge()
        throws Exception
    {
        // If two needed tables (e.g. product and product_class) share the
        // fact→product prefix, it must only be emitted once.
        RolapStar.Table fact = sales.getFactTable();
        RolapStar.Table product = findByAlias(fact, "product");
        RolapStar.Table productClass = findByAlias(fact, "product_class");
        assertNotNull(product);
        assertNotNull(productClass);

        PlannerRequest.Builder b =
            PlannerRequest.builder("sales_fact_1997")
                .addGroupBy(new PlannerRequest.Column(
                    "product", "brand_name"))
                .addGroupBy(new PlannerRequest.Column(
                    "product_class", "product_family"))
                .addMeasure(new PlannerRequest.Measure(
                    PlannerRequest.AggFn.SUM,
                    new PlannerRequest.Column(
                        "sales_fact_1997", "unit_sales"),
                    "m"));
        Set<String> joined = new LinkedHashSet<>();
        joined.add(fact.getAlias());
        invokeEnsureChain(b, fact, product, joined);
        invokeEnsureChain(b, fact, productClass, joined);

        PlannerRequest req = b.build();
        assertEquals(
            "expected exactly 2 joins (product + product_class) with "
                + "no duplicate fact→product edge",
            2, req.joins.size());
        assertNotEquals(
            req.joins.get(0).dimTable, req.joins.get(1).dimTable);
    }

    @Test public void emittedSqlRoundtripsThroughPlanner() throws Exception {
        // End-to-end: the request built by ensureJoinedChain must render
        // to valid SQL with two JOINs and a GROUP BY on product_family.
        RolapStar.Table fact = sales.getFactTable();
        RolapStar.Table productClass = findByAlias(fact, "product_class");
        assertNotNull(productClass);
        PlannerRequest.Builder b =
            PlannerRequest.builder("sales_fact_1997")
                .addGroupBy(new PlannerRequest.Column(
                    "product_class", "product_family"))
                .addMeasure(new PlannerRequest.Measure(
                    PlannerRequest.AggFn.SUM,
                    new PlannerRequest.Column(
                        "sales_fact_1997", "unit_sales"),
                    "m"));
        Set<String> joined = new LinkedHashSet<>();
        joined.add(fact.getAlias());
        invokeEnsureChain(b, fact, productClass, joined);
        PlannerRequest req = b.build();

        org.hsqldb.jdbc.jdbcDataSource ds =
            new org.hsqldb.jdbc.jdbcDataSource();
        ds.setDatabase(
            "jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(ds, "foodmart");
        CalciteSqlPlanner planner = new CalciteSqlPlanner(
            schema,
            org.apache.calcite.sql.dialect.HsqldbSqlDialect.DEFAULT);
        String sql = planner.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected product in: " + sql, lower.contains("product"));
        assertTrue("expected product_class in: " + sql,
            lower.contains("product_class"));
        assertTrue("expected product_family in: " + sql,
            lower.contains("product_family"));
        // Structural assertions on the join chain: two INNER JOINs with
        // the expected fact→product (product_id) and product→product_class
        // (product_class_id) equi-conditions.
        assertTrue(
            "expected fact→product join on product_id in: " + sql,
            lower.contains("sales_fact_1997.product_id = product.product_id")
                || lower.contains(
                    "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\""));
        assertTrue(
            "expected product→product_class join on product_class_id in: "
                + sql,
            lower.contains(
                "product.product_class_id = product_class.product_class_id")
                || lower.contains(
                    "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\""));
    }
}

// End SegmentLoadSnowflakeTranslationTest.java
