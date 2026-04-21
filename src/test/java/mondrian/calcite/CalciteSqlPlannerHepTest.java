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

import mondrian.test.FoodMartHsqldbBootstrap;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Exercises Phase 3 Task 9: the {@link CalciteSqlPlanner#optimize} step
 * that runs a {@link org.apache.calcite.plan.hep.HepPlanner} with a
 * curated rewrite ruleset over the built RelNode before unparse. Pins
 * the two rules most likely to fire on Mondrian-shaped queries
 * ({@code PROJECT_MERGE / PROJECT_REMOVE} and {@code FILTER_INTO_JOIN})
 * and sanity-checks that the emitted SQL still comes out well-formed.
 *
 * <p>End-to-end row-count equivalence is covered by the
 * calcite-equivalence harness (gated 44/44 at CI); no JDBC round-trip
 * here because the HSQLDB FoodMart fixture uses quoted lowercase
 * identifiers that the vanilla {@code HsqldbSqlDialect} unparser does
 * not always quote.
 */
public class CalciteSqlPlannerHepTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    private static DataSource foodmartDs() {
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase(
            "jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private static CalciteSqlPlanner planner() {
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        return new CalciteSqlPlanner(schema, HsqldbSqlDialect.DEFAULT);
    }

    /** Counts case-insensitive occurrences of {@code token} in {@code tree}. */
    private static int countSubstr(String tree, String token) {
        String needle = token.toLowerCase();
        String lower = tree.toLowerCase();
        int count = 0;
        int idx = 0;
        while ((idx = lower.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    @Test
    public void hepPlannerMergesAdjacentProjects() {
        // Grouped-aggregate request. The planner emits an Aggregate
        // followed by an explicit re-project (to force the grouping-
        // key order Mondrian expects). That re-project is frequently
        // a trivial identity projection which PROJECT_REMOVE should
        // drop. Tolerate "no change" too — the point is the Hep pass
        // must never GROW the projection count.
        CalciteSqlPlanner p = planner();
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();

        RelNode raw = p.planRel(req);
        RelNode optimized = p.optimize(raw);

        String rawTree = RelOptUtil.toString(raw);
        String optTree = RelOptUtil.toString(optimized);

        int rawProjects = countSubstr(rawTree, "LogicalProject");
        int optProjects = countSubstr(optTree, "LogicalProject");

        assertTrue(
            "HepPlanner should not add Projects. raw=" + rawProjects
                + " opt=" + optProjects + "\nraw:\n" + rawTree
                + "\nopt:\n" + optTree,
            optProjects <= rawProjects);
    }

    @Test
    public void hepPlannerPushesFilterIntoJoin() {
        // Filter on a dim column, sitting above an inner join. Either
        // FILTER_INTO_JOIN folds it into the Join's ON clause (Filter
        // rel disappears) or JOIN_CONDITION_PUSH moves the standalone
        // filter below the Join onto the dim scan — both are
        // acceptable, both reduce unnecessary mid-tree filters, and
        // both leave the unparsed SQL valid. The check is that the
        // filter is no longer sitting as a sibling of the Aggregate
        // in the top of the tree.
        CalciteSqlPlanner p = planner();
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                PlannerRequest.Operator.EQ,
                java.util.Arrays.<Object>asList(1997)))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();

        RelNode raw = p.planRel(req);
        RelNode optimized = p.optimize(raw);

        String rawTree = RelOptUtil.toString(raw);
        String optTree = RelOptUtil.toString(optimized);

        // In the raw tree the Filter sits *between* Aggregate and Join.
        // A successful pushdown moves it elsewhere (into the Join
        // condition, or beneath the Join onto a scan).
        boolean rawHasFilterAboveJoin =
            rawTree.contains("LogicalFilter")
                && rawTree.indexOf("LogicalFilter")
                    < rawTree.indexOf("LogicalJoin");
        assertTrue("pre-condition: raw tree has Filter above Join",
            rawHasFilterAboveJoin);

        boolean optHasFilterAboveJoin =
            optTree.contains("LogicalFilter")
                && optTree.indexOf("LogicalFilter")
                    < optTree.indexOf("LogicalJoin");
        assertFalse(
            "Filter should have been pushed into/below the Join."
                + "\nraw:\n" + rawTree + "\nopt:\n" + optTree,
            optHasFilterAboveJoin);
    }

    @Test
    public void optimizerStillEmitsWellFormedSql() {
        // End-to-end smoke: the plan() path (which now routes through
        // the Hep optimiser) still emits the key SQL lexemes we
        // expect for a typical grouped-aggregate request. Not a
        // byte-for-byte goldens check — the harness owns equivalence;
        // this just guards against a rule producing garbled SQL.
        CalciteSqlPlanner p = planner();
        PlannerRequest req = PlannerRequest.builder("sales_fact_1997")
            .addJoin(new PlannerRequest.Join(
                "time_by_day", "time_id", "time_id"))
            .addFilter(new PlannerRequest.Filter(
                new PlannerRequest.Column("time_by_day", "the_year"),
                PlannerRequest.Operator.EQ,
                java.util.Arrays.<Object>asList(1997)))
            .addGroupBy(new PlannerRequest.Column("time_by_day", "the_year"))
            .addMeasure(new PlannerRequest.Measure(
                PlannerRequest.AggFn.SUM,
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"),
                "m"))
            .build();

        String sql = p.plan(req);
        assertNotNull(sql);
        String lower = sql.toLowerCase();
        assertTrue("expected SELECT: " + sql, lower.contains("select"));
        assertTrue("expected FROM sales_fact_1997: " + sql,
            lower.contains("sales_fact_1997"));
        assertTrue("expected time_by_day: " + sql,
            lower.contains("time_by_day"));
        assertTrue("expected GROUP BY: " + sql, lower.contains("group by"));
        assertTrue("expected SUM(: " + sql, lower.contains("sum("));
        // Filter pushdown into JOIN ON is fine — in that case the
        // the_year=1997 predicate lives in ON rather than WHERE. Either
        // way the year literal must survive.
        assertTrue("expected 1997 literal: " + sql, sql.contains("1997"));
    }
}

// End CalciteSqlPlannerHepTest.java
