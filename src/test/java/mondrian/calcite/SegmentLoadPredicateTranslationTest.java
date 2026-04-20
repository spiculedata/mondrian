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
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.LiteralColumnPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.PredicateColumn;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.test.TestContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for the per-column and compound predicate walkers used by
 * {@link CalcitePlannerAdapters#fromSegmentLoad}.
 *
 * <p>Drives the package-private helpers directly against
 * {@link PlannerRequest.Builder}, without constructing a full
 * {@code GroupingSetsList} / {@code RolapStar} context. Real
 * {@link RolapStar.Column} handles are obtained from a live FoodMart
 * schema so the resulting predicates have a valid {@link PredicateColumn}
 * (column-bit-key derivation needs a real {@link RolapSchema.PhysColumn}).
 */
public class SegmentLoadPredicateTranslationTest {

    private static RolapStar sales;

    @BeforeClass public static void bootFoodMart() {
        // Grab the Sales star once; reused across tests.
        mondrian.olap.Connection conn = TestContext.instance().getConnection();
        RolapSchema schema = (RolapSchema) conn.getSchema();
        sales = schema.getStar("sales_fact_1997");
        assertNotNull("sales star", sales);
    }

    private static PredicateColumn pcFor(String table, String column) {
        RolapStar.Column starCol = sales.lookupColumn(table, column);
        assertNotNull(
            "lookupColumn " + table + "." + column, starCol);
        return new PredicateColumn(
            RolapSchema.BadRouter.INSTANCE, starCol.getExpression());
    }

    private static boolean walkColumn(
        PlannerRequest.Builder b,
        PlannerRequest.Column col,
        StarColumnPredicate p)
        throws Exception
    {
        Method m = CalcitePlannerAdapters.class.getDeclaredMethod(
            "addColumnPredicateFilters",
            PlannerRequest.Builder.class,
            PlannerRequest.Column.class,
            StarColumnPredicate.class);
        m.setAccessible(true);
        return (Boolean) m.invoke(null, b, col, p);
    }

    private static boolean walkCompound(
        PlannerRequest.Builder b, StarPredicate sp)
        throws Exception
    {
        Method m = CalcitePlannerAdapters.class.getDeclaredMethod(
            "addCompoundFilters",
            PlannerRequest.Builder.class,
            StarPredicate.class);
        m.setAccessible(true);
        return (Boolean) m.invoke(null, b, sp);
    }

    @Test public void valueColumnPredicateEmitsSingleEqFilter()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        ValueColumnPredicate vp = new ValueColumnPredicate(pc, 1997);
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            vp);
        PlannerRequest req = b.build();
        assertFalse(isFalse);
        assertEquals(1, req.filters.size());
        assertEquals(
            PlannerRequest.Operator.EQ, req.filters.get(0).op);
        assertEquals(1997, req.filters.get(0).literal());
    }

    @Test public void literalTruePredicateEmitsNoFilter()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        LiteralColumnPredicate tru = new LiteralColumnPredicate(pc, true);
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            tru);
        assertFalse(isFalse);
        assertEquals(0, b.build().filters.size());
    }

    @Test public void literalFalsePredicateSignalsUniversalFalse()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        LiteralColumnPredicate fls = new LiteralColumnPredicate(pc, false);
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            fls);
        assertTrue("literal FALSE → universalFalse signal", isFalse);
    }

    @Test public void listColumnPredicateMultiValueEmitsInFilter()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        List<StarColumnPredicate> kids = Arrays.asList(
            new ValueColumnPredicate(pc, 1997),
            new ValueColumnPredicate(pc, 1998),
            new ValueColumnPredicate(pc, 1999));
        ListColumnPredicate lp = new ListColumnPredicate(pc, kids);
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            lp);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(1, req.filters.size());
        PlannerRequest.Filter f = req.filters.get(0);
        assertEquals(PlannerRequest.Operator.IN, f.op);
        assertEquals(3, f.literals.size());
        assertTrue(f.literals.contains(1997));
        assertTrue(f.literals.contains(1998));
        assertTrue(f.literals.contains(1999));
    }

    @Test public void listColumnPredicateSingleValueCollapsesToEq()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        ListColumnPredicate lp = new ListColumnPredicate(
            pc,
            Collections.<StarColumnPredicate>singletonList(
                new ValueColumnPredicate(pc, 1997)));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            lp);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(1, req.filters.size());
        assertEquals(PlannerRequest.Operator.EQ, req.filters.get(0).op);
    }

    @Test public void listColumnPredicateEmptySignalsUniversalFalse()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        ListColumnPredicate lp = new ListColumnPredicate(
            pc, Collections.<StarColumnPredicate>emptyList());
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkColumn(
            b,
            new PlannerRequest.Column("time_by_day", "the_year"),
            lp);
        assertTrue("empty list → universalFalse signal", isFalse);
    }

    @Test public void andPredicateAcrossColumnsEmitsMultipleFilters()
        throws Exception
    {
        PredicateColumn pcYear = pcFor("time_by_day", "the_year");
        PredicateColumn pcGender = pcFor("customer", "gender");
        ValueColumnPredicate vYear = new ValueColumnPredicate(pcYear, 1997);
        ValueColumnPredicate vGender = new ValueColumnPredicate(pcGender, "F");
        AndPredicate and = new AndPredicate(
            Arrays.<StarPredicate>asList(vYear, vGender));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkCompound(b, and);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(2, req.filters.size());
    }

    /** Task M: OR of bare ValueColumnPredicates on different columns is
     *  still rejected (multi-column OR requires AndPredicate children so a
     *  tuple can be formed). */
    @Test public void orPredicateAcrossColumnsBareValuesRejected() {
        PredicateColumn pcYear = pcFor("time_by_day", "the_year");
        PredicateColumn pcGender = pcFor("customer", "gender");
        ValueColumnPredicate vYear = new ValueColumnPredicate(pcYear, 1997);
        ValueColumnPredicate vGender = new ValueColumnPredicate(pcGender, "F");
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(vYear, vGender));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        try {
            walkCompound(b, or);
            fail("expected UnsupportedTranslation");
        } catch (Exception ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            assertTrue(
                "message: " + cause.getMessage(),
                cause.getMessage().toLowerCase().contains("or")
                    && cause.getMessage().toLowerCase()
                           .contains("multi-column"));
        }
    }

    @Test public void orPredicateSingleColumnCollapsesToIn()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        ValueColumnPredicate v97 = new ValueColumnPredicate(pc, 1997);
        ValueColumnPredicate v98 = new ValueColumnPredicate(pc, 1998);
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(v97, v98));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkCompound(b, or);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(1, req.filters.size());
        assertEquals(PlannerRequest.Operator.IN, req.filters.get(0).op);
        assertEquals(2, req.filters.get(0).literals.size());
    }

    /** Task M: OR of AndPredicates across two columns → TupleFilter. */
    @Test public void orAcrossColumnsTranslatesToTupleFilter()
        throws Exception
    {
        PredicateColumn pcYear = pcFor("time_by_day", "the_year");
        PredicateColumn pcGender = pcFor("customer", "gender");
        AndPredicate row1 = new AndPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(pcYear, 1997),
                new ValueColumnPredicate(pcGender, "F")));
        AndPredicate row2 = new AndPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(pcYear, 1998),
                new ValueColumnPredicate(pcGender, "M")));
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(row1, row2));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkCompound(b, or);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(0, req.filters.size());
        assertEquals(1, req.tupleFilters.size());
        PlannerRequest.TupleFilter tf = req.tupleFilters.get(0);
        assertEquals(2, tf.columns.size());
        assertEquals(2, tf.rows.size());
    }

    /** Task M: OR of four AndPredicates, each with a single leaf on the
     *  same column → single-column IN-list. */
    @Test public void orSingleColumnCollapsesToIn()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        AndPredicate a97 = new AndPredicate(
            Collections.<StarPredicate>singletonList(
                new ValueColumnPredicate(pc, 1997)));
        AndPredicate a98 = new AndPredicate(
            Collections.<StarPredicate>singletonList(
                new ValueColumnPredicate(pc, 1998)));
        AndPredicate a99 = new AndPredicate(
            Collections.<StarPredicate>singletonList(
                new ValueColumnPredicate(pc, 1999)));
        AndPredicate a00 = new AndPredicate(
            Collections.<StarPredicate>singletonList(
                new ValueColumnPredicate(pc, 2000)));
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(a97, a98, a99, a00));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkCompound(b, or);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(1, req.filters.size());
        assertEquals(0, req.tupleFilters.size());
        assertEquals(PlannerRequest.Operator.IN, req.filters.get(0).op);
        assertEquals(4, req.filters.get(0).literals.size());
    }

    /** Task M: OR with mixed Value + single-leaf AND children on the same
     *  column → IN-list. */
    @Test public void orMixedChildrenTranslates()
        throws Exception
    {
        PredicateColumn pc = pcFor("time_by_day", "the_year");
        ValueColumnPredicate v97 = new ValueColumnPredicate(pc, 1997);
        AndPredicate a98 = new AndPredicate(
            Collections.<StarPredicate>singletonList(
                new ValueColumnPredicate(pc, 1998)));
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(v97, a98));
        PlannerRequest.Builder b = PlannerRequest.builder("sales_fact_1997")
            .addProjection(
                new PlannerRequest.Column("sales_fact_1997", "unit_sales"));
        boolean isFalse = walkCompound(b, or);
        assertFalse(isFalse);
        PlannerRequest req = b.build();
        assertEquals(1, req.filters.size());
        assertEquals(PlannerRequest.Operator.IN, req.filters.get(0).op);
        assertEquals(2, req.filters.get(0).literals.size());
    }
}

// End SegmentLoadPredicateTranslationTest.java
