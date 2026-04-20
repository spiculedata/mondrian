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

import mondrian.rolap.RolapAggregator;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link CalcitePlannerAdapters#mapAggregator} — the
 * measure-aggregator translation path used by {@code fromSegmentLoad}.
 *
 * <p>Covers Task G: distinct-count should map to {@code Measure(COUNT,
 * distinct=true)} rather than being rejected, while other built-in
 * aggregators stay on their SUM/COUNT/MIN/MAX/AVG lanes. Unknown
 * aggregators must throw {@link UnsupportedTranslation} with a clear
 * message so next-task triage has something to grep on.
 */
public class SegmentLoadAggregatorTranslationTest {

    private static Object invokeMap(RolapAggregator agg) throws Exception {
        Method m = CalcitePlannerAdapters.class.getDeclaredMethod(
            "mapAggregator", RolapAggregator.class);
        m.setAccessible(true);
        return m.invoke(null, agg);
    }

    private static PlannerRequest.AggFn fnOf(Object aggOp) throws Exception {
        Field f = aggOp.getClass().getDeclaredField("fn");
        f.setAccessible(true);
        return (PlannerRequest.AggFn) f.get(aggOp);
    }

    private static boolean distinctOf(Object aggOp) throws Exception {
        Field f = aggOp.getClass().getDeclaredField("distinct");
        f.setAccessible(true);
        return f.getBoolean(aggOp);
    }

    @Test public void sumTranslatesToSum() throws Exception {
        Object op = invokeMap(RolapAggregator.Sum);
        assertEquals(PlannerRequest.AggFn.SUM, fnOf(op));
        assertFalse(distinctOf(op));
    }

    @Test public void countTranslatesToCount() throws Exception {
        Object op = invokeMap(RolapAggregator.Count);
        assertEquals(PlannerRequest.AggFn.COUNT, fnOf(op));
        assertFalse(distinctOf(op));
    }

    @Test public void minTranslatesToMin() throws Exception {
        Object op = invokeMap(RolapAggregator.Min);
        assertEquals(PlannerRequest.AggFn.MIN, fnOf(op));
        assertFalse(distinctOf(op));
    }

    @Test public void maxTranslatesToMax() throws Exception {
        Object op = invokeMap(RolapAggregator.Max);
        assertEquals(PlannerRequest.AggFn.MAX, fnOf(op));
        assertFalse(distinctOf(op));
    }

    @Test public void avgTranslatesToAvg() throws Exception {
        Object op = invokeMap(RolapAggregator.Avg);
        assertEquals(PlannerRequest.AggFn.AVG, fnOf(op));
        assertFalse(distinctOf(op));
    }

    @Test public void distinctCountOfColumnTranslatesToCountDistinct()
        throws Exception
    {
        Object op = invokeMap(RolapAggregator.DistinctCount);
        assertEquals(PlannerRequest.AggFn.COUNT, fnOf(op));
        assertTrue(
            "DistinctCount must set Measure.distinct=true",
            distinctOf(op));
    }

    @Test public void unknownAggregatorThrows() throws Exception {
        // A made-up aggregator with a distinctive name so the error
        // message can be asserted on.
        RolapAggregator fake =
            new RolapAggregator("zogzog-mystery", 9999, false) {
                @Override public Object aggregate(
                    mondrian.olap.Evaluator evaluator,
                    mondrian.calc.TupleList members,
                    mondrian.calc.Calc exp)
                {
                    throw new UnsupportedOperationException();
                }
            };
        try {
            invokeMap(fake);
            fail("expected UnsupportedTranslation");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue(
                "wrong exception type: " + cause,
                cause instanceof UnsupportedTranslation);
            assertTrue(
                "message should name the aggregator: " + cause.getMessage(),
                cause.getMessage().contains("zogzog-mystery"));
        }
    }
}

// End SegmentLoadAggregatorTranslationTest.java
