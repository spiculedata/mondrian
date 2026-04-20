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

import mondrian.rolap.DefaultTupleConstraint;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.sql.TupleConstraint;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the typed
 * {@link CalcitePlannerAdapters#fromTupleRead(List, TupleConstraint)}
 * entry-point added in Task E. Exercises the rejection branches that
 * don't require a fully-loaded Mondrian schema (positive-path coverage
 * rides the calcite-harness runs, which drive real Sales-cube
 * schema-init through this translator).
 */
public class TupleReadLevelMembersTest {

    @Before public void reset() {
        CalcitePlannerAdapters.resetUnsupportedCount();
    }

    @After public void clearBackend() {
        System.clearProperty("mondrian.backend");
        CalcitePlannerAdapters.resetUnsupportedCount();
    }

    @Test public void emptyLevelsRejected() {
        long before = CalcitePlannerAdapters.tupleReadUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromTupleRead(
                Collections.<RolapCubeLevel>emptyList(),
                DefaultTupleConstraint.instance());
            fail("expected UnsupportedTranslation for empty levels");
        } catch (UnsupportedTranslation ex) {
            assertTrue(
                "message must mention empty levels: " + ex.getMessage(),
                ex.getMessage().toLowerCase().contains("empty"));
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.tupleReadUnsupportedCount());
    }

    @Test public void nullConstraintRejected() {
        long before = CalcitePlannerAdapters.tupleReadUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromTupleRead(
                Collections.<RolapCubeLevel>singletonList(null),
                null);
            fail("expected UnsupportedTranslation for null constraint");
        } catch (UnsupportedTranslation ex) {
            assertTrue(
                "message must mention TupleConstraint: " + ex.getMessage(),
                ex.getMessage().toLowerCase().contains("tupleconstraint"));
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.tupleReadUnsupportedCount());
    }

    @Test public void threeTargetCrossjoinRejected() {
        // Task H raises the multi-target cap to 2. Confirm 3+ still
        // throws cleanly with a message naming the arity.
        long before = CalcitePlannerAdapters.tupleReadUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromTupleRead(
                java.util.Arrays.<RolapCubeLevel>asList(null, null, null),
                DefaultTupleConstraint.instance());
            fail("expected UnsupportedTranslation for 3-target crossjoin");
        } catch (UnsupportedTranslation ex) {
            assertTrue(
                "message must mention >2 targets: " + ex.getMessage(),
                ex.getMessage().contains("levels.size=3"));
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.tupleReadUnsupportedCount());
    }

    @Test public void opaqueObjectEntryStillThrows() {
        // The original Object-typed fromTupleRead remains for back-compat.
        long before = CalcitePlannerAdapters.tupleReadUnsupportedCount();
        try {
            CalcitePlannerAdapters.fromTupleRead(new Object());
            fail("expected UnsupportedTranslation for opaque context");
        } catch (UnsupportedTranslation ex) {
            assertTrue(
                "message must mention opaque: " + ex.getMessage(),
                ex.getMessage().toLowerCase().contains("opaque"));
        }
        assertEquals(
            before + 1,
            CalcitePlannerAdapters.tupleReadUnsupportedCount());
    }
}

// End TupleReadLevelMembersTest.java
