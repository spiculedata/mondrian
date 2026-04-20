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

import mondrian.rolap.DescendantsConstraint;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapMember;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Task Q (worktree #2) — rejection-surface coverage for the
 * {@link DescendantsConstraint} branch of
 * {@link CalcitePlannerAdapters#fromTupleRead(List,
 * mondrian.rolap.sql.TupleConstraint)}. Positive-path coverage rides the
 * calcite-harness run of the {@code descendants} corpus query.
 */
public class TupleReadDescendantsTest {

    @Before public void reset() {
        CalcitePlannerAdapters.resetUnsupportedCount();
    }

    @After public void clearBackend() {
        System.clearProperty("mondrian.backend");
        CalcitePlannerAdapters.resetUnsupportedCount();
    }

    @Test public void emptyParentsRejected() {
        DescendantsConstraint dc =
            new DescendantsConstraint(
                Collections.<RolapMember>emptyList(), null);
        try {
            CalcitePlannerAdapters.fromTupleRead(
                Collections.<RolapCubeLevel>singletonList(null), dc);
            fail("expected UnsupportedTranslation for empty parentMembers");
        } catch (UnsupportedTranslation ex) {
            String msg = ex.getMessage().toLowerCase();
            assertTrue(
                "message must mention empty parentMembers: "
                + ex.getMessage(),
                msg.contains("empty")
                && msg.contains("descendantsconstraint"));
        }
    }

    @Test public void multiTargetRejected() {
        // Multi-target crossjoin with DescendantsConstraint is not modelled.
        DescendantsConstraint dc =
            new DescendantsConstraint(
                Collections.<RolapMember>emptyList(), null);
        try {
            CalcitePlannerAdapters.fromTupleRead(
                java.util.Arrays.<RolapCubeLevel>asList(null, null), dc);
            fail("expected UnsupportedTranslation for multi-target");
        } catch (UnsupportedTranslation ex) {
            String msg = ex.getMessage().toLowerCase();
            assertTrue(
                "message must mention multi-target: " + ex.getMessage(),
                msg.contains("multi-target")
                && msg.contains("descendantsconstraint"));
        }
    }
}

// End TupleReadDescendantsTest.java
