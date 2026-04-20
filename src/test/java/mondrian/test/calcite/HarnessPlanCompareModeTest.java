/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.test.calcite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit-tests the {@code -Dharness.planCompare} / {@code -Dharness.replan}
 * system-property parsing on {@link EquivalenceHarness}. Mirrors the
 * structure of {@link HarnessSqlDriftTest} but only exercises the mode
 * machinery — end-to-end PLAN_DRIFT behaviour is covered by
 * {@link EquivalenceSmokeTest} running a full corpus.
 */
public class HarnessPlanCompareModeTest {

    private static final String MODE_PROP =
        EquivalenceHarness.PLAN_COMPARE_SYS_PROP;
    private static final String REPLAN_PROP =
        EquivalenceHarness.REPLAN_SYS_PROP;

    private String priorMode;
    private String priorReplan;

    @Before
    public void save() {
        priorMode = System.getProperty(MODE_PROP);
        priorReplan = System.getProperty(REPLAN_PROP);
        System.clearProperty(MODE_PROP);
        System.clearProperty(REPLAN_PROP);
    }

    @After
    public void restore() {
        if (priorMode == null) {
            System.clearProperty(MODE_PROP);
        } else {
            System.setProperty(MODE_PROP, priorMode);
        }
        if (priorReplan == null) {
            System.clearProperty(REPLAN_PROP);
        } else {
            System.setProperty(REPLAN_PROP, priorReplan);
        }
    }

    @Test
    public void advisoryIsTheDefault() {
        assertEquals(
            EquivalenceHarness.SqlCompareMode.ADVISORY,
            EquivalenceHarness.planCompareMode());
    }

    @Test
    public void strictParses() {
        System.setProperty(MODE_PROP, "strict");
        assertEquals(
            EquivalenceHarness.SqlCompareMode.STRICT,
            EquivalenceHarness.planCompareMode());
    }

    @Test
    public void offParses() {
        System.setProperty(MODE_PROP, "off");
        assertEquals(
            EquivalenceHarness.SqlCompareMode.OFF,
            EquivalenceHarness.planCompareMode());
    }

    @Test
    public void unknownFallsBackToAdvisory() {
        System.setProperty(MODE_PROP, "loud");
        assertEquals(
            EquivalenceHarness.SqlCompareMode.ADVISORY,
            EquivalenceHarness.planCompareMode());
    }

    @Test
    public void replanDefaultsFalse() {
        assertFalse(EquivalenceHarness.replanRequested());
    }

    @Test
    public void replanTrueIsDetected() {
        System.setProperty(REPLAN_PROP, "true");
        assertTrue(EquivalenceHarness.replanRequested());
    }

    @Test
    public void replanFalseIsDetected() {
        System.setProperty(REPLAN_PROP, "false");
        assertFalse(EquivalenceHarness.replanRequested());
    }
}

// End HarnessPlanCompareModeTest.java
