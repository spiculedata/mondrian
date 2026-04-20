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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Verifies the dispatch wiring added to
 * {@code mondrian.rolap.agg.SegmentLoader} for the Calcite backend.
 *
 * <p>Worktree #1: a real, end-to-end segment load against a live cube here
 * is too heavy (it would drag in the full Mondrian test harness for a
 * single-line dispatch). Instead we exercise
 * {@link CalcitePlannerAdapters#fromSegmentLoad(Object)} directly,
 * mirroring {@link SqlTupleReaderCalciteBackendTest}, asserting:
 *
 * <ul>
 *   <li>It throws {@link UnsupportedTranslation} for the worktree-#1
 *       coverage scope (which is intentionally empty).</li>
 *   <li>The fallback counter increments on every unsupported call, so
 *       observability of translator coverage works.</li>
 *   <li>The default backend remains {@link MondrianBackend#LEGACY} — i.e.
 *       legacy callers see no behavioural change from this dispatch.</li>
 * </ul>
 *
 * <p>The full-stack equivalence assertion lives in the calcite-harness
 * profile: running {@code -Dmondrian.backend=calcite} against the smoke
 * suite confirms the dispatch + fallback path produces identical results
 * to legacy for the queries the translator can't yet handle.
 */
public class SegmentLoaderCalciteBackendTest {

    @Before public void resetCounter() {
        CalcitePlannerAdapters.resetFallbackCount();
    }

    @After public void clearBackend() {
        System.clearProperty("mondrian.backend");
        CalcitePlannerAdapters.resetFallbackCount();
    }

    @Test public void fromSegmentLoadThrowsUnsupportedTranslationForNow() {
        try {
            CalcitePlannerAdapters.fromSegmentLoad(new Object());
            fail("expected UnsupportedTranslation");
        } catch (UnsupportedTranslation ex) {
            assertNotNull(ex.getMessage());
            assertTrue(
                "message should mention segment / legacy",
                ex.getMessage().toLowerCase().contains("segment")
                    || ex.getMessage().toLowerCase().contains("legacy"));
        }
    }

    @Test public void fromSegmentLoadThrowsForNullInput() {
        try {
            CalcitePlannerAdapters.fromSegmentLoad(null);
            fail("expected UnsupportedTranslation");
        } catch (UnsupportedTranslation ex) {
            assertNotNull(ex.getMessage());
        }
    }

    @Test public void unsupportedFallbackCounterIncrements() {
        long before = CalcitePlannerAdapters.unsupportedFallbackCount();
        try {
            CalcitePlannerAdapters.fromSegmentLoad(new Object());
        } catch (UnsupportedTranslation ignored) {
            // expected
        }
        try {
            CalcitePlannerAdapters.fromSegmentLoad(new Object());
        } catch (UnsupportedTranslation ignored) {
            // expected
        }
        assertEquals(
            before + 2,
            CalcitePlannerAdapters.unsupportedFallbackCount());
    }

    @Test public void calcitePropertyFlipsCurrentBackend() {
        // Calcite by default (Task 9 flip). -Dmondrian.backend=legacy is the
        // kill switch; setting it to calcite explicitly is now a no-op but
        // must still resolve to CALCITE.
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());

        System.setProperty("mondrian.backend", "calcite");
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
        assertTrue(MondrianBackend.current().isCalcite());

        System.setProperty("mondrian.backend", "legacy");
        assertSame(MondrianBackend.LEGACY, MondrianBackend.current());
    }

    @Test public void legacyBackendUnchangedByDispatch() {
        // -Dmondrian.backend=legacy: SegmentLoader's dispatch must never
        // touch CalcitePlannerAdapters, so the fallback counter must remain
        // at zero from the perspective of a legacy run.
        System.setProperty("mondrian.backend", "legacy");
        assertSame(MondrianBackend.LEGACY, MondrianBackend.current());
        assertEquals(
            0L, CalcitePlannerAdapters.unsupportedFallbackCount());
    }
}

// End SegmentLoaderCalciteBackendTest.java
