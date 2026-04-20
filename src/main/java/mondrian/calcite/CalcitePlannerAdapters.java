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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridge between Mondrian-internal SQL-build contexts (e.g.
 * {@code SqlTupleReader.Target}, cell-request groups) and the
 * backend-neutral {@link PlannerRequest}.
 *
 * <p>Worktree #1 scope: the translation surface here is intentionally
 * minimal. The {@code SqlTupleReader} dispatch wires up the routing seam,
 * but actual translation coverage grows in later worktrees. When a shape
 * cannot be translated, methods throw {@link UnsupportedTranslation} so the
 * caller can fall back to the legacy {@code SqlQuery}-built string.
 *
 * <p>A static {@link AtomicLong} counter records every fallback so the
 * harness / observability layer can spot regressions in translation
 * coverage as it expands.
 */
public final class CalcitePlannerAdapters {

    private CalcitePlannerAdapters() {}

    private static final AtomicLong UNSUPPORTED_FALLBACK_COUNT =
        new AtomicLong();

    /**
     * Attempt to translate a tuple-read context into a
     * {@link PlannerRequest}. Currently always throws
     * {@link UnsupportedTranslation}: the {@code SqlTupleReader.Target}
     * structure (multi-target crossjoins, hierarchical level reads,
     * approxRowCount overrides, native filters) does not map cleanly onto
     * the worktree-#1 single-table projection shape without a deeper
     * refactor of the tuple-reader. Coverage lands in worktree #2.
     *
     * <p>The method exists now so the dispatch in
     * {@code SqlTupleReader.prepareTuples} is wired through a stable seam
     * — flipping translation on for a given shape is a one-file change
     * here, not a re-plumbing of the reader.
     *
     * @throws UnsupportedTranslation always (worktree #1).
     */
    public static PlannerRequest fromTupleRead(Object tupleReadContext) {
        recordFallback();
        throw new UnsupportedTranslation(
            "CalcitePlannerAdapters.fromTupleRead: tuple-read translation "
            + "is deferred to a later worktree; falling back to legacy SQL.");
    }

    /**
     * Attempt to translate an aggregate-segment-load context (the
     * {@code GroupingSetsList} + compound-predicate shape used by
     * {@code SegmentLoader.createExecuteSql}) into a
     * {@link PlannerRequest}. Currently always throws
     * {@link UnsupportedTranslation}: segment-load SQL emission ranges over
     * grouping sets, rollup indicator columns, and compound-predicate
     * pushdown that the worktree-#1 planner shape does not yet model.
     * Coverage lands in worktree #2.
     *
     * <p>The method exists now so the dispatch in
     * {@code SegmentLoader.createExecuteSql} is wired through a stable
     * seam — flipping translation on for a given shape is a one-file
     * change here, not a re-plumbing of the loader.
     *
     * @param segmentLoadContext opaque handle on the loader-local context
     *     (e.g. the {@code GroupingSetsList}); shape solidifies in
     *     worktree #2.
     * @throws UnsupportedTranslation always (worktree #1).
     */
    public static PlannerRequest fromSegmentLoad(Object segmentLoadContext) {
        recordFallback();
        throw new UnsupportedTranslation(
            "CalcitePlannerAdapters.fromSegmentLoad: segment-load "
            + "translation is deferred to a later worktree; falling back "
            + "to legacy SQL.");
    }

    /** Increments the unsupported-fallback counter. */
    public static void recordFallback() {
        UNSUPPORTED_FALLBACK_COUNT.incrementAndGet();
    }

    /** Total number of times a translation fell back to legacy. */
    public static long unsupportedFallbackCount() {
        return UNSUPPORTED_FALLBACK_COUNT.get();
    }

    /** Reset the fallback counter (test-only). */
    public static void resetFallbackCount() {
        UNSUPPORTED_FALLBACK_COUNT.set(0L);
    }
}

// End CalcitePlannerAdapters.java
