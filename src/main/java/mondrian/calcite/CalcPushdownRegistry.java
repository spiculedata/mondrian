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

import mondrian.olap.Exp;
import mondrian.olap.Member;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-thread registry of calc members associated with the currently
 * executing MDX query. Populated by the caller (today: tests and
 * optionally a RolapResult-side hook) before a segment-load dispatch;
 * consulted by {@link CalcitePlannerAdapters#fromSegmentLoad} to decide
 * which calcs can be pushed onto the segment-load SELECT list.
 *
 * <p>Kept out of {@link CalcitePlannerAdapters} to isolate the
 * (threadlocal) wiring from the (stateless) translator; the registry is
 * the only mutable surface for Task T and is test-visible via
 * {@link #activate} / {@link #clear}.
 */
public final class CalcPushdownRegistry {

    /** A calc member in the current query. */
    public static final class Entry {
        public final Member member;
        public final Exp expression;
        /** Cached analyzer verdict, if any. {@code null} means the
         *  registering code did not pre-classify — callers may
         *  re-classify on demand. */
        public final ArithmeticCalcAnalyzer.Classification classification;
        public Entry(Member m, Exp e) {
            this(m, e, null);
        }
        public Entry(
            Member m, Exp e,
            ArithmeticCalcAnalyzer.Classification c)
        {
            this.member = m;
            this.expression = e;
            this.classification = c;
        }
    }

    private static final ThreadLocal<List<Entry>> ACTIVE =
        new ThreadLocal<>();

    /**
     * Cross-thread registry keyed by the Mondrian query
     * {@link mondrian.server.Execution}. Needed because the MDX
     * submitter thread registers calcs, but segment-load translation
     * runs on the {@code SegmentCacheManager$ACTOR} or
     * {@code RolapResultShepherd} worker. A ThreadLocal alone is
     * insufficient.
     *
     * <p>Kept small: one entry per concurrent query. Cleared by the
     * {@link mondrian.rolap.RolapResult} finally block when the query
     * completes.
     */
    private static final java.util.concurrent.ConcurrentMap<Object, List<Entry>>
        BY_EXECUTION = new java.util.concurrent.ConcurrentHashMap<>();

    /** Counters — test-visible via {@link CalcitePlannerAdapters}. */
    private static final AtomicLong PUSHED = new AtomicLong();
    private static final AtomicLong REJECTED = new AtomicLong();

    private CalcPushdownRegistry() {}

    /** Register a list of calc members active on the current thread.
     *  Any later call replaces the list. Use {@link #clear} to drop. */
    public static void activate(List<Entry> entries) {
        if (entries == null || entries.isEmpty()) {
            ACTIVE.remove();
            return;
        }
        ACTIVE.set(new ArrayList<>(entries));
    }

    public static void clear() {
        ACTIVE.remove();
    }

    /**
     * Register entries for an {@link mondrian.server.Execution}. Used by
     * the RolapResult hook so the list is reachable from the segment-load
     * translator which runs on a different pool thread.
     */
    public static void activateForExecution(
        Object execution, List<Entry> entries)
    {
        if (execution == null) {
            return;
        }
        if (entries == null || entries.isEmpty()) {
            BY_EXECUTION.remove(execution);
            return;
        }
        BY_EXECUTION.put(execution, new ArrayList<>(entries));
    }

    /** Clear the execution-keyed registry slot. */
    public static void clearExecution(Object execution) {
        if (execution == null) {
            return;
        }
        BY_EXECUTION.remove(execution);
    }

    /**
     * Snapshot of active entries. Consults the thread-local first
     * (test-only callers populate this directly); falls back to the
     * execution-keyed map using {@code Locus.peek().execution} as the
     * key. Never null.
     */
    public static List<Entry> active() {
        List<Entry> e = ACTIVE.get();
        if (e != null && !e.isEmpty()) {
            return java.util.Collections.unmodifiableList(e);
        }
        // Fallback to the Execution-keyed map — the hot path for the
        // RolapResult hook, where segment loads run on pool workers.
        try {
            Object exec = mondrian.server.Locus.peek().execution;
            List<Entry> fromExec = BY_EXECUTION.get(exec);
            if (fromExec != null && !fromExec.isEmpty()) {
                return java.util.Collections.unmodifiableList(fromExec);
            }
        } catch (Throwable ignored) {
            // No Locus on this thread — fall through.
        }
        return java.util.Collections.emptyList();
    }

    /** Increment pushed-count. */
    public static void onPushed() {
        PUSHED.incrementAndGet();
    }

    /** Increment rejected-count. */
    public static void onRejected() {
        REJECTED.incrementAndGet();
    }

    public static long pushedCount() { return PUSHED.get(); }
    public static long rejectedCount() { return REJECTED.get(); }

    public static void resetCounters() {
        PUSHED.set(0L);
        REJECTED.set(0L);
    }
}

// End CalcPushdownRegistry.java
