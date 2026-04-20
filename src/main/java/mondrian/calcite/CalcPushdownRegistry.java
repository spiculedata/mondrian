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
        public Entry(Member m, Exp e) {
            this.member = m;
            this.expression = e;
        }
    }

    private static final ThreadLocal<List<Entry>> ACTIVE =
        new ThreadLocal<>();

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

    /** Snapshot; never null. */
    public static List<Entry> active() {
        List<Entry> e = ACTIVE.get();
        if (e == null || e.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        return java.util.Collections.unmodifiableList(e);
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
