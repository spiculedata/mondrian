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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Diagnostic accumulator for per-phase timing of the Calcite per-query path.
 *
 * <p>Off by default. Enabled via {@code -Dharness.calcite.profile=true}.
 * Call-sites that record timings read the static boolean once; when disabled
 * all record() calls are skipped so the production path is not affected.
 *
 * <p>Intended consumer: {@code CalciteOverheadProbeTest}, which resets the
 * buckets before each query and dumps per-phase totals / counts after each
 * iteration.
 *
 * <p>This class is deliberately minimal and non-thread-safe across
 * {@link #reset()} calls (it's a diagnostic tool, not a production metric).
 */
public final class CalciteProfile {

    private static final ConcurrentMap<String, Bucket> BUCKETS =
        new ConcurrentHashMap<String, Bucket>();

    private CalciteProfile() {}

    /** Record an elapsed-nanos sample against {@code bucket}. */
    public static void record(String bucket, long nanos) {
        Bucket b = BUCKETS.get(bucket);
        if (b == null) {
            Bucket added = new Bucket();
            Bucket prev = BUCKETS.putIfAbsent(bucket, added);
            b = prev != null ? prev : added;
        }
        b.totalNs.addAndGet(nanos);
        b.calls.incrementAndGet();
    }

    /** Clear every bucket. Used between probe iterations. */
    public static void reset() {
        BUCKETS.clear();
    }

    /** Snapshot of (bucket -> [totalNs, calls]). */
    public static Map<String, long[]> snapshot() {
        Map<String, long[]> out = new java.util.TreeMap<String, long[]>();
        for (Map.Entry<String, Bucket> e : BUCKETS.entrySet()) {
            Bucket b = e.getValue();
            out.put(e.getKey(),
                new long[] { b.totalNs.get(), b.calls.get() });
        }
        return out;
    }

    private static final class Bucket {
        final AtomicLong totalNs = new AtomicLong();
        final AtomicLong calls = new AtomicLong();
    }
}

// End CalciteProfile.java
