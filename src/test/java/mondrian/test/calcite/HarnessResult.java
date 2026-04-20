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

import java.util.List;

/**
 * Plain data record returned by {@link EquivalenceHarness#run}. Carries both
 * runs' artefacts so downstream reporting can render diffs. Plain public
 * fields — no builder, no getters.
 */
public final class HarnessResult {
    public final FailureClass failureClass;
    /** Human-readable description — suitable for an assertion message. */
    public final String detail;
    public final String runACellSet;
    public final List<CapturedExecution> runASql;
    /** Null if the pipeline short-circuited before Run B. */
    public final String runBCellSet;
    /** Null if the pipeline short-circuited before Run B. */
    public final List<CapturedExecution> runBSql;

    public HarnessResult(
        FailureClass failureClass,
        String detail,
        String runACellSet,
        List<CapturedExecution> runASql,
        String runBCellSet,
        List<CapturedExecution> runBSql)
    {
        this.failureClass = failureClass;
        this.detail = detail;
        this.runACellSet = runACellSet;
        this.runASql = runASql;
        this.runBCellSet = runBCellSet;
        this.runBSql = runBSql;
    }

    /**
     * Outcome of comparing a Run A capture against its committed golden.
     *
     * <p>Split from {@link HarnessResult} because the golden comparator
     * operates only on Run A and must distinguish three cases:
     * <ul>
     *   <li>{@code failureClass == null &amp;&amp; sqlDriftDetail == null} —
     *       clean pass, proceed to Run B.</li>
     *   <li>{@code failureClass == null &amp;&amp; sqlDriftDetail != null} —
     *       advisory SQL drift; caller records via {@link HarnessReporter}
     *       and proceeds to Run B as if it were a pass.</li>
     *   <li>{@code failureClass != null} — hard gate tripped; caller
     *       short-circuits with a failing {@link HarnessResult}.</li>
     * </ul>
     */
    public static final class Comparison {
        /** Null when the comparison passes (including advisory SQL drift). */
        public final FailureClass failureClass;
        /** Human-readable detail for {@link #failureClass}; null on pass. */
        public final String detail;
        /**
         * Populated only when SQL drift was observed under
         * {@code harness.sqlCompare=advisory}. Always null in {@code strict}
         * (the detail ends up in {@link #detail}) and always null in
         * {@code off} (comparison skipped).
         */
        public final String sqlDriftDetail;

        public Comparison(FailureClass failureClass,
                          String detail,
                          String sqlDriftDetail) {
            this.failureClass = failureClass;
            this.detail = detail;
            this.sqlDriftDetail = sqlDriftDetail;
        }

        public static Comparison pass() {
            return new Comparison(null, null, null);
        }

        public static Comparison fail(FailureClass fc, String detail) {
            return new Comparison(fc, detail, null);
        }

        public static Comparison advisorySqlDrift(String detail) {
            return new Comparison(null, null, detail);
        }
    }
}
