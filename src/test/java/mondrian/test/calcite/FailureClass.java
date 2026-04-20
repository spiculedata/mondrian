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

/**
 * Outcome of a single run through the {@link EquivalenceHarness}'s
 * three-gate pipeline.
 */
public enum FailureClass {
    /** All gates passed: Run A matches golden, Run B matches Run A. */
    PASS,
    /**
     * Gate 1 tripped: classic Mondrian (no interceptor) drifted from the
     * legacy golden. Renamed from the historical {@code BASELINE_DRIFT}
     * per the calcite-backend-rewrite design (§Harness evolution): once
     * Calcite becomes the default backend the legacy RolapNative pipeline
     * becomes the secondary baseline, so the failure name should reflect
     * that role.
     */
    LEGACY_DRIFT,
    /**
     * Cell-set parity is intact but the per-execution SQL string no longer
     * matches the committed golden. Soft gate — whether this fails the test
     * is controlled by {@code -Dharness.sqlCompare={strict,advisory,off}}
     * (default {@code advisory}, which records the drift via
     * {@link HarnessReporter} but does not fail).
     *
     * <p>Introduced for the Calcite rewrite: {@code RelToSqlConverter} emits
     * ANSI-join syntax while legacy {@code SqlQuery} emits comma-joins, so
     * byte-for-byte SQL parity is impossible during the transition. Cell-set
     * parity — plus rowCount and checksum, which remain under
     * {@link #LEGACY_DRIFT} — remains the real correctness signal.
     */
    SQL_DRIFT,
    /** Gate 2 tripped: interceptor run produced a different MDX cell set. */
    CELL_SET_DRIFT,
    /** Gate 3 tripped: interceptor run emitted a differing SQL rowset. */
    SQL_ROWSET_DRIFT,
    /**
     * Plan-snapshot gate tripped: under the Calcite backend the captured
     * {@code RelOptUtil.toString(rel)} for a query no longer matches its
     * checked-in {@code golden-plans/<name>.plan}. Scaffolding only in
     * worktree #1 — no plan goldens are committed yet, so this class is
     * unreachable from the harness pipeline until plan capture is wired up.
     */
    PLAN_DRIFT
}
