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
    /** Gate 1 tripped: classic Mondrian (no interceptor) drifted from the golden. */
    BASELINE_DRIFT,
    /** Gate 2 tripped: interceptor run produced a different MDX cell set. */
    CELL_SET_DRIFT,
    /** Gate 3 tripped: interceptor run emitted a differing SQL rowset. */
    SQL_ROWSET_DRIFT
}
