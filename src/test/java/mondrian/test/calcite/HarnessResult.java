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
}
