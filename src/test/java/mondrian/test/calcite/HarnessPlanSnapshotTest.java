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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit-tests {@link EquivalenceHarness#comparePlanSnapshot} — the pure-text
 * comparator that will back the {@link FailureClass#PLAN_DRIFT} gate once
 * worktree #2 wires plan capture into the harness pipeline.
 *
 * <p>Three behaviours under test:
 * <ol>
 *   <li>Absent plan file ⇒ no drift (Optional.empty).</li>
 *   <li>Present plan that matches (modulo trailing whitespace) ⇒ no drift.</li>
 *   <li>Present plan that differs ⇒ Optional with a diff summary.</li>
 * </ol>
 *
 * <p>All three cases use a {@link TemporaryFolder} as the {@code baseDir}
 * to avoid mutating {@code src/test/resources/calcite-harness/golden-plans}
 * from inside a test.
 */
public class HarnessPlanSnapshotTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void absentGoldenIsNoOp() throws Exception {
        Path baseDir = tmp.getRoot().toPath();
        Optional<String> result =
            EquivalenceHarness.comparePlanSnapshot(
                "no-such-query",
                "LogicalProject(...)\n  LogicalTableScan(table=[[FOO]])\n",
                baseDir);
        assertFalse(
            "Absent plan file must be silent (no drift); got: "
            + result.orElse("<empty>"),
            result.isPresent());
    }

    @Test
    public void matchingGoldenIsNoOp() throws Exception {
        Path baseDir = tmp.getRoot().toPath();
        String plan =
            "LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
            + "  LogicalTableScan(table=[[FOO]])\n";
        // Write golden with extra trailing newline / spaces; comparator
        // must trim both sides before comparing.
        Files.write(
            baseDir.resolve("matching.plan"),
            (plan + "  \n\n").getBytes(StandardCharsets.UTF_8));
        Optional<String> result =
            EquivalenceHarness.comparePlanSnapshot(
                "matching", plan, baseDir);
        assertFalse(
            "Identical (mod trailing-whitespace) plans must be silent; got: "
            + result.orElse("<empty>"),
            result.isPresent());
    }

    @Test
    public void differingGoldenRaisesDriftDetail() throws Exception {
        Path baseDir = tmp.getRoot().toPath();
        String golden =
            "LogicalProject(a=[$0])\n"
            + "  LogicalTableScan(table=[[FOO]])\n";
        String captured =
            "LogicalProject(a=[$0], b=[$1])\n"
            + "  LogicalTableScan(table=[[FOO]])\n";
        Files.write(
            baseDir.resolve("differing.plan"),
            golden.getBytes(StandardCharsets.UTF_8));
        Optional<String> result =
            EquivalenceHarness.comparePlanSnapshot(
                "differing", captured, baseDir);
        assertTrue(
            "Differing plans must raise drift detail",
            result.isPresent());
        String detail = result.get();
        assertTrue(
            "diff summary must mention query name; got: " + detail,
            detail.contains("differing"));
        assertTrue(
            "diff summary must mention golden plan text; got: " + detail,
            detail.contains("a=[$0]"));
        assertTrue(
            "diff summary must mention captured plan text; got: " + detail,
            detail.contains("b=[$1]"));
    }

    /**
     * Belt-and-braces: the rename hasn't accidentally dropped the new
     * failure class from the enum. Cheap guard against future enum churn.
     */
    @Test
    public void planDriftEnumValueExists() {
        assertEquals(
            "PLAN_DRIFT", FailureClass.PLAN_DRIFT.name());
        assertEquals(
            "LEGACY_DRIFT", FailureClass.LEGACY_DRIFT.name());
    }
}

// End HarnessPlanSnapshotTest.java
