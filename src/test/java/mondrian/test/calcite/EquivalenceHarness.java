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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

/**
 * Three-gate pipeline that detects drift introduced by inserting a
 * {@link SqlInterceptor} into Mondrian's query path.
 *
 * <p>Gates, in order:
 * <ol>
 *   <li><b>LEGACY_DRIFT:</b> classic Mondrian (no interceptor) produces a
 *       cell-set + per-execution SQL/checksum sequence that must match the
 *       recorded golden under {@code goldenDir}.</li>
 *   <li><b>CELL_SET_DRIFT:</b> with the interceptor installed, the MDX cell
 *       set must still equal Run A's.</li>
 *   <li><b>SQL_ROWSET_DRIFT:</b> pairing captured SQL executions by seq,
 *       rowCount and checksum must match between Run A and Run B.</li>
 * </ol>
 *
 * <p>The harness installs the interceptor via the {@code mondrian.sqlInterceptor}
 * system property — deliberately the production wiring — and restores the
 * prior value in a {@code finally} block.
 *
 * <p>A fourth gate, {@link FailureClass#PLAN_DRIFT}, is scaffolded via
 * {@link #comparePlanSnapshot(String, String, Path)} but not yet wired into
 * the pipeline — see TODO below.
 */
public final class EquivalenceHarness {

    public static final String SYS_PROP = FoodMartCapture.INTERCEPTOR_SYS_PROP;

    /**
     * Default location of plan-snapshot goldens. The directory exists (a
     * {@code .gitkeep} placeholder is committed) but is empty in worktree #1;
     * the snapshot comparator is therefore a no-op for every query until
     * worktree #2 starts populating {@code <name>.plan} files.
     */
    public static final Path DEFAULT_GOLDEN_PLANS_DIR =
        Paths.get("src/test/resources/calcite-harness/golden-plans");

    private final Path goldenDir;
    private final ObjectMapper mapper;

    public EquivalenceHarness(Path goldenDir) {
        this.goldenDir = goldenDir;
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public HarnessResult run(NamedMdx mdx,
                             Class<? extends SqlInterceptor> interceptorClass)
        throws IOException
    {
        Objects.requireNonNull(mdx, "mdx");
        Objects.requireNonNull(interceptorClass, "interceptorClass");

        // --- Run A: classic Mondrian, no interceptor ---
        FoodMartCapture.CapturedRun runA =
            FoodMartCapture.executeCold(mdx, null);

        // --- Gate 1: LEGACY_DRIFT ---
        Path goldenFile = goldenDir.resolve(mdx.name + ".json");
        if (!Files.exists(goldenFile)) {
            return new HarnessResult(
                FailureClass.LEGACY_DRIFT,
                "golden not found: " + goldenFile,
                runA.cellSet, runA.executions, null, null);
        }
        JsonNode golden = mapper.readTree(goldenFile.toFile());
        String baselineDetail = compareAgainstGolden(golden, runA);
        if (baselineDetail != null) {
            return new HarnessResult(
                FailureClass.LEGACY_DRIFT,
                baselineDetail,
                runA.cellSet, runA.executions, null, null);
        }

        // TODO(worktree-#2): capture RelOptUtil.toString at plan time and
        // pipe it through comparePlanSnapshot(mdx.name, planText,
        // DEFAULT_GOLDEN_PLANS_DIR). When it returns non-empty, raise a
        // PLAN_DRIFT HarnessResult here. Until plan capture is implemented
        // the comparator stays exposed only as a public method for unit tests.

        // --- Run B: interceptor installed via system property ---
        FoodMartCapture.CapturedRun runB =
            FoodMartCapture.executeCold(mdx, interceptorClass.getName());

        // --- Gate 2: CELL_SET_DRIFT ---
        if (!Objects.equals(runA.cellSet, runB.cellSet)) {
            return new HarnessResult(
                FailureClass.CELL_SET_DRIFT,
                "cell-set differs under interceptor "
                + interceptorClass.getSimpleName()
                + "\n--- runA ---\n" + runA.cellSet
                + "\n--- runB ---\n" + runB.cellSet,
                runA.cellSet, runA.executions,
                runB.cellSet, runB.executions);
        }

        // --- Gate 3: SQL_ROWSET_DRIFT ---
        if (runA.executions.size() != runB.executions.size()) {
            return new HarnessResult(
                FailureClass.SQL_ROWSET_DRIFT,
                "captured SQL count differs: runA="
                + runA.executions.size()
                + " runB=" + runB.executions.size(),
                runA.cellSet, runA.executions,
                runB.cellSet, runB.executions);
        }
        for (int i = 0; i < runA.executions.size(); i++) {
            CapturedExecution a = runA.executions.get(i);
            CapturedExecution b = runB.executions.get(i);
            if (a.rowCount != b.rowCount
                || !Objects.equals(a.checksum, b.checksum))
            {
                return new HarnessResult(
                    FailureClass.SQL_ROWSET_DRIFT,
                    "seq=" + a.seq
                    + " rowCount runA=" + a.rowCount
                    + " runB=" + b.rowCount
                    + " checksum runA=" + a.checksum
                    + " runB=" + b.checksum
                    + "\n--- runA sql ---\n" + a.sql
                    + "\n--- runB sql ---\n" + b.sql,
                    runA.cellSet, runA.executions,
                    runB.cellSet, runB.executions);
            }
        }

        return new HarnessResult(
            FailureClass.PASS, "ok",
            runA.cellSet, runA.executions,
            runB.cellSet, runB.executions);
    }

    /**
     * Pure-text plan-snapshot comparator for the upcoming PLAN_DRIFT gate.
     *
     * <p>Resolves {@code <baseDir>/<queryName>.plan}. If the file is absent
     * (the worktree-#1 default for every query) the method returns
     * {@link Optional#empty()} — i.e. "no drift, harness silent". If
     * present, both the on-disk plan and {@code planText} are stripped of
     * trailing whitespace and compared verbatim. On mismatch the returned
     * Optional carries a one-line diff summary suitable for the
     * {@link HarnessResult#detail} field.
     *
     * <p>{@code baseDir} is a parameter (not hard-coded to
     * {@link #DEFAULT_GOLDEN_PLANS_DIR}) so unit tests can point at a
     * scratch dir without writing into {@code src/test/resources}.
     */
    public static Optional<String> comparePlanSnapshot(
        String queryName,
        String planText,
        Path baseDir)
        throws IOException
    {
        Objects.requireNonNull(queryName, "queryName");
        Objects.requireNonNull(planText, "planText");
        Objects.requireNonNull(baseDir, "baseDir");
        Path planFile = baseDir.resolve(queryName + ".plan");
        if (!Files.exists(planFile)) {
            return Optional.empty();
        }
        String onDisk = new String(
            Files.readAllBytes(planFile), StandardCharsets.UTF_8);
        String left = stripTrailingWhitespace(onDisk);
        String right = stripTrailingWhitespace(planText);
        if (left.equals(right)) {
            return Optional.empty();
        }
        return Optional.of(
            "plan snapshot differs for " + queryName
            + " (file=" + planFile + ")"
            + "\n--- golden ---\n" + left
            + "\n--- captured ---\n" + right);
    }

    private static String stripTrailingWhitespace(String s) {
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                end--;
            } else {
                break;
            }
        }
        return s.substring(0, end);
    }

    /**
     * Returns {@code null} if runA matches the golden; otherwise a human-
     * readable description of the first mismatch.
     */
    private static String compareAgainstGolden(
        JsonNode golden,
        FoodMartCapture.CapturedRun run)
    {
        String goldenCellSet = golden.path("cellSet").asText();
        if (!Objects.equals(goldenCellSet, run.cellSet)) {
            return "cellSet differs from golden\n--- golden ---\n"
                + goldenCellSet
                + "\n--- runA ---\n" + run.cellSet;
        }
        JsonNode execs = golden.path("sqlExecutions");
        if (!execs.isArray()) {
            return "golden missing sqlExecutions array";
        }
        if (execs.size() != run.executions.size()) {
            return "sqlExecutions count differs: golden="
                + execs.size() + " runA=" + run.executions.size();
        }
        for (int i = 0; i < execs.size(); i++) {
            JsonNode ge = execs.get(i);
            CapturedExecution ae = run.executions.get(i);
            int gSeq = ge.path("seq").asInt(-1);
            String gSql = ge.path("sql").asText();
            int gRowCount = ge.path("rowCount").asInt(-1);
            String gChecksum = ge.path("checksum").asText();
            if (gSeq != ae.seq
                || !Objects.equals(gSql, ae.sql)
                || gRowCount != ae.rowCount
                || !Objects.equals(gChecksum, ae.checksum))
            {
                return "sqlExecution[" + i + "] differs\n"
                    + "  golden seq=" + gSeq
                    + " rowCount=" + gRowCount
                    + " checksum=" + gChecksum
                    + "\n  runA  seq=" + ae.seq
                    + " rowCount=" + ae.rowCount
                    + " checksum=" + ae.checksum
                    + "\n  golden sql=" + gSql
                    + "\n  runA  sql=" + ae.sql;
            }
        }
        return null;
    }
}
