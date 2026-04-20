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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Three-gate pipeline that detects drift introduced by inserting a
 * {@link SqlInterceptor} into Mondrian's query path.
 *
 * <p>Gates, in order:
 * <ol>
 *   <li><b>BASELINE_DRIFT:</b> classic Mondrian (no interceptor) produces a
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
 */
public final class EquivalenceHarness {

    public static final String SYS_PROP = FoodMartCapture.INTERCEPTOR_SYS_PROP;

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

        // --- Gate 1: BASELINE_DRIFT ---
        Path goldenFile = goldenDir.resolve(mdx.name + ".json");
        if (!Files.exists(goldenFile)) {
            return new HarnessResult(
                FailureClass.BASELINE_DRIFT,
                "golden not found: " + goldenFile,
                runA.cellSet, runA.executions, null, null);
        }
        JsonNode golden = mapper.readTree(goldenFile.toFile());
        String baselineDetail = compareAgainstGolden(golden, runA);
        if (baselineDetail != null) {
            return new HarnessResult(
                FailureClass.BASELINE_DRIFT,
                baselineDetail,
                runA.cellSet, runA.executions, null, null);
        }

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
