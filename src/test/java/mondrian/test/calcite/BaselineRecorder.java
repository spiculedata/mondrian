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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import mondrian.olap.Result;
import mondrian.test.TestContext;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Generates deterministic JSON "golden" files for each MDX in a corpus by
 * running the query against unmodified Mondrian with a {@link SqlCapture}
 * wrapped DataSource. Each golden contains:
 * <ul>
 *   <li>the MDX string</li>
 *   <li>the cell-set text ({@link TestContext#toString(Result)})</li>
 *   <li>every SQL executed during the query, with per-execution row count
 *       and SHA-256 checksum of the rowset</li>
 * </ul>
 *
 * <p>Each query is captured independently via {@link FoodMartCapture}, which
 * flushes the schema cache before each execution. This is load-bearing:
 * running all corpus queries on a single shared connection would warm the
 * schema/segment caches, causing later queries to emit fewer SQL statements
 * than a cold-start harness would see — guaranteeing
 * {@link FailureClass#LEGACY_DRIFT LEGACY_DRIFT} at
 * {@link EquivalenceHarness} time.
 *
 * <p><b>Deliberate deviation from the plan:</b> only {@code rowCount} and
 * {@code checksum} are persisted per SQL execution — never the full rowset.
 * FoodMart fact tables have hundreds of thousands of rows and full-rowset
 * goldens would balloon to multi-megabytes each with enormous diff noise.
 * The checksum is sufficient for drift detection; HSQLDB in embedded file
 * mode is deterministic across identical runs.
 */
public final class BaselineRecorder {

    private final Path goldenDir;
    private final ObjectMapper mapper;

    public BaselineRecorder(Path goldenDir) {
        this.goldenDir = goldenDir;
        this.mapper = new ObjectMapper();
        // Pretty-printing keeps goldens reviewable in PRs.
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Executes each MDX via {@link FoodMartCapture#executeCold} (cold schema
     * cache, fresh connection per query) and writes
     * {@code <goldenDir>/<name>.json}.
     *
     * <p>Existing goldens with the same name are overwritten.
     */
    public void record(List<NamedMdx> queries) throws IOException {
        Files.createDirectories(goldenDir);
        for (NamedMdx q : queries) {
            FoodMartCapture.CapturedRun run =
                FoodMartCapture.executeCold(q, null);
            writeGolden(q, run.cellSet, run.executions);
        }
    }

    private void writeGolden(
        NamedMdx q,
        String cellSet,
        List<CapturedExecution> execs)
        throws IOException
    {
        ObjectNode root = mapper.createObjectNode();
        root.put("mdx", q.mdx);
        root.put("cellSet", cellSet);
        ArrayNode arr = root.putArray("sqlExecutions");
        // execs are in natural capture order (seq 0..N-1); preserve it.
        for (CapturedExecution e : execs) {
            ObjectNode en = arr.addObject();
            en.put("seq", e.seq);
            en.put("sql", e.sql);
            en.put("rowCount", e.rowCount);
            en.put("checksum", e.checksum);
        }
        Path out = goldenDir.resolve(q.name + ".json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), root);
    }
}
