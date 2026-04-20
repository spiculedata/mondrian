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

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.test.TestContext;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Generates deterministic JSON "golden" files for each MDX in a corpus by
 * running the query against unmodified Mondrian with a {@link SqlCapture}
 * wrapped {@link DataSource}. Each golden contains:
 * <ul>
 *   <li>the MDX string</li>
 *   <li>the cell-set text ({@link TestContext#toString(Result)})</li>
 *   <li>every SQL executed during the query, with per-execution row count
 *       and SHA-256 checksum of the rowset</li>
 * </ul>
 *
 * <p><b>Deliberate deviation from the plan:</b> only {@code rowCount} and
 * {@code checksum} are persisted per SQL execution — never the full rowset.
 * FoodMart fact tables have hundreds of thousands of rows and full-rowset
 * goldens would balloon to multi-megabytes each with enormous diff noise.
 * The checksum is sufficient for drift detection; HSQLDB in embedded file
 * mode is deterministic across identical runs.
 *
 * <p><b>DataSource wiring:</b> this class builds a fresh HSQLDB
 * {@link DataSource} from {@code MondrianProperties}, wraps it with
 * {@link SqlCapture}, and passes it to
 * {@link DriverManager#getConnection(Util.PropertyList,
 * mondrian.spi.CatalogLocator, DataSource)}. Mondrian's {@code RolapUtil}
 * then executes all SQL through the wrapper. No reflection on
 * {@code RolapConnection}'s private fields is required.
 */
public final class BaselineRecorder {

    private final Path goldenDir;
    private final ObjectMapper mapper;

    public BaselineRecorder(Path goldenDir) {
        this.goldenDir = goldenDir;
        this.mapper = new ObjectMapper();
        // Pretty-printing keeps goldens reviewable in PRs.
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // Property order in generated nodes is controlled explicitly via
        // ObjectNode insertion order (LinkedHashMap-backed), so no extra
        // sorting is needed here. Jackson preserves insertion order.
    }

    /**
     * Executes each MDX against unmodified Mondrian with a SqlCapture-wrapped
     * DataSource and writes {@code <goldenDir>/<name>.json}.
     *
     * <p>Existing goldens with the same name are overwritten.
     */
    public void record(List<NamedMdx> queries) throws IOException {
        Files.createDirectories(goldenDir);

        // Flush every Mondrian cache (schema pool, aggregation star,
        // segment cache) BEFORE creating our connection, so repeated
        // record() invocations in the same JVM see the *same* SQL
        // emissions. Without this, run 1 populates caches and run 2 sees
        // cache hits → different SQL → non-deterministic goldens →
        // harness useless.
        //
        // We flush via a throwaway connection (cheap, no pooling) because
        // flushSchemaCache() closes the schema on the invoking connection.
        Connection flushConn = DriverManager.getConnection(
            baseProperties(), null, null);
        try {
            flushConn.getCacheControl(null).flushSchemaCache();
        } finally {
            flushConn.close();
        }

        DataSource underlying = buildUnderlyingDataSource();
        SqlCapture capture = new SqlCapture(underlying);

        Util.PropertyList props = baseProperties();
        // Disable the RolapSchema pool so each record() call starts fresh;
        // this keeps ordering of captured SQL stable across re-runs.
        props.put("UseSchemaPool", "false");

        Connection conn = DriverManager.getConnection(props, null, capture);
        try {
            for (NamedMdx q : queries) {
                capture.drain(); // clear any residue
                Query parsed = conn.parseQuery(q.mdx);
                Result result = conn.execute(parsed);
                String cellSet;
                try {
                    cellSet = TestContext.toString(result);
                } finally {
                    result.close();
                }
                List<CapturedExecution> execs = capture.drain();
                writeGolden(q, cellSet, execs);
            }
        } finally {
            conn.close();
        }
    }

    private Util.PropertyList baseProperties() {
        // Use TestContext's canonical connection string so catalog URL,
        // dialect, etc. match the test default exactly. The user/password/jdbc
        // entries are harmless even when a dataSource is passed directly.
        return Util.parseConnectString(TestContext.getDefaultConnectString());
    }

    /**
     * Builds a brand-new HSQLDB DataSource against the same JDBC URL that
     * {@link TestContext} uses. We intentionally do not reuse the
     * {@code RolapConnection}'s internal DataSource — that would require
     * reflectively swapping its private final field, which is fragile on
     * modern JDKs.
     */
    private DataSource buildUnderlyingDataSource() {
        String jdbcUrl =
            mondrian.olap.MondrianProperties.instance().FoodmartJdbcURL.get();
        String user =
            mondrian.olap.MondrianProperties.instance().TestJdbcUser.get();
        String password =
            mondrian.olap.MondrianProperties.instance().TestJdbcPassword.get();
        org.hsqldb.jdbc.jdbcDataSource ds =
            new org.hsqldb.jdbc.jdbcDataSource();
        ds.setDatabase(jdbcUrl);
        if (user != null) {
            ds.setUser(user);
        }
        if (password != null) {
            ds.setPassword(password);
        }
        return ds;
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
