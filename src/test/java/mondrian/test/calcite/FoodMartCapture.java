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

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.test.TestContext;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared cold-cache MDX capture helper used by both
 * {@link BaselineRecorder} (golden generation) and
 * {@link EquivalenceHarness} (Run A / Run B capture).
 *
 * <p>Every call to {@link #executeCold(NamedMdx, String)} runs the MDX under
 * the exact same conditions:
 * <ol>
 *   <li>Flush the Mondrian schema cache on a throwaway connection BEFORE
 *       opening the capture connection. Flushing via a connection that was
 *       built with the SqlCapture-wrapped DataSource would cause the cache
 *       flush itself to emit SQL through the wrapper, polluting the capture.
 *       It would also close the schema on our active connection.</li>
 *   <li>Build a fresh HSQLDB DataSource from
 *       {@link MondrianProperties}.</li>
 *   <li>Wrap it in {@link SqlCapture}.</li>
 *   <li>Open a fresh {@link Connection} with {@code UseSchemaPool=false} so
 *       ordering of captured SQL is stable across re-runs.</li>
 *   <li>If an interceptor class name is supplied, install it via the
 *       {@code mondrian.sqlInterceptor} system property for the duration of
 *       this one query, restoring the prior value in {@code finally}.</li>
 *   <li>Parse + execute the MDX, serialize the cell-set, drain
 *       {@link SqlCapture}.</li>
 * </ol>
 *
 * <p>This class is the single source of truth for cold-cache parity between
 * golden generation and harness Run A. Any parity bug (baseline drift) is
 * necessarily a bug here.
 */
final class FoodMartCapture {

    static final String INTERCEPTOR_SYS_PROP = "mondrian.sqlInterceptor";

    private FoodMartCapture() {
        // utility
    }

    /**
     * Result of a single cold-cache MDX execution.
     */
    static final class CapturedRun {
        final String cellSet;
        final List<CapturedExecution> executions;

        CapturedRun(String cellSet, List<CapturedExecution> executions) {
            this.cellSet = cellSet;
            this.executions = executions;
        }
    }

    /**
     * Executes {@code mdx} with a cold schema cache and a fresh capture
     * connection.
     *
     * @param mdx query to execute
     * @param interceptorClassName fully-qualified class name of a
     *     {@link mondrian.rolap.sql.SqlInterceptor} to install for the
     *     duration of this call, or {@code null} for classic Mondrian.
     */
    static CapturedRun executeCold(
        NamedMdx mdx,
        String interceptorClassName)
    {
        // Per-query schema cache flush. CRITICAL: do this BEFORE building
        // the SqlCapture-wrapped connection. Otherwise the flush would
        // either route through the wrapper (polluting capture) or close
        // the schema we're about to query on.
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
        // Disable the RolapSchema pool so ordering of captured SQL is
        // stable across re-runs — same reason as BaselineRecorder.
        props.put("UseSchemaPool", "false");

        String prev = System.getProperty(INTERCEPTOR_SYS_PROP);
        if (interceptorClassName != null) {
            System.setProperty(INTERCEPTOR_SYS_PROP, interceptorClassName);
        }
        try {
            Connection conn =
                DriverManager.getConnection(props, null, capture);
            try {
                capture.drain(); // clear any residue from connection init
                Query parsed = conn.parseQuery(mdx.mdx);
                Result result = conn.execute(parsed);
                String cellSet;
                try {
                    cellSet = TestContext.toString(result);
                } finally {
                    result.close();
                }
                List<CapturedExecution> execs =
                    Collections.unmodifiableList(
                        new ArrayList<>(capture.drain()));
                return new CapturedRun(cellSet, execs);
            } finally {
                conn.close();
            }
        } finally {
            if (interceptorClassName != null) {
                if (prev == null) {
                    System.clearProperty(INTERCEPTOR_SYS_PROP);
                } else {
                    System.setProperty(INTERCEPTOR_SYS_PROP, prev);
                }
            }
        }
    }

    static Util.PropertyList baseProperties() {
        return Util.parseConnectString(TestContext.getDefaultConnectString());
    }

    static DataSource buildUnderlyingDataSource() {
        String jdbcUrl =
            MondrianProperties.instance().FoodmartJdbcURL.get();
        String user =
            MondrianProperties.instance().TestJdbcUser.get();
        String password =
            MondrianProperties.instance().TestJdbcPassword.get();
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
}

// End FoodMartCapture.java
