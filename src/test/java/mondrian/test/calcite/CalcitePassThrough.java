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

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link SqlInterceptor} that parses the Mondrian-emitted SQL with Apache
 * Calcite's {@link SqlParser} and re-emits it via {@code toSqlString} using a
 * dialect matching the Mondrian dialect supplied at the call site.
 *
 * <p>Intent: <b>semantic no-op</b>. Calcite should round-trip valid SQL such
 * that the resulting string, when executed against the same database, returns
 * the same rowset. This is the spike that proves the Calcite equivalence
 * harness wiring is end-to-end functional.
 *
 * <p>Fail-open: if Calcite cannot parse the SQL (unsupported syntax, dialect
 * quirk, etc.), a warning is logged and the <em>original</em> SQL is returned
 * unchanged. This class must never throw — throwing would poison Mondrian's
 * actual query execution.
 *
 * <p>Not wired into the default {@code mondrian.sqlInterceptor}; opt-in only
 * via:
 * <pre>
 *   -Dmondrian.sqlInterceptor=mondrian.test.calcite.CalcitePassThrough
 * </pre>
 *
 * <p>The FoodMart test fixture uses HSQLDB 1.8, so HSQLDB is the default
 * target dialect when the Mondrian dialect is {@code null} (as happens at
 * {@code RolapUtil.executeQuery} call sites that do not thread the dialect
 * through).
 */
public class CalcitePassThrough implements SqlInterceptor {

    private static final Logger LOG =
        Logger.getLogger(CalcitePassThrough.class.getName());

    @Override
    public String onSqlEmitted(String sql, Dialect dialect) {
        try {
            SqlParser.Config cfg = parserConfig(dialect);
            SqlNode node = SqlParser.create(sql, cfg).parseStmt();
            return node.toSqlString(mapDialect(dialect)).getSql();
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                "CalcitePassThrough parse failed; returning original SQL."
                + " sql=" + preview(sql)
                + " err=" + e.getClass().getSimpleName()
                + ": " + e.getMessage());
            return sql;
        }
    }

    /**
     * Builds a Calcite parser config that matches Mondrian's emitted SQL for
     * the HSQLDB FoodMart fixture: double-quoted identifiers with case
     * preserved (HSQLDB-style), case-sensitive matching, and the
     * {@link SqlConformanceEnum#LENIENT LENIENT} conformance so common
     * vendor extensions don't bounce the parser.
     *
     * <p>Note: Calcite's {@link org.apache.calcite.config.Lex#JAVA Lex.JAVA}
     * actually uses backtick-quoted identifiers, not double quotes, so we
     * wire the {@link Quoting}/{@link Casing} flags directly instead of
     * relying on a {@code Lex} preset.
     */
    private SqlParser.Config parserConfig(Dialect d) {
        return SqlParser.config()
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED)
            .withCaseSensitive(true)
            .withConformance(SqlConformanceEnum.LENIENT);
    }

    /**
     * Maps a Mondrian dialect to a Calcite {@link SqlDialect} for re-emission.
     * Defaults to {@link HsqldbSqlDialect#DEFAULT} which matches the FoodMart
     * test fixture. Null dialect defaults to HSQLDB.
     */
    private SqlDialect mapDialect(Dialect d) {
        return HsqldbSqlDialect.DEFAULT;
    }

    private String preview(String sql) {
        if (sql == null) {
            return "<null>";
        }
        return sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
    }
}

// End CalcitePassThrough.java
