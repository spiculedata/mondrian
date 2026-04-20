/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Maps Mondrian-reported JDBC database product names to Calcite
 * {@link SqlDialect} instances configured for SQL emission compatible with
 * Mondrian's FoodMart fixture.
 *
 * <p>Worktree #1 supports HSQLDB only. The returned dialect is a subclass
 * of {@link HsqldbSqlDialect} that forces double-quoting of all identifiers
 * — the FoodMart HSQLDB fixture is populated via Mondrian's legacy
 * {@code SqlQuery} builder, which also quotes every identifier, so the
 * physical tables are stored in case-sensitive (delimited) form. Without
 * quoting, HSQLDB case-folds the identifier to UPPER and fails to resolve.
 */
public final class CalciteDialectMap {
    private CalciteDialectMap() {}

    /**
     * Resolves a Calcite {@link SqlDialect} for the given JDBC
     * {@link DataSource} by reading the database product name directly from
     * {@link DatabaseMetaData}. Production call sites under
     * {@code backend=calcite} must use this entry-point; we deliberately
     * avoid any dependency on {@code mondrian.spi.impl.*Dialect} classes so
     * that once worktree #4 removes them the Calcite path keeps compiling
     * and running end-to-end.
     */
    public static SqlDialect forDataSource(DataSource ds) {
        if (ds == null) {
            throw new IllegalArgumentException("null DataSource");
        }
        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData md = conn.getMetaData();
            String product = md.getDatabaseProductName();
            return forProductName(product);
        } catch (SQLException e) {
            throw new RuntimeException(
                "CalciteDialectMap.forDataSource: failed to read "
                + "DatabaseMetaData.getDatabaseProductName()", e);
        }
    }

    public static SqlDialect forProductName(String product) {
        if (product == null) {
            throw new IllegalArgumentException("null product name");
        }
        String p = product.toLowerCase(java.util.Locale.ROOT);
        if (p.contains("hsql")) {
            return QUOTING_HSQLDB;
        }
        if (p.contains("postgres")) {
            return PostgresqlSqlDialect.DEFAULT;
        }
        throw new IllegalArgumentException(
            "No Calcite SqlDialect mapping for JDBC product '" + product
            + "'. Supported: HSQLDB, PostgreSQL. Extend "
            + "CalciteDialectMap to add more.");
    }

    /**
     * HSQLDB dialect whose {@code identifierNeedsQuote} returns true for
     * every identifier, so unparsed SQL matches the case-sensitive
     * delimited form used by the FoodMart fixture.
     */
    private static final SqlDialect QUOTING_HSQLDB =
        new HsqldbSqlDialect(
            HsqldbSqlDialect.DEFAULT_CONTEXT
                .withIdentifierQuoteString("\""))
        {
            @Override
            protected boolean identifierNeedsQuote(String val) {
                return true;
            }
        };
}
