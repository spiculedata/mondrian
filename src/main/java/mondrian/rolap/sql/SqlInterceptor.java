/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.sql;

import mondrian.spi.Dialect;

/**
 * SPI seam for intercepting SQL emitted by Mondrian immediately before it is
 * handed to JDBC. The default implementation is {@link #IDENTITY}; an
 * alternative implementation can be selected at runtime via the system
 * property {@code mondrian.sqlInterceptor}.
 *
 * <p>This interface exists to support the Calcite equivalence harness: the
 * harness installs an interceptor that parses and re-emits SQL via Calcite so
 * that drift (if any) can be detected against an unmodified-Mondrian baseline.
 */
public interface SqlInterceptor {
    /**
     * Called once per SQL string, immediately before it is executed via JDBC.
     *
     * @param sql the SQL about to be executed
     * @param dialect the active Mondrian dialect, or {@code null} if not
     *     available at the call site
     * @return the SQL string that should actually be executed; implementations
     *     returning the input unchanged are no-ops
     */
    String onSqlEmitted(String sql, Dialect dialect);

    /** No-op interceptor — returns the input unchanged. */
    SqlInterceptor IDENTITY = (sql, dialect) -> sql;

    /**
     * Loads the interceptor configured via the {@code mondrian.sqlInterceptor}
     * system property. If the property is unset or empty, returns
     * {@link #IDENTITY}. Otherwise instantiates the named class via its public
     * no-arg constructor.
     *
     * @throws RuntimeException if the configured class cannot be loaded or
     *     instantiated
     */
    static SqlInterceptor loadFromSystemProperty() {
        String cls = System.getProperty("mondrian.sqlInterceptor");
        if (cls == null || cls.isEmpty()) {
            return IDENTITY;
        }
        try {
            return (SqlInterceptor) Class.forName(cls)
                .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to load SqlInterceptor: " + cls, e);
        }
    }
}

// End SqlInterceptor.java
