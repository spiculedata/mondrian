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

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Postgres-backed FoodMart {@link DataSource} for the Calcite harness.
 * Activated when {@link HarnessBackend#current()} returns
 * {@link HarnessBackend#POSTGRES}.
 *
 * <p>Configuration (env vars; system properties of the same lowercased,
 * dotted name also honoured):
 * <ul>
 *   <li>{@code CALCITE_HARNESS_POSTGRES_URL} — default
 *       {@code jdbc:postgresql://localhost:5432/foodmart_calcite}</li>
 *   <li>{@code CALCITE_HARNESS_POSTGRES_USER} — default {@code tombarber}</li>
 *   <li>{@code CALCITE_HARNESS_POSTGRES_PASSWORD} — default empty
 *       (peer auth works on local dev)</li>
 * </ul>
 *
 * <p>If the Postgres JDBC driver's {@code PGSimpleDataSource} is on the
 * classpath it is used directly; otherwise a {@link DriverManager}-backed
 * shim {@link DataSource} is returned.
 */
public final class PostgresFoodMartDataSource {

    public static final String URL_ENV = "CALCITE_HARNESS_POSTGRES_URL";
    public static final String USER_ENV = "CALCITE_HARNESS_POSTGRES_USER";
    public static final String PW_ENV = "CALCITE_HARNESS_POSTGRES_PASSWORD";

    public static final String DEFAULT_URL =
        "jdbc:postgresql://localhost:5432/foodmart_calcite";
    public static final String DEFAULT_USER = "tombarber";
    public static final String DEFAULT_PASSWORD = "";

    private PostgresFoodMartDataSource() {}

    public static DataSource create() {
        String url = resolve(URL_ENV, DEFAULT_URL);
        String user = resolve(USER_ENV, DEFAULT_USER);
        String password = resolve(PW_ENV, DEFAULT_PASSWORD);

        DataSource raw;
        // Try PGSimpleDataSource reflectively to avoid a hard compile-time
        // dependency on the Postgres driver (driver is test-scope so the
        // main tree can still compile even if tests skip).
        try {
            Class<?> pgDsClass =
                Class.forName("org.postgresql.ds.PGSimpleDataSource");
            Object ds = pgDsClass.getDeclaredConstructor().newInstance();
            pgDsClass.getMethod("setUrl", String.class).invoke(ds, url);
            if (user != null) {
                pgDsClass.getMethod("setUser", String.class).invoke(ds, user);
            }
            if (password != null) {
                pgDsClass.getMethod("setPassword", String.class)
                    .invoke(ds, password);
            }
            raw = (DataSource) ds;
        } catch (ClassNotFoundException cnfe) {
            raw = new DriverManagerShim(url, user, password);
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to build PGSimpleDataSource for url=" + url, e);
        }
        // Mondrian's RolapConnection calls getConnection(user, pw) with
        // HSQLDB-flavoured defaults (user="sa"). Override so the preset
        // peer-auth credentials win for Postgres.
        return new FixedCredentialsDataSource(raw);
    }

    /**
     * Ignores any caller-supplied user/password and always returns the
     * underlying DataSource's preset connection. Lets Mondrian's
     * {@code JdbcUser=sa} defaults coexist with Postgres peer auth.
     */
    static final class FixedCredentialsDataSource implements DataSource {
        private final DataSource delegate;
        FixedCredentialsDataSource(DataSource delegate) {
            this.delegate = delegate;
        }
        @Override public Connection getConnection() throws SQLException {
            return delegate.getConnection();
        }
        @Override public Connection getConnection(String u, String p)
            throws SQLException
        {
            return delegate.getConnection();
        }
        @Override public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }
        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }
        @Override public void setLoginTimeout(int s) throws SQLException {
            delegate.setLoginTimeout(s);
        }
        @Override public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }
        @Override public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
        {
            return delegate.getParentLogger();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface)
            throws SQLException
        {
            return delegate.isWrapperFor(iface);
        }
    }

    private static String resolve(String envVar, String defaultValue) {
        String sysProp = envVar.toLowerCase().replace('_', '.');
        String v = System.getProperty(sysProp);
        if (v != null && !v.isEmpty()) {
            return v;
        }
        v = System.getenv(envVar);
        if (v != null && !v.isEmpty()) {
            return v;
        }
        return defaultValue;
    }

    /**
     * Minimal {@link DataSource} that defers every call to
     * {@link DriverManager}. Used only if {@code PGSimpleDataSource} is not
     * on the classpath (should not occur in the normal test-scope setup).
     */
    static final class DriverManagerShim implements DataSource {
        private final String url;
        private final String user;
        private final String password;
        private PrintWriter logWriter;
        private int loginTimeout;

        DriverManagerShim(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @Override public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, user, password);
        }
        @Override public Connection getConnection(String u, String p)
            throws SQLException
        {
            return DriverManager.getConnection(url, u, p);
        }
        @Override public PrintWriter getLogWriter() { return logWriter; }
        @Override public void setLogWriter(PrintWriter out) {
            this.logWriter = out;
        }
        @Override public void setLoginTimeout(int seconds) {
            this.loginTimeout = seconds;
        }
        @Override public int getLoginTimeout() { return loginTimeout; }
        @Override public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
        {
            throw new SQLFeatureNotSupportedException();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException(
                "DriverManagerShim does not wrap " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}

// End PostgresFoodMartDataSource.java
