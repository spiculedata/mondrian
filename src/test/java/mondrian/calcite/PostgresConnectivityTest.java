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

import mondrian.test.calcite.HarnessBackend;
import mondrian.test.calcite.PostgresFoodMartDataSource;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Smoke test for Task Z: opens a connection to the configured Postgres
 * instance, asserts the {@link DatabaseMetaData} product name, and verifies
 * that {@link CalciteDialectMap#forDataSource(DataSource)} returns a
 * {@link PostgresqlSqlDialect}.
 *
 * <p>Skips when {@link HarnessBackend#current()} is not
 * {@link HarnessBackend#POSTGRES}, so the default {@code mvn test} is not
 * affected.
 *
 * <p>Does NOT assume FoodMart tables exist — Task AA loads data.
 */
public class PostgresConnectivityTest {

    @Before
    public void requirePostgresBackend() {
        Assume.assumeTrue(
            "PostgresConnectivityTest skipped: CALCITE_HARNESS_BACKEND != POSTGRES",
            HarnessBackend.current() == HarnessBackend.POSTGRES);
    }

    @Test
    public void connectsAndReportsPostgresProductName() throws Exception {
        DataSource ds = PostgresFoodMartDataSource.create();
        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData md = conn.getMetaData();
            String product = md.getDatabaseProductName();
            assertNotNull(product);
            assertTrue(
                "Expected Postgres product name, got: " + product,
                product.toLowerCase().contains("postgres"));

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT version()"))
            {
                assertTrue(rs.next());
                String version = rs.getString(1);
                assertNotNull(version);
                assertTrue(
                    "version() did not report PostgreSQL: " + version,
                    version.toLowerCase().contains("postgresql"));
            }
        }
    }

    @Test
    public void dialectMapRoutesToPostgresDialect() {
        DataSource ds = PostgresFoodMartDataSource.create();
        SqlDialect sd = CalciteDialectMap.forDataSource(ds);
        assertTrue(
            "Expected PostgresqlSqlDialect, got " + sd.getClass().getName(),
            sd instanceof PostgresqlSqlDialect);
    }
}

// End PostgresConnectivityTest.java
