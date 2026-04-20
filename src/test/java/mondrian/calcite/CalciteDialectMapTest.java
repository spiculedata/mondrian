package mondrian.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CalciteDialectMapTest {
    @Test public void hsqldbDialectMapsToHsqldb() {
        SqlDialect sd = CalciteDialectMap.forProductName("HSQL Database Engine");
        assertTrue(sd instanceof HsqldbSqlDialect);
    }
    @Test public void caseInsensitive() {
        SqlDialect sd = CalciteDialectMap.forProductName("hsql database engine");
        assertTrue(sd instanceof HsqldbSqlDialect);
    }
    @Test(expected = IllegalArgumentException.class)
    public void unknownProductThrows() {
        CalciteDialectMap.forProductName("Frobnitz 9.9");
    }
    @Test(expected = IllegalArgumentException.class)
    public void nullThrows() {
        CalciteDialectMap.forProductName(null);
    }

    // --- Task Z: Postgres mappings ---

    @Test public void postgresProductMapsToPostgres() {
        SqlDialect sd = CalciteDialectMap.forProductName("PostgreSQL");
        assertTrue(sd instanceof PostgresqlSqlDialect);
    }

    @Test public void postgresProductLowercaseMapsToPostgres() {
        SqlDialect sd = CalciteDialectMap.forProductName("postgresql");
        assertTrue(sd instanceof PostgresqlSqlDialect);
    }

    @Test public void postgresDataSourceRoundTrip() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(md);
        when(md.getDatabaseProductName()).thenReturn("PostgreSQL");
        SqlDialect sd = CalciteDialectMap.forDataSource(ds);
        assertTrue(sd instanceof PostgresqlSqlDialect);
    }

    @Test public void hsqldbDataSourceRoundTripUnchanged() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(md);
        when(md.getDatabaseProductName()).thenReturn("HSQL Database Engine");
        SqlDialect sd = CalciteDialectMap.forDataSource(ds);
        assertTrue(sd instanceof HsqldbSqlDialect);
    }

    /**
     * Calcite's stock PostgresqlSqlDialect already uses {@code "} to quote
     * identifiers — no subclass override needed. Confirms that SQL emitted
     * via this dialect will double-quote identifiers to match the FoodMart
     * fixture's case-sensitive column/table names.
     */
    @Test public void postgresDialectQuotesIdentifiersWithDoubleQuote() {
        SqlDialect pg = PostgresqlSqlDialect.DEFAULT;
        StringBuilder buf = new StringBuilder();
        pg.quoteIdentifier(buf, "sales_fact_1997");
        assertEquals("\"sales_fact_1997\"", buf.toString());
    }
}
