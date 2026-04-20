package mondrian.test.calcite;

import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TDD for {@link SqlCapture}. Uses HSQLDB 1.8 in-memory — the driver is
 * already on the test classpath via the FoodMart fixture.
 */
public class SqlCaptureTest {

    private static DataSource hsqldbMem(String db) throws Exception {
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase("jdbc:hsqldb:mem:" + db);
        ds.setUser("sa");
        ds.setPassword("");
        // HSQLDB 1.8 cannot do SELECT without FROM or VALUES(...);
        // create a tiny one-row table so tests have something to query.
        try (Connection c = ds.getConnection();
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE DUAL1 (n INT)");
            s.execute("INSERT INTO DUAL1 VALUES (1)");
        }
        return ds;
    }

    @Test
    public void capturesSqlAndRowset() throws Exception {
        DataSource underlying = hsqldbMem("sqlcapturetest1");
        SqlCapture capture = new SqlCapture(underlying);
        String sql = "select 1 as x, 'a' as y from DUAL1";
        try (Connection c = capture.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) { /* drain */ }
        }
        List<CapturedExecution> execs = capture.drain();
        assertEquals(1, execs.size());
        CapturedExecution e = execs.get(0);
        assertEquals(sql, e.sql);
        assertEquals(0, e.seq);
        assertEquals(1, e.rowCount);
        assertNotNull(e.checksum);
        assertTrue(e.checksum.startsWith("sha256:"));
        // drain clears
        assertEquals(0, capture.drain().size());
    }

    @Test
    public void capturesMultipleExecutionsInOrder() throws Exception {
        DataSource underlying = hsqldbMem("sqlcapturetest2");
        SqlCapture capture = new SqlCapture(underlying);
        String sqlA = "select 1 as x from DUAL1";
        String sqlB = "select 2 as x, 'b' as y from DUAL1";
        try (Connection c = capture.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement(sqlA);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* drain */ }
            }
            try (PreparedStatement ps = c.prepareStatement(sqlB);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* drain */ }
            }
        }
        List<CapturedExecution> execs = capture.drain();
        assertEquals(2, execs.size());
        assertEquals(0, execs.get(0).seq);
        assertEquals(1, execs.get(1).seq);
        assertEquals(sqlA, execs.get(0).sql);
        assertEquals(sqlB, execs.get(1).sql);
        assertNotEquals(execs.get(0).checksum, execs.get(1).checksum);
    }

    @Test
    public void replayingResultSetExposesValuesAndMetadata() throws Exception {
        DataSource underlying = hsqldbMem("sqlcapturetest3");
        SqlCapture capture = new SqlCapture(underlying);
        try (Connection c = capture.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "select 7 as x, 'hello' as y from DUAL1");
             ResultSet rs = ps.executeQuery()) {
            assertEquals(2, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals("hello", rs.getString(2));
            assertEquals("hello", rs.getString("Y"));
            assertTrue(!rs.next());
        }
        assertEquals(1, capture.drain().size());
    }
}
