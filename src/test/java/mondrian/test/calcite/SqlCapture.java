package mondrian.test.calcite;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Test-scope {@link DataSource} wrapper that records every
 * {@link PreparedStatement#executeQuery()} as a {@link CapturedExecution}
 * (sql + materialized rowset + sha256 checksum).
 *
 * <p>Uses JDK dynamic proxies for {@link Connection} and
 * {@link PreparedStatement}. Plain {@link java.sql.Statement} is not wrapped —
 * Mondrian's RolapUtil executes via {@code PreparedStatement} almost
 * exclusively. If a caller uses {@code Statement}, nothing will be captured
 * (execution still works via the underlying delegate).
 */
public final class SqlCapture implements DataSource {

    private final DataSource delegate;
    private final CopyOnWriteArrayList<CapturedExecution> executions =
        new CopyOnWriteArrayList<>();
    private final AtomicInteger seq = new AtomicInteger();

    public SqlCapture(DataSource delegate) {
        this.delegate = delegate;
    }

    /** Snapshot and clear captured executions. */
    public synchronized List<CapturedExecution> drain() {
        List<CapturedExecution> snapshot = new ArrayList<>(executions);
        executions.clear();
        seq.set(0);
        return snapshot;
    }

    // ---------- DataSource ----------

    @Override
    public Connection getConnection() throws SQLException {
        return wrapConnection(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String user, String password)
        throws SQLException
    {
        return wrapConnection(delegate.getConnection(user, password));
    }

    @Override public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }
    @Override public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }
    @Override public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }
    @Override public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }
    @Override public java.util.logging.Logger getParentLogger() {
        return Logger.getLogger("global");
    }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        if (iface.isInstance(delegate)) return iface.cast(delegate);
        return delegate.unwrap(iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || iface.isInstance(delegate)
            || delegate.isWrapperFor(iface);
    }

    // ---------- Proxy wiring ----------

    private Connection wrapConnection(Connection c) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            new ConnectionHandler(c));
    }

    private final class ConnectionHandler implements InvocationHandler {
        private final Connection target;
        ConnectionHandler(Connection target) { this.target = target; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
        {
            String name = method.getName();
            Object result = method.invoke(target, args);
            if ("prepareStatement".equals(name) && result instanceof PreparedStatement) {
                String sql = (String) args[0];
                return wrapPreparedStatement((PreparedStatement) result, sql);
            }
            return result;
        }
    }

    private PreparedStatement wrapPreparedStatement(PreparedStatement ps, String sql) {
        return (PreparedStatement) Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(),
            new Class<?>[]{PreparedStatement.class},
            new PreparedStatementHandler(ps, sql));
    }

    private final class PreparedStatementHandler implements InvocationHandler {
        private final PreparedStatement target;
        private final String sql;

        PreparedStatementHandler(PreparedStatement target, String sql) {
            this.target = target;
            this.sql = sql;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
        {
            String name = method.getName();
            if ("executeQuery".equals(name)
                && (args == null || args.length == 0))
            {
                ResultSet rs = (ResultSet) method.invoke(target, args);
                try {
                    return captureAndReplay(sql, rs);
                } finally {
                    try { rs.close(); } catch (SQLException ignored) { }
                }
            }
            return method.invoke(target, args);
        }
    }

    // ---------- Capture + replay ----------

    private ResultSet captureAndReplay(String sql, ResultSet rs)
        throws SQLException
    {
        ResultSetMetaData md = rs.getMetaData();
        int colCount = md.getColumnCount();
        List<String> colNames = new ArrayList<>(colCount);
        for (int i = 1; i <= colCount; i++) {
            // Prefer label (AS alias) so aliases like "X"/"Y" survive.
            String label = md.getColumnLabel(i);
            if (label == null || label.isEmpty()) {
                label = md.getColumnName(i);
            }
            colNames.add(label);
        }
        List<List<Object>> rowset = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(colCount);
            for (int i = 1; i <= colCount; i++) {
                row.add(rs.getObject(i));
            }
            rowset.add(row);
        }
        String checksum = checksum(colCount, rowset);
        int mySeq = seq.getAndIncrement();
        executions.add(new CapturedExecution(
            mySeq, sql, rowset, rowset.size(), checksum));
        return new ReplayingResultSet(colNames, rowset);
    }

    private static String checksum(int colCount, List<List<Object>> rowset) {
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toString(colCount));
        boolean firstRow = true;
        for (List<Object> row : rowset) {
            sb.append('\u001E');
            boolean firstCell = true;
            for (Object cell : row) {
                if (!firstCell) sb.append('\u001F');
                sb.append(String.valueOf(cell));
                firstCell = false;
            }
            firstRow = false;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder("sha256:");
            for (byte b : digest) {
                hex.append(String.format("%02x", b & 0xff));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }

    /**
     * Minimal in-memory {@link ResultSet} replay. Only the methods listed in
     * the Task 3 guardrails are supported; everything else throws
     * {@link SQLFeatureNotSupportedException}.
     */
    private static final class ReplayingResultSet implements ResultSet {
        private final List<String> colNames;
        private final List<List<Object>> rows;
        private int cursor = -1;
        private boolean closed = false;
        private boolean lastWasNull = false;

        ReplayingResultSet(List<String> colNames, List<List<Object>> rows) {
            this.colNames = colNames;
            this.rows = rows;
        }

        private void check() throws SQLException {
            if (closed) throw new SQLException("ResultSet closed");
            if (cursor < 0 || cursor >= rows.size()) {
                throw new SQLException("No current row");
            }
        }

        private int colIndexByName(String label) throws SQLException {
            for (int i = 0; i < colNames.size(); i++) {
                if (colNames.get(i).equalsIgnoreCase(label)) return i + 1;
            }
            throw new SQLException("Unknown column: " + label);
        }

        @Override public boolean next() {
            cursor++;
            return cursor < rows.size();
        }
        @Override public void close() { closed = true; }
        @Override public boolean wasNull() { return lastWasNull; }

        @Override public Object getObject(int idx) throws SQLException {
            check();
            Object v = rows.get(cursor).get(idx - 1);
            lastWasNull = (v == null);
            return v;
        }
        @Override public Object getObject(String label) throws SQLException {
            return getObject(colIndexByName(label));
        }
        @Override public String getString(int idx) throws SQLException {
            Object v = getObject(idx);
            return v == null ? null : String.valueOf(v);
        }
        @Override public String getString(String label) throws SQLException {
            return getString(colIndexByName(label));
        }
        @Override public int getInt(int idx) throws SQLException {
            Object v = getObject(idx);
            if (v == null) return 0;
            if (v instanceof Number) return ((Number) v).intValue();
            return Integer.parseInt(v.toString());
        }
        @Override public int getInt(String label) throws SQLException {
            return getInt(colIndexByName(label));
        }
        @Override public long getLong(int idx) throws SQLException {
            Object v = getObject(idx);
            if (v == null) return 0L;
            if (v instanceof Number) return ((Number) v).longValue();
            return Long.parseLong(v.toString());
        }
        @Override public long getLong(String label) throws SQLException {
            return getLong(colIndexByName(label));
        }

        @Override public ResultSetMetaData getMetaData() {
            return new ReplayMetaData(colNames);
        }

        // ---------- Unsupported below ----------
        private static SQLFeatureNotSupportedException nope(String what) {
            return new SQLFeatureNotSupportedException(
                "ReplayingResultSet does not support " + what);
        }
        @Override public boolean getBoolean(int i) throws SQLException { throw nope("getBoolean"); }
        @Override public boolean getBoolean(String s) throws SQLException { throw nope("getBoolean"); }
        @Override public byte getByte(int i) throws SQLException { throw nope("getByte"); }
        @Override public byte getByte(String s) throws SQLException { throw nope("getByte"); }
        @Override public short getShort(int i) throws SQLException { throw nope("getShort"); }
        @Override public short getShort(String s) throws SQLException { throw nope("getShort"); }
        @Override public float getFloat(int i) throws SQLException { throw nope("getFloat"); }
        @Override public float getFloat(String s) throws SQLException { throw nope("getFloat"); }
        @Override public double getDouble(int i) throws SQLException { throw nope("getDouble"); }
        @Override public double getDouble(String s) throws SQLException { throw nope("getDouble"); }
        @Override public java.math.BigDecimal getBigDecimal(int i, int s) throws SQLException { throw nope("getBigDecimal"); }
        @Override public java.math.BigDecimal getBigDecimal(String s, int x) throws SQLException { throw nope("getBigDecimal"); }
        @Override public java.math.BigDecimal getBigDecimal(int i) throws SQLException { throw nope("getBigDecimal"); }
        @Override public java.math.BigDecimal getBigDecimal(String s) throws SQLException { throw nope("getBigDecimal"); }
        @Override public byte[] getBytes(int i) throws SQLException { throw nope("getBytes"); }
        @Override public byte[] getBytes(String s) throws SQLException { throw nope("getBytes"); }
        @Override public java.sql.Date getDate(int i) throws SQLException { throw nope("getDate"); }
        @Override public java.sql.Date getDate(String s) throws SQLException { throw nope("getDate"); }
        @Override public java.sql.Date getDate(int i, java.util.Calendar c) throws SQLException { throw nope("getDate"); }
        @Override public java.sql.Date getDate(String s, java.util.Calendar c) throws SQLException { throw nope("getDate"); }
        @Override public java.sql.Time getTime(int i) throws SQLException { throw nope("getTime"); }
        @Override public java.sql.Time getTime(String s) throws SQLException { throw nope("getTime"); }
        @Override public java.sql.Time getTime(int i, java.util.Calendar c) throws SQLException { throw nope("getTime"); }
        @Override public java.sql.Time getTime(String s, java.util.Calendar c) throws SQLException { throw nope("getTime"); }
        @Override public java.sql.Timestamp getTimestamp(int i) throws SQLException { throw nope("getTimestamp"); }
        @Override public java.sql.Timestamp getTimestamp(String s) throws SQLException { throw nope("getTimestamp"); }
        @Override public java.sql.Timestamp getTimestamp(int i, java.util.Calendar c) throws SQLException { throw nope("getTimestamp"); }
        @Override public java.sql.Timestamp getTimestamp(String s, java.util.Calendar c) throws SQLException { throw nope("getTimestamp"); }
        @Override public java.io.InputStream getAsciiStream(int i) throws SQLException { throw nope("getAsciiStream"); }
        @Override public java.io.InputStream getAsciiStream(String s) throws SQLException { throw nope("getAsciiStream"); }
        @Override public java.io.InputStream getUnicodeStream(int i) throws SQLException { throw nope("getUnicodeStream"); }
        @Override public java.io.InputStream getUnicodeStream(String s) throws SQLException { throw nope("getUnicodeStream"); }
        @Override public java.io.InputStream getBinaryStream(int i) throws SQLException { throw nope("getBinaryStream"); }
        @Override public java.io.InputStream getBinaryStream(String s) throws SQLException { throw nope("getBinaryStream"); }
        @Override public java.io.Reader getCharacterStream(int i) throws SQLException { throw nope("getCharacterStream"); }
        @Override public java.io.Reader getCharacterStream(String s) throws SQLException { throw nope("getCharacterStream"); }
        @Override public java.sql.SQLWarning getWarnings() throws SQLException { return null; }
        @Override public void clearWarnings() { }
        @Override public String getCursorName() throws SQLException { throw nope("getCursorName"); }
        @Override public int findColumn(String s) throws SQLException { return colIndexByName(s); }
        @Override public boolean isBeforeFirst() { return cursor < 0; }
        @Override public boolean isAfterLast() { return cursor >= rows.size(); }
        @Override public boolean isFirst() { return cursor == 0; }
        @Override public boolean isLast() { return cursor == rows.size() - 1; }
        @Override public void beforeFirst() { cursor = -1; }
        @Override public void afterLast() { cursor = rows.size(); }
        @Override public boolean first() { cursor = 0; return !rows.isEmpty(); }
        @Override public boolean last() { cursor = rows.size() - 1; return !rows.isEmpty(); }
        @Override public int getRow() { return cursor < 0 || cursor >= rows.size() ? 0 : cursor + 1; }
        @Override public boolean absolute(int row) throws SQLException { throw nope("absolute"); }
        @Override public boolean relative(int rows) throws SQLException { throw nope("relative"); }
        @Override public boolean previous() throws SQLException { throw nope("previous"); }
        @Override public void setFetchDirection(int d) { }
        @Override public int getFetchDirection() { return FETCH_FORWARD; }
        @Override public void setFetchSize(int r) { }
        @Override public int getFetchSize() { return 0; }
        @Override public int getType() { return TYPE_FORWARD_ONLY; }
        @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
        @Override public boolean rowUpdated() { return false; }
        @Override public boolean rowInserted() { return false; }
        @Override public boolean rowDeleted() { return false; }
        @Override public void updateNull(int i) throws SQLException { throw nope("update"); }
        @Override public void updateBoolean(int i, boolean x) throws SQLException { throw nope("update"); }
        @Override public void updateByte(int i, byte x) throws SQLException { throw nope("update"); }
        @Override public void updateShort(int i, short x) throws SQLException { throw nope("update"); }
        @Override public void updateInt(int i, int x) throws SQLException { throw nope("update"); }
        @Override public void updateLong(int i, long x) throws SQLException { throw nope("update"); }
        @Override public void updateFloat(int i, float x) throws SQLException { throw nope("update"); }
        @Override public void updateDouble(int i, double x) throws SQLException { throw nope("update"); }
        @Override public void updateBigDecimal(int i, java.math.BigDecimal x) throws SQLException { throw nope("update"); }
        @Override public void updateString(int i, String x) throws SQLException { throw nope("update"); }
        @Override public void updateBytes(int i, byte[] x) throws SQLException { throw nope("update"); }
        @Override public void updateDate(int i, java.sql.Date x) throws SQLException { throw nope("update"); }
        @Override public void updateTime(int i, java.sql.Time x) throws SQLException { throw nope("update"); }
        @Override public void updateTimestamp(int i, java.sql.Timestamp x) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(int i, java.io.InputStream x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(int i, java.io.InputStream x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(int i, java.io.Reader x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateObject(int i, Object x, int s) throws SQLException { throw nope("update"); }
        @Override public void updateObject(int i, Object x) throws SQLException { throw nope("update"); }
        @Override public void updateNull(String s) throws SQLException { throw nope("update"); }
        @Override public void updateBoolean(String s, boolean x) throws SQLException { throw nope("update"); }
        @Override public void updateByte(String s, byte x) throws SQLException { throw nope("update"); }
        @Override public void updateShort(String s, short x) throws SQLException { throw nope("update"); }
        @Override public void updateInt(String s, int x) throws SQLException { throw nope("update"); }
        @Override public void updateLong(String s, long x) throws SQLException { throw nope("update"); }
        @Override public void updateFloat(String s, float x) throws SQLException { throw nope("update"); }
        @Override public void updateDouble(String s, double x) throws SQLException { throw nope("update"); }
        @Override public void updateBigDecimal(String s, java.math.BigDecimal x) throws SQLException { throw nope("update"); }
        @Override public void updateString(String s, String x) throws SQLException { throw nope("update"); }
        @Override public void updateBytes(String s, byte[] x) throws SQLException { throw nope("update"); }
        @Override public void updateDate(String s, java.sql.Date x) throws SQLException { throw nope("update"); }
        @Override public void updateTime(String s, java.sql.Time x) throws SQLException { throw nope("update"); }
        @Override public void updateTimestamp(String s, java.sql.Timestamp x) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(String s, java.io.InputStream x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(String s, java.io.InputStream x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(String s, java.io.Reader x, int l) throws SQLException { throw nope("update"); }
        @Override public void updateObject(String s, Object x, int sc) throws SQLException { throw nope("update"); }
        @Override public void updateObject(String s, Object x) throws SQLException { throw nope("update"); }
        @Override public void insertRow() throws SQLException { throw nope("insertRow"); }
        @Override public void updateRow() throws SQLException { throw nope("updateRow"); }
        @Override public void deleteRow() throws SQLException { throw nope("deleteRow"); }
        @Override public void refreshRow() throws SQLException { throw nope("refreshRow"); }
        @Override public void cancelRowUpdates() throws SQLException { throw nope("cancelRowUpdates"); }
        @Override public void moveToInsertRow() throws SQLException { throw nope("moveToInsertRow"); }
        @Override public void moveToCurrentRow() throws SQLException { throw nope("moveToCurrentRow"); }
        @Override public java.sql.Statement getStatement() { return null; }
        @Override public Object getObject(int i, java.util.Map<String, Class<?>> m) throws SQLException { throw nope("getObject(map)"); }
        @Override public java.sql.Ref getRef(int i) throws SQLException { throw nope("getRef"); }
        @Override public java.sql.Blob getBlob(int i) throws SQLException { throw nope("getBlob"); }
        @Override public java.sql.Clob getClob(int i) throws SQLException { throw nope("getClob"); }
        @Override public java.sql.Array getArray(int i) throws SQLException { throw nope("getArray"); }
        @Override public Object getObject(String s, java.util.Map<String, Class<?>> m) throws SQLException { throw nope("getObject(map)"); }
        @Override public java.sql.Ref getRef(String s) throws SQLException { throw nope("getRef"); }
        @Override public java.sql.Blob getBlob(String s) throws SQLException { throw nope("getBlob"); }
        @Override public java.sql.Clob getClob(String s) throws SQLException { throw nope("getClob"); }
        @Override public java.sql.Array getArray(String s) throws SQLException { throw nope("getArray"); }
        @Override public java.net.URL getURL(int i) throws SQLException { throw nope("getURL"); }
        @Override public java.net.URL getURL(String s) throws SQLException { throw nope("getURL"); }
        @Override public void updateRef(int i, java.sql.Ref x) throws SQLException { throw nope("update"); }
        @Override public void updateRef(String s, java.sql.Ref x) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(int i, java.sql.Blob x) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(String s, java.sql.Blob x) throws SQLException { throw nope("update"); }
        @Override public void updateClob(int i, java.sql.Clob x) throws SQLException { throw nope("update"); }
        @Override public void updateClob(String s, java.sql.Clob x) throws SQLException { throw nope("update"); }
        @Override public void updateArray(int i, java.sql.Array x) throws SQLException { throw nope("update"); }
        @Override public void updateArray(String s, java.sql.Array x) throws SQLException { throw nope("update"); }
        @Override public java.sql.RowId getRowId(int i) throws SQLException { throw nope("getRowId"); }
        @Override public java.sql.RowId getRowId(String s) throws SQLException { throw nope("getRowId"); }
        @Override public void updateRowId(int i, java.sql.RowId x) throws SQLException { throw nope("update"); }
        @Override public void updateRowId(String s, java.sql.RowId x) throws SQLException { throw nope("update"); }
        @Override public int getHoldability() { return HOLD_CURSORS_OVER_COMMIT; }
        @Override public boolean isClosed() { return closed; }
        @Override public void updateNString(int i, String x) throws SQLException { throw nope("update"); }
        @Override public void updateNString(String s, String x) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(int i, java.sql.NClob x) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(String s, java.sql.NClob x) throws SQLException { throw nope("update"); }
        @Override public java.sql.NClob getNClob(int i) throws SQLException { throw nope("getNClob"); }
        @Override public java.sql.NClob getNClob(String s) throws SQLException { throw nope("getNClob"); }
        @Override public java.sql.SQLXML getSQLXML(int i) throws SQLException { throw nope("getSQLXML"); }
        @Override public java.sql.SQLXML getSQLXML(String s) throws SQLException { throw nope("getSQLXML"); }
        @Override public void updateSQLXML(int i, java.sql.SQLXML x) throws SQLException { throw nope("update"); }
        @Override public void updateSQLXML(String s, java.sql.SQLXML x) throws SQLException { throw nope("update"); }
        @Override public String getNString(int i) throws SQLException { throw nope("getNString"); }
        @Override public String getNString(String s) throws SQLException { throw nope("getNString"); }
        @Override public java.io.Reader getNCharacterStream(int i) throws SQLException { throw nope("getNCharacterStream"); }
        @Override public java.io.Reader getNCharacterStream(String s) throws SQLException { throw nope("getNCharacterStream"); }
        @Override public void updateNCharacterStream(int i, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateNCharacterStream(String s, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(int i, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(int i, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(int i, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(String s, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(String s, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(String s, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(int i, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(String s, java.io.InputStream x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateClob(int i, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateClob(String s, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(int i, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(String s, java.io.Reader x, long l) throws SQLException { throw nope("update"); }
        @Override public void updateNCharacterStream(int i, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateNCharacterStream(String s, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(int i, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(int i, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(int i, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateAsciiStream(String s, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateBinaryStream(String s, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateCharacterStream(String s, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(int i, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateBlob(String s, java.io.InputStream x) throws SQLException { throw nope("update"); }
        @Override public void updateClob(int i, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateClob(String s, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(int i, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public void updateNClob(String s, java.io.Reader x) throws SQLException { throw nope("update"); }
        @Override public <T> T getObject(int i, Class<T> t) throws SQLException {
            Object v = getObject(i);
            return v == null ? null : t.cast(v);
        }
        @Override public <T> T getObject(String s, Class<T> t) throws SQLException {
            return getObject(colIndexByName(s), t);
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("Cannot unwrap to " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
    }

    private static final class ReplayMetaData implements ResultSetMetaData {
        private final List<String> names;
        ReplayMetaData(List<String> names) { this.names = names; }
        private void check(int col) throws SQLException {
            if (col < 1 || col > names.size()) {
                throw new SQLException("Column out of range: " + col);
            }
        }
        @Override public int getColumnCount() { return names.size(); }
        @Override public String getColumnName(int c) throws SQLException { check(c); return names.get(c - 1); }
        @Override public String getColumnLabel(int c) throws SQLException { check(c); return names.get(c - 1); }
        @Override public boolean isAutoIncrement(int c) { return false; }
        @Override public boolean isCaseSensitive(int c) { return true; }
        @Override public boolean isSearchable(int c) { return false; }
        @Override public boolean isCurrency(int c) { return false; }
        @Override public int isNullable(int c) { return columnNullableUnknown; }
        @Override public boolean isSigned(int c) { return false; }
        @Override public int getColumnDisplaySize(int c) { return 0; }
        @Override public String getSchemaName(int c) { return ""; }
        @Override public int getPrecision(int c) { return 0; }
        @Override public int getScale(int c) { return 0; }
        @Override public String getTableName(int c) { return ""; }
        @Override public String getCatalogName(int c) { return ""; }
        @Override public int getColumnType(int c) { return java.sql.Types.OTHER; }
        @Override public String getColumnTypeName(int c) { return "OTHER"; }
        @Override public boolean isReadOnly(int c) { return true; }
        @Override public boolean isWritable(int c) { return false; }
        @Override public boolean isDefinitelyWritable(int c) { return false; }
        @Override public String getColumnClassName(int c) { return Object.class.getName(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("Cannot unwrap");
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
    }
}
