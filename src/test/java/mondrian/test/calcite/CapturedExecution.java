package mondrian.test.calcite;

import java.util.List;

/**
 * Plain data record for a single JDBC execution captured by {@link SqlCapture}.
 */
public final class CapturedExecution {
    public final int seq;
    public final String sql;
    public final List<List<Object>> rowset;
    public final int rowCount;
    public final String checksum;

    public CapturedExecution(int seq,
                             String sql,
                             List<List<Object>> rowset,
                             int rowCount,
                             String checksum) {
        this.seq = seq;
        this.sql = sql;
        this.rowset = rowset;
        this.rowCount = rowCount;
        this.checksum = checksum;
    }
}
