package mondrian.test.calcite;

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SqlInterceptorTest {
    @Test
    public void defaultInterceptorIsIdentity() {
        SqlInterceptor i = SqlInterceptor.IDENTITY;
        assertEquals("select 1", i.onSqlEmitted("select 1", null));
    }

    @Test
    public void systemPropertySelectsInterceptor() throws Exception {
        String prop = "mondrian.sqlInterceptor";
        String prev = System.getProperty(prop);
        try {
            System.setProperty(prop, RecordingInterceptor.class.getName());
            SqlInterceptor i = SqlInterceptor.loadFromSystemProperty();
            assertEquals("RECORDED:select 2", i.onSqlEmitted("select 2", null));
        } finally {
            if (prev == null) System.clearProperty(prop); else System.setProperty(prop, prev);
        }
    }

    public static class RecordingInterceptor implements SqlInterceptor {
        @Override public String onSqlEmitted(String sql, Dialect dialect) { return "RECORDED:" + sql; }
    }
}
