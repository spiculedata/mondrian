package mondrian.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.junit.Test;
import static org.junit.Assert.*;

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
}
