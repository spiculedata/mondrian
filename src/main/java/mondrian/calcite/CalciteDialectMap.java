package mondrian.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;

public final class CalciteDialectMap {
    private CalciteDialectMap() {}

    public static SqlDialect forProductName(String product) {
        if (product == null) throw new IllegalArgumentException("null product name");
        String p = product.toLowerCase(java.util.Locale.ROOT);
        if (p.contains("hsql")) return HsqldbSqlDialect.DEFAULT;
        throw new IllegalArgumentException(
            "No Calcite SqlDialect mapping for JDBC product '" + product + "'. "
            + "Worktree #1 supports HSQLDB only; extend CalciteDialectMap to add more.");
    }
}
