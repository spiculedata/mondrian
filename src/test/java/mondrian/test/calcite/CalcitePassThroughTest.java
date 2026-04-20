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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CalcitePassThrough} — the no-op Calcite interceptor
 * that parses Mondrian-emitted SQL via Calcite's {@code SqlParser} and
 * re-emits it through a matching dialect's {@code SqlPrettyWriter}.
 *
 * <p>These tests pin the two core contracts:
 * <ol>
 *   <li>Valid SQL round-trips and preserves the essential identifiers.</li>
 *   <li>Parse failures are fail-open (original SQL returned, never thrown).</li>
 * </ol>
 */
public class CalcitePassThroughTest {

    @Test
    public void roundTripsSimpleSelect() {
        CalcitePassThrough pt = new CalcitePassThrough();
        String out = pt.onSqlEmitted(
            "select \"product_id\" from \"product\"", null);
        assertTrue("round-tripped SQL should mention product_id: " + out,
            out.toLowerCase().contains("product_id"));
        assertTrue("round-tripped SQL should mention product: " + out,
            out.toLowerCase().contains("product"));
    }

    @Test
    public void failOpensOnParseError() {
        CalcitePassThrough pt = new CalcitePassThrough();
        String garbage = "this is not sql <<<";
        assertEquals(garbage, pt.onSqlEmitted(garbage, null));
    }

    @Test
    public void roundTripsRealMondrianFoodmartSql() throws Exception {
        CalcitePassThrough pt = new CalcitePassThrough();
        // Actual shape from one of our goldens (basic-select.json):
        String in =
            "select \"time_by_day\".\"the_year\" as \"c0\", "
            + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
            + "from \"time_by_day\" \"time_by_day\", "
            + "\"sales_fact_1997\" \"sales_fact_1997\" "
            + "where \"sales_fact_1997\".\"time_id\" "
            + "= \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "group by \"time_by_day\".\"the_year\"";
        String out = pt.onSqlEmitted(in, null);
        // We can't assert string equality — Calcite will reformat.
        // Assert it still parses back cleanly via Calcite (same HSQLDB-ish
        // double-quoted identifier config the interceptor uses):
        SqlParser.create(out,
            SqlParser.config()
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(true))
            .parseStmt();
        // And contains the essentials:
        assertTrue(out.toLowerCase().contains("sales_fact_1997"));
        assertTrue(out.toLowerCase().contains("the_year"));
        assertTrue(out.toLowerCase().contains("sum("));
    }
}

// End CalcitePassThroughTest.java
