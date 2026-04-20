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

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test-only: re-emits SQL via Calcite like {@link CalcitePassThrough}, but
 * first walks the parsed tree and rewrites {@code =} comparisons inside
 * predicate contexts (WHERE, HAVING, JOIN ON) to {@code &lt;&gt;}. Used by
 * {@code HarnessMutationTest} to prove the equivalence harness catches real
 * semantic drift — not just tautological identity round-trips.
 *
 * <p>Scope is deliberately narrow on three axes:
 * <ul>
 *   <li><b>Clause:</b> only WHERE / HAVING predicate sub-trees are walked.
 *       SELECT list, GROUP BY, ORDER BY, alias clauses, and JOIN ON
 *       conditions are left untouched.</li>
 *   <li><b>Operand shape:</b> only {@code col = literal} (or
 *       {@code literal = col}) predicates are flipped. Column-to-column
 *       comparisons are preserved so structural predicates like
 *       {@code t1."key" = t2."key"} pushed into WHERE are not mutated.</li>
 *   <li><b>Target column:</b> only predicates on the FoodMart time_by_day
 *       {@code the_year} column are rewritten. This column is present in
 *       19 of the 20 smoke-corpus queries (all the ones with time
 *       filters) but is never touched during Mondrian's schema-loading
 *       member lookups, which use store / product keys. Targeting it
 *       specifically forces broad cell-set drift while keeping cube
 *       construction and member resolution intact.</li>
 * </ul>
 *
 * <p>Fail-open: if parsing or re-emission throws for any reason the original
 * SQL is returned unchanged, matching {@link CalcitePassThrough}'s contract.
 * A mutation that fails to apply on one query simply means that query won't
 * drift; the mutation witness is corpus-wide, not per-query.
 */
public class MutatingCalcitePassThrough implements SqlInterceptor {

    private static final Logger LOG =
        Logger.getLogger(MutatingCalcitePassThrough.class.getName());

    @Override
    public String onSqlEmitted(String sql, Dialect dialect) {
        try {
            SqlParser.Config cfg = SqlParser.config()
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(true)
                .withConformance(SqlConformanceEnum.LENIENT);
            SqlNode node = SqlParser.create(sql, cfg).parseStmt();
            mutatePredicates(node);
            SqlDialect.Context ctx = HsqldbSqlDialect.DEFAULT_CONTEXT
                .withIdentifierQuoteString("\"");
            SqlDialect out = new HsqldbSqlDialect(ctx);
            return node.toSqlString(c ->
                c.withDialect(out).withQuoteAllIdentifiers(true))
                .getSql();
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                "MutatingCalcitePassThrough failed; returning original SQL."
                + " sql=" + preview(sql)
                + " err=" + e.getClass().getSimpleName()
                + ": " + e.getMessage());
            return sql;
        }
    }

    /**
     * Walk the parsed statement, descending into sub-queries, and rewrite
     * only the predicate-bearing clauses (WHERE, HAVING, JOIN ON) with the
     * {@link EqualsToNotEquals} shuttle.
     */
    private static void mutatePredicates(SqlNode node) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlOrderBy) {
            mutatePredicates(((SqlOrderBy) node).query);
            return;
        }
        if (node instanceof SqlWith) {
            SqlWith with = (SqlWith) node;
            for (SqlNode item : with.withList) {
                if (item instanceof SqlWithItem) {
                    mutatePredicates(((SqlWithItem) item).query);
                }
            }
            mutatePredicates(with.body);
            return;
        }
        if (node instanceof SqlSelect) {
            SqlSelect sel = (SqlSelect) node;
            // Recurse into FROM clause for nested sub-queries (but not join
            // conditions — see class javadoc for why those are preserved).
            mutateFrom(sel.getFrom());
            SqlNode where = sel.getWhere();
            if (where != null) {
                SqlNode rewritten = where.accept(new EqualsToNotEquals());
                if (rewritten != where) {
                    sel.setWhere(rewritten);
                }
            }
            SqlNode having = sel.getHaving();
            if (having != null) {
                SqlNode rewritten = having.accept(new EqualsToNotEquals());
                if (rewritten != having) {
                    sel.setHaving(rewritten);
                }
            }
        }
    }

    private static void mutateFrom(SqlNode from) {
        if (from == null) {
            return;
        }
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) from;
            mutateFrom(join.getLeft());
            mutateFrom(join.getRight());
            // Deliberately skip join.getCondition() — see class javadoc.
        } else if (from instanceof SqlCall) {
            // Sub-query wrapped as AS(...), or parenthesised SqlSelect, etc.
            mutatePredicates(from);
            for (SqlNode op : ((SqlCall) from).getOperandList()) {
                mutatePredicates(op);
            }
        }
    }

    /** FoodMart-specific column chosen because (a) it appears in 19/20
     *  smoke queries' WHERE predicates, (b) it never appears in the
     *  schema-loading member-lookup SQL, so mutating it cannot break cube
     *  construction. */
    private static final String TARGET_COLUMN = "the_year";

    /**
     * Rewrites {@code "the_year" = literal} (and {@code literal = "the_year"})
     * to {@code "the_year" <> literal}. Every other equality survives.
     */
    private static final class EqualsToNotEquals extends SqlShuttle {
        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlBasicCall
                && call.getOperator() == SqlStdOperatorTable.EQUALS
                && isTargetColLiteralPair(call.operand(0), call.operand(1)))
            {
                SqlNode left = call.operand(0).accept(this);
                SqlNode right = call.operand(1).accept(this);
                return SqlStdOperatorTable.NOT_EQUALS.createCall(
                    call.getParserPosition(), left, right);
            }
            return super.visit(call);
        }

        private static boolean isTargetColLiteralPair(SqlNode a, SqlNode b) {
            return (isTargetCol(a) && b instanceof SqlLiteral)
                || (isTargetCol(b) && a instanceof SqlLiteral);
        }

        private static boolean isTargetCol(SqlNode n) {
            if (!(n instanceof SqlIdentifier)) {
                return false;
            }
            SqlIdentifier id = (SqlIdentifier) n;
            // Match the last segment — handles both bare "the_year"
            // and qualified "time_by_day"."the_year".
            return !id.names.isEmpty()
                && TARGET_COLUMN.equals(id.names.get(id.names.size() - 1));
        }
    }

    private static String preview(String sql) {
        if (sql == null) {
            return "<null>";
        }
        return sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
    }
}

// End MutatingCalcitePassThrough.java
