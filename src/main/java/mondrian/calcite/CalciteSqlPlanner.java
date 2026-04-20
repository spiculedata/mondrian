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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a {@link PlannerRequest} into a dialect-specific SQL string by
 * building a Calcite {@link RelNode} via {@link RelBuilder} and unparsing it
 * through {@link RelToSqlConverter}.
 *
 * <p>Worktree #1 feature coverage: scan, inner equi-join, equality WHERE
 * filter, SUM/COUNT/MIN/MAX/AVG aggregation, GROUP BY, ORDER BY. Nested
 * subqueries, complex predicates, DISTINCT, window functions are deferred.
 */
public final class CalciteSqlPlanner {

    private final CalciteMondrianSchema schema;
    private final SqlDialect dialect;

    public CalciteSqlPlanner(
        CalciteMondrianSchema schema, SqlDialect dialect)
    {
        if (schema == null) {
            throw new IllegalArgumentException("schema is null");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("dialect is null");
        }
        this.schema = schema;
        this.dialect = dialect;
    }

    /** Render the request as a SQL string in the configured dialect. */
    public String plan(PlannerRequest req) {
        RelNode rel = planRel(req);
        SqlNode sqlNode =
            new RelToSqlConverter(dialect).visitRoot(rel).asStatement();
        return sqlNode.toSqlString(dialect).getSql();
    }

    /** Build the Calcite {@link RelNode} (used by plan-snapshot tests). */
    public RelNode planRel(PlannerRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("request is null");
        }
        FrameworkConfig cfg = Frameworks.newConfigBuilder()
            .defaultSchema(schema.schema())
            .build();
        RelBuilder b = RelBuilder.create(cfg);
        return build(b, req);
    }

    private RelNode build(RelBuilder b, PlannerRequest req) {
        b.scan(req.factTable);

        for (PlannerRequest.Join j : req.joins) {
            b.scan(j.dimTable);
            if (j.kind == PlannerRequest.JoinKind.CROSS) {
                // Unconditional cross-join: RelBuilder has no native
                // cartesian helper, so emit an INNER join on TRUE. Calcite's
                // unparser renders that as a CROSS JOIN in most dialects.
                b.join(JoinRelType.INNER, b.literal(true));
            } else {
                // For single-hop joins (leftTable == null), the LHS is the
                // fact table; unqualified field(2,0,name) resolves the FK
                // column in the flat LHS row-type.
                //
                // For snowflake multi-hop joins (leftTable != null), the
                // LHS is an already-joined chain of tables. Use the
                // alias-qualified field() overload so the FK column is
                // unambiguously resolved against the LHS's named input —
                // critical when the same column name (e.g. product_class_id)
                // appears on more than one table in the chain.
                org.apache.calcite.rex.RexNode lhs;
                if (j.leftTable == null) {
                    lhs = b.field(2, 0, j.factKey);
                } else {
                    lhs = b.field(2, j.leftTable, j.factKey);
                }
                org.apache.calcite.rex.RexNode rhs =
                    b.field(2, j.dimTable, j.dimKey);
                b.join(JoinRelType.INNER, b.equals(lhs, rhs));
            }
        }

        if (req.universalFalse) {
            b.filter(b.literal(false));
        } else {
            for (PlannerRequest.Filter f : req.filters) {
                b.filter(filterRex(b, f));
            }
            for (PlannerRequest.TupleFilter tf : req.tupleFilters) {
                b.filter(tupleFilterRex(b, tf));
            }
        }

        if (req.isAggregation()) {
            List<RexNode> keys = new ArrayList<>();
            for (PlannerRequest.Column c : req.groupBy) {
                keys.add(fieldRef(b, c));
            }
            List<RelBuilder.AggCall> aggs = new ArrayList<>();
            for (PlannerRequest.Measure m : req.measures) {
                aggs.add(aggCall(b, m));
            }
            // HAVING predicates ride along in the aggregate so their
            // measure alias is resolvable by the subsequent filter()
            // call. Distinct aliases are guaranteed by the builder —
            // user measures already have stable aliases, and HAVING
            // translation in CalcitePlannerAdapters emits h0..hN.
            // Measures that pre-exist the aggregate by matching
            // fn+column+distinct+alias are NOT deduped; the
            // post-aggregate reproject drops the HAVING-only ones
            // by name, preserving the user's SELECT layout.
            for (PlannerRequest.Having h : req.havings) {
                aggs.add(aggCall(b, h.measure));
            }
            b.aggregate(b.groupKey(keys), aggs);

            // HAVING: filter on the aggregate's output columns. Calcite
            // recognises a Filter immediately above an Aggregate whose
            // predicate references aggregate-output columns and unparses
            // it as a HAVING clause.
            for (PlannerRequest.Having h : req.havings) {
                b.filter(havingRex(b, h));
            }

            // Calcite's Aggregate normalises the group set to an
            // ImmutableBitSet, which re-orders group columns into the
            // input-row's column-ordinal order. That means the emitted
            // SELECT list's group-by prefix may not match the order we
            // passed to groupKey(). Mondrian's segment consumer positionally
            // maps SELECT columns onto GroupingSet.columns[i], so a
            // reordered SELECT assigns axis values to the wrong column and
            // cell lookups miss. Re-project to force the intended order:
            // groupBy columns in request order, followed by measures in
            // request order. HAVING-only measures (h0..hN) are dropped
            // from the final SELECT so they don't leak out.
            List<RexNode> restored = new ArrayList<>(
                req.groupBy.size() + req.measures.size());
            List<String> restoredAliases = new ArrayList<>(
                req.groupBy.size() + req.measures.size());
            for (PlannerRequest.Column c : req.groupBy) {
                restored.add(b.field(c.name));
                restoredAliases.add(c.name);
            }
            for (PlannerRequest.Measure m : req.measures) {
                restored.add(b.field(m.alias));
                restoredAliases.add(m.alias);
            }
            b.project(restored, restoredAliases, true);
        } else {
            List<RexNode> projs = new ArrayList<>();
            for (PlannerRequest.Column c : req.projections) {
                projs.add(b.field(c.name));
            }
            b.project(projs);
            if (req.distinct) {
                b.distinct();
            }
        }

        if (!req.orderBy.isEmpty()) {
            List<RexNode> exprs = new ArrayList<>();
            for (PlannerRequest.OrderBy o : req.orderBy) {
                RexNode ref = b.field(o.column.name);
                exprs.add(
                    o.direction == PlannerRequest.Order.DESC
                        ? b.desc(ref)
                        : ref);
            }
            b.sort(exprs);
        }

        return b.build();
    }

    /** Table-qualified field ref when {@link PlannerRequest.Column#table}
     *  is non-null; unqualified otherwise. Qualified lookup is required
     *  when the same column name appears on more than one scan in the
     *  input (classic example: {@code product_id} on both
     *  {@code sales_fact_1997} and {@code product}). */
    private static RexNode fieldRef(
        RelBuilder b, PlannerRequest.Column c)
    {
        if (c.table == null) {
            return b.field(c.name);
        }
        return b.field(c.table, c.name);
    }

    private static RexNode filterRex(
        RelBuilder b, PlannerRequest.Filter f)
    {
        RexNode col = fieldRef(b, f.column);
        if (f.literals.size() == 1) {
            return b.equals(col, b.literal(f.literals.get(0)));
        }
        // Multi-literal → OR-chain of equalities (friendlier to dialects
        // than IN; avoids Calcite's SEARCH/SARG unparse surprises).
        List<RexNode> ors = new ArrayList<>(f.literals.size());
        for (Object lit : f.literals) {
            ors.add(b.equals(col, b.literal(lit)));
        }
        return b.or(ors);
    }

    private static RexNode tupleFilterRex(
        RelBuilder b, PlannerRequest.TupleFilter tf)
    {
        // Single-column tuple filter collapses to an OR-chain of equalities
        // (identical in shape to a multi-literal Filter) so single-column
        // OR reuses the same IN-list-like rendering.
        if (tf.columns.size() == 1) {
            RexNode col = b.field(tf.columns.get(0).name);
            List<RexNode> ors = new ArrayList<>(tf.rows.size());
            for (List<Object> row : tf.rows) {
                ors.add(b.equals(col, b.literal(row.get(0))));
            }
            return ors.size() == 1 ? ors.get(0) : b.or(ors);
        }
        // Multi-column: OR of ANDs.
        List<RexNode> ors = new ArrayList<>(tf.rows.size());
        for (List<Object> row : tf.rows) {
            List<RexNode> ands = new ArrayList<>(tf.columns.size());
            for (int i = 0; i < tf.columns.size(); i++) {
                RexNode col = b.field(tf.columns.get(i).name);
                ands.add(b.equals(col, b.literal(row.get(i))));
            }
            ors.add(ands.size() == 1 ? ands.get(0) : b.and(ands));
        }
        return ors.size() == 1 ? ors.get(0) : b.or(ors);
    }

    private static RexNode havingRex(
        RelBuilder b, PlannerRequest.Having h)
    {
        RexNode col = b.field(h.measure.alias);
        RexNode lit = b.literal(h.literal);
        switch (h.op) {
        case GT: return b.greaterThan(col, lit);
        case LT: return b.lessThan(col, lit);
        case GE: return b.greaterThanOrEqual(col, lit);
        case LE: return b.lessThanOrEqual(col, lit);
        case EQ: return b.equals(col, lit);
        case NE: return b.not(b.equals(col, lit));
        default:
            throw new IllegalStateException(
                "unhandled Having op: " + h.op);
        }
    }

    private static RelBuilder.AggCall aggCall(
        RelBuilder b, PlannerRequest.Measure m)
    {
        RexNode ref = b.field(m.column.name);
        switch (m.fn) {
        case SUM:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got SUM)");
            }
            return b.sum(ref).as(m.alias);
        case COUNT:
            return b.count(m.distinct, m.alias, ref);
        case MIN:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got MIN)");
            }
            return b.min(ref).as(m.alias);
        case MAX:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got MAX)");
            }
            return b.max(ref).as(m.alias);
        case AVG:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got AVG)");
            }
            return b.avg(ref).as(m.alias);
        default:
            throw new IllegalStateException("unhandled AggFn: " + m.fn);
        }
    }
}

// End CalciteSqlPlanner.java
