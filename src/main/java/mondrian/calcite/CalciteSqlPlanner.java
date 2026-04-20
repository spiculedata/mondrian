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
                b.join(
                    JoinRelType.INNER,
                    b.equals(
                        b.field(2, 0, j.factKey),
                        b.field(2, 1, j.dimKey)));
            }
        }

        if (req.universalFalse) {
            b.filter(b.literal(false));
        } else {
            for (PlannerRequest.Filter f : req.filters) {
                b.filter(filterRex(b, f));
            }
        }

        if (req.isAggregation()) {
            List<RexNode> keys = new ArrayList<>();
            for (PlannerRequest.Column c : req.groupBy) {
                keys.add(b.field(c.name));
            }
            List<RelBuilder.AggCall> aggs = new ArrayList<>();
            for (PlannerRequest.Measure m : req.measures) {
                aggs.add(aggCall(b, m));
            }
            b.aggregate(b.groupKey(keys), aggs);
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

    private static RexNode filterRex(
        RelBuilder b, PlannerRequest.Filter f)
    {
        RexNode col = b.field(f.column.name);
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
