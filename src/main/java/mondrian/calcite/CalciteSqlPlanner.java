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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import mondrian.olap.Exp;
import mondrian.olap.Member;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /** Opt-in profiling switch ({@code -Dharness.calcite.profile=true}).
     *  When enabled each phase of {@link #plan} / {@link #planRel} records
     *  elapsed nanos under {@link CalciteProfile}. Off by default — the
     *  only overhead is a single final-boolean read per entry point. */
    private static final boolean PROFILE =
        Boolean.getBoolean("harness.calcite.profile");

    private final CalciteMondrianSchema schema;
    private final SqlDialect dialect;
    /** Stable per-planner salt used by {@link CalciteSqlTemplateCache}
     *  so shape-identical requests rendered through different dialects
     *  cache separately. Derived from dialect identity + schema name. */
    private final long cacheSalt;

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
        // Use identityHashCode over the dialect instance — dialects are
        // typically shared singletons per planner, so equal-identity →
        // same cache namespace. Mix in the schema's identity too so
        // different schemas (different datasources, different names)
        // don't collide.
        long a = System.identityHashCode(dialect);
        long b = System.identityHashCode(schema);
        this.cacheSalt = (a * 0x9E3779B97F4A7C15L) ^ b;
    }

    // ------------------------------------------------------------------
    // Plan-snapshot capture (harness hook).
    //
    // When a thread calls beginCapture() every subsequent plan() on that
    // thread appends RelOptUtil.toString(rel) to a ThreadLocal list. The
    // EquivalenceHarness drains the list after each run to feed the
    // PLAN_DRIFT gate (see docs/plans/2026-04-19-calcite-plan-as-lingua-
    // franca-design.md §Harness evolution). Zero overhead when not
    // capturing: plan() only performs an int != null check.
    // ------------------------------------------------------------------

    private static final ThreadLocal<java.util.List<String>> CAPTURE =
        new ThreadLocal<java.util.List<String>>();

    /** Start accumulating plan snapshots on the current thread. */
    public static void beginCapture() {
        CAPTURE.set(new java.util.ArrayList<String>());
    }

    /**
     * Stop capturing on the current thread and return the snapshots that
     * were recorded, in call order. Returns an empty list if
     * {@link #beginCapture()} was not called.
     */
    public static java.util.List<String> endCapture() {
        java.util.List<String> out = CAPTURE.get();
        CAPTURE.remove();
        if (out == null) {
            return java.util.Collections.emptyList();
        }
        return out;
    }

    /** Render the request as a SQL string in the configured dialect.
     *
     *  <p>Consults {@link CalciteSqlTemplateCache} keyed by the
     *  request's structural hash so repeated same-shape requests skip
     *  RelBuilder + RelToSqlConverter and just render literals into a
     *  pre-split template string. Plan-snapshot capture
     *  ({@link #beginCapture}) disables the cache for the current
     *  thread so the rel tree is always available to callers that
     *  need it. */
    public String plan(PlannerRequest req) {
        if (CAPTURE.get() != null) {
            // Snapshot capture requires the RelNode, so bypass the cache
            // on this thread. planUncached() will call planRel() which
            // appends to the capture sink.
            return planUncached(req);
        }
        return CalciteSqlTemplateCache.plan(
            req, cacheSalt, this::planUncached);
    }

    /** Uncached planning path — the original RelBuilder +
     *  RelToSqlConverter pipeline. Called by the cache on a miss. */
    private String planUncached(PlannerRequest req) {
        long tPlanStart = PROFILE ? System.nanoTime() : 0L;
        RelNode rel = planRel(req);
        java.util.List<String> sink = CAPTURE.get();
        if (sink != null) {
            sink.add(normalisePlan(RelOptUtil.toString(rel)));
        }
        long tUnparseStart = PROFILE ? System.nanoTime() : 0L;
        SqlNode sqlNode =
            new RelToSqlConverter(dialect).visitRoot(rel).asStatement();
        String out = sqlNode.toSqlString(dialect).getSql();
        if (PROFILE) {
            long now = System.nanoTime();
            CalciteProfile.record(
                "CalciteSqlPlanner.unparse", now - tUnparseStart);
            CalciteProfile.record(
                "CalciteSqlPlanner.plan.total", now - tPlanStart);
        }
        return out;
    }

    /**
     * Normalise a {@code RelOptUtil.toString} rendering so goldens are
     * deterministic across JVM runs. Calcite's toString includes a per-rel
     * numeric suffix on the outer rel name (e.g. {@code LogicalProject_5})
     * derived from a cluster-scoped counter; stripped here so a new plan
     * with the same shape produces identical text. Safe because structural
     * changes (different rels, different field lists, different
     * expressions) all alter the rendering elsewhere in the tree.
     */
    private static String normalisePlan(String raw) {
        // Strip "_<digits>" that appears directly after a rel class name —
        // only inside the first token on each line, i.e. between the first
        // run of letters and the opening parenthesis.
        StringBuilder out = new StringBuilder(raw.length());
        int i = 0;
        int n = raw.length();
        while (i < n) {
            char c = raw.charAt(i);
            if (c == '_' && i + 1 < n
                && Character.isDigit(raw.charAt(i + 1)))
            {
                // Look back: preceding char should be a letter (rel-name
                // suffix). Otherwise keep as-is (don't munge literal
                // identifiers like column names).
                int back = out.length() - 1;
                if (back >= 0 && Character.isLetter(out.charAt(back))) {
                    int j = i + 1;
                    while (j < n && Character.isDigit(raw.charAt(j))) {
                        j++;
                    }
                    // Only strip when followed by '(' (the rel's opening
                    // paren) — that's the positive signature of a rel
                    // name, not e.g. an alias token.
                    if (j < n && raw.charAt(j) == '(') {
                        i = j;
                        continue;
                    }
                }
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /** Build the Calcite {@link RelNode} (used by plan-snapshot tests). */
    public RelNode planRel(PlannerRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("request is null");
        }
        long tCfg = PROFILE ? System.nanoTime() : 0L;
        FrameworkConfig cfg = Frameworks.newConfigBuilder()
            .defaultSchema(schema.schema())
            .build();
        RelBuilder b = RelBuilder.create(cfg);
        long tBuild = PROFILE ? System.nanoTime() : 0L;
        RelNode out = build(b, req);
        if (PROFILE) {
            long now = System.nanoTime();
            CalciteProfile.record(
                "CalciteSqlPlanner.relBuilderCreate", tBuild - tCfg);
            CalciteProfile.record(
                "CalciteSqlPlanner.build", now - tBuild);
        }
        return out;
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
            // Append pushed-calc projections as extra columns. They
            // reference the base-measure aliases (also in `restored`)
            // via the ComputedMeasure's baseMeasureAliases map.
            //
            // Task T.1: the calc is rendered alongside {groupBy,
            // measures} in the aggregate's output projection. A second
            // outer project then drops the calc aliases so the result
            // set seen by SegmentLoader stays shape-compatible with
            // the legacy path (row checksum parity in the equivalence
            // harness). We use "force=true" to stop RelBuilder from
            // collapsing the outer project into the inner one — the
            // inner projection's extra columns then survive into the
            // unparsed SQL as observational evidence that pushdown
            // fired, even though the outer select drops them.
            boolean hasComputed = !req.computedMeasures.isEmpty();
            int innerGroupAndMeasureCount = restored.size();
            for (PlannerRequest.ComputedMeasure cm : req.computedMeasures) {
                Map<Member, RexNode> refs = new HashMap<>();
                for (Map.Entry<Object, String> e
                    : cm.baseMeasureAliases.entrySet())
                {
                    refs.put((Member) e.getKey(), b.field(e.getValue()));
                }
                ArithmeticCalcTranslator tx =
                    new ArithmeticCalcTranslator(
                        b.getRexBuilder(),
                        ArithmeticCalcTranslator.mapResolver(refs));
                restored.add(tx.translate((Exp) cm.expression));
                restoredAliases.add(cm.alias);
            }
            b.project(restored, restoredAliases, true);
            if (hasComputed) {
                // RelBuilder eagerly folds adjacent Projects, which
                // would erase the computed-measure expressions from
                // the plan. Build the inner project as a fully-formed
                // RelNode and wrap it in a trivial-predicate
                // LogicalProject via direct construction so the outer
                // projection cannot collapse into the inner one.
                org.apache.calcite.rel.RelNode inner = b.build();
                List<RexNode> outer = new ArrayList<>(
                    innerGroupAndMeasureCount);
                List<String> outerAliases = new ArrayList<>(
                    innerGroupAndMeasureCount);
                org.apache.calcite.rel.type.RelDataType innerRow =
                    inner.getRowType();
                for (int i = 0; i < innerGroupAndMeasureCount; i++) {
                    String alias = restoredAliases.get(i);
                    outer.add(
                        b.getRexBuilder().makeInputRef(
                            innerRow.getFieldList().get(i).getType(),
                            i));
                    outerAliases.add(alias);
                }
                org.apache.calcite.rel.type.RelDataType outerRowType =
                    b.getTypeFactory().createStructType(
                        org.apache.calcite.util.Pair.right(
                            innerRow.getFieldList()
                                .subList(0,
                                    innerGroupAndMeasureCount)),
                        outerAliases);
                org.apache.calcite.rel.RelNode wrapped =
                    org.apache.calcite.rel.logical.LogicalProject.create(
                        inner,
                        java.util.Collections.<
                            org.apache.calcite.rel.hint.RelHint>emptyList(),
                        outer,
                        outerRowType,
                        java.util.Collections.<
                            org.apache.calcite.rel.core.CorrelationId>emptySet());
                b.push(wrapped);
            }
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
