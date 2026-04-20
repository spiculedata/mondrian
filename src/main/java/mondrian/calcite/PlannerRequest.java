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

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable description of a Calcite plan request: fact table, optional
 * inner-equi-joins onto dimensions, projections (when not aggregating),
 * group-by columns + measures (when aggregating), equality filters, and
 * order-by.
 *
 * <p>Worktree #1 scope is intentionally narrow: equality filters only,
 * inner equi-join only, no nested subqueries, no DISTINCT, no window
 * functions. See {@link CalciteSqlPlanner} for the rendering side.
 */
public final class PlannerRequest {

    public enum AggFn { SUM, COUNT, MIN, MAX, AVG }
    public enum Order { ASC, DESC }

    public static final class Column {
        /** Optional table qualifier; null when unambiguous. */
        public final String table;
        public final String name;
        public Column(String table, String name) {
            this.table = table;
            this.name = name;
        }
    }

    public static final class Measure {
        public final AggFn fn;
        public final Column column;
        public final String alias;
        /** Whether the aggregator applies DISTINCT (only honoured for
         *  {@link AggFn#COUNT} today; rendered as {@code count(distinct x)}).
         *  Added for cardinality-probe dispatch (Task C). */
        public final boolean distinct;
        public Measure(AggFn fn, Column column, String alias) {
            this(fn, column, alias, false);
        }
        public Measure(
            AggFn fn, Column column, String alias, boolean distinct)
        {
            this.fn = fn;
            this.column = column;
            this.alias = alias;
            this.distinct = distinct;
        }
    }

    public enum Operator { EQ, IN }

    public static final class Filter {
        public final Column column;
        public final Operator op;
        public final List<Object> literals;
        /** Back-compat shortcut: single-literal EQ filter. */
        public Filter(Column column, Object literal) {
            this(column, Operator.EQ,
                java.util.Collections.singletonList(literal));
        }
        public Filter(Column column, Operator op, List<Object> literals) {
            this.column = column;
            this.op = op;
            this.literals = List.copyOf(literals);
            if (op == Operator.EQ && this.literals.size() != 1) {
                throw new IllegalArgumentException(
                    "EQ filter requires exactly one literal; got "
                    + this.literals.size());
            }
            if (op == Operator.IN && this.literals.isEmpty()) {
                throw new IllegalArgumentException(
                    "IN filter requires at least one literal");
            }
        }
        /** Back-compat accessor: EQ filter's single literal. */
        public Object literal() {
            if (op != Operator.EQ) {
                throw new IllegalStateException(
                    "literal() only valid on EQ filters; this is " + op);
            }
            return literals.get(0);
        }
    }

    public static final class OrderBy {
        public final Column column;
        public final Order direction;
        public OrderBy(Column column, Order direction) {
            this.column = column;
            this.direction = direction;
        }
    }

    /** Join flavour. INNER = equi-join on ({@link Join#factKey},
     *  {@link Join#dimKey}); CROSS = unconditional cross-join (the key
     *  fields are ignored). Added for multi-target tuple-read (Task H). */
    public enum JoinKind { INNER, CROSS }

    public static final class Join {
        public final String dimTable;
        public final String factKey;
        public final String dimKey;
        public final JoinKind kind;
        /** Optional LHS of the edge. {@code null} means "the fact table"
         *  (single-hop, back-compat with callers that only expressed
         *  fact→dim joins). When non-null, this identifies an already-
         *  joined table alias on the builder stack so the renderer can
         *  resolve {@link #factKey} against the correct input of a
         *  multi-hop snowflake chain (Task I). */
        public final String leftTable;
        public Join(String dimTable, String factKey, String dimKey) {
            this(dimTable, factKey, dimKey, JoinKind.INNER, null);
        }
        public Join(
            String dimTable, String factKey, String dimKey, JoinKind kind)
        {
            this(dimTable, factKey, dimKey, kind, null);
        }
        public Join(
            String dimTable, String factKey, String dimKey, JoinKind kind,
            String leftTable)
        {
            this.dimTable = dimTable;
            this.factKey = factKey;
            this.dimKey = dimKey;
            this.kind = kind;
            this.leftTable = leftTable;
        }
        /** Convenience factory for an unconditional CROSS JOIN. */
        public static Join cross(String dimTable) {
            return new Join(dimTable, null, null, JoinKind.CROSS, null);
        }
        /** Convenience factory for an inner equi-join whose LHS is an
         *  already-joined non-fact table (Task I snowflake multi-hop). */
        public static Join chained(
            String leftTable, String leftKey,
            String dimTable, String dimKey)
        {
            return new Join(
                dimTable, leftKey, dimKey, JoinKind.INNER, leftTable);
        }
    }

    public final String factTable;
    public final List<Join> joins;
    public final List<Column> groupBy;
    public final List<Measure> measures;
    public final List<Column> projections;
    public final List<Filter> filters;
    public final List<OrderBy> orderBy;
    /** Row-level DISTINCT on projections. Only valid when not aggregating.
     *  Added for tuple-read / level-member dispatch (Task E). */
    public final boolean distinct;
    /** When true, the request has a universal FALSE filter (zero rows).
     *  Other {@link #filters} are ignored by the renderer.
     *  Added for segment-load predicate translation (Task F). */
    public final boolean universalFalse;

    private PlannerRequest(Builder b) {
        this.factTable = b.factTable;
        this.joins = List.copyOf(b.joins);
        this.groupBy = List.copyOf(b.groupBy);
        this.measures = List.copyOf(b.measures);
        this.projections = List.copyOf(b.projections);
        this.filters = List.copyOf(b.filters);
        this.orderBy = List.copyOf(b.orderBy);
        this.distinct = b.distinct;
        this.universalFalse = b.universalFalse;
        if (this.distinct
            && (!this.measures.isEmpty() || !this.groupBy.isEmpty()))
        {
            throw new IllegalStateException(
                "PlannerRequest.distinct is mutually exclusive with "
                + "aggregation (measures / group-by)");
        }
    }

    public boolean isAggregation() {
        return !measures.isEmpty() || !groupBy.isEmpty();
    }

    public static Builder builder(String factTable) {
        return new Builder(factTable);
    }

    public static final class Builder {
        private final String factTable;
        private final List<Join> joins = new ArrayList<>();
        private final List<Column> groupBy = new ArrayList<>();
        private final List<Measure> measures = new ArrayList<>();
        private final List<Column> projections = new ArrayList<>();
        private final List<Filter> filters = new ArrayList<>();
        private final List<OrderBy> orderBy = new ArrayList<>();
        private boolean distinct;
        private boolean universalFalse;

        private Builder(String factTable) {
            if (factTable == null || factTable.isEmpty()) {
                throw new IllegalArgumentException("factTable required");
            }
            this.factTable = factTable;
        }

        public Builder addJoin(Join j) { joins.add(j); return this; }
        public Builder addGroupBy(Column c) { groupBy.add(c); return this; }
        public Builder addMeasure(Measure m) { measures.add(m); return this; }
        public Builder addProjection(Column c) {
            projections.add(c);
            return this;
        }
        public Builder addFilter(Filter f) { filters.add(f); return this; }
        public Builder addOrderBy(OrderBy o) { orderBy.add(o); return this; }
        public Builder distinct(boolean d) { this.distinct = d; return this; }
        public Builder universalFalse(boolean f) {
            this.universalFalse = f;
            return this;
        }

        public PlannerRequest build() {
            if (projections.isEmpty()
                && measures.isEmpty()
                && groupBy.isEmpty())
            {
                throw new IllegalStateException(
                    "PlannerRequest needs at least one projection, "
                    + "measure, or group-by column");
            }
            return new PlannerRequest(this);
        }
    }
}

// End PlannerRequest.java
