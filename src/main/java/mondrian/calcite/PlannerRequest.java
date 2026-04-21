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

    /** HAVING comparison operator set — the binary-compare subset the
     *  corpus exercises for {@code RolapNativeFilter$FilterConstraint}.
     *  Added for Task P. */
    public enum Comparison { GT, LT, GE, LE, EQ, NE }

    /**
     * HAVING predicate: {@code <measure> <op> <literal>}. Used for
     * native-filter translation where the MDX Filter expression is a
     * single binary comparison of a stored measure against a numeric
     * literal. The {@link #measure} is projected in the same aggregate
     * as the GROUP BY so the renderer can filter on its alias, then
     * dropped by the post-aggregate reprojection so the SELECT list
     * remains {groupBy, userMeasures}. Added in Task P.
     */
    public static final class Having {
        public final Measure measure;
        public final Comparison op;
        public final Object literal;
        public Having(Measure measure, Comparison op, Object literal) {
            if (measure == null) {
                throw new IllegalArgumentException("measure is null");
            }
            if (op == null) {
                throw new IllegalArgumentException("op is null");
            }
            if (literal == null) {
                throw new IllegalArgumentException("literal is null");
            }
            this.measure = measure;
            this.op = op;
            this.literal = literal;
        }
    }

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

    /**
     * OR-of-AND cross-column tuple filter. Expresses
     * {@code (col0=v00 AND col1=v01) OR (col0=v10 AND col1=v11) OR ...}
     * where every row is aligned to the shared {@link #columns} list.
     * A single-column TupleFilter collapses to an IN-list at render time.
     * Added for segment-load cross-column OR predicates (Task M).
     */
    public static final class TupleFilter {
        public final List<Column> columns;
        public final List<List<Object>> rows;
        public TupleFilter(List<Column> columns, List<List<Object>> rows) {
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException(
                    "TupleFilter requires at least one column");
            }
            if (rows == null || rows.isEmpty()) {
                throw new IllegalArgumentException(
                    "TupleFilter requires at least one row");
            }
            List<Column> cols = List.copyOf(columns);
            List<List<Object>> rs = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                if (row.size() != cols.size()) {
                    throw new IllegalArgumentException(
                        "TupleFilter row arity " + row.size()
                        + " != columns arity " + cols.size());
                }
                List<Object> copy = new ArrayList<>(row.size());
                for (Object v : row) {
                    copy.add(v);
                }
                rs.add(java.util.Collections.unmodifiableList(copy));
            }
            this.columns = cols;
            this.rows = java.util.Collections.unmodifiableList(rs);
        }
    }

    /**
     * A post-aggregate computed measure: an arithmetic expression over
     * already-aggregated base measures. Rendered by
     * {@link CalciteSqlPlanner} as an extra projection on top of the
     * aggregate, built by {@link ArithmeticCalcTranslator}.
     *
     * <p>The {@link #expression} is an opaque MDX {@code Exp} tree; the
     * planner owns a translator and the {@link #baseMeasureAliases}
     * mapping resolves a calc's base-measure references to the
     * aggregate row's columns.
     *
     * <p>Added in Task T (arithmetic calc-member pushdown).
     */
    public static final class ComputedMeasure {
        public final String alias;
        public final Object expression; // mondrian.olap.Exp, kept Object
                                        // to avoid an import-cycle burden
                                        // on callers that don't touch
                                        // MDX directly.
        /** Map from mondrian.olap.Member (base measure) → alias of the
         *  Measure entry in {@link PlannerRequest#measures}. */
        public final java.util.Map<Object, String> baseMeasureAliases;

        public ComputedMeasure(
            String alias, Object expression,
            java.util.Map<Object, String> baseMeasureAliases)
        {
            if (alias == null || alias.isEmpty()) {
                throw new IllegalArgumentException("alias required");
            }
            if (expression == null) {
                throw new IllegalArgumentException("expression required");
            }
            if (baseMeasureAliases == null) {
                throw new IllegalArgumentException(
                    "baseMeasureAliases required");
            }
            this.alias = alias;
            this.expression = expression;
            this.baseMeasureAliases =
                java.util.Collections.unmodifiableMap(
                    new java.util.LinkedHashMap<>(baseMeasureAliases));
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
    public final List<TupleFilter> tupleFilters;
    public final List<Having> havings;
    public final List<ComputedMeasure> computedMeasures;
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
        this.tupleFilters = List.copyOf(b.tupleFilters);
        this.havings = List.copyOf(b.havings);
        this.computedMeasures = List.copyOf(b.computedMeasures);
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

    // ------------------------------------------------------------------
    // Structural-hash / literal extraction helpers, used by
    // CalciteSqlTemplateCache (Task 6 perf: reuse unparsed SQL across
    // same-shape requests that differ only in literal values).
    // ------------------------------------------------------------------

    /**
     * Stable 64-bit structural hash: two {@code PlannerRequest}s hash
     * identically iff they have the same fact, joins (order-sensitive),
     * groupBy columns, measures (fn+column+alias+distinct),
     * projections, filter columns+operators (NOT literal values),
     * tupleFilter columns (NOT literal rows), orderBy, distinct,
     * universalFalse, havings (measure+op, NOT literal threshold),
     * and computed-measure aliases.
     *
     * <p>Literal values are intentionally excluded so requests that
     * differ only in the values (e.g. {@code year=1997} vs
     * {@code year=1998}) collide to the same shape — the cache then
     * substitutes literals into the pre-unparsed SQL template.
     */
    public long structuralHash() {
        long h = 1125899906842597L; // large prime seed
        h = 31 * h + factTable.hashCode();
        for (Join j : joins) {
            h = 31 * h + (j.dimTable == null ? 0 : j.dimTable.hashCode());
            h = 31 * h + (j.factKey == null ? 0 : j.factKey.hashCode());
            h = 31 * h + (j.dimKey == null ? 0 : j.dimKey.hashCode());
            h = 31 * h + j.kind.hashCode();
            h = 31 * h + (j.leftTable == null ? 0 : j.leftTable.hashCode());
        }
        for (Column c : groupBy) {
            h = 31 * h + colHash(c);
        }
        for (Measure m : measures) {
            h = 31 * h + m.fn.hashCode();
            h = 31 * h + colHash(m.column);
            h = 31 * h + m.alias.hashCode();
            h = 31 * h + Boolean.hashCode(m.distinct);
        }
        for (Column c : projections) {
            h = 31 * h + colHash(c);
        }
        for (Filter f : filters) {
            h = 31 * h + colHash(f.column);
            h = 31 * h + f.op.hashCode();
            // Exclude literal values; include literal count (IN arity
            // affects SQL shape: col=v vs col=v1 OR col=v2).
            h = 31 * h + f.literals.size();
        }
        for (TupleFilter tf : tupleFilters) {
            for (Column c : tf.columns) {
                h = 31 * h + colHash(c);
            }
            h = 31 * h + tf.rows.size(); // row count affects OR-chain arity
        }
        for (Having hav : havings) {
            h = 31 * h + hav.measure.alias.hashCode();
            h = 31 * h + hav.op.hashCode();
        }
        for (ComputedMeasure cm : computedMeasures) {
            h = 31 * h + cm.alias.hashCode();
        }
        for (OrderBy o : orderBy) {
            h = 31 * h + colHash(o.column);
            h = 31 * h + o.direction.hashCode();
        }
        h = 31 * h + Boolean.hashCode(distinct);
        h = 31 * h + Boolean.hashCode(universalFalse);
        return h;
    }

    private static int colHash(Column c) {
        int h = c.name == null ? 0 : c.name.hashCode();
        h = 31 * h + (c.table == null ? 0 : c.table.hashCode());
        return h;
    }

    /**
     * Collision-safe structural equality. Two requests are structurally
     * equal iff their {@link #structuralHash()} components match
     * field-for-field (excluding literal values). Used by
     * {@code CalciteSqlTemplateCache} as a confirmation check on
     * hash-table lookup.
     */
    public boolean structurallyEqual(PlannerRequest other) {
        if (other == null) return false;
        if (!factTable.equals(other.factTable)) return false;
        if (joins.size() != other.joins.size()) return false;
        for (int i = 0; i < joins.size(); i++) {
            Join a = joins.get(i), b = other.joins.get(i);
            if (!java.util.Objects.equals(a.dimTable, b.dimTable)) return false;
            if (!java.util.Objects.equals(a.factKey, b.factKey)) return false;
            if (!java.util.Objects.equals(a.dimKey, b.dimKey)) return false;
            if (a.kind != b.kind) return false;
            if (!java.util.Objects.equals(a.leftTable, b.leftTable))
                return false;
        }
        if (!sameColumns(groupBy, other.groupBy)) return false;
        if (measures.size() != other.measures.size()) return false;
        for (int i = 0; i < measures.size(); i++) {
            Measure a = measures.get(i), b = other.measures.get(i);
            if (a.fn != b.fn) return false;
            if (!sameColumn(a.column, b.column)) return false;
            if (!a.alias.equals(b.alias)) return false;
            if (a.distinct != b.distinct) return false;
        }
        if (!sameColumns(projections, other.projections)) return false;
        if (filters.size() != other.filters.size()) return false;
        for (int i = 0; i < filters.size(); i++) {
            Filter a = filters.get(i), b = other.filters.get(i);
            if (!sameColumn(a.column, b.column)) return false;
            if (a.op != b.op) return false;
            if (a.literals.size() != b.literals.size()) return false;
        }
        if (tupleFilters.size() != other.tupleFilters.size()) return false;
        for (int i = 0; i < tupleFilters.size(); i++) {
            TupleFilter a = tupleFilters.get(i),
                b = other.tupleFilters.get(i);
            if (!sameColumns(a.columns, b.columns)) return false;
            if (a.rows.size() != b.rows.size()) return false;
        }
        if (havings.size() != other.havings.size()) return false;
        for (int i = 0; i < havings.size(); i++) {
            Having a = havings.get(i), b = other.havings.get(i);
            if (!a.measure.alias.equals(b.measure.alias)) return false;
            if (a.op != b.op) return false;
        }
        if (computedMeasures.size() != other.computedMeasures.size())
            return false;
        for (int i = 0; i < computedMeasures.size(); i++) {
            ComputedMeasure a = computedMeasures.get(i),
                b = other.computedMeasures.get(i);
            if (!a.alias.equals(b.alias)) return false;
            // Note: expression trees are not compared here — calc
            // pushdown paths don't participate in the cache anyway
            // (noop fallback if needed).
        }
        if (orderBy.size() != other.orderBy.size()) return false;
        for (int i = 0; i < orderBy.size(); i++) {
            OrderBy a = orderBy.get(i), b = other.orderBy.get(i);
            if (!sameColumn(a.column, b.column)) return false;
            if (a.direction != b.direction) return false;
        }
        if (distinct != other.distinct) return false;
        if (universalFalse != other.universalFalse) return false;
        return true;
    }

    private static boolean sameColumn(Column a, Column b) {
        return java.util.Objects.equals(a.name, b.name)
            && java.util.Objects.equals(a.table, b.table);
    }

    private static boolean sameColumns(List<Column> a, List<Column> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!sameColumn(a.get(i), b.get(i))) return false;
        }
        return true;
    }

    /**
     * Ordered list of literal values that appear in the rendered SQL,
     * in the same traversal order used by {@link CalciteSqlPlanner}:
     * {@code filters} (each Filter's literals in order),
     * {@code tupleFilters} (row-major, column-minor), then
     * {@code havings} (in declaration order).
     *
     * <p>{@code universalFalse} is not expressed via these literals — it
     * emits a fixed {@code WHERE FALSE}. Projections/measures/groupBy/
     * joins contribute no literal values to the SQL.
     */
    public List<Object> literals() {
        List<Object> out = new ArrayList<>();
        if (!universalFalse) {
            for (Filter f : filters) {
                for (Object v : f.literals) {
                    out.add(v);
                }
            }
            for (TupleFilter tf : tupleFilters) {
                for (List<Object> row : tf.rows) {
                    for (Object v : row) {
                        out.add(v);
                    }
                }
            }
        }
        for (Having h : havings) {
            out.add(h.literal);
        }
        return out;
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
        private final List<TupleFilter> tupleFilters = new ArrayList<>();
        private final List<Having> havings = new ArrayList<>();
        private final List<ComputedMeasure> computedMeasures =
            new ArrayList<>();
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
        public Builder addTupleFilter(TupleFilter f) {
            tupleFilters.add(f);
            return this;
        }
        public Builder addHaving(Having h) {
            if (h == null) {
                throw new IllegalArgumentException("having is null");
            }
            havings.add(h);
            return this;
        }
        public Builder addComputedMeasure(ComputedMeasure cm) {
            if (cm == null) {
                throw new IllegalArgumentException("computedMeasure is null");
            }
            computedMeasures.add(cm);
            return this;
        }
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
