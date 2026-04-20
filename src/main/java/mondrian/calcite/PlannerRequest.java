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
        public Measure(AggFn fn, Column column, String alias) {
            this.fn = fn;
            this.column = column;
            this.alias = alias;
        }
    }

    public static final class Filter {
        public final Column column;
        public final Object literal;
        public Filter(Column column, Object literal) {
            this.column = column;
            this.literal = literal;
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

    public static final class Join {
        public final String dimTable;
        public final String factKey;
        public final String dimKey;
        public Join(String dimTable, String factKey, String dimKey) {
            this.dimTable = dimTable;
            this.factKey = factKey;
            this.dimKey = dimKey;
        }
    }

    public final String factTable;
    public final List<Join> joins;
    public final List<Column> groupBy;
    public final List<Measure> measures;
    public final List<Column> projections;
    public final List<Filter> filters;
    public final List<OrderBy> orderBy;

    private PlannerRequest(Builder b) {
        this.factTable = b.factTable;
        this.joins = List.copyOf(b.joins);
        this.groupBy = List.copyOf(b.groupBy);
        this.measures = List.copyOf(b.measures);
        this.projections = List.copyOf(b.projections);
        this.filters = List.copyOf(b.filters);
        this.orderBy = List.copyOf(b.orderBy);
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
