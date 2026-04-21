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

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hand-rolled materialized-view matcher operating at the
 * {@link PlannerRequest} level (Option D —
 * {@code docs/reports/perf-investigation-volcano-mv-win.md}).
 *
 * <p>Walks {@link MvRegistry#shapeSpecs() registered shapes}; when one
 * subsumes the request, rewrites the request so {@link
 * PlannerRequest#factTable} points at the agg table, joins whose
 * columns are denormalized on the agg are removed, and measures
 * reference agg-side columns. Determistic, relies on no Calcite
 * SubstitutionVisitor / MaterializedViewRule gymnastics.
 *
 * <p><b>Shape subsumption predicate.</b> A shape {@code s} matches
 * request {@code req} iff:
 * <ol>
 *   <li>{@code req.factTable == s.factTable}.</li>
 *   <li>Every {@code req.groupBy} column is resolvable on the agg —
 *       either denormalized on the agg, or reachable by a join the
 *       agg still supports (via its FK columns).</li>
 *   <li>Every {@code req.measures} entry is SUM/MIN/MAX over a base
 *       column that the agg pre-aggregates (additive rollup), OR
 *       COUNT(*) where the agg carries a fact_count (not handled in
 *       this first cut — corpus measures are all SUM). DISTINCT
 *       variants, AVG, are rejected (no lossless rollup).</li>
 *   <li>Every {@code req.filters} column is resolvable by the same
 *       rule as group-by.</li>
 *   <li>No HAVING, no TupleFilters on non-resolvable cols, no
 *       ComputedMeasures (out of scope for first cut), no DISTINCT
 *       projections (non-aggregation).</li>
 * </ol>
 *
 * <p>On match the rewritten request:
 * <ul>
 *   <li>Scans {@code s.aggTable}.</li>
 *   <li>Drops joins to dims whose columns the agg has denormalized.</li>
 *   <li>Keeps joins to dims still needed for a non-denormalized
 *       group-by / filter column — rewritten to join from the agg's
 *       FK column to the dim PK.</li>
 *   <li>Rewrites measure/groupBy/filter column references from
 *       {@code factTable.X} to {@code aggTable.X} when the agg has
 *       that column.</li>
 * </ul>
 */
public final class MvMatcher {

    private static final Logger LOGGER = Logger.getLogger(MvMatcher.class);

    private static final boolean TRACE =
        Boolean.getBoolean("mondrian.calcite.trace");

    private MvMatcher() {
        // utility
    }

    /**
     * If any registered shape on {@code registry} subsumes {@code req},
     * return a new {@link PlannerRequest} that scans the agg table.
     * Otherwise return {@code req} unchanged (same reference — callers
     * can use {@code rewritten != req} as the "did we rewrite" flag).
     */
    public static PlannerRequest tryRewrite(
        PlannerRequest req, MvRegistry registry)
    {
        if (req == null || registry == null || registry.size() == 0) {
            return req;
        }
        List<MvRegistry.ShapeSpec> shapes = registry.shapeSpecs();
        if (shapes == null || shapes.isEmpty()) {
            return req;
        }
        if (!canRewrite(req)) {
            return req;
        }
        for (MvRegistry.ShapeSpec s : shapes) {
            PlannerRequest rewritten = tryMatch(req, s);
            if (rewritten != null) {
                if (TRACE) {
                    System.err.println(
                        "[mv-match] " + s.name + " -> scan "
                        + s.aggTable);
                }
                return rewritten;
            }
        }
        return req;
    }

    /** Up-front predicates cheap to check once per request. */
    private static boolean canRewrite(PlannerRequest req) {
        if (!req.isAggregation()) {
            return false;
        }
        if (!req.havings.isEmpty()) {
            return false;
        }
        if (!req.computedMeasures.isEmpty()) {
            return false;
        }
        if (req.universalFalse) {
            return false;
        }
        // All measures must be additively rolluppable.
        for (PlannerRequest.Measure m : req.measures) {
            if (m.distinct) {
                return false;
            }
            switch (m.fn) {
            case SUM: case MIN: case MAX:
                break;
            default:
                // COUNT / AVG don't roll up straightforwardly from
                // pre-aggregated rows (COUNT needs SUM(fact_count);
                // AVG needs SUM/COUNT pair). Skip for first cut.
                return false;
            }
        }
        return true;
    }

    /**
     * Attempt to rewrite {@code req} against shape {@code s}; return
     * the rewritten request on success, or {@code null} if the shape
     * doesn't subsume.
     */
    static PlannerRequest tryMatch(
        PlannerRequest req, MvRegistry.ShapeSpec s)
    {
        if (!s.factTable.equals(req.factTable)) {
            return null;
        }

        // Require EXACT groupBy-set equality between user request and
        // shape. SUM-over-SUM rollup from a finer-grained MV is
        // semantically valid, but produces cell-set checksum drift
        // against legacy's fact-scan golden (rowset values identical,
        // but column projection / row iteration order can differ in
        // ways the harness's byte-level checksum catches). Exact
        // equality prevents false matches like user [year, gender,
        // marital_status] against shape [year, family, gender,
        // marital_status]. If rollup is wanted later, a separate
        // shape entry at the coarser grain is the right mechanism —
        // explicit beats inferred here.
        if (req.groupBy.size() != s.groups.size()) {
            return null;
        }

        // Build a directory of what the agg provides:
        //   denormalized group cols — keyed by (dimAlias, column)
        //     map → (aggTable, aggColumn)
        //   non-denormalized group cols — keyed by (dimAlias, column)
        //     map → the corresponding DimJoin (so the rewritten
        //     request keeps that join).
        Map<ColKey, String> denormalized = new LinkedHashMap<>();
        Map<ColKey, MvRegistry.GroupCol> joinResolvable =
            new LinkedHashMap<>();
        for (MvRegistry.GroupCol g : s.groups) {
            ColKey k = new ColKey(g.table, g.column);
            if (g.aggTable != null && g.aggColumn != null) {
                denormalized.put(k, g.aggColumn);
            } else {
                joinResolvable.put(k, g);
            }
        }

        // Map of dimAlias → DimJoin (fact→dim) so we can translate
        // req.joins.
        Map<String, MvRegistry.DimJoin> shapeJoinsByDim =
            new LinkedHashMap<>();
        for (MvRegistry.DimJoin j : s.joins) {
            shapeJoinsByDim.put(j.dimAlias, j);
        }

        // ----- Measure mapping -----
        // Map base-column name -> agg-column name. For first-cut we
        // match by base column name; SUM/MIN/MAX of the base column
        // projects onto SUM/MIN/MAX of the agg's pre-summed column
        // (SUM-over-SUM is the invariant; MIN-over-MIN / MAX-over-MAX
        // are also associative).
        Map<String, String> measureCol = new LinkedHashMap<>();
        for (MvRegistry.MeasureRef mr : s.measures) {
            if (mr.baseColumn == null || mr.aggColumn == null) {
                continue;
            }
            // Only pre-summed measures (fn=sum on the agg side) are
            // safe for SUM-over-SUM rollup. MIN/MAX pre-aggregated
            // columns aren't emitted by FoodMart aggs so we'd hit
            // them only if a schema exposes them — guard by fn.
            if (!"sum".equalsIgnoreCase(mr.fn)) {
                continue;
            }
            measureCol.put(mr.baseColumn, mr.aggColumn);
        }

        // Every requested measure must have a rollup mapping AND be
        // referenced against the base fact table.
        List<PlannerRequest.Measure> newMeasures =
            new ArrayList<>(req.measures.size());
        for (PlannerRequest.Measure m : req.measures) {
            // Measure column must be on the fact table (or unqualified,
            // which is the fact table by default).
            String colTable = m.column.table;
            if (colTable != null && !colTable.equals(req.factTable)) {
                return null;
            }
            // Only SUM rolls up lossless over pre-summed columns.
            // MIN/MAX would too, but FoodMart aggs don't emit them —
            // reject so we don't silently corrupt cell values on a
            // fixture where the agg column is pre-summed.
            if (m.fn != PlannerRequest.AggFn.SUM) {
                return null;
            }
            String aggCol = measureCol.get(m.column.name);
            if (aggCol == null) {
                return null;
            }
            newMeasures.add(new PlannerRequest.Measure(
                m.fn,
                new PlannerRequest.Column(s.aggTable, aggCol),
                m.alias,
                m.distinct));
        }

        // Every group-by column must be resolvable on the agg.
        // resolution outcome for each column we'll carry to the
        // rewritten request:
        //   tableName = s.aggTable  (if denormalized) — rewrite col
        //   tableName = req column's table (if reached via join) —
        //     retained join.
        List<PlannerRequest.Column> newGroupBy =
            new ArrayList<>(req.groupBy.size());
        Set<String> dimsToKeep = new LinkedHashSet<>();
        for (PlannerRequest.Column c : req.groupBy) {
            ColKey k = resolveColKey(c, req);
            if (k == null) {
                return null;
            }
            if (denormalized.containsKey(k)) {
                newGroupBy.add(new PlannerRequest.Column(
                    s.aggTable, denormalized.get(k)));
            } else if (joinResolvable.containsKey(k)) {
                // Column lives on a dim we must still join to —
                // keep original col reference.
                dimsToKeep.add(k.table);
                newGroupBy.add(c);
            } else {
                return null;
            }
        }

        // Filters: same resolution rule.
        List<PlannerRequest.Filter> newFilters =
            new ArrayList<>(req.filters.size());
        for (PlannerRequest.Filter f : req.filters) {
            ColKey k = resolveColKey(f.column, req);
            if (k == null) {
                return null;
            }
            if (denormalized.containsKey(k)) {
                newFilters.add(new PlannerRequest.Filter(
                    new PlannerRequest.Column(
                        s.aggTable, denormalized.get(k)),
                    f.op,
                    f.literals));
            } else if (joinResolvable.containsKey(k)) {
                dimsToKeep.add(k.table);
                newFilters.add(f);
            } else {
                return null;
            }
        }

        // TupleFilters — reject unless every column is resolvable.
        // For the first cut we reject any tuple filter to stay
        // conservative (segment-load cross-column predicates are
        // segment-scoped and usually reference the fact table's FK
        // columns, which the agg carries verbatim — but the shape
        // catalog doesn't flag FK columns explicitly yet).
        if (!req.tupleFilters.isEmpty()) {
            return null;
        }

        // OrderBy: same rule as groupBy — translate denormalized cols,
        // keep dim-routed cols (and ensure we kept the dim join).
        List<PlannerRequest.OrderBy> newOrderBy =
            new ArrayList<>(req.orderBy.size());
        for (PlannerRequest.OrderBy o : req.orderBy) {
            ColKey k = resolveColKey(o.column, req);
            if (k == null) {
                return null;
            }
            if (denormalized.containsKey(k)) {
                newOrderBy.add(new PlannerRequest.OrderBy(
                    new PlannerRequest.Column(
                        s.aggTable, denormalized.get(k)),
                    o.direction));
            } else if (joinResolvable.containsKey(k)) {
                dimsToKeep.add(k.table);
                newOrderBy.add(o);
            } else {
                return null;
            }
        }

        // Build rewritten PlannerRequest.
        PlannerRequest.Builder out = PlannerRequest.builder(s.aggTable);

        // Retain only joins whose dim we need. Rewrite the fact side
        // of the join to reference the agg table (the FK column name
        // is preserved — FoodMart aggs use the same FK column names
        // as the base fact table).
        for (PlannerRequest.Join j : req.joins) {
            if (!dimsToKeep.contains(j.dimTable)) {
                continue;
            }
            // Only INNER fact→dim joins are handled for rewrite. Snow-
            // flake chained joins (leftTable != null) and CROSS joins
            // are not part of FoodMart agg shapes; if present, reject
            // the match conservatively.
            if (j.kind != PlannerRequest.JoinKind.INNER
                || j.leftTable != null)
            {
                return null;
            }
            // The shape must also have declared this dim as joinable
            // (its FK column is on the agg). We've already guaranteed
            // that via joinResolvable check, but double-check the
            // shape's declared join exists for alias equivalence.
            MvRegistry.DimJoin sj = shapeJoinsByDim.get(j.dimTable);
            if (sj == null) {
                return null;
            }
            out.addJoin(new PlannerRequest.Join(
                j.dimTable, j.factKey, j.dimKey, j.kind));
        }

        for (PlannerRequest.Column gc : newGroupBy) {
            out.addGroupBy(gc);
        }
        for (PlannerRequest.Measure nm : newMeasures) {
            out.addMeasure(nm);
        }
        for (PlannerRequest.Filter nf : newFilters) {
            out.addFilter(nf);
        }
        for (PlannerRequest.OrderBy ob : newOrderBy) {
            out.addOrderBy(ob);
        }
        // If the user didn't supply explicit orderBy, force deterministic
        // row order by sorting on the rewritten groupBy columns. Without
        // this, Postgres/HSQLDB return agg-scan rows in a different
        // physical order than the legacy fact-scan — harness checksum
        // is computed over iteration order, so identical values in a
        // different order trip LEGACY_DRIFT. Legacy works because its
        // row order is stable under the fact-scan plan the golden was
        // recorded against; we need the same determinism.
        if (newOrderBy.isEmpty()) {
            for (PlannerRequest.Column gc : newGroupBy) {
                out.addOrderBy(new PlannerRequest.OrderBy(
                    gc, PlannerRequest.Order.ASC));
            }
        }
        // Measures-only aggregation (no group-by) isn't possible with
        // the empty-builder constraint — we have measures above so
        // build() is safe.
        try {
            return out.build();
        } catch (RuntimeException re) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "MvMatcher: build() rejected rewrite for shape "
                    + s.name, re);
            }
            return null;
        }
    }

    /** Resolve a PlannerRequest column to a (table, column) key whose
     *  {@code table} is the dim alias the column lives on. Unqualified
     *  column references are resolved against the fact table. */
    private static ColKey resolveColKey(
        PlannerRequest.Column c, PlannerRequest req)
    {
        String table = c.table;
        if (table == null) {
            table = req.factTable;
        }
        return new ColKey(table, c.name);
    }

    /** Composite key (table, column). */
    private static final class ColKey {
        final String table;
        final String column;
        ColKey(String table, String column) {
            this.table = table;
            this.column = column;
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ColKey)) return false;
            ColKey k = (ColKey) o;
            return table.equals(k.table) && column.equals(k.column);
        }
        @Override public int hashCode() {
            return 31 * table.hashCode() + column.hashCode();
        }
        @Override public String toString() {
            return table + "." + column;
        }
    }
}

// End MvMatcher.java
