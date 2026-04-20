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

import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.agg.GroupingSet;
import mondrian.rolap.agg.GroupingSetsList;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.Segment;
import mondrian.rolap.agg.ValueColumnPredicate;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridge between Mondrian-internal SQL-build contexts (e.g.
 * {@code SqlTupleReader.Target}, cell-request groups) and the
 * backend-neutral {@link PlannerRequest}.
 *
 * <p>Worktree #1 scope: the translation surface here is intentionally
 * minimal. The {@code SqlTupleReader} dispatch wires up the routing seam,
 * but actual translation coverage grows in later worktrees. When a shape
 * cannot be translated, methods throw {@link UnsupportedTranslation} so the
 * caller can fall back to the legacy {@code SqlQuery}-built string.
 *
 * <p>A static {@link AtomicLong} counter records every fallback so the
 * harness / observability layer can spot regressions in translation
 * coverage as it expands.
 */
public final class CalcitePlannerAdapters {

    private CalcitePlannerAdapters() {}

    private static final AtomicLong UNSUPPORTED_FALLBACK_COUNT =
        new AtomicLong();

    /**
     * Per-dispatch-surface fallback counters. Lets tests assert that e.g.
     * segment-load translation reached coverage while tuple-read is still
     * deferred. The aggregate {@link #UNSUPPORTED_FALLBACK_COUNT} stays as
     * the legacy single-number signal.
     */
    private static final AtomicLong SEGMENT_LOAD_FALLBACK_COUNT =
        new AtomicLong();
    private static final AtomicLong TUPLE_READ_FALLBACK_COUNT =
        new AtomicLong();

    /**
     * Attempt to translate a tuple-read context into a
     * {@link PlannerRequest}. Currently always throws
     * {@link UnsupportedTranslation}: the {@code SqlTupleReader.Target}
     * structure (multi-target crossjoins, hierarchical level reads,
     * approxRowCount overrides, native filters) does not map cleanly onto
     * the worktree-#1 single-table projection shape without a deeper
     * refactor of the tuple-reader. Coverage lands in worktree #2.
     *
     * <p>The method exists now so the dispatch in
     * {@code SqlTupleReader.prepareTuples} is wired through a stable seam
     * — flipping translation on for a given shape is a one-file change
     * here, not a re-plumbing of the reader.
     *
     * @throws UnsupportedTranslation always (worktree #1).
     */
    public static PlannerRequest fromTupleRead(Object tupleReadContext) {
        TUPLE_READ_FALLBACK_COUNT.incrementAndGet();
        recordFallback();
        throw new UnsupportedTranslation(
            "CalcitePlannerAdapters.fromTupleRead: tuple-read translation "
            + "is deferred to a later worktree; falling back to legacy SQL.");
    }

    /**
     * Attempt to translate an aggregate-segment-load context (the
     * {@code GroupingSetsList} + compound-predicate shape used by
     * {@code SegmentLoader.createExecuteSql}) into a
     * {@link PlannerRequest}. Throws {@link UnsupportedTranslation} when
     * the shape is outside the currently-supported subset; the caller
     * then falls back to the legacy SQL string.
     *
     * <p>Worktree-#1 (Task B) supported shape:
     * <ul>
     *   <li>Single grouping set (no GROUPING SETS rollup).</li>
     *   <li>No compound predicates.</li>
     *   <li>All measures use SUM aggregator, live on the fact table,
     *       and have a {@link RolapSchema.PhysRealColumn} expression.</li>
     *   <li>All grouping columns have a {@link RolapSchema.PhysRealColumn}
     *       expression and either live on the fact table or hang off it
     *       via a single-hop dimension join (one {@link
     *       RolapSchema.PhysLink}).</li>
     *   <li>All column predicates are either null (wildcard) or a single
     *       {@link ValueColumnPredicate}.</li>
     * </ul>
     * Anything else throws {@link UnsupportedTranslation}.
     */
    public static PlannerRequest fromSegmentLoad(Object segmentLoadContext) {
        if (!(segmentLoadContext instanceof GroupingSetsList)) {
            SEGMENT_LOAD_FALLBACK_COUNT.incrementAndGet();
            recordFallback();
            throw new UnsupportedTranslation(
                "CalcitePlannerAdapters.fromSegmentLoad: expected "
                + "GroupingSetsList context; got "
                + (segmentLoadContext == null
                    ? "null"
                    : segmentLoadContext.getClass().getName()));
        }
        return fromSegmentLoad(
            (GroupingSetsList) segmentLoadContext,
            java.util.Collections.<StarPredicate>emptyList());
    }

    /**
     * Typed entry-point for {@link #fromSegmentLoad(Object)}. Takes the
     * loader-supplied {@link GroupingSetsList} and any compound
     * predicates, returns a {@link PlannerRequest} modelling the
     * equivalent single-grouping-set scan/aggregate, or throws
     * {@link UnsupportedTranslation} if the shape is unsupported.
     */
    public static PlannerRequest fromSegmentLoad(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        try {
            return translateSegmentLoad(
                groupingSetsList, compoundPredicateList);
        } catch (UnsupportedTranslation ex) {
            SEGMENT_LOAD_FALLBACK_COUNT.incrementAndGet();
            recordFallback();
            throw ex;
        }
    }

    private static PlannerRequest translateSegmentLoad(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        if (groupingSetsList == null) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: null GroupingSetsList");
        }
        if (compoundPredicateList != null
            && !compoundPredicateList.isEmpty())
        {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: compound predicates not yet supported");
        }
        if (groupingSetsList.useGroupingSets()) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: GROUPING SETS rollup not yet supported");
        }
        List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        if (groupingSets.size() != 1) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: expected exactly 1 grouping set, got "
                + groupingSets.size());
        }
        GroupingSet gs = groupingSets.get(0);
        RolapStar star = groupingSetsList.getStar();
        RolapStar.Table factTable = star.getFactTable();
        String factName = realTableName(factTable);
        if (factName == null) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: fact table is not a PhysTable");
        }

        PlannerRequest.Builder b = PlannerRequest.builder(factName);

        // Track joined dimension aliases so we don't emit duplicate joins
        // if two grouping columns share the same dim.
        java.util.Set<String> joinedAliases = new java.util.LinkedHashSet<>();

        RolapStar.Column[] columns = gs.getColumns();
        StarColumnPredicate[] predicates = gs.getPredicates();
        if (columns.length != predicates.length) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: columns/predicates arity mismatch");
        }

        // 1) Grouping columns + their joins + equality filters from
        //    per-column predicates.
        for (int i = 0; i < columns.length; i++) {
            RolapStar.Column col = columns[i];
            RolapSchema.PhysColumn expr = col.getExpression();
            if (!(expr instanceof RolapSchema.PhysRealColumn)) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: non-real grouping column expression "
                    + expr);
            }
            RolapStar.Table colTable = col.getTable();
            String colTableAlias = colTable.getAlias();
            String colName = ((RolapSchema.PhysRealColumn) expr).name;

            // If the column hangs off a dim table, ensure a join edge is
            // present. Worktree-#1 supports single-hop dims only.
            if (colTable != factTable) {
                if (joinedAliases.add(colTableAlias)) {
                    addSingleHopJoin(b, factTable, colTable);
                }
            }

            b.addGroupBy(
                new PlannerRequest.Column(colTableAlias, colName));

            // Per-column predicate → equality filter.
            StarColumnPredicate p = predicates[i];
            if (p == null) {
                continue;
            }
            ValueColumnPredicate eq = asSingleValuePredicate(p);
            if (eq == null) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: unsupported column predicate "
                    + p.getClass().getName());
            }
            b.addFilter(
                new PlannerRequest.Filter(
                    new PlannerRequest.Column(colTableAlias, colName),
                    eq.getValue()));
        }

        // 2) Measures.
        List<Segment> segments = gs.getSegments();
        if (segments.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: no segments (no measures)");
        }
        for (int i = 0; i < segments.size(); i++) {
            RolapStar.Measure m = segments.get(i).aggMeasure;
            if (m.getTable() != factTable) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: measure not on fact table: "
                    + m.getName());
            }
            RolapSchema.PhysColumn mexpr = m.getExpression();
            if (!(mexpr instanceof RolapSchema.PhysRealColumn)) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: non-real measure expression "
                    + mexpr);
            }
            PlannerRequest.AggFn fn = mapAggregator(m.getAggregator());
            String mcol = ((RolapSchema.PhysRealColumn) mexpr).name;
            b.addMeasure(
                new PlannerRequest.Measure(
                    fn,
                    new PlannerRequest.Column(factTable.getAlias(), mcol),
                    "m" + i));
        }

        return b.build();
    }

    private static void addSingleHopJoin(
        PlannerRequest.Builder b,
        RolapStar.Table factTable,
        RolapStar.Table dimTable)
    {
        RolapSchema.PhysPath path = dimTable.getPath();
        if (path.hopList.size() != 2) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: only single-hop dimension joins "
                + "supported; path length=" + path.hopList.size());
        }
        RolapSchema.PhysHop lastHop = path.hopList.get(1);
        RolapSchema.PhysLink link = lastHop.link;
        if (link == null) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: null link on dim hop");
        }
        List<RolapSchema.PhysColumn> fkCols = link.getColumnList();
        if (fkCols.size() != 1) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: composite join keys not supported");
        }
        RolapSchema.PhysColumn fkCol = fkCols.get(0);
        if (!(fkCol instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: non-real FK column");
        }
        RolapSchema.PhysKey srcKey = link.getSourceKey();
        List<RolapSchema.PhysColumn> pkCols = srcKey.getColumnList();
        if (pkCols.size() != 1) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: composite PK keys not supported");
        }
        RolapSchema.PhysColumn pkCol = pkCols.get(0);
        if (!(pkCol instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: non-real PK column");
        }
        // Verify the FK really lives on the fact table (target of link).
        if (link.targetRelation != factTable.getRelation()) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: multi-hop / snowflake join not supported");
        }
        if (srcKey.getRelation() != dimTable.getRelation()) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: dim relation mismatch");
        }
        String factKey = ((RolapSchema.PhysRealColumn) fkCol).name;
        String dimKey = ((RolapSchema.PhysRealColumn) pkCol).name;
        b.addJoin(
            new PlannerRequest.Join(
                dimTable.getAlias(), factKey, dimKey));
    }

    /**
     * Unwraps a predicate to a single {@link ValueColumnPredicate} when it
     * represents an equality constraint against one value, or null
     * otherwise. Handles the common case of a
     * {@link ListColumnPredicate} wrapping a single value (legacy emits
     * "x = v" for that, not "x in (v)").
     */
    private static ValueColumnPredicate asSingleValuePredicate(
        StarColumnPredicate p)
    {
        if (p instanceof ValueColumnPredicate) {
            return (ValueColumnPredicate) p;
        }
        if (p instanceof ListColumnPredicate) {
            ListColumnPredicate lp = (ListColumnPredicate) p;
            if (lp.getPredicates().size() == 1
                && lp.getPredicates().get(0) instanceof ValueColumnPredicate)
            {
                return (ValueColumnPredicate) lp.getPredicates().get(0);
            }
        }
        return null;
    }

    private static String realTableName(RolapStar.Table t) {
        RolapSchema.PhysRelation rel = t.getRelation();
        if (rel instanceof RolapSchema.PhysTable) {
            return ((RolapSchema.PhysTable) rel).getName();
        }
        return null;
    }

    private static PlannerRequest.AggFn mapAggregator(RolapAggregator agg) {
        if (agg == RolapAggregator.Sum) {
            return PlannerRequest.AggFn.SUM;
        }
        if (agg == RolapAggregator.Count) {
            return PlannerRequest.AggFn.COUNT;
        }
        if (agg == RolapAggregator.Min) {
            return PlannerRequest.AggFn.MIN;
        }
        if (agg == RolapAggregator.Max) {
            return PlannerRequest.AggFn.MAX;
        }
        if (agg == RolapAggregator.Avg) {
            return PlannerRequest.AggFn.AVG;
        }
        throw new UnsupportedTranslation(
            "fromSegmentLoad: unsupported aggregator " + agg.getName());
    }

    /** Increments the unsupported-fallback counter. */
    public static void recordFallback() {
        UNSUPPORTED_FALLBACK_COUNT.incrementAndGet();
    }

    /** Increments the segment-load fallback counter (used when the
     *  planner itself throws after a successful translation attempt,
     *  so the caller falls back to legacy SQL). */
    public static void recordSegmentLoadFallback() {
        SEGMENT_LOAD_FALLBACK_COUNT.incrementAndGet();
        UNSUPPORTED_FALLBACK_COUNT.incrementAndGet();
    }

    /** Count of fallbacks that happened specifically on segment-load
     *  dispatch (fromSegmentLoad). */
    public static long segmentLoadFallbackCount() {
        return SEGMENT_LOAD_FALLBACK_COUNT.get();
    }

    /** Count of fallbacks that happened specifically on tuple-read
     *  dispatch (fromTupleRead). */
    public static long tupleReadFallbackCount() {
        return TUPLE_READ_FALLBACK_COUNT.get();
    }

    /** Total number of times a translation fell back to legacy. */
    public static long unsupportedFallbackCount() {
        return UNSUPPORTED_FALLBACK_COUNT.get();
    }

    /** Reset the fallback counter (test-only). */
    public static void resetFallbackCount() {
        UNSUPPORTED_FALLBACK_COUNT.set(0L);
        SEGMENT_LOAD_FALLBACK_COUNT.set(0L);
        TUPLE_READ_FALLBACK_COUNT.set(0L);
    }
}

// End CalcitePlannerAdapters.java
