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

import mondrian.rolap.DefaultTupleConstraint;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapAttribute;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.GroupingSet;
import mondrian.rolap.agg.GroupingSetsList;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.LiteralColumnPredicate;
import mondrian.rolap.agg.MinusStarPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.PredicateColumn;
import mondrian.rolap.agg.Segment;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.rolap.sql.TupleConstraint;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridge between Mondrian-internal SQL-build contexts (e.g.
 * {@code SqlTupleReader.Target}, cell-request groups) and the
 * backend-neutral {@link PlannerRequest}.
 *
 * <p>Worktree #1 scope: the translation surface here is intentionally
 * minimal. The {@code SqlTupleReader} dispatch wires up the routing seam,
 * but actual translation coverage grows in later worktrees. When a shape
 * cannot be translated, methods throw {@link UnsupportedTranslation}.
 *
 * <p>Under {@code backend=calcite} there is no fallback: the exception
 * propagates to the caller and the query (or test) fails. This is
 * deliberate — once worktree #4 deletes {@code SqlQuery} and the legacy
 * dialects there is no fallback to fall back to, and production deployments
 * cannot silently depend on which shapes the translator happens to cover.
 *
 * <p>The {@link AtomicLong} counters below remain as pure observability:
 * they record the number of shapes that failed translation in this run,
 * for tests to observe (e.g. "translation must have succeeded zero times
 * on this code path") — not as a fallback signal.
 */
public final class CalcitePlannerAdapters {

    private CalcitePlannerAdapters() {}

    private static final AtomicLong UNSUPPORTED_COUNT =
        new AtomicLong();

    /**
     * Per-dispatch-surface unsupported counters. Lets tests assert that
     * e.g. segment-load translation reached coverage while tuple-read is
     * still deferred. The aggregate {@link #UNSUPPORTED_COUNT} stays as a
     * single-number signal.
     */
    private static final AtomicLong SEGMENT_LOAD_UNSUPPORTED_COUNT =
        new AtomicLong();
    private static final AtomicLong TUPLE_READ_UNSUPPORTED_COUNT =
        new AtomicLong();
    private static final AtomicLong CARDINALITY_PROBE_UNSUPPORTED_COUNT =
        new AtomicLong();

    /**
     * Legacy opaque entry-point — always throws
     * {@link UnsupportedTranslation}. Callers that carry typed context
     * should prefer {@link #fromTupleRead(List, TupleConstraint)}.
     *
     * <p>The method exists now so the dispatch in
     * {@code SqlTupleReader.prepareTuples} is wired through a stable seam
     * — flipping translation on for a given shape is a one-file change
     * here, not a re-plumbing of the reader.
     */
    public static PlannerRequest fromTupleRead(Object tupleReadContext) {
        TUPLE_READ_UNSUPPORTED_COUNT.incrementAndGet();
        UNSUPPORTED_COUNT.incrementAndGet();
        throw new UnsupportedTranslation(
            "CalcitePlannerAdapters.fromTupleRead: opaque tuple-read "
            + "context (" + (tupleReadContext == null ? "null"
                : tupleReadContext.getClass().getName()) + ") not "
            + "supported; pass a typed (levels, constraint) pair instead.");
    }

    /**
     * Typed tuple-read entry-point. Worktree-#1 Task E: covers the
     * single-level, single-table member-list shape emitted by Mondrian
     * schema-init:
     * <pre>
     *   select distinct "t"."k" [, "t"."name", "t"."caption"]
     *   from "t" as "t"
     *   order by "t"."k" ASC NULLS LAST
     * </pre>
     *
     * <p>Explicitly rejects — with a message that surfaces in downstream
     * shopping-list reports:
     * <ul>
     *   <li>multi-target crossjoins ({@code levels.size() != 1});</li>
     *   <li>any {@link TupleConstraint} other than
     *       {@link DefaultTupleConstraint} (i.e. SqlConstraint-carrying
     *       filters, context, member-key restrictions);</li>
     *   <li>snowflake hierarchies (key column not on a single
     *       {@link RolapSchema.PhysTable});</li>
     *   <li>composite keys (more than one key column);</li>
     *   <li>parent-child levels (parentAttribute set);</li>
     *   <li>non-real key expressions ({@link RolapSchema.PhysCalcColumn}).</li>
     * </ul>
     *
     * @throws UnsupportedTranslation when the shape is outside the
     *     supported subset.
     */
    public static PlannerRequest fromTupleRead(
        List<RolapCubeLevel> levels, TupleConstraint constraint)
    {
        try {
            return translateTupleRead(levels, constraint);
        } catch (UnsupportedTranslation ex) {
            TUPLE_READ_UNSUPPORTED_COUNT.incrementAndGet();
            UNSUPPORTED_COUNT.incrementAndGet();
            throw ex;
        }
    }

    private static PlannerRequest translateTupleRead(
        List<RolapCubeLevel> levels, TupleConstraint constraint)
    {
        if (levels == null || levels.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: empty levels list");
        }
        if (levels.size() != 1) {
            throw new UnsupportedTranslation(
                "fromTupleRead: multi-target crossjoin not yet supported "
                + "(levels.size=" + levels.size() + ")");
        }
        if (!(constraint instanceof DefaultTupleConstraint)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: non-default TupleConstraint not yet "
                + "supported (got "
                + (constraint == null
                    ? "null"
                    : constraint.getClass().getName())
                + ")");
        }
        RolapCubeLevel level = levels.get(0);
        if (level.isAll()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: all-level read not yet supported");
        }
        if (level.isParentChild()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: parent-child hierarchy not yet supported");
        }
        if (level.getParentAttribute() != null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: parent-attribute hierarchy not yet "
                + "supported");
        }
        RolapAttribute attribute = level.getAttribute();
        List<RolapSchema.PhysColumn> keyList = attribute.getKeyList();
        if (keyList == null || keyList.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: level has no key columns");
        }
        if (keyList.size() != 1) {
            throw new UnsupportedTranslation(
                "fromTupleRead: composite-key level not yet supported "
                + "(keyList.size=" + keyList.size() + ")");
        }
        RolapSchema.PhysColumn keyCol = keyList.get(0);
        if (!(keyCol instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: non-real key expression "
                + keyCol.getClass().getName());
        }
        RolapSchema.PhysRelation relation = keyCol.relation;
        if (!(relation instanceof RolapSchema.PhysTable)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: snowflake hierarchy / non-table relation "
                + "not yet supported ("
                + relation.getClass().getName() + ")");
        }
        RolapSchema.PhysTable table = (RolapSchema.PhysTable) relation;
        String tableName = table.getName();
        String tableAlias = table.getAlias();

        // Assemble projections in the same order as addLevelMemberSql:
        // (1) orderBy columns, (2) key columns, (3) nameExp, (4) captionExp.
        // Columns that duplicate an earlier projection (same table+name) are
        // skipped — matching the legacy `layoutBuilder.lookup` dedup.
        PlannerRequest.Builder b = PlannerRequest.builder(tableName);
        Set<String> seen = new LinkedHashSet<>();

        // Order-by list.
        PlannerRequest.Column firstKeyCol = null;
        for (RolapSchema.PhysColumn o : attribute.getOrderByList()) {
            PlannerRequest.Column c = asProjection(o, tableAlias, "order-by");
            if (seen.add(c.name)) {
                b.addProjection(c);
            }
            // order-by clause: sort on this column in ASC order, nulls last
            // — mirrors the legacy emission (collateNullsLast=true).
            b.addOrderBy(new PlannerRequest.OrderBy(
                c, PlannerRequest.Order.ASC));
        }

        // Key column(s).
        PlannerRequest.Column keyProj = asProjection(keyCol, tableAlias, "key");
        if (seen.add(keyProj.name)) {
            b.addProjection(keyProj);
        }
        firstKeyCol = keyProj;

        // Name expression (optional).
        RolapSchema.PhysColumn nameExp = attribute.getNameExp();
        if (nameExp != null) {
            PlannerRequest.Column nameProj =
                asProjection(nameExp, tableAlias, "name");
            if (seen.add(nameProj.name)) {
                b.addProjection(nameProj);
            }
        }

        // Caption expression (optional).
        RolapSchema.PhysColumn captionExp = attribute.getCaptionExp();
        if (captionExp != null) {
            PlannerRequest.Column capProj =
                asProjection(captionExp, tableAlias, "caption");
            if (seen.add(capProj.name)) {
                b.addProjection(capProj);
            }
        }

        // If there were no order-by columns in the attribute, legacy still
        // emits an ORDER BY on the key column (via SELECT_GROUP_ORDER /
        // SELECT_ORDER clauses in addLevelMemberSql). Mirror that.
        if (attribute.getOrderByList().isEmpty() && firstKeyCol != null) {
            b.addOrderBy(new PlannerRequest.OrderBy(
                firstKeyCol, PlannerRequest.Order.ASC));
        }

        b.distinct(true);
        return b.build();
    }

    private static PlannerRequest.Column asProjection(
        RolapSchema.PhysColumn col, String tableAlias, String role)
    {
        if (!(col instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: non-real " + role + " expression "
                + col.getClass().getName());
        }
        if (col.relation == null
            || !tableAlias.equals(col.relation.getAlias()))
        {
            throw new UnsupportedTranslation(
                "fromTupleRead: " + role + " column on different relation "
                + "(expected alias=" + tableAlias + ", got "
                + (col.relation == null ? "null" : col.relation.getAlias())
                + ")");
        }
        return new PlannerRequest.Column(tableAlias, col.name);
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
            SEGMENT_LOAD_UNSUPPORTED_COUNT.incrementAndGet();
            UNSUPPORTED_COUNT.incrementAndGet();
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
            SEGMENT_LOAD_UNSUPPORTED_COUNT.incrementAndGet();
            UNSUPPORTED_COUNT.incrementAndGet();
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

            // Per-column predicate → filter(s).
            StarColumnPredicate p = predicates[i];
            if (p == null) {
                continue;
            }
            PlannerRequest.Column filterCol =
                new PlannerRequest.Column(colTableAlias, colName);
            if (addColumnPredicateFilters(b, filterCol, p)) {
                // universalFalse wins; short-circuit all remaining work.
                b.universalFalse(true);
            }
        }

        // 1b) Compound predicates (AND/OR across columns). Translated as
        //    per-child filters appended to the request. We route each leaf
        //    predicate's column back to its table alias via the RolapStar.
        if (compoundPredicateList != null
            && !compoundPredicateList.isEmpty())
        {
            // Ensure joins are added for any dim table referenced by a
            // compound predicate before filters are emitted against it.
            java.util.Set<RolapStar.Table> compoundTables =
                collectCompoundTables(compoundPredicateList, star);
            for (RolapStar.Table t : compoundTables) {
                if (t != factTable && joinedAliases.add(t.getAlias())) {
                    addSingleHopJoin(b, factTable, t);
                }
            }
            for (StarPredicate sp : compoundPredicateList) {
                if (addCompoundFilters(b, sp)) {
                    b.universalFalse(true);
                }
            }
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
     * Translates a per-grouping-column {@link StarColumnPredicate} into one
     * or more filters on the given column and appends them to the builder.
     *
     * <p>Returns {@code true} when the predicate reduces to FALSE (no rows
     * can match) — the caller should flip {@code universalFalse}.
     *
     * <p>Supported shapes:
     * <ul>
     *   <li>{@link ValueColumnPredicate} → single EQ filter.</li>
     *   <li>{@link LiteralColumnPredicate} TRUE → no filter contribution.
     *       FALSE → universalFalse.</li>
     *   <li>{@link ListColumnPredicate} with N value children → IN-style
     *       filter (OR-of-equalities at render time). Single-value list
     *       collapses to EQ. Empty list → universalFalse.</li>
     * </ul>
     */
    static boolean addColumnPredicateFilters(
        PlannerRequest.Builder b,
        PlannerRequest.Column col,
        StarColumnPredicate p)
    {
        if (p instanceof ValueColumnPredicate) {
            b.addFilter(
                new PlannerRequest.Filter(
                    col, ((ValueColumnPredicate) p).getValue()));
            return false;
        }
        if (p instanceof LiteralColumnPredicate) {
            boolean v = ((LiteralColumnPredicate) p).getValue();
            return !v; // TRUE → no filter; FALSE → universalFalse
        }
        if (p instanceof ListColumnPredicate) {
            ListColumnPredicate lp = (ListColumnPredicate) p;
            List<StarColumnPredicate> kids = lp.getPredicates();
            if (kids.isEmpty()) {
                return true; // empty OR = never matches
            }
            List<Object> literals = new java.util.ArrayList<>(kids.size());
            for (StarColumnPredicate kid : kids) {
                if (!(kid instanceof ValueColumnPredicate)) {
                    throw new UnsupportedTranslation(
                        "fromSegmentLoad: ListColumnPredicate child is not "
                        + "a ValueColumnPredicate: "
                        + kid.getClass().getName());
                }
                literals.add(((ValueColumnPredicate) kid).getValue());
            }
            if (literals.size() == 1) {
                b.addFilter(new PlannerRequest.Filter(col, literals.get(0)));
            } else {
                b.addFilter(
                    new PlannerRequest.Filter(
                        col, PlannerRequest.Operator.IN, literals));
            }
            return false;
        }
        if (p instanceof MinusStarPredicate) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: MinusStarPredicate not yet supported");
        }
        throw new UnsupportedTranslation(
            "fromSegmentLoad: unsupported column predicate "
            + p.getClass().getName());
    }

    /**
     * Translates a compound {@link StarPredicate} (AndPredicate /
     * OrPredicate / bare column predicate) and appends filter
     * contributions to the builder. Returns {@code true} when the
     * predicate reduces to FALSE.
     *
     * <p>An {@link AndPredicate} with N children contributes a conjunction:
     * each child is translated independently and the resulting filters are
     * AND-ed together (which is what successive {@code b.filter(...)} calls
     * already do on the Calcite side).
     *
     * <p>{@link OrPredicate} across columns is rejected — single-column OR
     * is already modelled by {@link ListColumnPredicate}, and true
     * cross-column disjunction would require a different filter shape on
     * {@link PlannerRequest}.
     */
    static boolean addCompoundFilters(
        PlannerRequest.Builder b, StarPredicate sp)
    {
        if (sp instanceof StarColumnPredicate) {
            StarColumnPredicate cp = (StarColumnPredicate) sp;
            PredicateColumn pc = cp.getColumn();
            PlannerRequest.Column col = columnForPredicate(pc);
            return addColumnPredicateFilters(b, col, cp);
        }
        if (sp instanceof AndPredicate) {
            boolean anyFalse = false;
            for (StarPredicate child : ((AndPredicate) sp).getChildren()) {
                anyFalse |= addCompoundFilters(b, child);
            }
            return anyFalse;
        }
        if (sp instanceof OrPredicate) {
            OrPredicate or = (OrPredicate) sp;
            // Single-column OR is semantically equivalent to a
            // ListColumnPredicate; detect via the column bit-key.
            List<PredicateColumn> cols = or.getColumnList();
            if (cols.size() == 1
                && allChildrenAreValueOnSameColumn(or, cols.get(0)))
            {
                PlannerRequest.Column col = columnForPredicate(cols.get(0));
                List<Object> literals = new java.util.ArrayList<>();
                for (StarPredicate child : or.getChildren()) {
                    literals.add(
                        ((ValueColumnPredicate) child).getValue());
                }
                if (literals.isEmpty()) {
                    return true;
                }
                if (literals.size() == 1) {
                    b.addFilter(
                        new PlannerRequest.Filter(col, literals.get(0)));
                } else {
                    b.addFilter(
                        new PlannerRequest.Filter(
                            col, PlannerRequest.Operator.IN, literals));
                }
                return false;
            }
            throw new UnsupportedTranslation(
                "fromSegmentLoad: OrPredicate across columns not yet "
                + "supported (cols=" + cols.size() + ")");
        }
        if (sp instanceof MinusStarPredicate) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: MinusStarPredicate not yet supported");
        }
        throw new UnsupportedTranslation(
            "fromSegmentLoad: unsupported compound predicate "
            + sp.getClass().getName());
    }

    private static boolean allChildrenAreValueOnSameColumn(
        OrPredicate or, PredicateColumn pc)
    {
        for (StarPredicate child : or.getChildren()) {
            if (!(child instanceof ValueColumnPredicate)) {
                return false;
            }
            if (((ValueColumnPredicate) child).getColumn() != pc) {
                return false;
            }
        }
        return true;
    }

    private static PlannerRequest.Column columnForPredicate(PredicateColumn pc)
    {
        RolapSchema.PhysColumn phys = pc.physColumn;
        if (!(phys instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: compound predicate on non-real column "
                + phys);
        }
        String alias = phys.relation == null ? null : phys.relation.getAlias();
        return new PlannerRequest.Column(
            alias, ((RolapSchema.PhysRealColumn) phys).name);
    }

    /**
     * Collects the set of {@link RolapStar.Table} instances referenced by
     * the columns in a compound-predicate list. Used to ensure joins are
     * added for any dim table the compound predicates filter on.
     */
    private static java.util.Set<RolapStar.Table> collectCompoundTables(
        List<StarPredicate> predicates, RolapStar star)
    {
        java.util.Set<RolapStar.Table> out = new java.util.LinkedHashSet<>();
        for (StarPredicate sp : predicates) {
            for (PredicateColumn pc : sp.getColumnList()) {
                RolapStar.Table t = findStarTable(
                    star.getFactTable(), pc.physColumn.relation);
                if (t == null) {
                    throw new UnsupportedTranslation(
                        "fromSegmentLoad: compound predicate references "
                        + "column on unknown relation "
                        + pc.physColumn.relation);
                }
                out.add(t);
            }
        }
        return out;
    }

    private static RolapStar.Table findStarTable(
        RolapStar.Table root, RolapSchema.PhysRelation rel)
    {
        if (root.getRelation() == rel) {
            return root;
        }
        for (RolapStar.Table child : root.getChildren()) {
            RolapStar.Table hit = findStarTable(child, rel);
            if (hit != null) {
                return hit;
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

    /**
     * Translate a single-column cardinality probe
     * (<code>select count(distinct "col") from "schema"."table"</code>)
     * into a {@link PlannerRequest}.
     *
     * <p>Third dispatch seam (Task C) — mirrors segment-load / tuple-read
     * wiring. The probe shape is trivially translatable: one measure
     * (COUNT DISTINCT), zero group-by, no filter, no join. We only reject
     * shapes that would confuse the Calcite schema lookup — namely a
     * non-empty DB-schema qualifier that doesn't match the one the
     * per-DataSource {@link CalciteMondrianSchema} was built against.
     *
     * @param schema table's DB schema, or null for unqualified/default
     * @param table table name (required)
     * @param column column whose cardinality is being probed (required)
     * @throws UnsupportedTranslation if the shape cannot be translated
     */
    public static PlannerRequest fromCardinalityProbe(
        String schema, String table, String column)
    {
        try {
            return translateCardinalityProbe(schema, table, column);
        } catch (UnsupportedTranslation ex) {
            CARDINALITY_PROBE_UNSUPPORTED_COUNT.incrementAndGet();
            UNSUPPORTED_COUNT.incrementAndGet();
            throw ex;
        }
    }

    private static PlannerRequest translateCardinalityProbe(
        String schema, String table, String column)
    {
        if (table == null || table.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromCardinalityProbe: null/empty table");
        }
        if (column == null || column.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromCardinalityProbe: null/empty column");
        }
        // Worktree-#1 schemas are unqualified (or resolved by the per-
        // DataSource JdbcSchema reflection in CalciteMondrianSchema). If a
        // non-empty DB-schema qualifier ever appears, bail — the legacy
        // string handles it correctly, and probing the right resolution
        // story for multi-schema setups belongs in a later worktree.
        if (schema != null && !schema.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromCardinalityProbe: qualified schema '" + schema
                + "' not yet supported");
        }
        PlannerRequest.Builder b = PlannerRequest.builder(table);
        b.addMeasure(
            new PlannerRequest.Measure(
                PlannerRequest.AggFn.COUNT,
                new PlannerRequest.Column(null, column),
                "c",
                true));
        return b.build();
    }

    /** Count of shapes rejected on cardinality-probe dispatch
     *  (fromCardinalityProbe) — pure observability, not a fallback signal. */
    public static long cardinalityProbeUnsupportedCount() {
        return CARDINALITY_PROBE_UNSUPPORTED_COUNT.get();
    }

    /** Count of shapes rejected on segment-load dispatch (fromSegmentLoad)
     *  — pure observability, not a fallback signal. */
    public static long segmentLoadUnsupportedCount() {
        return SEGMENT_LOAD_UNSUPPORTED_COUNT.get();
    }

    /** Count of shapes rejected on tuple-read dispatch (fromTupleRead)
     *  — pure observability, not a fallback signal. */
    public static long tupleReadUnsupportedCount() {
        return TUPLE_READ_UNSUPPORTED_COUNT.get();
    }

    /** Total number of shapes rejected by the translator across every
     *  dispatch site in this run. Under backend=calcite each increment
     *  corresponds to an UnsupportedTranslation that propagated to the
     *  caller (no fallback). */
    public static long unsupportedCount() {
        return UNSUPPORTED_COUNT.get();
    }

    /** Reset the unsupported counters (test-only). */
    public static void resetUnsupportedCount() {
        UNSUPPORTED_COUNT.set(0L);
        SEGMENT_LOAD_UNSUPPORTED_COUNT.set(0L);
        TUPLE_READ_UNSUPPORTED_COUNT.set(0L);
        CARDINALITY_PROBE_UNSUPPORTED_COUNT.set(0L);
    }
}

// End CalcitePlannerAdapters.java
