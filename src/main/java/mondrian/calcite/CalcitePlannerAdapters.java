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
        if (!(constraint instanceof DefaultTupleConstraint)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: non-default TupleConstraint not yet "
                + "supported (got "
                + (constraint == null
                    ? "null"
                    : constraint.getClass().getName())
                + ")");
        }
        if (levels.size() > 2) {
            throw new UnsupportedTranslation(
                "fromTupleRead: multi-target crossjoin with >2 targets not "
                + "yet supported (levels.size=" + levels.size() + ")");
        }

        // Pre-validate every target and compute its table binding. An
        // unsupported-shape on any target throws with a composite message
        // that surfaces the offender.
        TargetShape[] shapes = new TargetShape[levels.size()];
        for (int i = 0; i < levels.size(); i++) {
            try {
                shapes[i] = shapeFor(levels.get(i));
            } catch (UnsupportedTranslation ex) {
                if (levels.size() == 1) {
                    throw ex;
                }
                throw new UnsupportedTranslation(
                    "fromTupleRead: target[" + i + "] unsupported — "
                    + ex.getMessage());
            }
        }

        // Root-table is the first target's dim table; any additional
        // targets are added as joins (CROSS when the dim table differs,
        // nothing when they share a table — rare, but snowflake-to-same-
        // base can happen).
        TargetShape first = shapes[0];
        PlannerRequest.Builder b = PlannerRequest.builder(first.tableName);
        Set<String> seen = new LinkedHashSet<>();
        Set<String> crossJoined = new LinkedHashSet<>();
        crossJoined.add(first.tableAlias);

        emitTargetProjections(b, seen, first);
        for (int i = 1; i < shapes.length; i++) {
            TargetShape t = shapes[i];
            if (!crossJoined.contains(t.tableAlias)) {
                b.addJoin(PlannerRequest.Join.cross(t.tableName));
                crossJoined.add(t.tableAlias);
            }
            emitTargetProjections(b, seen, t);
        }

        b.distinct(true);
        return b.build();
    }

    /** Pre-computed per-target binding: resolved table + attribute refs.
     *  Shared by the single-target and multi-target paths. */
    private static final class TargetShape {
        final RolapCubeLevel level;
        final RolapAttribute attribute;
        final RolapSchema.PhysTable table;
        final String tableName;
        final String tableAlias;
        TargetShape(
            RolapCubeLevel level,
            RolapAttribute attribute,
            RolapSchema.PhysTable table)
        {
            this.level = level;
            this.attribute = attribute;
            this.table = table;
            this.tableName = table.getName();
            this.tableAlias = table.getAlias();
        }
    }

    private static TargetShape shapeFor(RolapCubeLevel level) {
        if (level == null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: null level in targets");
        }
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
        // Composite keys are supported (Task H) — the key columns must all
        // live on the same table as the rest of the attribute, though.
        RolapSchema.PhysRelation relation = keyList.get(0).relation;
        if (!(relation instanceof RolapSchema.PhysTable)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: snowflake hierarchy / non-table relation "
                + "not yet supported ("
                + (relation == null ? "null" : relation.getClass().getName())
                + ")");
        }
        RolapSchema.PhysTable table = (RolapSchema.PhysTable) relation;
        // All key columns must share the same relation (otherwise we'd have
        // a snowflake-like composite, which today's single-table path does
        // not express).
        for (RolapSchema.PhysColumn kc : keyList) {
            if (!(kc instanceof RolapSchema.PhysRealColumn)) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: non-real key expression "
                    + kc.getClass().getName());
            }
            if (kc.relation != relation) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: composite key spans multiple relations "
                    + "(expected " + relation.getAlias() + ", got "
                    + (kc.relation == null ? "null" : kc.relation.getAlias())
                    + ")");
            }
        }
        return new TargetShape(level, attribute, table);
    }

    /**
     * Emits a single target's projections + order-by into the shared
     * builder. Projection order mirrors legacy {@code addLevelMemberSql}:
     * order-by columns, then key columns (full list), then nameExp, then
     * captionExp. Duplicate columns (same alias+name) are skipped so the
     * cell-set column layout matches the legacy shape for shared columns.
     */
    private static void emitTargetProjections(
        PlannerRequest.Builder b,
        Set<String> seen,
        TargetShape t)
    {
        String tableAlias = t.tableAlias;
        RolapAttribute attribute = t.attribute;

        // 1) Order-by columns.
        for (RolapSchema.PhysColumn o : attribute.getOrderByList()) {
            PlannerRequest.Column c = asProjection(o, tableAlias, "order-by");
            if (seen.add(tableAlias + "." + c.name)) {
                b.addProjection(c);
            }
            b.addOrderBy(
                new PlannerRequest.OrderBy(c, PlannerRequest.Order.ASC));
        }

        // 2) Key columns (full list — composite keys emit every key
        // column, and every one contributes to the ORDER BY so cell-set
        // key ordering matches legacy).
        PlannerRequest.Column firstKeyCol = null;
        boolean attributeHasOrderBy = !attribute.getOrderByList().isEmpty();
        for (RolapSchema.PhysColumn kc : attribute.getKeyList()) {
            PlannerRequest.Column kp = asProjection(kc, tableAlias, "key");
            if (seen.add(tableAlias + "." + kp.name)) {
                b.addProjection(kp);
            }
            if (firstKeyCol == null) {
                firstKeyCol = kp;
            }
            // Legacy addLevelMemberSql flags every key column with
            // SELECT_GROUP_ORDER / SELECT_ORDER when there is no explicit
            // order-by, making each key column part of the ORDER BY list.
            if (!attributeHasOrderBy) {
                b.addOrderBy(
                    new PlannerRequest.OrderBy(
                        kp, PlannerRequest.Order.ASC));
            }
        }

        // 3) Name expression (optional).
        RolapSchema.PhysColumn nameExp = attribute.getNameExp();
        if (nameExp != null) {
            PlannerRequest.Column nameProj =
                asProjection(nameExp, tableAlias, "name");
            if (seen.add(tableAlias + "." + nameProj.name)) {
                b.addProjection(nameProj);
            }
        }

        // 4) Caption expression (optional).
        RolapSchema.PhysColumn captionExp = attribute.getCaptionExp();
        if (captionExp != null) {
            PlannerRequest.Column capProj =
                asProjection(captionExp, tableAlias, "caption");
            if (seen.add(tableAlias + "." + capProj.name)) {
                b.addProjection(capProj);
            }
        }
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
        // if two grouping columns share the same dim. For snowflake
        // multi-hop chains (Task I) this holds every intermediate edge we
        // have already stitched into the request.
        java.util.Set<String> joinedAliases = new java.util.LinkedHashSet<>();
        joinedAliases.add(factTable.getAlias());

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

            // If the column hangs off a dim table, ensure every edge from
            // the fact to that table has been stitched into the request.
            // Task I: snowflake multi-hop chains (path length > 2).
            if (colTable != factTable) {
                ensureJoinedChain(b, factTable, colTable, joinedAliases);
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
                if (t != factTable) {
                    ensureJoinedChain(b, factTable, t, joinedAliases);
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
            AggOp op = mapAggregator(m.getAggregator());
            String mcol = ((RolapSchema.PhysRealColumn) mexpr).name;
            b.addMeasure(
                new PlannerRequest.Measure(
                    op.fn,
                    new PlannerRequest.Column(factTable.getAlias(), mcol),
                    "m" + i,
                    op.distinct));
        }

        return b.build();
    }

    /**
     * Stitch every join edge from {@code factTable} to {@code leaf} into
     * the request, in fact→leaf order, skipping edges already emitted.
     *
     * <p>Walks {@link RolapStar.Table#getParentTable()} from {@code leaf}
     * up to {@code factTable}, then replays the chain downward so the
     * renderer sees fact first, then each intermediate dim in attachment
     * order. Each edge is translated from the {@link RolapSchema.PhysLink}
     * on that intermediate table's last {@link RolapSchema.PhysHop}.
     *
     * <p>For the first-hop edge (child of fact), the emitted Join keeps
     * {@code leftTable == null} — back-compat with single-hop callers and
     * with the renderer's fact-rooted {@code b.field(2, 0, ...)} lookup.
     * For every subsequent edge, {@code leftTable} is set to the parent
     * table's alias so the renderer can disambiguate columns that share
     * names across the chain (e.g. {@code product_class_id}).
     */
    private static void ensureJoinedChain(
        PlannerRequest.Builder b,
        RolapStar.Table factTable,
        RolapStar.Table leaf,
        java.util.Set<String> joinedAliases)
    {
        // Collect fact → leaf chain (excluding fact itself).
        java.util.Deque<RolapStar.Table> chain = new java.util.ArrayDeque<>();
        for (RolapStar.Table t = leaf; t != null && t != factTable;
             t = t.getParentTable())
        {
            chain.push(t);
        }
        if (chain.isEmpty()) {
            return;
        }
        // Sanity: the top of the chain's parent must be the fact (the
        // walk terminated on `t == factTable`). If it didn't, we never
        // reached the fact — bail.
        RolapStar.Table top = chain.peek();
        if (top.getParentTable() != factTable) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: dim table " + leaf.getAlias()
                + " does not descend from fact "
                + factTable.getAlias());
        }

        RolapStar.Table prev = factTable;
        while (!chain.isEmpty()) {
            RolapStar.Table next = chain.pop();
            String nextAlias = next.getAlias();
            if (!joinedAliases.add(nextAlias)) {
                prev = next;
                continue;
            }
            addChainEdge(b, prev, next);
            prev = next;
        }
    }

    /**
     * Emit a single chain edge: join {@code parent}'s row on LHS to
     * {@code child}'s row on RHS. The {@link RolapSchema.PhysLink} is
     * read from {@code child.getPath()}'s last hop, where
     * {@code link.targetRelation} is the FK-bearing relation (the RHS
     * from the link's perspective) and {@code link.sourceKey.relation}
     * is the PK-bearing relation.
     *
     * <p>In a classic star schema the parent is fact and the FK lives on
     * fact; in a snowflake mid-chain the parent is itself a dim that
     * bears the FK to the child dim. Either way, FK-side = link target,
     * PK-side = link source — nothing in the link itself changes.
     */
    private static void addChainEdge(
        PlannerRequest.Builder b,
        RolapStar.Table parent,
        RolapStar.Table child)
    {
        RolapSchema.PhysPath path = child.getPath();
        if (path.hopList.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: empty PhysPath on "
                + child.getAlias());
        }
        RolapSchema.PhysHop lastHop =
            path.hopList.get(path.hopList.size() - 1);
        RolapSchema.PhysLink link = lastHop.link;
        if (link == null) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: null link on dim hop for "
                + child.getAlias());
        }
        List<RolapSchema.PhysColumn> fkCols = link.getColumnList();
        if (fkCols.size() != 1) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: composite join keys not supported "
                + "(edge " + parent.getAlias() + "→" + child.getAlias()
                + ", arity=" + fkCols.size() + ")");
        }
        RolapSchema.PhysColumn fkCol = fkCols.get(0);
        if (!(fkCol instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: non-real FK column on edge "
                + parent.getAlias() + "→" + child.getAlias());
        }
        RolapSchema.PhysKey srcKey = link.getSourceKey();
        List<RolapSchema.PhysColumn> pkCols = srcKey.getColumnList();
        if (pkCols.size() != 1) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: composite PK keys not supported "
                + "(edge " + parent.getAlias() + "→" + child.getAlias()
                + ")");
        }
        RolapSchema.PhysColumn pkCol = pkCols.get(0);
        if (!(pkCol instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: non-real PK column on edge "
                + parent.getAlias() + "→" + child.getAlias());
        }
        // Figure out which side of the link is parent vs child. The FK
        // column lives on link.targetRelation; the PK column on
        // link.sourceKey.relation. Either could be parent or child
        // depending on hop direction.
        RolapSchema.PhysRelation parentRel = parent.getRelation();
        RolapSchema.PhysRelation childRel = child.getRelation();
        String fkColName = ((RolapSchema.PhysRealColumn) fkCol).name;
        String pkColName = ((RolapSchema.PhysRealColumn) pkCol).name;
        String leftKey;
        String rightKey;
        if (link.targetRelation == parentRel
            && srcKey.getRelation() == childRel)
        {
            // Parent holds the FK (e.g. fact→product: fact.product_id
            // points at product.product_id PK).
            leftKey = fkColName;
            rightKey = pkColName;
        } else if (link.targetRelation == childRel
            && srcKey.getRelation() == parentRel)
        {
            // Parent holds the PK; child holds the FK. In FoodMart this
            // is rare at the fact→dim edge but plausible mid-chain if the
            // schema is declared with the "reverse" link orientation.
            leftKey = pkColName;
            rightKey = fkColName;
        } else {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: PhysLink endpoints do not match the "
                + "parent/child RolapStar.Table pair "
                + "(edge " + parent.getAlias() + "→" + child.getAlias()
                + "; link "
                + (link.targetRelation == null
                    ? "null"
                    : link.targetRelation.getAlias())
                + "→"
                + (srcKey.getRelation() == null
                    ? "null"
                    : srcKey.getRelation().getAlias())
                + ")");
        }
        // Fact-rooted edge: keep leftTable null for back-compat with
        // single-hop callers and the renderer's b.field(2,0,...) path.
        // Snowflake mid-chain edge: set leftTable to the parent alias.
        String leftTableAlias =
            parent.getParentTable() == null ? null : parent.getAlias();
        b.addJoin(
            new PlannerRequest.Join(
                child.getAlias(),
                leftKey,
                rightKey,
                PlannerRequest.JoinKind.INNER,
                leftTableAlias));
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

    /** Pair: translated {@link PlannerRequest.AggFn} + DISTINCT flag. */
    static final class AggOp {
        final PlannerRequest.AggFn fn;
        final boolean distinct;
        AggOp(PlannerRequest.AggFn fn, boolean distinct) {
            this.fn = fn;
            this.distinct = distinct;
        }
    }

    static AggOp mapAggregator(RolapAggregator agg) {
        if (agg == RolapAggregator.Sum) {
            return new AggOp(PlannerRequest.AggFn.SUM, false);
        }
        if (agg == RolapAggregator.Count) {
            return new AggOp(PlannerRequest.AggFn.COUNT, false);
        }
        if (agg == RolapAggregator.Min) {
            return new AggOp(PlannerRequest.AggFn.MIN, false);
        }
        if (agg == RolapAggregator.Max) {
            return new AggOp(PlannerRequest.AggFn.MAX, false);
        }
        if (agg == RolapAggregator.Avg) {
            return new AggOp(PlannerRequest.AggFn.AVG, false);
        }
        if (agg == RolapAggregator.DistinctCount) {
            return new AggOp(PlannerRequest.AggFn.COUNT, true);
        }
        throw new UnsupportedTranslation(
            "fromSegmentLoad: unsupported aggregator "
            + (agg == null ? "null" : agg.getName()));
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
