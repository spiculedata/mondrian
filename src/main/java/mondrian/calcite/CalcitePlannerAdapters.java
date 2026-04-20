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

import mondrian.olap.Member;
import mondrian.rolap.DefaultTupleConstraint;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapAttribute;
import mondrian.rolap.RolapCubeDimension;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapMeasureGroup;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapNativeSet;
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
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
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
        // Handles NonEmptyCrossJoinConstraint (Task N),
        // TopCountConstraint (Task O), and FilterConstraint (Task P).
        // We gate by class-name for NECJ to avoid a reverse dependency on
        // RolapNativeCrossJoin; TopCount and Filter dispatch via instanceof
        // on their respective public inner classes.
        if (constraint instanceof RolapNativeSet.SetConstraint) {
            String cls = constraint.getClass().getName();
            boolean isNecj =
                "mondrian.rolap.RolapNativeCrossJoin$NonEmptyCrossJoinConstraint"
                    .equals(cls);
            boolean isTopCount =
                constraint instanceof
                    mondrian.rolap.RolapNativeTopCount.TopCountConstraint;
            boolean isFilter =
                constraint instanceof
                    mondrian.rolap.RolapNativeFilter.FilterConstraint;
            if (isNecj || isTopCount || isFilter) {
                return translateSetConstraintTupleRead(
                    levels, (RolapNativeSet.SetConstraint) constraint);
            }
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

        // Task L: stitch any snowflake chain joins (dim-key-table → leaf)
        // in before target projections so orphan catalog rows get filtered
        // out of DISTINCT member lists. Emitted in request order before
        // the projections so the RelBuilder stack resolves LHS correctly
        // (first edge joins leaf ↔ parent, etc).
        for (PlannerRequest.Join edge : first.dimChain) {
            b.addJoin(edge);
            crossJoined.add(edge.dimTable);
        }
        emitTargetProjections(b, seen, first);
        for (int i = 1; i < shapes.length; i++) {
            TargetShape t = shapes[i];
            if (!crossJoined.contains(t.tableAlias)) {
                b.addJoin(PlannerRequest.Join.cross(t.tableName));
                crossJoined.add(t.tableAlias);
            }
            // Emit this target's snowflake chain too (same reasoning as
            // above). Cross-target chains rarely share edges so dedup by
            // alias is a correctness no-op but keeps the renderer stable.
            for (PlannerRequest.Join edge : t.dimChain) {
                if (crossJoined.add(edge.dimTable)) {
                    b.addJoin(edge);
                }
            }
            emitTargetProjections(b, seen, t);
        }

        b.distinct(true);
        return b.build();
    }

    /**
     * Task N (worktree #2): translate a {@link RolapNativeSet.SetConstraint}
     * into a <em>fact-rooted</em> {@link PlannerRequest}. This is the shape
     * Mondrian emits for native {@code NON EMPTY CrossJoin(...)} and its
     * single-arg variant {@code NON EMPTY &lt;level&gt;.members}: the fact
     * table is the FROM root and every referenced dim table hangs off it
     * via an inner equi-join, with a GROUP BY on every projected dim
     * column so the result set enumerates tuples that appear in the fact.
     *
     * <p>Currently handles:
     * <ul>
     *   <li>{@link mondrian.rolap.RolapNativeCrossJoin.NonEmptyCrossJoinConstraint}
     *       — the simplest {@link RolapNativeSet.SetConstraint} subclass,
     *       which carries a {@link CrossJoinArg}[] and uses the context
     *       evaluator's slicer for WHERE constraints.</li>
     *   <li>Per-arg {@link MemberListCrossJoinArg} where every member lives
     *       on the arg's level — emits an EQ filter (single member) or an
     *       IN-list filter (multi-member) on the level's leaf key column.</li>
     *   <li>Per-arg {@link DescendantsCrossJoinArg} with {@code member == null}
     *       (level.members) — contributes projections + joins only, no
     *       additional filters.</li>
     * </ul>
     *
     * <p>Any other {@code CrossJoinArg} subclass — or a
     * {@code MemberListCrossJoinArg} whose members span levels, or a
     * {@code DescendantsCrossJoinArg} rooted at a real member — throws
     * {@link UnsupportedTranslation} with the concrete class name so the
     * shopping-list report surfaces the gap.
     *
     * <p>Slicer contribution: the evaluator's non-all, non-measure members
     * are each translated into an EQ filter on the member's leaf key
     * column. Members with composite keys are rejected (would need
     * TupleFilter semantics that the corpus doesn't exercise yet).
     */
    private static PlannerRequest translateSetConstraintTupleRead(
        List<RolapCubeLevel> levels,
        RolapNativeSet.SetConstraint constraint)
    {
        CrossJoinArg[] args = constraint.getArgs();
        if (args == null || args.length == 0) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint with empty CrossJoinArg[]");
        }

        // The `levels` list (from SqlTupleReader) enumerates the target
        // levels; the `args` array includes those plus any extra args that
        // safeToConstrainByOtherAxes pushed in (filter-only, no projection).
        // NECJ (the NonEmptyCrossJoinFunDef path) does NOT push extras in —
        // safeToConstrainByOtherAxes returns false for NonEmptyCrossJoinFunDef
        // — so for the currently-supported constraint subclass every arg
        // contributes one target level. We defer the "filter-only arg" case
        // (SetConstraint subclasses over safe funs like CrossJoin) to a
        // later task by rejecting when args.length != levels.size().
        if (args.length != levels.size()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint arg count " + args.length
                + " != target level count " + levels.size()
                + " (filter-only args not yet supported)");
        }

        // Evaluator + measure group — the fact table anchors the whole
        // request. SetConstraint always carries at least one measure group
        // (checkValidContext asserts this); we take the first.
        Object evalObj = constraint.getEvaluator();
        if (!(evalObj instanceof RolapEvaluator)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint evaluator is not a "
                + "RolapEvaluator (got "
                + (evalObj == null ? "null" : evalObj.getClass().getName())
                + ")");
        }
        RolapEvaluator evaluator = (RolapEvaluator) evalObj;
        List<RolapMeasureGroup> mgs = constraint.getMeasureGroupList();
        if (mgs == null || mgs.isEmpty()) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint has no measure group "
                + "(virtual cube with no applicable base cube?)");
        }
        if (mgs.size() > 1) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint spans multiple measure groups "
                + "(virtual cube UNION shape not yet supported; got "
                + mgs.size() + ")");
        }
        RolapStar star = mgs.get(0).getStar();
        RolapStar.Table factTable = star.getFactTable();
        RolapSchema.PhysRelation factRel = factTable.getRelation();
        if (!(factRel instanceof RolapSchema.PhysTable)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: fact table is not a PhysTable ("
                + (factRel == null ? "null" : factRel.getClass().getName())
                + ")");
        }
        String factName = ((RolapSchema.PhysTable) factRel).getName();
        PlannerRequest.Builder b = PlannerRequest.builder(factName);
        Set<String> joinedAliases = new LinkedHashSet<>();
        joinedAliases.add(factTable.getAlias());

        // Per-target shape pre-computation.
        TargetShape[] shapes = new TargetShape[levels.size()];
        for (int i = 0; i < levels.size(); i++) {
            try {
                shapes[i] = shapeFor(levels.get(i));
            } catch (UnsupportedTranslation ex) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: SetConstraint target[" + i
                    + "] unsupported — " + ex.getMessage());
            }
        }

        // First pass: join every dim table referenced by any target's
        // hierarchy walk. For a snowflake like Product→Product_Class we
        // need `product` joined (the target leaf) AND `product_class` (the
        // parent levels' home). Per-target ensureJoinedChain handles the
        // fact→leaf hop; walking the parent-level's column relations via
        // ensureJoinedChain picks up intermediates like product_class.
        for (int i = 0; i < shapes.length; i++) {
            TargetShape shape = shapes[i];
            for (RolapSchema.PhysRelation rel :
                collectNecjRelations(shape))
            {
                RolapStar.Table relStar = findStarTable(factTable, rel);
                if (relStar == null) {
                    throw new UnsupportedTranslation(
                        "fromTupleRead: SetConstraint target[" + i + "] "
                        + "referenced table " + rel.getAlias()
                        + " is not reachable from fact "
                        + factTable.getAlias());
                }
                if (relStar != factTable) {
                    ensureJoinedChain(
                        b, factTable, relStar, joinedAliases);
                }
            }
        }

        // Second pass: emit projections + group-by for each target in
        // target order, and each target's CrossJoinArg filter contribution.
        Set<String> projectedKeys = new LinkedHashSet<>();
        Set<String> orderedKeys = new LinkedHashSet<>();
        for (int i = 0; i < levels.size(); i++) {
            TargetShape shape = shapes[i];
            CrossJoinArg arg = args[i];
            emitNecjTargetProjections(b, projectedKeys, shape);
            addCrossJoinArgFilter(b, shape, arg);
        }

        // TopCount-only: add the sort-measure projection + primary
        // ORDER BY on that measure. Must happen after dim projections
        // (so the measure renders AFTER group-by cols, matching the
        // legacy SELECT layout) and BEFORE per-target ORDER BYs
        // (which become tiebreakers). The JDBC setMaxRows on the
        // statement trims to `topCount` rows — no SQL LIMIT needed.
        if (constraint instanceof
            mondrian.rolap.RolapNativeTopCount.TopCountConstraint)
        {
            addTopCountOrderByMeasure(
                b,
                (mondrian.rolap.RolapNativeTopCount.TopCountConstraint)
                    constraint);
        }

        // FilterConstraint: translate filterExpr into a HAVING predicate
        // on the tuple-read aggregate. See addFilterHaving() for the
        // MDX shapes accepted.
        if (constraint instanceof
            mondrian.rolap.RolapNativeFilter.FilterConstraint)
        {
            addFilterHaving(
                b,
                (mondrian.rolap.RolapNativeFilter.FilterConstraint)
                    constraint);
        }

        for (int i = 0; i < levels.size(); i++) {
            addNecjOrderBy(b, orderedKeys, shapes[i]);
        }

        // Slicer contribution: any non-all, non-measure member in the
        // evaluator context that isn't already pinned by a CrossJoinArg.
        addSlicerFilters(b, evaluator, factTable, joinedAliases, args);

        return b.build();
    }

    /**
     * Task O: translate the TopCount sort-measure expression. Accepts the
     * narrow shape exercised by the goldens — a {@link mondrian.mdx.MemberExpr}
     * whose member is a simple {@link mondrian.rolap.RolapStoredMeasure} with
     * a real {@link mondrian.rolap.RolapSchema.PhysRealColumn} expression and
     * one of {Sum, Count, Min, Max, Avg} as its aggregator. Anything else
     * (calculated measures, level-column expressions, DistinctCount which
     * needs COUNT DISTINCT wiring, non-real columns) throws
     * {@link UnsupportedTranslation} so the shopping-list surface stays
     * honest.
     *
     * <p>Emits:
     * <ol>
     *   <li>A {@link PlannerRequest.Measure} on {@code req.measures}. The
     *       alias is a stable {@code "m0"} — the renderer's post-aggregate
     *       reprojection places this AFTER the group-by columns in request
     *       order so the overall SELECT layout matches legacy's (dim keys
     *       first, then the sort measure).</li>
     *   <li>An {@link PlannerRequest.OrderBy} on that measure alias with
     *       direction taken from {@link RolapNativeTopCount.TopCountConstraint#isAscending()}.
     *       This becomes the primary sort; per-target dim ORDER BYs are
     *       appended afterwards as tiebreakers (matching legacy's order by
     *       measure-direction, then ancestor-ASC, then leaf-key-ASC).</li>
     * </ol>
     */
    private static void addTopCountOrderByMeasure(
        PlannerRequest.Builder b,
        mondrian.rolap.RolapNativeTopCount.TopCountConstraint constraint)
    {
        mondrian.olap.Exp orderByExpr = constraint.getOrderByExpr();
        if (orderByExpr == null) {
            // TopCount without an explicit sort expression (3-arg form
            // elided). Goldens always carry the third arg; rejecting here
            // keeps the surface honest.
            throw new UnsupportedTranslation(
                "fromTupleRead: TopCountConstraint has no orderBy expression "
                + "(2-arg TopCount form not yet supported)");
        }
        if (!(orderByExpr instanceof mondrian.mdx.MemberExpr)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: TopCountConstraint orderBy shape "
                + orderByExpr.getClass().getName()
                + " not yet supported (expected MemberExpr)");
        }
        mondrian.olap.Member member =
            ((mondrian.mdx.MemberExpr) orderByExpr).getMember();
        if (!(member instanceof mondrian.rolap.RolapStoredMeasure)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: TopCountConstraint orderBy member "
                + (member == null ? "null" : member.getClass().getName())
                + " is not a RolapStoredMeasure (calculated measures and "
                + "level columns not yet supported)");
        }
        mondrian.rolap.RolapStoredMeasure measure =
            (mondrian.rolap.RolapStoredMeasure) member;
        RolapSchema.PhysColumn expr = measure.getExpr();
        if (!(expr instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: TopCountConstraint measure expression "
                + (expr == null ? "null" : expr.getClass().getName())
                + " is not a real column");
        }
        PlannerRequest.AggFn fn =
            aggFnFor(measure.getAggregator());
        if (fn == null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: TopCountConstraint aggregator "
                + measure.getAggregator().getName()
                + " not yet supported");
        }
        String tableAlias =
            expr.relation == null ? null : expr.relation.getAlias();
        String colName = ((RolapSchema.PhysRealColumn) expr).name;
        String alias = "m0";
        boolean distinct =
            measure.getAggregator() == RolapAggregator.DistinctCount;
        b.addMeasure(
            new PlannerRequest.Measure(
                fn,
                new PlannerRequest.Column(tableAlias, colName),
                alias,
                distinct));
        b.addOrderBy(
            new PlannerRequest.OrderBy(
                new PlannerRequest.Column(null, alias),
                constraint.isAscending()
                    ? PlannerRequest.Order.ASC
                    : PlannerRequest.Order.DESC));
    }

    /**
     * Task P: translate a {@link mondrian.rolap.RolapNativeFilter.FilterConstraint}'s
     * filter expression into a {@link PlannerRequest.Having} predicate.
     *
     * <p>Narrow supported shape — the full corpus exercises exactly this:
     * a {@link mondrian.mdx.ResolvedFunCall} whose {@link mondrian.olap.FunDef}
     * name is one of {@code >, <, >=, <=, =, <>} (the infix binary
     * comparators), with two args:
     * <ol>
     *   <li>A {@link mondrian.mdx.MemberExpr} resolving to a
     *       {@link mondrian.rolap.RolapStoredMeasure} with a real
     *       {@link mondrian.rolap.RolapSchema.PhysRealColumn} expression
     *       and one of {Sum, Count, Min, Max, Avg, DistinctCount} as its
     *       aggregator — same constraints as the TopCount sort-measure
     *       translator (Task O).</li>
     *   <li>A {@link mondrian.olap.Literal} whose value is a {@link Number}.</li>
     * </ol>
     *
     * <p>Richer MDX filter predicates — compound boolean ({@code AND},
     * {@code OR}, {@code NOT}), arithmetic on either side, {@code IsEmpty},
     * calculated measures, two-measure comparisons — throw
     * {@link UnsupportedTranslation} with the offending expression's
     * concrete class name so the shopping-list surface stays honest.
     */
    private static void addFilterHaving(
        PlannerRequest.Builder b,
        mondrian.rolap.RolapNativeFilter.FilterConstraint constraint)
    {
        mondrian.olap.Exp filterExpr = constraint.getFilterExpr();
        if (filterExpr == null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint has no filter expression");
        }
        if (!(filterExpr instanceof mondrian.mdx.ResolvedFunCall)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint filter expression "
                + filterExpr.getClass().getName()
                + " not yet supported (expected ResolvedFunCall binary "
                + "comparison)");
        }
        mondrian.mdx.ResolvedFunCall call =
            (mondrian.mdx.ResolvedFunCall) filterExpr;
        String opName = call.getFunDef().getName();
        PlannerRequest.Comparison cmp = comparisonFor(opName);
        if (cmp == null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint operator '" + opName
                + "' not yet supported (expected one of >, <, >=, <=, "
                + "=, <>)");
        }
        mondrian.olap.Exp[] args = call.getArgs();
        if (args.length != 2) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint '" + opName
                + "' arity " + args.length + " not yet supported "
                + "(expected 2)");
        }
        // Left side: measure reference.
        if (!(args[0] instanceof mondrian.mdx.MemberExpr)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint lhs shape "
                + args[0].getClass().getName()
                + " not yet supported (expected MemberExpr over a "
                + "stored measure)");
        }
        mondrian.olap.Member member =
            ((mondrian.mdx.MemberExpr) args[0]).getMember();
        if (!(member instanceof mondrian.rolap.RolapStoredMeasure)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint lhs member "
                + (member == null ? "null" : member.getClass().getName())
                + " is not a RolapStoredMeasure (calculated measures not "
                + "yet supported)");
        }
        mondrian.rolap.RolapStoredMeasure measure =
            (mondrian.rolap.RolapStoredMeasure) member;
        RolapSchema.PhysColumn expr = measure.getExpr();
        if (!(expr instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint lhs measure expression "
                + (expr == null ? "null" : expr.getClass().getName())
                + " is not a real column");
        }
        PlannerRequest.AggFn fn = aggFnFor(measure.getAggregator());
        if (fn == null) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint lhs aggregator "
                + measure.getAggregator().getName() + " not yet supported");
        }

        // Right side: numeric literal.
        if (!(args[1] instanceof mondrian.olap.Literal)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint rhs shape "
                + args[1].getClass().getName()
                + " not yet supported (expected numeric Literal)");
        }
        Object litValue = ((mondrian.olap.Literal) args[1]).getValue();
        if (!(litValue instanceof Number)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: FilterConstraint rhs literal value "
                + (litValue == null ? "null" : litValue.getClass().getName())
                + " is not a Number");
        }

        String tableAlias =
            expr.relation == null ? null : expr.relation.getAlias();
        String colName = ((RolapSchema.PhysRealColumn) expr).name;
        boolean distinct =
            measure.getAggregator() == RolapAggregator.DistinctCount;
        PlannerRequest.Measure havingMeasure =
            new PlannerRequest.Measure(
                fn,
                new PlannerRequest.Column(tableAlias, colName),
                "h0",
                distinct);
        b.addHaving(new PlannerRequest.Having(havingMeasure, cmp, litValue));
    }

    private static PlannerRequest.Comparison comparisonFor(String op) {
        if (">".equals(op)) {
            return PlannerRequest.Comparison.GT;
        }
        if ("<".equals(op)) {
            return PlannerRequest.Comparison.LT;
        }
        if (">=".equals(op)) {
            return PlannerRequest.Comparison.GE;
        }
        if ("<=".equals(op)) {
            return PlannerRequest.Comparison.LE;
        }
        if ("=".equals(op)) {
            return PlannerRequest.Comparison.EQ;
        }
        if ("<>".equals(op)) {
            return PlannerRequest.Comparison.NE;
        }
        return null;
    }

    private static PlannerRequest.AggFn aggFnFor(RolapAggregator agg) {
        if (agg == RolapAggregator.Sum) {
            return PlannerRequest.AggFn.SUM;
        }
        if (agg == RolapAggregator.Count
            || agg == RolapAggregator.DistinctCount)
        {
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
        return null;
    }

    /**
     * NECJ variant of {@link #emitTargetProjections}: mirrors the legacy
     * {@code SqlTupleReader.addLevelMemberSql} outer loop, which walks
     * every non-all level in the target's hierarchy from the root down to
     * (and including) the target level. At each level it emits the
     * attribute's key columns, then name, then caption — as both
     * projections <em>and</em> GROUP BY entries. No ORDER BY. The
     * resulting SELECT-list column count matches the
     * {@link mondrian.rolap.LevelColumnLayout} the reader built from the
     * same walk, so positional lookup at row-read time still lands on
     * the right column.
     */
    private static void emitNecjTargetProjections(
        PlannerRequest.Builder b, Set<String> seen, TargetShape t)
    {
        RolapCubeLevel leaf = t.level;
        List<? extends RolapCubeLevel> hierarchyLevels =
            leaf.getHierarchy().getLevelList();
        int leafDepth = leaf.getDepth();
        for (int i = 0; i <= leafDepth; i++) {
            RolapCubeLevel currLevel = hierarchyLevels.get(i);
            if (currLevel.isAll()) {
                continue;
            }
            if (currLevel.getParentAttribute() != null) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: SetConstraint hierarchy has a "
                    + "parent-attribute level (" + currLevel + ")");
            }
            RolapAttribute attr = currLevel.getAttribute();
            // The column's table alias comes from the column's own
            // relation — on a snowflake like Product→Product_Class each
            // level's key may live on a different table (product_class
            // for family/department/category/subcategory, product for
            // brand_name/product_name/product_id).
            //
            // Order mirrors legacy addLevelMemberSql per-level emission:
            // (1) explicit orderBy columns, (2) keyList, (3) nameExp,
            // (4) captionExp — all with dedup. For a single-key level
            // where orderBy defaults to the nameExp column, the nameExp
            // ends up *before* the key column in the SELECT list (e.g.
            // [Product Name] emits product_name before product_id),
            // matching legacy's cell-set column layout. Composite-key
            // levels are different: the ancestor keys have already been
            // emitted at their own level in this walk, so order-by for
            // the leaf composite level simply dedups.
            for (RolapSchema.PhysColumn o : attr.getOrderByList()) {
                emitNecjProjection(b, seen, o, "order-by");
            }
            for (RolapSchema.PhysColumn kc : attr.getKeyList()) {
                emitNecjProjection(b, seen, kc, "key");
            }
            RolapSchema.PhysColumn nameExp = attr.getNameExp();
            if (nameExp != null) {
                emitNecjProjection(b, seen, nameExp, "name");
            }
            RolapSchema.PhysColumn captionExp = attr.getCaptionExp();
            if (captionExp != null) {
                emitNecjProjection(b, seen, captionExp, "caption");
            }
        }
    }

    /**
     * Collects every {@link RolapSchema.PhysRelation} referenced by a
     * NECJ target's hierarchy walk (all non-all levels from root to leaf,
     * every attribute's keyList / orderByList / nameExp / captionExp).
     * Used to ensure every such table is joined to the request before
     * any projection references its columns.
     */
    private static Set<RolapSchema.PhysRelation> collectNecjRelations(
        TargetShape t)
    {
        Set<RolapSchema.PhysRelation> out = new LinkedHashSet<>();
        RolapCubeLevel leaf = t.level;
        List<? extends RolapCubeLevel> hierarchyLevels =
            leaf.getHierarchy().getLevelList();
        int leafDepth = leaf.getDepth();
        for (int i = 0; i <= leafDepth; i++) {
            RolapCubeLevel currLevel = hierarchyLevels.get(i);
            if (currLevel.isAll()) {
                continue;
            }
            RolapAttribute attr = currLevel.getAttribute();
            addRelationIfReal(out, attr.getKeyList());
            addRelationIfReal(out, attr.getOrderByList());
            RolapSchema.PhysColumn nameExp = attr.getNameExp();
            if (nameExp != null) {
                addRelationIfReal(out,
                    java.util.Collections.singletonList(nameExp));
            }
            RolapSchema.PhysColumn captionExp = attr.getCaptionExp();
            if (captionExp != null) {
                addRelationIfReal(out,
                    java.util.Collections.singletonList(captionExp));
            }
        }
        return out;
    }

    private static void addRelationIfReal(
        Set<RolapSchema.PhysRelation> out, List<RolapSchema.PhysColumn> cols)
    {
        for (RolapSchema.PhysColumn c : cols) {
            if (c.relation != null) {
                out.add(c.relation);
            }
        }
    }

    /**
     * Emits an ORDER BY ... ASC NULLS LAST clause on the target's key
     * columns (parent-to-leaf, including every ancestor level's keys),
     * deduped across targets by {@code (tableAlias, name)}. Legacy's
     * NECJ SQL has no explicit ORDER BY, relying on HSQLDB's natural
     * row order for the specific FROM/WHERE shape it emits. Calcite
     * rewrites the plan to a different join topology, so the DB's
     * "natural" order changes — we have to pin the axis order
     * explicitly so the tuple reader returns members in the same
     * sequence the legacy reader did.
     */
    private static void addNecjOrderBy(
        PlannerRequest.Builder b, Set<String> seen, TargetShape t)
    {
        RolapCubeLevel leaf = t.level;
        List<? extends RolapCubeLevel> hierarchyLevels =
            leaf.getHierarchy().getLevelList();
        int leafDepth = leaf.getDepth();
        for (int i = 0; i <= leafDepth; i++) {
            RolapCubeLevel currLevel = hierarchyLevels.get(i);
            if (currLevel.isAll()) {
                continue;
            }
            RolapAttribute attr = currLevel.getAttribute();
            // Emit per-level ORDER BY in the same relative order as the
            // per-level SELECT list: orderByList (name column by default for
            // simple attributes) first, then keyList. For composite-key
            // levels where orderBy == last key component, the dedup set
            // collapses duplicates. This reproduces the axis sequence that
            // legacy's tuple reader produces from HSQLDB's natural order:
            // alphabetical by ancestor keys, then by leaf name column
            // (e.g. [Product Name] sorts by product_name not product_id).
            for (RolapSchema.PhysColumn o : attr.getOrderByList()) {
                addNecjOrderByColumn(b, seen, o);
            }
            for (RolapSchema.PhysColumn kc : attr.getKeyList()) {
                addNecjOrderByColumn(b, seen, kc);
            }
        }
    }

    private static void addNecjOrderByColumn(
        PlannerRequest.Builder b, Set<String> seen, RolapSchema.PhysColumn c)
    {
        if (!(c instanceof RolapSchema.PhysRealColumn)) {
            return;
        }
        String tableAlias = c.relation == null ? null : c.relation.getAlias();
        String colName = ((RolapSchema.PhysRealColumn) c).name;
        String tag = tableAlias + "." + colName;
        if (seen.add(tag)) {
            b.addOrderBy(
                new PlannerRequest.OrderBy(
                    new PlannerRequest.Column(tableAlias, colName),
                    PlannerRequest.Order.ASC));
        }
    }

    private static void emitNecjProjection(
        PlannerRequest.Builder b,
        Set<String> seen,
        RolapSchema.PhysColumn col,
        String role)
    {
        if (!(col instanceof RolapSchema.PhysRealColumn)) {
            throw new UnsupportedTranslation(
                "fromTupleRead: SetConstraint " + role
                + " column is non-real (" + col.getClass().getName() + ")");
        }
        String tableAlias =
            col.relation == null ? null : col.relation.getAlias();
        PlannerRequest.Column projection = new PlannerRequest.Column(
            tableAlias, ((RolapSchema.PhysRealColumn) col).name);
        String tag = tableAlias + "." + projection.name;
        if (seen.add(tag)) {
            b.addProjection(projection);
            b.addGroupBy(projection);
        }
    }

    /**
     * Emit WHERE contribution for a single {@link CrossJoinArg}. Supports
     * {@link MemberListCrossJoinArg} (EQ / IN on the level's leaf key) and
     * {@link DescendantsCrossJoinArg} with null member (no filter). Every
     * other shape throws {@link UnsupportedTranslation}.
     */
    private static void addCrossJoinArgFilter(
        PlannerRequest.Builder b, TargetShape shape, CrossJoinArg arg)
    {
        if (arg instanceof DescendantsCrossJoinArg) {
            List<RolapMember> members = arg.getMembers();
            if (members == null || members.isEmpty()) {
                // Level.members — no additional filter.
                return;
            }
            // Descendants of a real member — would require constraining
            // every ancestor column, defer.
            throw new UnsupportedTranslation(
                "fromTupleRead: DescendantsCrossJoinArg rooted at a real "
                + "member not yet supported (arg level="
                + arg.getLevel() + ")");
        }
        if (arg instanceof MemberListCrossJoinArg) {
            List<RolapMember> members = arg.getMembers();
            if (members == null || members.isEmpty()) {
                // Mondrian treats this as a hard-coded FALSE predicate.
                b.universalFalse(true);
                return;
            }
            // Every member must live on the arg's level; the leaf key
            // column is what we filter on. For composite-key levels the
            // members' keys are compound — reject for now (no corpus query
            // uses this shape and TupleFilter would be needed).
            List<RolapSchema.PhysColumn> keyList =
                shape.attribute.getKeyList();
            if (keyList.size() != 1) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: MemberListCrossJoinArg on composite-key "
                    + "level " + shape.level + " (keyList.size="
                    + keyList.size() + ") not yet supported");
            }
            PlannerRequest.Column col =
                new PlannerRequest.Column(shape.tableAlias, keyList.get(0)
                    .toString().equals("") ? null
                    : ((RolapSchema.PhysRealColumn) keyList.get(0)).name);
            // Avoid the toString shortcut — just use the real column.
            if (!(keyList.get(0) instanceof RolapSchema.PhysRealColumn)) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: MemberListCrossJoinArg key is a non-real "
                    + "column (" + keyList.get(0).getClass().getName() + ")");
            }
            col = new PlannerRequest.Column(
                shape.tableAlias,
                ((RolapSchema.PhysRealColumn) keyList.get(0)).name);
            List<Object> values = new java.util.ArrayList<>(members.size());
            for (RolapMember m : members) {
                if (m.isCalculated() || m.isAll()) {
                    throw new UnsupportedTranslation(
                        "fromTupleRead: MemberListCrossJoinArg contains "
                        + "calc/all member " + m);
                }
                values.add(memberKeyLiteral(m));
            }
            if (values.size() == 1) {
                b.addFilter(new PlannerRequest.Filter(col, values.get(0)));
            } else {
                b.addFilter(
                    new PlannerRequest.Filter(
                        col, PlannerRequest.Operator.IN, values));
            }
            return;
        }
        throw new UnsupportedTranslation(
            "fromTupleRead: unsupported CrossJoinArg subclass "
            + arg.getClass().getName());
    }

    /** Extract a member's leaf key as a filter literal. Rejects composite
     *  keys (List values) since those need TupleFilter semantics. */
    private static Object memberKeyLiteral(RolapMember m) {
        Object key = m.getKey();
        if (key instanceof List) {
            throw new UnsupportedTranslation(
                "fromTupleRead: composite member key not yet supported "
                + "(member=" + m + ")");
        }
        return key;
    }

    /**
     * Slicer-driven WHERE: for every evaluator non-all, non-measure member
     * not already pinned by a CrossJoinArg, emit an EQ filter on the
     * member's leaf key column. Joins the member's dim chain into the
     * request first if not already present.
     */
    private static void addSlicerFilters(
        PlannerRequest.Builder b,
        RolapEvaluator evaluator,
        RolapStar.Table factTable,
        Set<String> joinedAliases,
        CrossJoinArg[] args)
    {
        // Hierarchies already pinned by CJ args — don't re-emit a filter
        // for a member the arg already constrained.
        Set<Object> argHierarchies = new LinkedHashSet<>();
        for (CrossJoinArg a : args) {
            if (a.getLevel() != null) {
                argHierarchies.add(a.getLevel().getHierarchy());
            }
        }
        RolapMember[] members = evaluator.getNonAllMembers();
        for (RolapMember m : members) {
            if (m.isMeasure() || m.isAll() || m.isCalculated()) {
                continue;
            }
            if (argHierarchies.contains(m.getHierarchy())) {
                continue;
            }
            // Mirror legacy removeCalculatedAndDefaultMembers: a member
            // that is the hierarchy's default (outside the measures
            // hierarchy) does not contribute a filter. Example: the Sales
            // cube's Time dimension defaults to [Time].[1997] for the
            // first loaded year; legacy's addContextConstraint drops it
            // silently and the NECJ SQL has no WHERE on "the_year".
            Member defaultMember = m.getHierarchy().getDefaultMember();
            if (defaultMember != null && defaultMember.equals(m)) {
                continue;
            }
            RolapCubeLevel memLevel;
            if (!(m.getLevel() instanceof RolapCubeLevel)) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: slicer member " + m
                    + " level is not a RolapCubeLevel");
            }
            memLevel = (RolapCubeLevel) m.getLevel();
            TargetShape shape;
            try {
                shape = shapeFor(memLevel);
            } catch (UnsupportedTranslation ex) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: slicer member " + m
                    + " — " + ex.getMessage());
            }
            RolapStar.Table leafStar = findStarTable(factTable, shape.table);
            if (leafStar == null) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: slicer member " + m
                    + " dim " + shape.tableAlias
                    + " not reachable from fact "
                    + factTable.getAlias());
            }
            if (leafStar != factTable) {
                ensureJoinedChain(b, factTable, leafStar, joinedAliases);
            }
            List<RolapSchema.PhysColumn> keyList =
                shape.attribute.getKeyList();
            if (keyList.size() != 1) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: slicer member " + m
                    + " on composite-key level (keyList.size="
                    + keyList.size() + ") not yet supported");
            }
            RolapSchema.PhysColumn kc = keyList.get(0);
            if (!(kc instanceof RolapSchema.PhysRealColumn)) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: slicer member " + m
                    + " key is a non-real column");
            }
            PlannerRequest.Column col =
                new PlannerRequest.Column(
                    shape.tableAlias,
                    ((RolapSchema.PhysRealColumn) kc).name);
            b.addFilter(
                new PlannerRequest.Filter(col, memberKeyLiteral(m)));
        }
    }

    /** Pre-computed per-target binding: resolved table + attribute refs
     *  plus any snowflake chain from the dimension-key table to the leaf
     *  (Task L). Shared by the single-target and multi-target paths. */
    private static final class TargetShape {
        final RolapCubeLevel level;
        final RolapAttribute attribute;
        final RolapSchema.PhysTable table;
        final String tableName;
        final String tableAlias;
        /**
         * Chain of {@link PlannerRequest.Join} edges that must be stitched
         * into the request <em>before</em> this target's projections, so
         * orphan catalog rows on the leaf (rows with no corresponding
         * dim-key row) are filtered out. Emitted in leaf→ancestor order
         * (first edge joins the leaf's immediate parent, etc). The first
         * edge uses {@code leftTable=null} so the renderer resolves the
         * factKey against the leaf scan (LHS of the built rel); subsequent
         * edges use {@code leftTable=} the previous dim alias. Empty for
         * flat (non-snowflake) levels.
         */
        final List<PlannerRequest.Join> dimChain;
        TargetShape(
            RolapCubeLevel level,
            RolapAttribute attribute,
            RolapSchema.PhysTable table,
            List<PlannerRequest.Join> dimChain)
        {
            this.level = level;
            this.attribute = attribute;
            this.table = table;
            this.tableName = table.getName();
            this.tableAlias = table.getAlias();
            this.dimChain = dimChain;
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
        // Task L: if the level's key columns live on a snowflaked dim table
        // reached through intermediate dim tables (e.g. [Product Department]
        // keyed on product_class, reached via product → product_class), emit
        // the full chain as join edges so orphan catalog rows on the leaf
        // table don't leak into the member list. Legacy tuple-read
        // (SqlQueryBuilder.flush + joinToDimensionKey) joins the dim-key
        // table back to the leaf for exactly this reason; see
        // golden-legacy/slicer-where.json for the canonical shape
        // (FROM "product","product_class" WHERE "product"."product_class_id"
        // = "product_class"."product_class_id").
        List<PlannerRequest.Join> dimChain =
            computeTupleReadDimChain(level, table);
        return new TargetShape(level, attribute, table, dimChain);
    }

    /**
     * Returns the chain of joins from the level's dimension-key table down
     * to the leaf {@code keyTable}, excluding the leaf itself (which is
     * already the target's scan). Empty when the dimension is flat (key
     * columns live on the dim-key table). Returned edges are ordered so
     * that callers can emit them in request order (leaf-to-ancestor):
     * the first edge joins the leaf's immediate parent, the second the
     * grandparent, etc. Each edge's {@code leftTable} is null for the
     * first edge (LHS is the leaf scan) and set to the previous edge's
     * dim alias for subsequent edges so the renderer can disambiguate
     * column names shared across the chain.
     */
    private static List<PlannerRequest.Join> computeTupleReadDimChain(
        RolapCubeLevel level, RolapSchema.PhysTable keyTable)
    {
        RolapCubeDimension dim = level.getDimension();
        if (dim == null) {
            return java.util.Collections.emptyList();
        }
        RolapSchema.PhysRelation dimKeyTable;
        try {
            dimKeyTable = dim.getKeyTable();
        } catch (RuntimeException ex) {
            // Some synthetic dimensions (e.g. Measures) throw here; a
            // level on such a dim never reaches a snowflake shape.
            return java.util.Collections.emptyList();
        }
        if (dimKeyTable == null || dimKeyTable == keyTable) {
            return java.util.Collections.emptyList();
        }
        RolapSchema.PhysColumn firstKey =
            level.getAttribute().getKeyList().get(0);
        RolapSchema.PhysPath path;
        try {
            path = dim.getKeyPath(firstKey);
        } catch (RuntimeException ex) {
            throw new UnsupportedTranslation(
                "fromTupleRead: cannot resolve join path from dimension "
                + "key " + dimKeyTable.getAlias() + " to key column "
                + firstKey + ": " + ex.getMessage());
        }
        // path.hopList: Hop(dimKeyTable, null), Hop(next, link1), …,
        //   Hop(leaf, linkN). We want every hop EXCEPT the leaf, emitted
        //   in reverse (leaf-adjacent first) so PlannerRequest Join
        //   sequencing works out.
        List<RolapSchema.PhysHop> hops = path.hopList;
        if (hops.isEmpty() || hops.get(hops.size() - 1).relation != keyTable) {
            throw new UnsupportedTranslation(
                "fromTupleRead: dim-key path does not end at leaf table "
                + keyTable.getAlias()
                + " (got " + (hops.isEmpty()
                    ? "<empty>"
                    : hops.get(hops.size() - 1).relation.getAlias())
                + ")");
        }
        List<PlannerRequest.Join> out = new java.util.ArrayList<>();
        String prevAlias = null; // null => LHS is the leaf scan
        // Walk from the leaf's immediate parent upwards.
        for (int i = hops.size() - 2; i >= 0; i--) {
            RolapSchema.PhysHop hop = hops.get(i);
            RolapSchema.PhysRelation ancestor = hop.relation;
            if (!(ancestor instanceof RolapSchema.PhysTable)) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: non-table dim in chain: "
                    + (ancestor == null
                        ? "null"
                        : ancestor.getClass().getName()));
            }
            // The link connecting this ancestor to the next-child-toward-
            // leaf lives on hops.get(i+1).link. For the canonical
            // fact→product→product_class snowflake the link targetRelation
            // is `product` (FK side) and sourceKey.relation is
            // `product_class` (PK side); but either orientation must
            // work mid-chain.
            RolapSchema.PhysHop nextChildHop = hops.get(i + 1);
            RolapSchema.PhysLink link = nextChildHop.link;
            if (link == null) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: null link on dim hop "
                    + nextChildHop.relation.getAlias());
            }
            RolapSchema.PhysRelation childRel = nextChildHop.relation;
            List<RolapSchema.PhysColumn> fkCols = link.getColumnList();
            if (fkCols.size() != 1) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: composite FK keys not supported in "
                    + "snowflake chain (arity=" + fkCols.size() + ")");
            }
            RolapSchema.PhysColumn fkCol = fkCols.get(0);
            RolapSchema.PhysKey srcKey = link.getSourceKey();
            List<RolapSchema.PhysColumn> pkCols = srcKey.getColumnList();
            if (pkCols.size() != 1) {
                throw new UnsupportedTranslation(
                    "fromTupleRead: composite PK keys not supported in "
                    + "snowflake chain (arity=" + pkCols.size() + ")");
            }
            RolapSchema.PhysColumn pkCol = pkCols.get(0);
            if (!(fkCol instanceof RolapSchema.PhysRealColumn)
                || !(pkCol instanceof RolapSchema.PhysRealColumn))
            {
                throw new UnsupportedTranslation(
                    "fromTupleRead: non-real FK/PK column in snowflake "
                    + "chain edge " + ancestor.getAlias() + "→"
                    + childRel.getAlias());
            }
            String fkName = ((RolapSchema.PhysRealColumn) fkCol).name;
            String pkName = ((RolapSchema.PhysRealColumn) pkCol).name;
            // Figure out which side of the link is the "child" (LHS, i.e.
            // closer to the leaf in our reverse walk) vs the "ancestor".
            // The renderer expects leftKey on the LHS table and dimKey on
            // the RHS. RHS = ancestor here (we're joining ancestor onto
            // an already-built chain rooted at leaf).
            String leftKey;
            String rightKey;
            if (link.targetRelation == childRel
                && srcKey.getRelation() == ancestor)
            {
                // Child holds FK (e.g. product.product_class_id → product_class.product_class_id).
                leftKey = fkName;
                rightKey = pkName;
            } else if (link.targetRelation == ancestor
                && srcKey.getRelation() == childRel)
            {
                // Ancestor holds FK, child holds PK — reverse link.
                leftKey = pkName;
                rightKey = fkName;
            } else {
                throw new UnsupportedTranslation(
                    "fromTupleRead: PhysLink endpoints do not match "
                    + "ancestor/child pair ("
                    + ancestor.getAlias() + " / " + childRel.getAlias()
                    + ")");
            }
            String ancestorAlias = ancestor.getAlias();
            PlannerRequest.Join edge = new PlannerRequest.Join(
                ancestorAlias, leftKey, rightKey,
                PlannerRequest.JoinKind.INNER, prevAlias);
            out.add(edge);
            prevAlias = ancestorAlias;
        }
        return out;
    }

    /**
     * Emits a single target's projections + order-by into the shared
     * builder. Projection order mirrors legacy {@code addLevelMemberSql}'s
     * net effect for the leaf level: key columns (full list, parent-most
     * to leaf-most), then order-by columns, then nameExp, then captionExp.
     * Duplicate columns (same alias+name) are skipped.
     *
     * <p>Legacy builds this order indirectly — its outer loop walks from
     * the root level down to {@code levelDepth} and for each ancestor
     * level issues the orderBy/key/name/caption batch in source order.
     * The ancestor's orderBy is the parent-most key column, which is why
     * parent keys end up first in the SELECT list. Because the leaf's
     * attribute keyList already enumerates every ancestor key in
     * parent-to-leaf order (e.g. {@code [product_family,
     * product_department]} for {@code [Product].[Product Department]}),
     * emitting keyList first — before the leaf's own orderBy, which for
     * a composite-key level is just the leaf name column — reproduces
     * the same column ordinals legacy's layout builder recorded. See
     * Task J for the drift this fixes: parent/leaf positions were
     * swapped on the axis because orderBy was emitted first and
     * consumed the leaf column before the key loop got to it.
     *
     * <p>Every key column is added to the ORDER BY, in parent-most-first
     * order — matching legacy's behaviour, which flags each key column
     * with {@code SELECT_ORDER} / {@code SELECT_GROUP_ORDER} on the
     * last target. An explicit {@code getOrderByList()} (non-key) adds
     * additional ORDER BY entries after the key ones.
     */
    private static void emitTargetProjections(
        PlannerRequest.Builder b,
        Set<String> seen,
        TargetShape t)
    {
        String tableAlias = t.tableAlias;
        RolapAttribute attribute = t.attribute;

        // 1) Key columns (full list, parent-most to leaf-most — composite
        // keys emit every key column, and every one contributes to the
        // ORDER BY so cell-set key ordering matches legacy).
        java.util.List<RolapSchema.PhysColumn> keyList =
            attribute.getKeyList();
        java.util.Set<String> keyColNames =
            new java.util.LinkedHashSet<String>();
        for (RolapSchema.PhysColumn kc : keyList) {
            PlannerRequest.Column kp = asProjection(kc, tableAlias, "key");
            if (seen.add(tableAlias + "." + kp.name)) {
                b.addProjection(kp);
            }
            keyColNames.add(kp.name);
            b.addOrderBy(
                new PlannerRequest.OrderBy(kp, PlannerRequest.Order.ASC));
        }

        // 2) Order-by columns (any column not already contributed by the
        //    key list — e.g. an explicit non-key ordering expression).
        for (RolapSchema.PhysColumn o : attribute.getOrderByList()) {
            PlannerRequest.Column c = asProjection(o, tableAlias, "order-by");
            if (seen.add(tableAlias + "." + c.name)) {
                b.addProjection(c);
            }
            if (!keyColNames.contains(c.name)) {
                b.addOrderBy(
                    new PlannerRequest.OrderBy(c, PlannerRequest.Order.ASC));
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
            return addOrPredicateFilter(b, (OrPredicate) sp);
        }
        if (sp instanceof MinusStarPredicate) {
            throw new UnsupportedTranslation(
                "fromSegmentLoad: MinusStarPredicate not yet supported");
        }
        throw new UnsupportedTranslation(
            "fromSegmentLoad: unsupported compound predicate "
            + sp.getClass().getName());
    }

    /**
     * Translate an {@link OrPredicate} into one filter contribution.
     *
     * <p>Three supported child shapes:
     * <ul>
     *   <li>All children {@link ValueColumnPredicate} / single-leaf
     *       {@link AndPredicate} over the <em>same</em> column →
     *       collapse to IN-list (EQ when only one value).</li>
     *   <li>All children {@link AndPredicate} over the same multi-column
     *       column set → emit a {@link PlannerRequest.TupleFilter} with
     *       one row per AND.</li>
     *   <li>Mixed Value + single-leaf AND on the same column → promote
     *       Values to single-leaf ANDs and collapse to IN-list.</li>
     * </ul>
     * Returns {@code true} on universalFalse (empty OR).
     */
    private static boolean addOrPredicateFilter(
        PlannerRequest.Builder b, OrPredicate or)
    {
        List<StarPredicate> children = or.getChildren();
        if (children.isEmpty()) {
            return true;
        }
        List<PredicateColumn> cols = or.getColumnList();

        // Single-column: collapse to IN-list regardless of whether each
        // child is a ValueColumnPredicate or an AndPredicate-with-one-leaf.
        if (cols.size() == 1) {
            PredicateColumn pc = cols.get(0);
            List<Object> literals = new java.util.ArrayList<>();
            for (StarPredicate child : children) {
                Object v = singleColumnValue(child, pc);
                if (v == UNPROMOTABLE) {
                    throw new UnsupportedTranslation(
                        "fromSegmentLoad: OrPredicate single-column child "
                        + "shape not supported: "
                        + child.getClass().getName());
                }
                literals.add(v);
            }
            PlannerRequest.Column col = columnForPredicate(pc);
            if (literals.size() == 1) {
                b.addFilter(new PlannerRequest.Filter(col, literals.get(0)));
            } else {
                b.addFilter(
                    new PlannerRequest.Filter(
                        col, PlannerRequest.Operator.IN, literals));
            }
            return false;
        }

        // Multi-column: every child must be an AndPredicate whose leaves
        // are ValueColumnPredicates covering the same column set.
        List<PredicateColumn> orderedCols =
            new java.util.ArrayList<>(cols);
        List<List<Object>> rows = new java.util.ArrayList<>(children.size());
        for (StarPredicate child : children) {
            if (!(child instanceof AndPredicate)) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: OrPredicate multi-column child is "
                    + "not an AndPredicate: "
                    + child.getClass().getName());
            }
            AndPredicate and = (AndPredicate) child;
            java.util.Map<RolapSchema.PhysColumn, Object> byCol =
                new java.util.HashMap<>();
            for (StarPredicate leaf : and.getChildren()) {
                if (!(leaf instanceof ValueColumnPredicate)) {
                    throw new UnsupportedTranslation(
                        "fromSegmentLoad: OrPredicate AND leaf is not a "
                        + "ValueColumnPredicate: "
                        + leaf.getClass().getName());
                }
                ValueColumnPredicate v = (ValueColumnPredicate) leaf;
                byCol.put(v.getColumn().physColumn, v.getValue());
            }
            if (byCol.size() != orderedCols.size()) {
                throw new UnsupportedTranslation(
                    "fromSegmentLoad: OrPredicate AND child covers "
                    + byCol.size() + " columns, expected "
                    + orderedCols.size());
            }
            List<Object> row = new java.util.ArrayList<>(orderedCols.size());
            for (PredicateColumn pc : orderedCols) {
                if (!byCol.containsKey(pc.physColumn)) {
                    throw new UnsupportedTranslation(
                        "fromSegmentLoad: OrPredicate AND child missing "
                        + "column " + pc.physColumn);
                }
                row.add(byCol.get(pc.physColumn));
            }
            rows.add(row);
        }
        List<PlannerRequest.Column> outCols =
            new java.util.ArrayList<>(orderedCols.size());
        for (PredicateColumn pc : orderedCols) {
            outCols.add(columnForPredicate(pc));
        }
        b.addTupleFilter(new PlannerRequest.TupleFilter(outCols, rows));
        return false;
    }

    /** Sentinel: child is not a promotable single-column equality. */
    private static final Object UNPROMOTABLE = new Object();

    /**
     * If {@code child} is a single-column equality on {@code pc} (either a
     * {@link ValueColumnPredicate} or an {@link AndPredicate} with exactly
     * one {@link ValueColumnPredicate} leaf on the same column), returns
     * the literal value. Otherwise returns {@link #UNPROMOTABLE}.
     */
    private static Object singleColumnValue(
        StarPredicate child, PredicateColumn pc)
    {
        if (child instanceof ValueColumnPredicate) {
            ValueColumnPredicate v = (ValueColumnPredicate) child;
            if (v.getColumn().physColumn != pc.physColumn) {
                return UNPROMOTABLE;
            }
            return v.getValue();
        }
        if (child instanceof AndPredicate) {
            AndPredicate and = (AndPredicate) child;
            if (and.getChildren().size() != 1) {
                return UNPROMOTABLE;
            }
            StarPredicate leaf = and.getChildren().get(0);
            if (!(leaf instanceof ValueColumnPredicate)) {
                return UNPROMOTABLE;
            }
            ValueColumnPredicate v = (ValueColumnPredicate) leaf;
            if (v.getColumn().physColumn != pc.physColumn) {
                return UNPROMOTABLE;
            }
            return v.getValue();
        }
        return UNPROMOTABLE;
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
