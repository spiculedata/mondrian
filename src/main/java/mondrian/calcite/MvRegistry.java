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

import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapMeasureGroup;
import mondrian.rolap.RolapMeasureGroup.RolapMeasureRef;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.util.Pair;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registry of Calcite {@link RelOptMaterialization} entries derived from
 * Mondrian-4 {@code <MeasureGroup type='aggregate'>} declarations.
 *
 * <p>Phase 3 Task 10: builds one materialization per declared aggregate
 * MeasureGroup so a Calcite planner can, in principle, cost-pick the
 * aggregate table over the base fact table. This is a parallel path to
 * Mondrian's in-situ {@code RolapGalaxy.findAgg} matcher; it does not
 * replace it.
 *
 * <p><b>Planner integration status:</b> Calcite's materialization rewrite
 * machinery ({@code MaterializedViewRule} and friends) is cost-based and
 * requires a {@link org.apache.calcite.plan.volcano.VolcanoPlanner} stage
 * to run. {@link CalciteSqlPlanner} currently optimises with HepPlanner,
 * which is fixpoint-driven and does not run cost-based rule selection.
 * This registry is therefore wired as data infrastructure that a future
 * Volcano pass can consume; it is <em>not</em> currently registered on
 * the HepPlanner program. See Task U investigation notes in
 * {@code docs/plans/2026-04-19-calcite-backend-agg-and-calc-checkpoint.md}.
 *
 * <p><b>Coverage model.</b> For each aggregate MeasureGroup we attempt to
 * build two {@link RelNode} trees:
 * <ul>
 *   <li><b>tableRel</b>: a plain {@code Scan(aggregateTable)} — this is
 *       what the planner would substitute in when rewriting.</li>
 *   <li><b>queryRel</b>: the semantic defining query — an
 *       {@code Aggregate(SUM..., COUNT*) over Join(fact, dim1, dim2, ...)}
 *       with equi-joins on the FK/PK columns from each {@code
 *       ForeignKeyLink}, and rolled-up copy-columns from each
 *       {@code CopyLink}. {@code NoLink} dimensions are omitted
 *       entirely.</li>
 * </ul>
 *
 * <p>If either side can't be built (missing physical tables, complex
 * expressions, multi-column foreign keys, non-real columns), the entry
 * is skipped with a {@code WARN} log rather than failing the registry.
 */
public final class MvRegistry {

    private static final Logger LOGGER = Logger.getLogger(MvRegistry.class);

    private final List<RelOptMaterialization> materializations;
    private final List<String> skippedAggs;

    private MvRegistry(
        List<RelOptMaterialization> materializations,
        List<String> skippedAggs)
    {
        this.materializations =
            Collections.unmodifiableList(materializations);
        this.skippedAggs = Collections.unmodifiableList(skippedAggs);
    }

    /** Registered MV entries, one per successfully-built aggregate. */
    public List<RelOptMaterialization> materializations() {
        return materializations;
    }

    /** Names of aggregate MeasureGroups that were skipped during build. */
    public List<String> skippedAggregates() {
        return skippedAggs;
    }

    /** Total count of registered MVs. */
    public int size() {
        return materializations.size();
    }

    /**
     * Walks every cube in {@code rolapSchema}, enumerates
     * {@link RolapMeasureGroup#isAggregate() aggregate measure groups},
     * and builds a registry entry per aggregate where possible.
     */
    public static MvRegistry fromSchema(
        RolapSchema rolapSchema,
        CalciteMondrianSchema calciteSchema)
    {
        if (rolapSchema == null) {
            throw new IllegalArgumentException("rolapSchema is null");
        }
        if (calciteSchema == null) {
            throw new IllegalArgumentException("calciteSchema is null");
        }
        List<RelOptMaterialization> out = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        Set<String> registeredTables = new LinkedHashSet<>();
        FrameworkConfig cfg = Frameworks.newConfigBuilder()
            .defaultSchema(calciteSchema.schema())
            .build();
        for (RolapCube cube : rolapSchema.getCubeList()) {
            for (RolapMeasureGroup mg : cube.getMeasureGroups()) {
                if (!mg.isAggregate()) {
                    continue;
                }
                String aggTable = tableNameOf(mg.getFactRelation());
                if (aggTable == null) {
                    skipped.add(mg.getName() + " (non-table relation)");
                    LOGGER.warn(
                        "MvRegistry: skipping " + mg.getName()
                        + " — fact relation is not a PhysTable");
                    continue;
                }
                // Avoid double-registering the same agg table across
                // cubes that share an aggregate MeasureGroup.
                if (!registeredTables.add(aggTable)) {
                    continue;
                }
                try {
                    RelOptMaterialization mat =
                        buildMaterialization(cfg, mg, aggTable);
                    if (mat == null) {
                        skipped.add(mg.getName() + " (unreachable agg)");
                    } else {
                        out.add(mat);
                    }
                } catch (RuntimeException re) {
                    skipped.add(
                        mg.getName() + " (" + re.getClass().getSimpleName()
                        + ": " + re.getMessage() + ")");
                    LOGGER.warn(
                        "MvRegistry: skipping " + mg.getName()
                        + " — " + re, re);
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "MvRegistry built: " + out.size() + " materialization(s), "
                + skipped.size() + " skipped");
        }
        return new MvRegistry(out, skipped);
    }

    /**
     * Builds a single materialization for an aggregate measure group,
     * or returns {@code null} if this aggregate is structurally
     * unreachable (e.g. a {@code NoLink} on a hierarchy whose
     * {@code hasAll='false'} disqualifies it).
     */
    private static RelOptMaterialization buildMaterialization(
        FrameworkConfig cfg,
        RolapMeasureGroup mg,
        String aggTable)
    {
        // ----- target (tableRel): plain scan of the aggregate table -----
        RelBuilder tb = RelBuilder.create(cfg);
        tb.scan(aggTable);
        RelNode tableRel = tb.build();
        // starRelOptTable is for the deprecated star-table optimisation
        // path; RelOptMaterialization's constructor unwraps it to
        // StarTable.class and NPEs when passed a regular JdbcTable.
        // For agg-table materializations we always pass null here —
        // the planner treats this as "materialization without star
        // optimisation", which is exactly our case.
        RelOptTable starRelOptTable = null;

        // ----- defining query (queryRel): Aggregate over Join chain -----
        // The defining query is expressed over the BASE fact table
        // (e.g. sales_fact_1997), not the agg table itself — the point
        // of a materialization is "this table stores the result of
        // <this query over base tables>".
        RelBuilder qb = RelBuilder.create(cfg);
        String factTable = null;
        for (RolapMeasureRef ref : mg.getMeasureRefList()) {
            if (ref.measure == null) {
                continue;
            }
            RolapSchema.PhysColumn baseExpr = ref.measure.getExpr();
            if (baseExpr != null && baseExpr.relation != null) {
                factTable = baseExpr.relation.getAlias();
                break;
            }
        }
        if (factTable == null) {
            // Fall back to the agg table itself — produces a trivial
            // self-referential defining query, but still well-formed.
            factTable = aggTable;
        }
        qb.scan(factTable);

        // Walk dimensionMap3 for ForeignKey-linked dims: each path's
        // second hop carries the PhysLink with (factFK, dimPK) cols.
        // Walk copyColumnList for CopyLinked dims: the (starCol.agg ->
        // phys.dimCol) pair tells us which dim table to join, but we
        // need to find the FK/PK pair for that dim from the star's
        // other metadata. In FoodMart every CopyLink dim has a
        // corresponding FK column on the agg table with a predictable
        // name (e.g. CopyLink Time → time_id on fact). We discover
        // that by looking at the dim's underlying base-fact path
        // via the RolapStar.Column.getTable().getPath().
        //
        // Build the list of dim tables to join + their equi-join
        // columns.
        LinkedHashMap<String, List<Pair<String, String>>> joinsNeeded =
            new LinkedHashMap<>();

        // 1) ForeignKeyLink dimensions (dimensionMap3 with non-null
        //    second-hop link).
        for (Map.Entry<mondrian.rolap.RolapCubeDimension,
                RolapSchema.PhysPath> e : mg.dimensionMap3.entrySet())
        {
            RolapSchema.PhysPath path = e.getValue();
            if (path == null || path.hopList.size() < 2) {
                continue;
            }
            RolapSchema.PhysHop hop = path.hopList.get(1);
            if (hop.link == null) {
                continue;
            }
            String dimAlias = hop.relation.getAlias();
            if (joinsNeeded.containsKey(dimAlias)) {
                continue;
            }
            List<Pair<String, String>> equi = new ArrayList<>();
            List<RolapSchema.PhysColumn> fkCols =
                hop.link.getColumnList();
            List<RolapSchema.PhysColumn> pkCols =
                hop.link.getSourceKey().getColumnList();
            for (int k = 0;
                 k < fkCols.size() && k < pkCols.size(); k++)
            {
                RolapSchema.PhysColumn fk = fkCols.get(k);
                RolapSchema.PhysColumn pk = pkCols.get(k);
                if (!(fk instanceof RolapSchema.PhysRealColumn)
                    || !(pk instanceof RolapSchema.PhysRealColumn))
                {
                    continue;
                }
                equi.add(Pair.of(
                    ((RolapSchema.PhysRealColumn) fk).name,
                    ((RolapSchema.PhysRealColumn) pk).name));
            }
            if (!equi.isEmpty()) {
                joinsNeeded.put(dimAlias, equi);
            }
        }

        // 2) CopyLink dimensions (copyColumnList) — extract the set of
        //    source dim tables. We DO NOT try to synthesise a join
        //    predicate for pure-CopyLink dims: the CopyLink rewrites
        //    happen at query time, and the agg column IS the dim
        //    column, so no join is actually required in the defining
        //    query. The semantic value: project the dim-side copy
        //    column name so the rewriter can see the equivalence.
        //    We treat a pure-CopyLink dim as if it were already joined
        //    with a TRUE predicate (identity copy). That is lossy as a
        //    "real" defining query but faithful as an equivalence
        //    declaration — which is what RelOptMaterialization
        //    consumes.
        LinkedHashMap<String, List<Pair<String, String>>> copyTables =
            new LinkedHashMap<>();
        // entry.left = agg column name; entry.right = dim column name
        Map<String, List<Pair<String, String>>> copyColsByDim =
            new LinkedHashMap<>();
        for (Pair<RolapStar.Column, RolapSchema.PhysColumn> p
            : mg.getCopyColumnList())
        {
            RolapSchema.PhysColumn dimCol = p.right;
            if (!(dimCol instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            String dimTable = dimCol.relation.getAlias();
            String dimColName =
                ((RolapSchema.PhysRealColumn) dimCol).name;
            RolapStar.Column sc = p.left;
            RolapSchema.PhysColumn aggExpr =
                sc == null ? null : sc.getExpression();
            if (!(aggExpr instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            String aggColName =
                ((RolapSchema.PhysRealColumn) aggExpr).name;
            copyColsByDim
                .computeIfAbsent(dimTable, k -> new ArrayList<>())
                .add(Pair.of(aggColName, dimColName));
            if (!joinsNeeded.containsKey(dimTable)) {
                copyTables.put(dimTable, Collections.emptyList());
            }
        }

        // After all joins, the builder stack has a single relation
        // whose row-type flattens all inputs. We resolve columns
        // via qb.field(alias, name) — RelBuilder tracks scan aliases
        // through joins, so this works cleanly regardless of nesting.
        for (Map.Entry<String, List<Pair<String, String>>> e
            : joinsNeeded.entrySet())
        {
            String dim = e.getKey();
            List<Pair<String, String>> equi = e.getValue();
            qb.scan(dim);
            List<RexNode> conds = new ArrayList<>(equi.size());
            for (Pair<String, String> p : equi) {
                conds.add(qb.equals(
                    qb.field(2, 0, p.left),
                    qb.field(2, 1, p.right)));
            }
            qb.join(JoinRelType.INNER,
                conds.size() == 1 ? conds.get(0) : qb.and(conds));
        }
        for (String dim : copyTables.keySet()) {
            if (joinsNeeded.containsKey(dim)) {
                continue;
            }
            qb.scan(dim);
            // Pure CopyLink equivalence: no FK join available.
            qb.join(JoinRelType.INNER, qb.literal(true));
        }

        // ----- group keys -----
        LinkedHashMap<String, RexNode> groupKeys = new LinkedHashMap<>();
        // FK columns from joined (ForeignKeyLink) dims — from the
        // fact-side by column name (fact is the first scan, alias =
        // factTable).
        for (List<Pair<String, String>> equi : joinsNeeded.values()) {
            for (Pair<String, String> p : equi) {
                if (groupKeys.containsKey(p.left)) {
                    continue;
                }
                try {
                    groupKeys.put(p.left, qb.field(factTable, p.left));
                } catch (RuntimeException re) {
                    // Base fact doesn't have this column by that name
                    // — skip silently.
                }
            }
        }
        // Copy columns: project from the joined dim table.
        for (Map.Entry<String, List<Pair<String, String>>> e
            : copyColsByDim.entrySet())
        {
            String dimTable = e.getKey();
            for (Pair<String, String> p : e.getValue()) {
                String aggColName = p.left;
                String dimColName = p.right;
                if (groupKeys.containsKey(aggColName)) {
                    continue;
                }
                try {
                    groupKeys.put(
                        aggColName, qb.field(dimTable, dimColName));
                } catch (RuntimeException re) {
                    LOGGER.warn(
                        "MvRegistry: " + mg.getName() + " — copy column "
                        + dimTable + "." + dimColName
                        + " not resolvable, skipping MV");
                    return null;
                }
            }
        }
        if (groupKeys.isEmpty()) {
            LOGGER.warn(
                "MvRegistry: " + mg.getName() + " — empty group key, "
                + "skipping MV (no FK links and no copy columns)");
            return null;
        }

        // ----- measures: one aggregator per RolapMeasureRef -----
        List<RelBuilder.AggCall> aggs = new ArrayList<>();
        boolean hasFactCount = false;
        for (RolapMeasureRef ref : mg.getMeasureRefList()) {
            if (ref.measure == null) {
                continue;
            }
            if (ref.aggColumn == null
                || !(ref.aggColumn instanceof RolapSchema.PhysRealColumn))
            {
                continue;
            }
            String aggAlias =
                ((RolapSchema.PhysRealColumn) ref.aggColumn).name;
            // Base measure expression — what the aggregator summarises
            // on the fact table. For FoodMart all refs are SUM.
            RolapSchema.PhysColumn baseExpr =
                ref.measure.getExpr();
            String baseName =
                baseExpr instanceof RolapSchema.PhysRealColumn
                    ? ((RolapSchema.PhysRealColumn) baseExpr).name
                    : aggAlias;
            String aggName =
                ref.measure.getAggregator() == null
                    ? "sum"
                    : ref.measure.getAggregator().getName();
            if ("count".equalsIgnoreCase(aggName)) {
                // Fact-count-style measures.
                aggs.add(qb.count(false, aggAlias).as(aggAlias));
                if ("fact_count".equalsIgnoreCase(aggAlias)) {
                    hasFactCount = true;
                }
            } else {
                // Default to SUM (covers SUM, AVG-component, MAX/MIN
                // which also all become SUM in the agg table).
                try {
                    aggs.add(
                        qb.sum(qb.field(factTable, baseName)).as(aggAlias));
                } catch (RuntimeException re) {
                    LOGGER.warn(
                        "MvRegistry: " + mg.getName() + " — base column "
                        + baseName + " not on fact; skipping measure");
                }
            }
        }
        if (!hasFactCount) {
            aggs.add(qb.countStar("fact_count"));
        }

        List<RexNode> keyExprs = new ArrayList<>(groupKeys.values());
        qb.aggregate(qb.groupKey(keyExprs), aggs);
        RelNode queryRel = qb.build();

        List<String> qualifiedName = Collections.singletonList(aggTable);
        return new RelOptMaterialization(
            tableRel, queryRel, starRelOptTable, qualifiedName);
    }

    /**
     * Legacy helper (no longer called from main path). Retained as a
     * diagnostic aid for subclass tests and future CopyLink-driven
     * join resolution work.
     */
    @SuppressWarnings("unused")
    private static Map<String, List<Pair<String, String>>>
        collectJoinsNeeded(RolapMeasureGroup mg, String factTable)
    {
        Map<String, List<Pair<String, String>>> out =
            new LinkedHashMap<>();
        for (Pair<RolapStar.Column, RolapSchema.PhysColumn> p
            : mg.getCopyColumnList())
        {
            RolapStar.Column sc = p.left;
            if (sc == null) {
                continue;
            }
            RolapSchema.PhysColumn src = sc.getExpression();
            if (!(src instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            String dimTable = src.relation.getAlias();
            if (dimTable.equals(factTable)) {
                continue; // same-table copy, no join needed
            }
            // Resolve the FK equi-join for this dim by walking the
            // star path. The RolapStar.Column has a path from fact
            // to its own table; we need the last hop's link columns.
            RolapStar.Table starTable = sc.getTable();
            RolapSchema.PhysPath path =
                starTable == null ? null : starTable.getPath();
            if (path == null || path.hopList.isEmpty()) {
                out.computeIfAbsent(
                    dimTable, k -> new ArrayList<>());
                continue;
            }
            if (out.containsKey(dimTable)) {
                continue; // already have the join keys
            }
            List<Pair<String, String>> equi = new ArrayList<>();
            // Find the first hop whose link ends at (or passes
            // through) the dim table.
            for (int i = 1; i < path.hopList.size(); i++) {
                RolapSchema.PhysLink link = path.hopList.get(i).link;
                if (link == null) {
                    continue;
                }
                if (!link.targetRelation.getAlias().equals(factTable)
                    && !link.getSourceKey().getRelation().getAlias()
                        .equals(dimTable))
                {
                    continue;
                }
                List<RolapSchema.PhysColumn> fkCols = link.getColumnList();
                List<RolapSchema.PhysColumn> pkCols =
                    link.getSourceKey().getColumnList();
                for (int k = 0;
                     k < fkCols.size() && k < pkCols.size(); k++)
                {
                    RolapSchema.PhysColumn fk = fkCols.get(k);
                    RolapSchema.PhysColumn pk = pkCols.get(k);
                    if (!(fk instanceof RolapSchema.PhysRealColumn)
                        || !(pk instanceof RolapSchema.PhysRealColumn))
                    {
                        continue;
                    }
                    equi.add(Pair.of(
                        ((RolapSchema.PhysRealColumn) fk).name,
                        ((RolapSchema.PhysRealColumn) pk).name));
                }
                break;
            }
            out.put(dimTable, equi);
        }
        return out;
    }

    /**
     * Legacy helper (no longer called from main path). Retained as a
     * diagnostic aid.
     */
    @SuppressWarnings("unused")
    private static List<RolapStar.Column> fkColumnsOf(RolapMeasureGroup mg) {
        List<RolapStar.Column> out = new ArrayList<>();
        RolapStar.Table factTable = mg.getStar().getFactTable();
        for (RolapStar.Column c : factTable.getColumns()) {
            RolapSchema.PhysColumn expr = c.getExpression();
            if (!(expr instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            // Only take "FK-like" columns — those ending in _id.
            // FoodMart's convention is reliable here; a stricter
            // filter would walk every declared ForeignKeyLink's
            // foreignKeyColumn, but that path is more invasive.
            String name = ((RolapSchema.PhysRealColumn) expr).name;
            if (name.endsWith("_id")) {
                out.add(c);
            }
        }
        return out;
    }

    /** Best-effort table-name extraction for a {@link RolapSchema.PhysRelation}. */
    private static String tableNameOf(RolapSchema.PhysRelation rel) {
        if (rel instanceof RolapSchema.PhysTable) {
            return ((RolapSchema.PhysTable) rel).getName();
        }
        return null;
    }

    /** String form for debugging / test assertions. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MvRegistry{size=").append(materializations.size())
            .append(", skipped=").append(skippedAggs).append('}');
        for (RelOptMaterialization m : materializations) {
            sb.append("\n  ").append(m.qualifiedTableName).append(":");
            sb.append("\n    target: ")
                .append(RelOptUtil.toString(m.tableRel).trim().split("\n")[0]);
            sb.append("\n    query:\n      ")
                .append(
                    RelOptUtil.toString(m.queryRel)
                        .replace("\n", "\n      "));
        }
        return sb.toString();
    }
}

// End MvRegistry.java
