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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of Calcite {@link RelOptMaterialization} entries derived from
 * Mondrian-4 {@code <MeasureGroup type='aggregate'>} declarations.
 *
 * <p><b>Shape-aware design (post-Y.4.1 rewrite).</b> For each declared
 * aggregate {@link RolapMeasureGroup} we emit <em>N</em> materializations,
 * one per per-query-shape ({@link ShapeSpec}). Each shape's
 * {@code queryRel} is a minimal
 * {@code Aggregate(Join(fact, <only-the-dims-this-shape-uses>))} whose
 * group keys match the dim-side columns a user query would reference,
 * and whose {@code tableRel} is a {@code Project(Scan(aggTable))}
 * selecting the agg-side columns that carry the same semantic values.
 *
 * <p>Motivation — the original "single full-star MV" design emitted a
 * 5-way join with 7 group keys; Calcite 1.41's {@code
 * SubstitutionVisitor} could not rewrite user queries (2-dim star with
 * dim-attribute group keys) onto it because the row-type / group-key
 * gap required functional-dependency metadata Calcite's JDBC
 * reflection doesn't surface. See {@code
 * docs/reports/perf-investigation-volcano-mv-diagnosis.md} for the full
 * analysis.
 *
 * <p>Initial shape catalog targets the four MvHit corpus queries:
 * <ol>
 *   <li>{@code product_family × gender} → {@code agg_g_ms_pcat_sales_fact_1997}</li>
 *   <li>{@code the_year × store_country} → {@code agg_c_14_sales_fact_1997}</li>
 *   <li>{@code the_year × quarter × store_country} → {@code agg_c_14_sales_fact_1997}</li>
 *   <li>{@code product_family × gender × marital_status} →
 *       {@code agg_g_ms_pcat_sales_fact_1997}</li>
 * </ol>
 */
public class MvRegistry {

    private static final Logger LOGGER = Logger.getLogger(MvRegistry.class);

    private final List<RelOptMaterialization> materializations;
    private final List<String> skippedAggs;
    /** Shape specs for lazy per-planner materialization construction.
     *  Populated by {@link #fromSchema}; null for test fixtures. */
    private final List<ShapeSpec> shapeSpecs;
    /** Calcite schema needed to build per-planner materializations. */
    private final CalciteMondrianSchema calciteSchema;

    private MvRegistry(
        List<RelOptMaterialization> materializations,
        List<String> skippedAggs,
        List<ShapeSpec> shapeSpecs,
        CalciteMondrianSchema calciteSchema)
    {
        this.materializations =
            Collections.unmodifiableList(materializations);
        this.skippedAggs = Collections.unmodifiableList(skippedAggs);
        this.shapeSpecs = shapeSpecs == null
            ? Collections.<ShapeSpec>emptyList()
            : Collections.unmodifiableList(shapeSpecs);
        this.calciteSchema = calciteSchema;
    }

    private MvRegistry(
        List<RelOptMaterialization> materializations,
        List<String> skippedAggs)
    {
        this(materializations, skippedAggs, null, null);
    }

    /**
     * Empty registry — test seam and explicit "no MVs" sentinel.
     */
    protected MvRegistry() {
        this(
            Collections.<RelOptMaterialization>emptyList(),
            Collections.<String>emptyList());
    }

    /**
     * Test seam — construct a registry from a fixed list of
     * pre-built materializations.
     */
    protected MvRegistry(List<RelOptMaterialization> materializations) {
        this(materializations, Collections.<String>emptyList());
    }

    /** Registered MV entries. */
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
     * and builds per-shape materializations.
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
        List<ShapeSpec> allSpecs = new ArrayList<>();
        java.util.Set<String> seenAgg = new java.util.LinkedHashSet<>();
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
                    continue;
                }
                if (!seenAgg.add(aggTable)) {
                    continue;
                }
                String factTable = resolveFactTable(mg);
                if (factTable == null) {
                    skipped.add(mg.getName() + " (no fact table)");
                    continue;
                }
                List<ShapeSpec> specs =
                    buildShapeSpecs(mg, aggTable, factTable);
                if (specs.isEmpty()) {
                    skipped.add(
                        mg.getName() + " (no shapes for " + aggTable + ")");
                    continue;
                }
                int built = 0;
                for (ShapeSpec s : specs) {
                    try {
                        RelOptMaterialization mat =
                            buildMaterialization(cfg, s);
                        if (mat != null) {
                            out.add(mat);
                            allSpecs.add(s);
                            built++;
                        }
                    } catch (RuntimeException re) {
                        LOGGER.warn(
                            "MvRegistry: skipping shape "
                            + s.name + " — " + re);
                    }
                }
                if (built == 0) {
                    skipped.add(
                        mg.getName() + " (all shapes failed to build)");
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "MvRegistry built: " + out.size() + " materialization(s), "
                + skipped.size() + " skipped");
        }
        return new MvRegistry(out, skipped, allSpecs, calciteSchema);
    }

    /**
     * Builds fresh {@link RelOptMaterialization} instances in the
     * given schema's {@link FrameworkConfig}. Used to re-emit
     * materializations sharing the TypeFactory with a user-rel
     * cluster so {@code SubstitutionVisitor}'s RexNode-equals
     * matching can match type references.
     *
     * <p>Returns the registry's pre-built materializations if no
     * shape specs were captured (e.g. test fixtures).
     */
    public List<RelOptMaterialization> materializationsFor(
        FrameworkConfig cfg)
    {
        if (shapeSpecs.isEmpty()) {
            return materializations;
        }
        List<RelOptMaterialization> out =
            new ArrayList<>(shapeSpecs.size());
        for (ShapeSpec s : shapeSpecs) {
            try {
                RelOptMaterialization m = buildMaterialization(cfg, s);
                if (m != null) {
                    out.add(m);
                }
            } catch (RuntimeException re) {
                LOGGER.warn(
                    "MvRegistry.materializationsFor: "
                    + s.name + " — " + re);
            }
        }
        return out;
    }

    /** @return Calcite schema used to build materializations. */
    public CalciteMondrianSchema calciteSchema() {
        return calciteSchema;
    }

    /**
     * Exposes the underlying {@link ShapeSpec} list that drives both
     * the Calcite-MV-rule registration path and the hand-rolled
     * {@link MvMatcher} PlannerRequest-level rewrite (Option D,
     * {@code docs/reports/perf-investigation-volcano-mv-win.md}).
     * Read-only; empty for registries built from pre-packaged
     * materialization lists (test fixtures).
     */
    public List<ShapeSpec> shapeSpecs() {
        return shapeSpecs;
    }

    /**
     * Returns the list of {@link ShapeSpec}s for this aggregate
     * MeasureGroup. The current implementation hardcodes the four
     * shapes that match the MvHit corpus — future work will
     * generalise to power-set enumeration over dim subsets. Shapes
     * that don't apply to {@code aggTable} are filtered out by
     * matching on the agg-table name.
     */
    static List<ShapeSpec> buildShapeSpecs(
        RolapMeasureGroup mg, String aggTable, String factTable)
    {
        List<ShapeSpec> out = new ArrayList<>();
        // Measure mapping shared by all shapes of a given agg table.
        List<MeasureRef> measures = resolveMeasures(mg);

        if ("agg_c_14_sales_fact_1997".equals(aggTable)) {
            // Shape 2: Aggregate[time_by_day.the_year, store.store_country]
            //          (Join(fact, store, time_by_day))
            out.add(new ShapeSpec(
                "agg_c_14::year-country",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "store",
                        new JoinCol("sales_fact_1997", "store_id"),
                        new JoinCol("store", "store_id")),
                    new DimJoin(
                        "time_by_day",
                        new JoinCol("sales_fact_1997", "time_id"),
                        new JoinCol("time_by_day", "time_id"))),
                Arrays.asList(
                    new GroupCol(
                        "time_by_day", "the_year",
                        aggTable, "the_year"),
                    new GroupCol(
                        "store", "store_country",
                        null, null))));
            // Shape 3: Aggregate[the_year, quarter, store_country]
            out.add(new ShapeSpec(
                "agg_c_14::year-quarter-country",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "store",
                        new JoinCol("sales_fact_1997", "store_id"),
                        new JoinCol("store", "store_id")),
                    new DimJoin(
                        "time_by_day",
                        new JoinCol("sales_fact_1997", "time_id"),
                        new JoinCol("time_by_day", "time_id"))),
                Arrays.asList(
                    new GroupCol(
                        "time_by_day", "the_year",
                        aggTable, "the_year"),
                    new GroupCol(
                        "time_by_day", "quarter",
                        aggTable, "quarter"),
                    new GroupCol(
                        "store", "store_country",
                        null, null))));
            // Bonus shape: Aggregate[the_year] — common rollup over Time.
            out.add(new ShapeSpec(
                "agg_c_14::year",
                aggTable, factTable, measures,
                Collections.singletonList(
                    new DimJoin(
                        "time_by_day",
                        new JoinCol("sales_fact_1997", "time_id"),
                        new JoinCol("time_by_day", "time_id"))),
                Collections.singletonList(
                    new GroupCol(
                        "time_by_day", "the_year",
                        aggTable, "the_year"))));
        }

        if ("agg_g_ms_pcat_sales_fact_1997".equals(aggTable)) {
            // Shape 1: Aggregate[product_class.product_family, customer.gender]
            //          (Join(fact, product, product_class, customer))
            // The agg table has these as denormalized CopyLink columns, so
            // the tableRel projects agg_table.product_family / gender.
            out.add(new ShapeSpec(
                "agg_g_ms_pcat::family-gender",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "product",
                        new JoinCol("sales_fact_1997", "product_id"),
                        new JoinCol("product", "product_id")),
                    new DimJoin(
                        "product_class",
                        new JoinCol("product", "product_class_id"),
                        new JoinCol("product_class", "product_class_id")),
                    new DimJoin(
                        "customer",
                        new JoinCol("sales_fact_1997", "customer_id"),
                        new JoinCol("customer", "customer_id"))),
                Arrays.asList(
                    new GroupCol(
                        "product_class", "product_family",
                        aggTable, "product_family"),
                    new GroupCol(
                        "customer", "gender",
                        aggTable, "gender"))));
            // Shape 1-b: year × family × gender
            // Mondrian always slices by [Time].[Year] under hasAll='false',
            // so segment-load groupBy always prefixes the_year. Without
            // this variant the 2-col family-gender shape never matches the
            // runtime 3-col request.
            out.add(new ShapeSpec(
                "agg_g_ms_pcat::year-family-gender",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "product",
                        new JoinCol("sales_fact_1997", "product_id"),
                        new JoinCol("product", "product_id")),
                    new DimJoin(
                        "product_class",
                        new JoinCol("product", "product_class_id"),
                        new JoinCol("product_class", "product_class_id")),
                    new DimJoin(
                        "customer",
                        new JoinCol("sales_fact_1997", "customer_id"),
                        new JoinCol("customer", "customer_id")),
                    new DimJoin(
                        "time_by_day",
                        new JoinCol("sales_fact_1997", "time_id"),
                        new JoinCol("time_by_day", "time_id"))),
                Arrays.asList(
                    new GroupCol(
                        "time_by_day", "the_year",
                        aggTable, "the_year"),
                    new GroupCol(
                        "product_class", "product_family",
                        aggTable, "product_family"),
                    new GroupCol(
                        "customer", "gender",
                        aggTable, "gender"))));
            // Shape 1-c: year × family × gender × marital_status
            out.add(new ShapeSpec(
                "agg_g_ms_pcat::year-family-gender-marital",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "product",
                        new JoinCol("sales_fact_1997", "product_id"),
                        new JoinCol("product", "product_id")),
                    new DimJoin(
                        "product_class",
                        new JoinCol("product", "product_class_id"),
                        new JoinCol("product_class", "product_class_id")),
                    new DimJoin(
                        "customer",
                        new JoinCol("sales_fact_1997", "customer_id"),
                        new JoinCol("customer", "customer_id")),
                    new DimJoin(
                        "time_by_day",
                        new JoinCol("sales_fact_1997", "time_id"),
                        new JoinCol("time_by_day", "time_id"))),
                Arrays.asList(
                    new GroupCol(
                        "time_by_day", "the_year",
                        aggTable, "the_year"),
                    new GroupCol(
                        "product_class", "product_family",
                        aggTable, "product_family"),
                    new GroupCol(
                        "customer", "gender",
                        aggTable, "gender"),
                    new GroupCol(
                        "customer", "marital_status",
                        aggTable, "marital_status"))));
            // Shape 4: family × gender × marital_status
            out.add(new ShapeSpec(
                "agg_g_ms_pcat::family-gender-marital",
                aggTable, factTable, measures,
                Arrays.asList(
                    new DimJoin(
                        "product",
                        new JoinCol("sales_fact_1997", "product_id"),
                        new JoinCol("product", "product_id")),
                    new DimJoin(
                        "product_class",
                        new JoinCol("product", "product_class_id"),
                        new JoinCol("product_class", "product_class_id")),
                    new DimJoin(
                        "customer",
                        new JoinCol("sales_fact_1997", "customer_id"),
                        new JoinCol("customer", "customer_id"))),
                Arrays.asList(
                    new GroupCol(
                        "product_class", "product_family",
                        aggTable, "product_family"),
                    new GroupCol(
                        "customer", "gender",
                        aggTable, "gender"),
                    new GroupCol(
                        "customer", "marital_status",
                        aggTable, "marital_status"))));
        }

        // agg_c_special and agg_l_05 — no shape catalog yet; the Mondrian-4
        // findAgg path still routes these queries to them via
        // RolapGalaxy.findAgg. The MV rule is purely additive.
        return out;
    }

    /** Resolves the measure refs on {@code mg} into a lightweight
     *  {@link MeasureRef} list (base column name + agg column name + fn).
     *  Entries with non-real base or agg expressions are skipped. */
    private static List<MeasureRef> resolveMeasures(RolapMeasureGroup mg) {
        List<MeasureRef> out = new ArrayList<>();
        for (RolapMeasureRef ref : mg.getMeasureRefList()) {
            if (ref.measure == null) {
                continue;
            }
            if (!(ref.aggColumn instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            String aggName =
                ((RolapSchema.PhysRealColumn) ref.aggColumn).name;
            RolapSchema.PhysColumn baseExpr = ref.measure.getExpr();
            String baseName = null;
            if (baseExpr instanceof RolapSchema.PhysRealColumn) {
                baseName = ((RolapSchema.PhysRealColumn) baseExpr).name;
            }
            String fn = ref.measure.getAggregator() == null
                ? "sum" : ref.measure.getAggregator().getName();
            out.add(new MeasureRef(aggName, baseName, fn));
        }
        return out;
    }

    /** Resolve the base-fact table name from any measure's base expr. */
    private static String resolveFactTable(RolapMeasureGroup mg) {
        for (RolapMeasureRef ref : mg.getMeasureRefList()) {
            if (ref.measure == null) {
                continue;
            }
            RolapSchema.PhysColumn baseExpr = ref.measure.getExpr();
            if (baseExpr != null && baseExpr.relation != null) {
                return baseExpr.relation.getAlias();
            }
        }
        return null;
    }

    /**
     * Builds one {@link RelOptMaterialization} for a single
     * {@link ShapeSpec}.
     *
     * <p>The {@code queryRel} is {@code Aggregate(Join(fact, dims...))}
     * whose group keys reference the dim-side columns named in the
     * spec. The {@code tableRel} is {@code Project(Scan(aggTable))}
     * projecting the agg-side columns that are semantically equivalent
     * to those group keys, followed by the measure columns in the
     * same order as the aggregate's output.
     *
     * <p>Row-type alignment: Calcite's {@code SubstitutionVisitor}
     * requires the MV's {@code tableRel} and {@code queryRel} outputs
     * to have the same column count and compatible types — the rule
     * substitutes {@code tableRel} for {@code queryRel} and expects
     * downstream rels to consume unchanged column ordinals.
     */
    static RelOptMaterialization buildMaterialization(
        FrameworkConfig cfg, ShapeSpec s)
    {
        // ---- queryRel: Aggregate(Join(fact, dim1, ..., dimN)) ----
        RelBuilder qb = RelBuilder.create(cfg);
        qb.scan(s.factTable);
        for (DimJoin j : s.joins) {
            qb.scan(j.dimAlias);
            RexNode lhs = qb.field(2, 0, j.fact.column);
            RexNode rhs = qb.field(2, 1, j.dim.column);
            qb.join(JoinRelType.INNER, qb.equals(lhs, rhs));
        }
        List<RexNode> groupKeys = new ArrayList<>(s.groups.size());
        for (GroupCol g : s.groups) {
            groupKeys.add(qb.field(g.table, g.column));
        }
        List<RelBuilder.AggCall> aggs = buildAggCalls(qb, s);
        qb.aggregate(qb.groupKey(groupKeys), aggs);
        // NB: no top-Project here. The user query (post-Hep) lacks a
        // top Project, and SubstitutionVisitor's structural match
        // requires queryRel to mirror the user tree.
        RelNode queryRel = qb.build();

        // ---- tableRel ----
        // For columns with a denormalized agg-side counterpart, project
        // from the agg scan directly. For group columns without a
        // denormalized counterpart (e.g. store.store_country on agg_c_14,
        // where agg has store_id but not store_country), re-join the
        // needed dim table to the agg scan and re-Aggregate summing the
        // pre-aggregated measures. This produces rows semantically
        // identical to queryRel, which is what SubstitutionVisitor's
        // substitution requires.
        RelBuilder tb = RelBuilder.create(cfg);
        tb.scan(s.aggTable);
        // Figure out which dim tables the tableRel must join to
        // resolve non-denormalized group cols, and which agg-side FK
        // column to join on.
        LinkedHashMap<String, DimJoin> tableJoins = new LinkedHashMap<>();
        for (GroupCol g : s.groups) {
            if (g.aggTable != null && g.aggColumn != null) {
                continue;
            }
            DimJoin dj = findJoinForDim(s.joins, g.table);
            if (dj == null) {
                return null; // unresolvable shape
            }
            // The join predicate on the agg side uses the SAME FK column
            // name convention as the fact table (e.g. store_id).
            tableJoins.putIfAbsent(g.table, dj);
        }
        for (Map.Entry<String, DimJoin> e : tableJoins.entrySet()) {
            DimJoin dj = e.getValue();
            tb.scan(dj.dimAlias);
            RexNode lhs = tb.field(2, 0, dj.fact.column);
            RexNode rhs = tb.field(2, 1, dj.dim.column);
            tb.join(JoinRelType.INNER, tb.equals(lhs, rhs));
        }
        // Always re-aggregate the agg scan. A pure-projection shortcut
        // would be correct only when the user's group-by set is a
        // unique key on the agg table — otherwise multiple agg rows
        // map to a single user-output row and skipping re-aggregation
        // produces duplicates and mis-summed measures. Shape-aware
        // MVs target varying subsets of the agg's natural key, so
        // re-aggregation is always the safe form. The agg table is
        // still cheaper to scan than the fact table because it's
        // summarised at a finer grain and pre-sums the measures.
        List<RexNode> groupRefs = new ArrayList<>();
        for (GroupCol g : s.groups) {
            if (g.aggTable != null && g.aggColumn != null) {
                groupRefs.add(tb.field(s.aggTable, g.aggColumn));
            } else {
                groupRefs.add(tb.field(g.table, g.column));
            }
        }
        List<RelBuilder.AggCall> tAggs = new ArrayList<>();
        for (MeasureRef m : s.measures) {
            // count-on-agg is pre-counted; SUM of the agg's
            // fact_count column preserves semantics, identical to the
            // SUM(pre-summed-measure) form for SUM aggregates.
            tAggs.add(
                tb.sum(tb.field(s.aggTable, m.aggColumn))
                    .as(m.aggColumn));
        }
        tb.aggregate(tb.groupKey(groupRefs), tAggs);
        // Align tableRel's output column ORDER to queryRel's natural
        // output order. Both sides aggregated independently and each
        // re-ordered group cols by ascending source-ordinal —
        // different source orderings produce different output
        // orderings. An explicit canonicalising Project maps
        // tableRel's output into queryRel's rowType exactly (field
        // names + types), which is what SubstitutionVisitor's
        // row-type invariant requires.
        List<String> canonNames = new ArrayList<>();
        for (org.apache.calcite.rel.type.RelDataTypeField f
            : queryRel.getRowType().getFieldList())
        {
            canonNames.add(f.getName());
        }
        List<RexNode> canon = new ArrayList<>(canonNames.size());
        for (String a : canonNames) {
            canon.add(tb.field(a));
        }
        tb.project(canon, canonNames, true);
        RelNode tableRel = tb.build();

        // starRelOptTable is for the deprecated star-table optimisation
        // and must be null for plain-table MVs (NPE otherwise).
        RelOptTable starRelOptTable = null;
        List<String> qualifiedName = Collections.singletonList(s.aggTable);
        return new RelOptMaterialization(
            tableRel, queryRel, starRelOptTable, qualifiedName);
    }

    /**
     * Build aggregate call list — SUM for additive measures, COUNT for
     * fact-count. Adds a synthetic COUNT(*) AS fact_count if the spec
     * has none (keeps row-type stable across shapes).
     */
    private static List<RelBuilder.AggCall> buildAggCalls(
        RelBuilder qb, ShapeSpec s)
    {
        List<RelBuilder.AggCall> aggs = new ArrayList<>();
        for (MeasureRef m : s.measures) {
            if ("count".equalsIgnoreCase(m.fn)) {
                aggs.add(qb.count(false, m.aggColumn).as(m.aggColumn));
            } else if (m.baseColumn != null) {
                try {
                    aggs.add(
                        qb.sum(qb.field(s.factTable, m.baseColumn))
                            .as(m.aggColumn));
                } catch (RuntimeException re) {
                    // fall through — skip this measure
                }
            }
        }
        return aggs;
    }

    /** Look up the DimJoin that terminates at {@code dimAlias} within
     *  the shape's join chain. Returns null if not present. */
    private static DimJoin findJoinForDim(
        List<DimJoin> joins, String dimAlias)
    {
        for (DimJoin j : joins) {
            if (j.dimAlias.equals(dimAlias)) {
                return j;
            }
        }
        return null;
    }

    /** Best-effort table-name extraction for a PhysRelation. */
    private static String tableNameOf(RolapSchema.PhysRelation rel) {
        if (rel instanceof RolapSchema.PhysTable) {
            return ((RolapSchema.PhysTable) rel).getName();
        }
        return null;
    }

    /**
     * Deduplicates shapes across MeasureGroups: when two shapes cover
     * the same group-col set and measure set, keeps the one whose
     * agg-table is "smaller". Tiebreakers in order:
     * <ol>
     *   <li>Both agg tables report a row count &gt; 0: pick the
     *       smaller row count.</li>
     *   <li>Only one reports &gt; 0: pick it (populated stats are
     *       more trustworthy than unpopulated).</li>
     *   <li>Neither reports &gt; 0: alphabetical on agg-table name,
     *       deterministic. (Column-count fallback would require schema
     *       introspection that isn't threaded here.)</li>
     * </ol>
     *
     * <p>Per plan revision 2026-04-21: {@code PhysTable.getRowCount}
     * exists but is only populated for tables whose stats have been
     * probed; tables loaded without stats report 0 and fall through
     * to the alphabetical tiebreaker — this is flagged here so the
     * non-determinism cost is visible.
     */
    static List<ShapeSpec> dedupeByCoverage(
        List<ShapeSpec> shapes,
        java.util.function.ToIntFunction<String> rowCountOf)
    {
        LinkedHashMap<String, ShapeSpec> best = new LinkedHashMap<>();
        for (ShapeSpec s : shapes) {
            String key = coverageKey(s);
            ShapeSpec cur = best.get(key);
            if (cur == null || beats(s, cur, rowCountOf)) {
                best.put(key, s);
            }
        }
        return new ArrayList<>(best.values());
    }

    private static String coverageKey(ShapeSpec s) {
        List<String> cols = new ArrayList<>(s.groups.size());
        for (GroupCol g : s.groups) {
            cols.add(g.table + "." + g.column);
        }
        Collections.sort(cols);
        List<String> ms = new ArrayList<>(s.measures.size());
        for (MeasureRef m : s.measures) {
            ms.add(m.aggColumn + ":" + m.fn);
        }
        Collections.sort(ms);
        return String.join(",", cols) + "|" + String.join(",", ms);
    }

    private static boolean beats(
        ShapeSpec candidate, ShapeSpec incumbent,
        java.util.function.ToIntFunction<String> rowCountOf)
    {
        int rc = rowCountOf.applyAsInt(candidate.aggTable);
        int ri = rowCountOf.applyAsInt(incumbent.aggTable);
        if (rc > 0 && ri > 0) {
            if (rc != ri) {
                return rc < ri;
            }
            return candidate.aggTable.compareTo(incumbent.aggTable) < 0;
        }
        if (rc > 0) {
            return true;
        }
        if (ri > 0) {
            return false;
        }
        return candidate.aggTable.compareTo(incumbent.aggTable) < 0;
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
                .append(RelOptUtil.toString(m.tableRel)
                    .trim().split("\n")[0]);
            sb.append("\n    query:\n      ")
                .append(
                    RelOptUtil.toString(m.queryRel)
                        .replace("\n", "\n      "));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------
    // Spec value types
    // ------------------------------------------------------------

    /** A shape: target agg table + minimal defining query description. */
    static final class ShapeSpec {
        final String name;
        final String aggTable;
        final String factTable;
        final List<MeasureRef> measures;
        final List<DimJoin> joins;
        final List<GroupCol> groups;
        ShapeSpec(
            String name, String aggTable, String factTable,
            List<MeasureRef> measures, List<DimJoin> joins,
            List<GroupCol> groups)
        {
            this.name = name;
            this.aggTable = aggTable;
            this.factTable = factTable;
            this.measures = measures;
            this.joins = joins;
            this.groups = groups;
        }
    }

    /** One dim join in a shape's defining-query join chain. */
    static final class DimJoin {
        final String dimAlias;
        final JoinCol fact;   // LHS column
        final JoinCol dim;    // RHS column
        DimJoin(String dimAlias, JoinCol fact, JoinCol dim) {
            this.dimAlias = dimAlias;
            this.fact = fact;
            this.dim = dim;
        }
    }

    /** (table, column) pair used in a join predicate. */
    static final class JoinCol {
        final String table;
        final String column;
        JoinCol(String table, String column) {
            this.table = table;
            this.column = column;
        }
    }

    /** A group-by column in a shape, with its agg-side counterpart. */
    static final class GroupCol {
        final String table;      // dim-side table alias in the join chain
        final String column;     // dim-side column name
        final String aggTable;   // agg-side table (may be null if not
                                 //   directly projectable from the agg).
        final String aggColumn;  // agg-side column name.
        GroupCol(
            String table, String column,
            String aggTable, String aggColumn)
        {
            this.table = table;
            this.column = column;
            this.aggTable = aggTable;
            this.aggColumn = aggColumn;
        }
    }

    /** A measure's (aggColumn, baseColumn, fn) triple. */
    static final class MeasureRef {
        final String aggColumn;
        final String baseColumn;
        final String fn;
        MeasureRef(String aggColumn, String baseColumn, String fn) {
            this.aggColumn = aggColumn;
            this.baseColumn = baseColumn;
            this.fn = fn;
        }
    }
}

// End MvRegistry.java
