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

import mondrian.rolap.RolapMeasureGroup;
import mondrian.rolap.RolapSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Enumerates {@link MvRegistry.ShapeSpec}s by taking the power-set of
 * a MeasureGroup's copy-linked columns (Phase 1 scope — see
 * {@code docs/plans/2026-04-21-shape-catalog-enumeration.md}).
 *
 * <p>For each non-empty subset of copy-linked columns (bounded by
 * {@code maxSubsetSize}), emits one {@code ShapeSpec} whose:
 * <ul>
 *   <li>{@code groups} are the subset's copy-linked columns with
 *       agg-side counterparts filled in (so the tableRel can project
 *       them directly from the agg scan).</li>
 *   <li>{@code joins} are the fact→dim FK joins needed to reach each
 *       unique dim-side table in the subset (derived from the fact
 *       relation's {@link RolapSchema.PhysSchema#hardLinksFrom}).</li>
 * </ul>
 *
 * <p>FK-only reachable dim attributes (e.g. {@code store.store_country}
 * on {@code agg_c_14}) are out of scope here — they require the
 * Phase-2 FK-dim enumerator. Until that lands, the hand-curated
 * fallback in {@link MvRegistry} preserves those shapes.
 */
final class ShapeEnumerator {

    static final int DEFAULT_MAX_SUBSET_SIZE = 4;

    private ShapeEnumerator() {
    }

    /**
     * Enumerates shapes for {@code mg}'s copy-linked columns.
     *
     * @param mg            declared aggregate MeasureGroup
     * @param aggTable      agg-side table name (e.g.
     *                      {@code agg_c_14_sales_fact_1997})
     * @param factTable     base-fact table alias (e.g.
     *                      {@code sales_fact_1997})
     * @param measures      resolved measure references for {@code mg}
     * @param maxSubsetSize upper bound on subset size (cap)
     * @return shapes — may be empty if {@code mg} has no copy-linked
     *         columns or join resolution fails
     */
    static List<MvRegistry.ShapeSpec> enumerate(
        RolapMeasureGroup mg,
        String aggTable,
        String factTable,
        List<MvRegistry.MeasureRef> measures,
        int maxSubsetSize)
    {
        List<MvRegistry.GroupCol> cols =
            MeasureGroupShapeInspector.copyLinkedColumns(mg);
        if (cols.isEmpty()) {
            return new ArrayList<>();
        }
        // Dedupe on (dim-side table, column) — one copy-link declaration
        // may surface the same column from multiple measure groups.
        LinkedHashMap<String, MvRegistry.GroupCol> unique =
            new LinkedHashMap<>();
        for (MvRegistry.GroupCol c : cols) {
            unique.putIfAbsent(c.table + "." + c.column, c);
        }
        List<MvRegistry.GroupCol> base = new ArrayList<>(unique.values());
        int n = base.size();
        int cap = Math.min(maxSubsetSize, n);
        if (cap < 1) {
            return new ArrayList<>();
        }

        RolapMeasureGroup baseMg = findBaseMeasureGroup(mg, factTable);
        if (baseMg == null) {
            return new ArrayList<>();
        }
        RolapSchema.PhysRelation factRel = baseMg.getFactRelation();
        Map<String, RolapSchema.PhysLink> factToDimLinks =
            collectFactToDimLinks(baseMg, factRel);

        List<MvRegistry.ShapeSpec> out = new ArrayList<>();
        LinkedHashSet<String> seenShapeKey = new LinkedHashSet<>();
        for (int mask = 1; mask < (1 << n); mask++) {
            int k = Integer.bitCount(mask);
            if (k > cap) {
                continue;
            }
            List<MvRegistry.GroupCol> subset = new ArrayList<>(k);
            for (int i = 0; i < n; i++) {
                if ((mask & (1 << i)) != 0) {
                    subset.add(base.get(i));
                }
            }
            MvRegistry.ShapeSpec spec =
                buildShape(
                    factToDimLinks, aggTable, factTable,
                    measures, subset);
            if (spec != null && seenShapeKey.add(shapeKey(spec))) {
                out.add(spec);
            }
        }
        // Year-prefix variants: Mondrian's implicit slicer often adds
        // [Time].[Year] even when the MDX query doesn't mention it.
        // For every enumerated shape that does NOT already contain
        // the_year — and for which this MeasureGroup copy-links
        // time_by_day.the_year — emit {year} ∪ subset. Matters most at
        // the subset-size cap: the base power-set can't exceed cap, so
        // year-prefix variants are the only way a cap-sized no-year
        // subset gets its year-prefixed form into the registry.
        MvRegistry.GroupCol yearCol = findYearCol(base);
        if (yearCol != null) {
            List<MvRegistry.ShapeSpec> yearVariants = new ArrayList<>();
            for (MvRegistry.ShapeSpec s : out) {
                if (containsYear(s.groups)) {
                    continue;
                }
                List<MvRegistry.GroupCol> prefixed =
                    new ArrayList<>(s.groups.size() + 1);
                prefixed.add(yearCol);
                prefixed.addAll(s.groups);
                MvRegistry.ShapeSpec variant =
                    buildShape(
                        factToDimLinks, aggTable, factTable,
                        measures, prefixed);
                if (variant != null
                    && seenShapeKey.add(shapeKey(variant)))
                {
                    yearVariants.add(variant);
                }
            }
            out.addAll(yearVariants);
        }
        return out;
    }

    private static MvRegistry.GroupCol findYearCol(
        List<MvRegistry.GroupCol> base)
    {
        for (MvRegistry.GroupCol g : base) {
            if ("time_by_day".equals(g.table)
                && "the_year".equals(g.column))
            {
                return g;
            }
        }
        return null;
    }

    private static boolean containsYear(List<MvRegistry.GroupCol> groups) {
        for (MvRegistry.GroupCol g : groups) {
            if ("time_by_day".equals(g.table)
                && "the_year".equals(g.column))
            {
                return true;
            }
        }
        return false;
    }

    /** Default max subset size, overridable via system property. */
    static int defaultMaxSubsetSize() {
        String override = System.getProperty(
            "mondrian.calcite.mvMaxSubsetSize");
        if (override != null) {
            try {
                int v = Integer.parseInt(override);
                if (v >= 1) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return DEFAULT_MAX_SUBSET_SIZE;
    }

    private static MvRegistry.ShapeSpec buildShape(
        Map<String, RolapSchema.PhysLink> factToDimLinks,
        String aggTable,
        String factTable,
        List<MvRegistry.MeasureRef> measures,
        List<MvRegistry.GroupCol> subset)
    {
        List<MvRegistry.DimJoin> joins =
            requiredJoinsFor(factToDimLinks, factTable, subset);
        if (joins == null) {
            return null;
        }
        return new MvRegistry.ShapeSpec(
            shapeName(aggTable, subset),
            aggTable,
            factTable,
            measures,
            joins,
            new ArrayList<>(subset));
    }

    private static List<MvRegistry.DimJoin> requiredJoinsFor(
        Map<String, RolapSchema.PhysLink> factToDimLinks,
        String factTable,
        List<MvRegistry.GroupCol> subset)
    {
        LinkedHashSet<String> dimTables = new LinkedHashSet<>();
        for (MvRegistry.GroupCol g : subset) {
            dimTables.add(g.table);
        }
        List<MvRegistry.DimJoin> joins = new ArrayList<>(dimTables.size());
        for (String dim : dimTables) {
            RolapSchema.PhysLink link = factToDimLinks.get(dim);
            if (link == null) {
                return null;
            }
            List<RolapSchema.PhysColumn> fkCols = link.getColumnList();
            List<RolapSchema.PhysColumn> pkCols =
                link.getSourceKey().getColumnList();
            if (fkCols.size() != 1 || pkCols.size() != 1) {
                // Composite keys: out of scope for Phase 1; fall back.
                return null;
            }
            joins.add(new MvRegistry.DimJoin(
                dim,
                new MvRegistry.JoinCol(factTable, fkCols.get(0).name),
                new MvRegistry.JoinCol(dim, pkCols.get(0).name)));
        }
        return joins;
    }

    /**
     * Collects fact→dim FK links from this MeasureGroup's dimension
     * paths. Schema-level {@code <Link>} is used only for dim→dim
     * (snowflake) links; fact→dim FK joins are stored per-MG in
     * {@code dimensionMap3} (keyed by {@link mondrian.rolap.RolapCubeDimension}).
     * Map key is the dim-side relation alias; value is the PhysLink
     * that starts at {@code factRel} and ends at that dim.
     */
    private static Map<String, RolapSchema.PhysLink> collectFactToDimLinks(
        RolapMeasureGroup mg, RolapSchema.PhysRelation factRel)
    {
        Map<String, RolapSchema.PhysLink> out = new LinkedHashMap<>();
        for (RolapSchema.PhysPath path : mg.dimensionMap3.values()) {
            if (path == null) {
                continue;
            }
            for (RolapSchema.PhysHop hop : path.hopList) {
                if (hop.link == null) {
                    continue;
                }
                if (hop.link.getFrom() != factRel) {
                    continue;
                }
                RolapSchema.PhysRelation dimRel =
                    hop.link.getSourceKey().getRelation();
                if (dimRel == null) {
                    continue;
                }
                out.putIfAbsent(dimRel.getAlias(), hop.link);
            }
        }
        return out;
    }

    /** Locate the non-aggregate MeasureGroup in {@code mg}'s cube whose
     *  fact relation is {@code factTableAlias}. Its {@code dimensionMap3}
     *  owns the canonical fact→dim FK paths for the base star —
     *  {@code mg.dimensionMap3} doesn't (aggregate MGs store agg→dim
     *  paths, and in CopyLink cases not even that). */
    private static RolapMeasureGroup findBaseMeasureGroup(
        RolapMeasureGroup mg, String factTableAlias)
    {
        if (mg.getCube() == null) {
            return null;
        }
        for (RolapMeasureGroup candidate : mg.getCube().getMeasureGroups()) {
            if (candidate.isAggregate()) {
                continue;
            }
            RolapSchema.PhysRelation rel = candidate.getFactRelation();
            if (rel != null
                && factTableAlias.equals(rel.getAlias()))
            {
                return candidate;
            }
        }
        return null;
    }

    private static String shapeName(
        String aggTable, List<MvRegistry.GroupCol> subset)
    {
        // Use a short agg prefix (strip trailing _sales_fact_1997 etc.
        // is not worth doing robustly — just use the full agg name).
        StringBuilder sb = new StringBuilder(aggTable).append("::");
        for (int i = 0; i < subset.size(); i++) {
            if (i > 0) {
                sb.append('+');
            }
            sb.append(subset.get(i).column);
        }
        return sb.toString();
    }

    /** Canonical key for cross-enumeration dedup: agg table + sorted
     *  dim-side group columns. */
    private static String shapeKey(MvRegistry.ShapeSpec spec) {
        List<String> cols = new ArrayList<>(spec.groups.size());
        for (MvRegistry.GroupCol g : spec.groups) {
            cols.add(g.table + "." + g.column);
        }
        java.util.Collections.sort(cols);
        return spec.aggTable + "|" + String.join(",", cols);
    }
}

// End ShapeEnumerator.java
