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
import mondrian.rolap.RolapStar;
import mondrian.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Inspects a {@link RolapMeasureGroup} and exposes the set of
 * columns that are directly projectable from the aggregate table —
 * i.e. columns declared via {@code <CopyLink>} in the schema. These
 * are the candidate group-by columns for power-set shape enumeration.
 *
 * <p>Phase-1 scope: copy-linked columns only. FK-reachable dim
 * attributes (e.g. {@code store.store_country} on {@code agg_c_14})
 * are handled by the Phase-2 FK-dim enumerator; until that lands,
 * FK-reachable shapes remain in the hand-curated fallback.
 *
 * <p>Runtime source of truth is {@link RolapMeasureGroup#getCopyColumnList()}
 * — a {@code List<Pair<RolapStar.Column aggStar, PhysColumn dimCol>>}
 * populated by {@code RolapSchemaLoader.addCopyLink} where the left
 * wraps the agg-side {@code PhysColumn} and the right is the base
 * dim-side {@code PhysColumn}.
 */
final class MeasureGroupShapeInspector {

    private MeasureGroupShapeInspector() {
    }

    /**
     * Returns one {@link MvRegistry.GroupCol} per copy-linked column
     * on {@code mg}. Each result carries:
     * <ul>
     *   <li>{@code table, column} — dim-side table alias + column name
     *       (the group key a user query would reference).</li>
     *   <li>{@code aggTable, aggColumn} — agg-side table alias + column
     *       name (the denormalised counterpart projectable directly
     *       from the agg scan).</li>
     * </ul>
     *
     * <p>Entries whose agg or dim side is not a real physical column
     * are skipped (matches the robustness pattern used by
     * {@code MvRegistry.resolveMeasures}).
     */
    static List<MvRegistry.GroupCol> copyLinkedColumns(
        RolapMeasureGroup mg)
    {
        List<MvRegistry.GroupCol> out = new ArrayList<>();
        for (Pair<RolapStar.Column, RolapSchema.PhysColumn> p
            : mg.getCopyColumnList())
        {
            RolapStar.Column starAgg = p.left;
            RolapSchema.PhysColumn dimCol = p.right;
            if (starAgg == null || dimCol == null) {
                continue;
            }
            RolapSchema.PhysColumn aggCol = starAgg.getExpression();
            if (!(aggCol instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            if (!(dimCol instanceof RolapSchema.PhysRealColumn)) {
                continue;
            }
            if (aggCol.relation == null || dimCol.relation == null) {
                continue;
            }
            out.add(new MvRegistry.GroupCol(
                dimCol.relation.getAlias(),
                dimCol.name,
                aggCol.relation.getAlias(),
                aggCol.name));
        }
        return out;
    }

    /**
     * True if this MeasureGroup copy-links {@code time_by_day.the_year}
     * — the signal used by the year-prefix variant in
     * {@link ShapeEnumerator}.
     */
    static boolean hasCopyLinkedYear(RolapMeasureGroup mg) {
        for (MvRegistry.GroupCol g : copyLinkedColumns(mg)) {
            if ("time_by_day".equals(g.table)
                && "the_year".equals(g.column))
            {
                return true;
            }
        }
        return false;
    }
}

// End MeasureGroupShapeInspector.java
