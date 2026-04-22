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
import java.util.Collections;
import java.util.List;

/**
 * Canonical key for indexing {@link MvRegistry.ShapeSpec}s by the
 * {@code (factTable, sorted group columns)} coverage they offer —
 * enables O(1) lookup from {@link MvMatcher} instead of a linear
 * scan over every registered shape.
 *
 * <p>{@code MvMatcher}'s current iteration model is size-equality plus
 * per-spec column matching; this key collapses both predicates into a
 * single hashable value. PlannerRequests produce the same key shape
 * by sorting the request's groupBy columns the same way.
 *
 * <p>Introduced for Task 6 of the shape-catalog enumeration plan
 * (2026-04-21) — the post-Phase-2 registry is expected to scale from
 * tens to thousands of shapes, and a linear matcher scan there
 * becomes a hot-path issue.
 */
final class GroupColKey {

    private final String factTable;
    private final List<String> sortedCols;
    private final int hash;

    private GroupColKey(String factTable, List<String> sortedCols) {
        this.factTable = factTable;
        this.sortedCols = sortedCols;
        this.hash = computeHash(factTable, sortedCols);
    }

    /**
     * Build a key from a {@link MvRegistry.ShapeSpec}'s fact-table
     * and group columns (dim-side table + column pairs).
     */
    static GroupColKey forSpec(MvRegistry.ShapeSpec s) {
        List<String> cols = new ArrayList<>(s.groups.size());
        for (MvRegistry.GroupCol g : s.groups) {
            cols.add(g.table + "." + g.column);
        }
        Collections.sort(cols);
        return new GroupColKey(s.factTable, cols);
    }

    /**
     * Build a key from a {@code (factTable, table.column strings)}
     * pair — used by {@link MvMatcher} to look up candidate shapes
     * for a PlannerRequest.
     */
    static GroupColKey forRequest(String factTable, List<String> cols) {
        List<String> sorted = new ArrayList<>(cols);
        Collections.sort(sorted);
        return new GroupColKey(factTable, sorted);
    }

    int size() {
        return sortedCols.size();
    }

    private static int computeHash(
        String factTable, List<String> sortedCols)
    {
        int h = factTable == null ? 0 : factTable.hashCode();
        for (String c : sortedCols) {
            h = 31 * h + c.hashCode();
        }
        return h;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupColKey)) {
            return false;
        }
        GroupColKey that = (GroupColKey) obj;
        return this.hash == that.hash
            && java.util.Objects.equals(this.factTable, that.factTable)
            && this.sortedCols.equals(that.sortedCols);
    }

    @Override
    public String toString() {
        return factTable + "|" + String.join(",", sortedCols);
    }
}

// End GroupColKey.java
