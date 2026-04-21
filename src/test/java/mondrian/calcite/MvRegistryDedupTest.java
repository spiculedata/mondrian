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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit-level coverage for {@link MvRegistry#dedupeByCoverage}. Uses
 * synthetic shapes — two agg tables covering the same
 * {@code (group-cols, measure-set)} combination, with known row
 * counts — and asserts the dedup retains the smaller-row-count
 * shape.
 */
public class MvRegistryDedupTest {

    @Test
    public void picksSmallerAggregateOnRowCountTie() {
        MvRegistry.GroupCol year = new MvRegistry.GroupCol(
            "time_by_day", "the_year",
            "agg_small", "the_year");
        MvRegistry.GroupCol yearBig = new MvRegistry.GroupCol(
            "time_by_day", "the_year",
            "agg_big", "the_year");
        MvRegistry.MeasureRef sum = new MvRegistry.MeasureRef(
            "unit_sales", "unit_sales", "sum");

        MvRegistry.ShapeSpec small = new MvRegistry.ShapeSpec(
            "small::year", "agg_small", "sales_fact_1997",
            Collections.singletonList(sum),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(year));
        MvRegistry.ShapeSpec big = new MvRegistry.ShapeSpec(
            "big::year", "agg_big", "sales_fact_1997",
            Collections.singletonList(sum),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(yearBig));

        ToIntFunction<String> rows = name -> {
            if ("agg_small".equals(name)) {
                return 100;
            }
            if ("agg_big".equals(name)) {
                return 10_000;
            }
            return 0;
        };

        List<MvRegistry.ShapeSpec> out = MvRegistry.dedupeByCoverage(
            Arrays.asList(big, small), rows);
        assertEquals(1, out.size());
        assertEquals("agg_small", out.get(0).aggTable);
    }

    @Test
    public void prefersPopulatedRowCountOverUnpopulated() {
        MvRegistry.GroupCol g = new MvRegistry.GroupCol(
            "time_by_day", "the_year", "agg_populated", "the_year");
        MvRegistry.GroupCol g2 = new MvRegistry.GroupCol(
            "time_by_day", "the_year", "agg_unknown", "the_year");

        MvRegistry.ShapeSpec populated = new MvRegistry.ShapeSpec(
            "p::year", "agg_populated", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(g));
        MvRegistry.ShapeSpec unknown = new MvRegistry.ShapeSpec(
            "u::year", "agg_unknown", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(g2));

        ToIntFunction<String> rows = name ->
            "agg_populated".equals(name) ? 500 : 0;

        List<MvRegistry.ShapeSpec> out = MvRegistry.dedupeByCoverage(
            Arrays.asList(unknown, populated), rows);
        assertEquals(1, out.size());
        assertEquals("agg_populated", out.get(0).aggTable);
    }

    @Test
    public void alphabeticalFallbackWhenNoStats() {
        MvRegistry.GroupCol g1 = new MvRegistry.GroupCol(
            "store", "store_country", "agg_zeta", "store_country");
        MvRegistry.GroupCol g2 = new MvRegistry.GroupCol(
            "store", "store_country", "agg_alpha", "store_country");

        MvRegistry.ShapeSpec z = new MvRegistry.ShapeSpec(
            "z::country", "agg_zeta", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(g1));
        MvRegistry.ShapeSpec a = new MvRegistry.ShapeSpec(
            "a::country", "agg_alpha", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(g2));

        List<MvRegistry.ShapeSpec> out = MvRegistry.dedupeByCoverage(
            Arrays.asList(z, a), name -> 0);
        assertEquals(1, out.size());
        assertEquals("agg_alpha", out.get(0).aggTable);
    }

    @Test
    public void distinctCoverageKeysPreserved() {
        MvRegistry.GroupCol year = new MvRegistry.GroupCol(
            "time_by_day", "the_year", "agg_a", "the_year");
        MvRegistry.GroupCol quarter = new MvRegistry.GroupCol(
            "time_by_day", "quarter", "agg_a", "quarter");

        MvRegistry.ShapeSpec s1 = new MvRegistry.ShapeSpec(
            "a::year", "agg_a", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(year));
        MvRegistry.ShapeSpec s2 = new MvRegistry.ShapeSpec(
            "a::quarter", "agg_a", "sales_fact_1997",
            Collections.<MvRegistry.MeasureRef>emptyList(),
            Collections.<MvRegistry.DimJoin>emptyList(),
            Collections.singletonList(quarter));

        List<MvRegistry.ShapeSpec> out = MvRegistry.dedupeByCoverage(
            Arrays.asList(s1, s2), name -> 0);
        assertEquals(2, out.size());
        assertTrue(out.contains(s1));
        assertTrue(out.contains(s2));
    }
}

// End MvRegistryDedupTest.java
