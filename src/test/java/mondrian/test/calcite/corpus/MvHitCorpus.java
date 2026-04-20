/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.test.calcite.corpus;

import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Task-U.1 corpus: MDX queries crafted to hit FoodMart's declared
 * aggregate {@code MeasureGroup}s (demo/FoodMart.mondrian.xml).
 *
 * <p>Each entry pairs a {@link NamedMdx} with the set of aggregate-table
 * names that are acceptable in the captured SQL. The Mondrian-4 aggregate
 * matcher ({@code RolapGalaxy.findAgg}) picks the lowest-cost candidate
 * covering the query's grain + measure set.
 *
 * <h3>Coverage</h3>
 * <ul>
 *   <li><b>agg_g_ms_pcat_sales_fact_1997</b> — uniquely hit by
 *       {@code product_family × gender × unit_sales} (no other agg
 *       carries both {@code product_family} and {@code gender} columns).
 *       See {@link #AGG_G_MS_PCAT_UNIQUE}.</li>
 *   <li><b>agg_c_special_sales_fact_1997 / agg_c_14_sales_fact_1997</b>
 *       — interchangeable from the matcher's POV (identical dimensions,
 *       identical measure set, differ only in {@code aggColumn} aliases).
 *       Queries #2 and #3 cover this family; the assertion accepts
 *       either.</li>
 *   <li><b>agg_l_05_sales_fact_1997</b> — <b>structurally unreachable</b>
 *       by pure MDX in the stock FoodMart schema: the agg has
 *       {@code NoLink Time}, but {@code [Time]} is declared with
 *       {@code hasAll='false'}, so every MDX query inherits a
 *       {@code [Time].[1997]} default-member filter. The matcher needs
 *       a {@code the_year} column to enforce that filter, which
 *       {@code agg_l_05} does not carry — it is therefore always
 *       rejected. Documented in Task-U.1 report; no attempt is made to
 *       reach it via schema tweaks (out of scope).</li>
 * </ul>
 *
 * <p>Entries here are NOT added to the existing {@link AggregateCorpus}
 * because the equivalence harness compares cell-set goldens and
 * SQL-string drift envelopes, whereas MvHit asserts on the SQL
 * {@code FROM} clause directly and toggles {@code ReadAggregates} /
 * {@code UseAggregates} — a test-fixture-scoped change.
 */
public final class MvHitCorpus {

    private MvHitCorpus() {}

    /** Only {@code agg_g_ms_pcat_sales_fact_1997} carries both
     *  {@code product_family} and {@code gender} columns, so its
     *  selection is unambiguous. */
    public static final Set<String> AGG_G_MS_PCAT_UNIQUE =
        Collections.singleton("agg_g_ms_pcat_sales_fact_1997");

    /** {@code agg_c_special_*} and {@code agg_c_14_*} differ only in
     *  aggColumn aliases; either is a legitimate hit for Year/Quarter
     *  × Store-level queries on Unit Sales. */
    public static final Set<String> AGG_C_FAMILY =
        Collections.unmodifiableSet(new java.util.HashSet<>(Arrays.asList(
            "agg_c_14_sales_fact_1997",
            "agg_c_special_sales_fact_1997")));

    /** Each entry: {@link NamedMdx} + set of acceptable agg-table hits. */
    public static final class Entry {
        public final NamedMdx mdx;
        public final Set<String> acceptableAggTables;
        /** Human-readable description of the primary expected agg. */
        public final String primary;

        public Entry(NamedMdx mdx, String primary,
                     Set<String> acceptableAggTables) {
            this.mdx = mdx;
            this.primary = primary;
            this.acceptableAggTables = acceptableAggTables;
        }
    }

    public static List<Entry> entries() {
        return Collections.unmodifiableList(Arrays.asList(
            // #1 — agg_g_ms_pcat_sales_fact_1997 (UNIQUE):
            // Only this agg has both product_family and gender columns.
            // Using Unit Sales (additive) so rollup across marital_status,
            // product_department/category and quarter/month is allowed —
            // distinct-count [Customer Count] would block rollup because
            // the measure is non-additive (see RolapGalaxy.findAgg rollup
            // safety check).
            new Entry(
                new NamedMdx(
                    "agg-g-ms-pcat-family-gender",
                    "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                    + "  CROSSJOIN([Product].[Product Family].Members,"
                    + " [Gender].[Gender].Members) ON ROWS\n"
                    + "FROM Sales"),
                "agg_g_ms_pcat_sales_fact_1997",
                AGG_G_MS_PCAT_UNIQUE),

            // #2 — agg_c_* family: Year × Country × Unit Sales. Both
            // agg_c_14 and agg_c_special cover this grain.
            new Entry(
                new NamedMdx(
                    "agg-c-year-country",
                    "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                    + "  CROSSJOIN([Time].[Year].Members,"
                    + " [Store].[Store Country].Members) ON ROWS\n"
                    + "FROM Sales"),
                "agg_c_special_sales_fact_1997 or agg_c_14_sales_fact_1997",
                AGG_C_FAMILY),

            // #3 — agg_c_* family again (quarterly grain + two measures).
            // Using [Time].[1997] children (four quarter members) and
            // [Store].[Store Country].Members — two other level
            // enumerations trip an unrelated member-cache SQL-typing
            // assertion in SqlStatement.guessTypes when UseAggregates=true,
            // so we stick with the Country shape (also used in #2). The
            // two-measure projection forces both Unit Sales and Store
            // Sales through the same grain.
            new Entry(
                new NamedMdx(
                    "agg-c-quarter-country",
                    "SELECT {[Measures].[Unit Sales],"
                    + " [Measures].[Store Sales]} ON COLUMNS,\n"
                    + "  CROSSJOIN([Time].[1997].Children,"
                    + " [Store].[Store Country].Members) ON ROWS\n"
                    + "FROM Sales"),
                "agg_c_14_sales_fact_1997 or agg_c_special_sales_fact_1997",
                AGG_C_FAMILY),

            // #4 — agg_g_ms_pcat again, this time on the full CopyLink
            // grain (Family × Gender × Marital Status × Year) to
            // exercise the marital_status / gender two-column agg path.
            // Still additive (Unit Sales) so rollup across product_dept /
            // category and quarter/month is allowed.
            new Entry(
                new NamedMdx(
                    "agg-g-ms-pcat-family-gender-marital",
                    "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                    + "  CROSSJOIN("
                    + "CROSSJOIN([Product].[Product Family].Members,"
                    + " [Gender].[Gender].Members),"
                    + " [Marital Status].[Marital Status].Members"
                    + ") ON ROWS\n"
                    + "FROM Sales"),
                "agg_g_ms_pcat_sales_fact_1997",
                AGG_G_MS_PCAT_UNIQUE)
        ));
    }
}

// End MvHitCorpus.java
