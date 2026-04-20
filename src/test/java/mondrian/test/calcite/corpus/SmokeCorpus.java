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

import java.util.Arrays;
import java.util.List;

/**
 * Curated smoke corpus of FoodMart-compatible MDX queries used as the golden
 * baseline for the Calcite equivalence harness.
 *
 * <p>Every query is lifted (verbatim or near-verbatim) from a currently-green
 * Mondrian test class — see Task 4 of the calcite-equivalence-harness plan.
 * The corpus intentionally stays inside the FoodMart schema and avoids
 * subsystems known to be broken on this branch (XMLA drillthrough,
 * ChunkList, ClearView, Mondrian3 schemas, SteelWheels, AdventureWorks).
 */
public final class SmokeCorpus {

    private SmokeCorpus() {}

    public static List<NamedMdx> queries() {
        return Arrays.asList(
            new NamedMdx(
                "basic-select",
                // BasicQueryTest#testSample0 / query0
                "select {[Measures].[Unit Sales]} on columns from Sales"),

            new NamedMdx(
                "crossjoin",
                // BasicQueryTest#testCrossjoin-style
                "select CrossJoin(\n"
                + "    {[Gender].[All Gender].children},\n"
                + "    {[Marital Status].[All Marital Status].children}) on columns,\n"
                + "  {[Measures].[Unit Sales]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "non-empty-rows",
                // BasicQueryTest#testNonEmpty style
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "  NON EMPTY [Store].[Store Name].members on rows\n"
                + "from Sales"),

            new NamedMdx(
                "calc-member",
                // BasicQueryTest#testSample5 / query5 (Store Profit Rate)
                "with member [Measures].[Store Profit Rate] as"
                + " '([Measures].[Store Sales]-[Measures].[Store Cost])"
                + "/[Measures].[Store Cost]', format = '#.00%'\n"
                + "select\n"
                + "  {[Measures].[Store Cost],"
                + "   [Measures].[Store Sales],"
                + "   [Measures].[Store Profit Rate]} on columns,\n"
                + "  Order([Product].[Product Department].members,"
                + " [Measures].[Store Profit Rate], BDESC) on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "named-set",
                // BasicQueryTest#_testSet pattern, simplified for FoodMart
                "with set [Top Food Departments] as\n"
                + "  'TopCount([Product].[Product Department].members,"
                + " 3, [Measures].[Unit Sales])'\n"
                + "select {[Measures].[Unit Sales]} on columns,\n"
                + "  [Top Food Departments] on rows\n"
                + "from Sales"),

            new NamedMdx(
                "time-fn",
                // BasicQueryTest query1 period-based select
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "  {[Time].[1997].[Q1], [Time].[1997].[Q2],"
                + " [Time].[1997].[Q3], [Time].[1997].[Q4]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "slicer-where",
                // BasicQueryTest query3 slicer
                "select {[Measures].[Store Sales]} on columns,\n"
                + "  {[Product].[Product Department].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997].[Q1])"),

            new NamedMdx(
                "topcount",
                // BasicQueryTest query3 TopCount
                "select {[Measures].[Store Sales]} on columns,\n"
                + "  TopCount([Product].[Product Department].members,"
                + " 5, [Measures].[Store Sales]) on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "filter",
                // BasicQueryTest Filter pattern
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "  Filter([Product].[Product Department].members,"
                + " [Measures].[Unit Sales] > 10000) on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "order",
                // BasicQueryTest query5 Order
                "select {[Measures].[Store Sales]} on columns,\n"
                + "  Order([Product].[Product Department].members,"
                + " [Measures].[Store Sales], BDESC) on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "aggregate-measure",
                // BasicQueryTest Sum over YTD
                "with member [Measures].[Total Store Sales] as"
                + " 'Sum(YTD(), [Measures].[Store Sales])',"
                + " format_string='#.00'\n"
                + "select {[Measures].[Total Store Sales]} on columns,\n"
                + "  {[Time].[1997].[Q2].[4]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "distinct-count",
                // Standard FoodMart Customer Count distinct-count measure
                "select {[Measures].[Customer Count]} on columns,\n"
                + "  {[Gender].[All Gender].children} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "hierarchy-children",
                // BasicQueryTest children pattern
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "  [Store].[USA].children on rows\n"
                + "from Sales"),

            new NamedMdx(
                "hierarchy-parent",
                // BasicQueryTest .Parent pattern
                "select {[Measures].[Unit Sales]} on columns,\n"
                + "  {[Store].[USA].[CA].[Los Angeles].Parent} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "descendants",
                // BasicQueryTest query#Accumulated Sales uses Descendants
                "select {[Measures].[Store Sales]} on columns,\n"
                + "  Descendants([Time].[1997], [Time].[Month]) on rows\n"
                + "from Sales"),

            new NamedMdx(
                "ancestor",
                // BasicQueryTest Ancestor pattern
                "with member [Measures].[Store Country] as"
                + " 'Ancestor([Store].[Stores].CurrentMember,"
                + " [Store].[Stores].[Store Country]).Name'\n"
                + "select {[Measures].[Store Country]} on columns,\n"
                + "  {[Store].[USA].[CA].[Los Angeles]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "ytd",
                // BasicQueryTest query4 YTD
                "with member [Measures].[YTD Sales] as"
                + " 'Sum(YTD(), [Measures].[Store Sales])',"
                + " format_string='#.00'\n"
                + "select {[Measures].[YTD Sales]} on columns,\n"
                + "  {[Time].[1997].[Q2].[4]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "parallelperiod",
                // FunctionTest ParallelPeriod pattern (green subset)
                "with member [Measures].[Prev Qtr Sales] as"
                + " '([Measures].[Unit Sales],"
                + " ParallelPeriod([Time].[Time].[Quarter], 1,"
                + " [Time].[Time].CurrentMember))'\n"
                + "select {[Measures].[Unit Sales],"
                + " [Measures].[Prev Qtr Sales]} on columns,\n"
                + "  {[Time].[1997].[Q2], [Time].[1997].[Q3]} on rows\n"
                + "from Sales"),

            new NamedMdx(
                "iif",
                // BasicQueryTest#testIIf pattern (line 1517-ish)
                "with member [Measures].[Beer Flag] as\n"
                + "  'IIf([Measures].[Unit Sales] > 100, \"Yes\", \"No\")'\n"
                + "select {[Measures].[Unit Sales],"
                + " [Measures].[Beer Flag]} on columns,\n"
                + "  {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "format-string",
                // BasicQueryTest query5 format string pattern
                "with member [Measures].[Sales Pct] as"
                + " '[Measures].[Store Sales] / [Measures].[Store Cost]',"
                + " format_string = '#.00%'\n"
                + "select {[Measures].[Sales Pct]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])")
        );
    }

    public static final class NamedMdx {
        public final String name;
        public final String mdx;

        public NamedMdx(String name, String mdx) {
            this.name = name;
            this.mdx = mdx;
        }
    }
}
