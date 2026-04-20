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
import java.util.List;

/**
 * Tier-3 aggregate / native-evaluator corpus for the Calcite equivalence
 * harness (Task 11). These queries deliberately exercise paths most likely
 * to drift under Calcite re-emission because they produce multi-phase SQL
 * (distinct-count probes, aggregation-list predicates, native crossjoin /
 * filter / topcount subqueries).
 *
 * <p>Every MDX below is lifted (verbatim or near-verbatim) from a currently-
 * green Mondrian test method. Source classes:
 * <ul>
 *   <li>{@code mondrian.rolap.agg.AggregationOnDistinctCountMeasuresTest}
 *       (37/37 green)</li>
 *   <li>{@code mondrian.rolap.FastBatchingCellReaderTest} (45/45 green)</li>
 *   <li>{@code mondrian.test.NativeSetEvaluationTest} (3/3 active green) —
 *       only MDX that resolves against the stock FoodMart schema.</li>
 * </ul>
 *
 * <p>Capped at 12 entries by design: each MDX runs the full cold-cache
 * harness (two executions) plus the sanity and baseline tests — keep the
 * whole calcite-harness profile under ~30 minutes.
 */
public final class AggregateCorpus {

    private AggregateCorpus() {}

    public static List<NamedMdx> queries() {
        return Arrays.asList(
            new NamedMdx(
                "agg-distinct-count-set-of-members",
                // AggregationOnDistinctCountMeasuresTest
                //   #testDistinctCountOnSetOfMembersFromOneDimension
                "WITH MEMBER Gender.X AS"
                + " 'Aggregate({[Gender].[Gender].Members})'\n"
                + "SELECT Gender.X ON 0,"
                + " [Measures].[Customer Count] ON 1 FROM Sales"),

            new NamedMdx(
                "agg-distinct-count-two-states",
                // AggregationOnDistinctCountMeasuresTest
                //   #testDistinctCountOnSetOfMembers — Store dimension in the
                //   stock schema has multiple hierarchies so the calc member
                //   must be attached to the explicit [Stores] hierarchy path
                //   (cf. SmokeCorpus "ancestor": [Store].[Stores]...).
                "WITH MEMBER [Store].[Stores].[X] as"
                + " 'Aggregate({[Store].[All Stores].[USA].[CA],"
                + " [Store].[All Stores].[USA].[WA]})'\n"
                + "SELECT [Store].[Stores].[X] ON ROWS,"
                + " {[Measures].[Customer Count]} ON COLUMNS\n"
                + "FROM [Sales]"),

            new NamedMdx(
                "agg-crossjoin-gender-states",
                // AggregationOnDistinctCountMeasuresTest
                //   #testCrossJoinMembersWithSetOfMembers (simplified: drop
                //   [Canada] to stay inside the cube's joinable members)
                "WITH MEMBER Gender.X AS"
                + " 'Aggregate({[Gender].[Gender].Members} *"
                + " {[Store].[All Stores].[USA].[CA]})',"
                + " solve_order=100\n"
                + "SELECT Gender.X ON 0,"
                + " [Measures].[Customer Count] ON 1 FROM Sales"),

            new NamedMdx(
                "agg-distinct-count-measure-tuple",
                // AggregationOnDistinctCountMeasuresTest
                //   #testDistinctCountWithAMeasureAsPartOfTuple
                "SELECT [Store].[All Stores].[USA].[CA] ON 0,"
                + " ([Measures].[Customer Count], [Gender].[M]) ON 1"
                + " FROM Sales"),

            new NamedMdx(
                "agg-distinct-count-particular-tuple",
                // AggregationOnDistinctCountMeasuresTest
                //   #testCrossJoinParticularMembersFromTwoDimensions
                "WITH MEMBER Gender.X AS"
                + " 'Aggregate({[Gender].[M]} *"
                + " {[Store].[All Stores].[USA].[CA]})',"
                + " solve_order=100\n"
                + "SELECT Gender.X ON 0,"
                + " [Measures].[Customer Count] ON 1 FROM Sales"),

            new NamedMdx(
                "agg-distinct-count-quarters",
                // FastBatchingCellReaderTest#testAggregateDistinctCount
                "WITH MEMBER [Time].[Time].[1997 Q1 plus Q2] AS"
                + " 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})',"
                + " solve_order=1\n"
                + "SELECT {[Measures].[Customer Count]} ON COLUMNS,\n"
                + "  {[Time].[1997].[Q1], [Time].[1997].[Q2],"
                + " [Time].[1997 Q1 plus Q2]} ON ROWS\n"
                + "FROM Sales\n"
                + "WHERE ([Store].[USA].[CA])"),

            new NamedMdx(
                "agg-distinct-count-mixed-levels",
                // FastBatchingCellReaderTest#testAggregateDistinctCount2
                "WITH MEMBER [Time].[Time].[1997 Q1 plus July] AS\n"
                + " 'Aggregate({[Time].[1997].[Q1],"
                + " [Time].[1997].[Q3].[7]})', solve_order=1\n"
                + "SELECT {[Measures].[Unit Sales],"
                + " [Measures].[Customer Count]} ON COLUMNS,\n"
                + "  {[Time].[1997].[Q1],\n"
                + "   [Time].[1997].[Q2],\n"
                + "   [Time].[1997].[Q3].[7],\n"
                + "   [Time].[1997 Q1 plus July]} ON ROWS\n"
                + "FROM Sales\n"
                + "WHERE ([Store].[USA].[CA])"),

            new NamedMdx(
                "agg-distinct-count-two-calcs",
                // FastBatchingCellReaderTest#testAggregateDistinctCount3
                "WITH\n"
                + "  MEMBER [Promotion].[Media Type].[TV plus Radio] AS"
                + " 'Aggregate({[Promotion].[Media Type].[TV],"
                + " [Promotion].[Media Type].[Radio]})', solve_order=1\n"
                + "  MEMBER [Time].[Time].[1997 Q1 plus July] AS"
                + " 'Aggregate({[Time].[1997].[Q1],"
                + " [Time].[1997].[Q3].[7]})', solve_order=1\n"
                + "SELECT {[Promotion].[Media Type].[TV plus Radio],\n"
                + "        [Promotion].[Media Type].[TV],\n"
                + "        [Promotion].[Media Type].[Radio]} ON COLUMNS,\n"
                + "       {[Time].[1997],\n"
                + "        [Time].[1997].[Q1],\n"
                + "        [Time].[1997 Q1 plus July]} ON ROWS\n"
                + "FROM Sales\n"
                + "WHERE [Measures].[Customer Count]"),

            new NamedMdx(
                "native-cj-usa-product-names",
                // NativeSetEvaluationTest#testNativeHonorsRoleRestrictions
                //   -- base MDX (role-free form): native crossjoin across
                //   a small enumerated Store member and the Product Name
                //   level.
                "select non empty"
                + " crossjoin({[Store].[USA]},"
                + " [Product].[Product Name].members) on 0"
                + " from sales"),

            new NamedMdx(
                "native-topcount-product-names",
                // NativeSetEvaluationTest#testNativeHonorsRoleRestrictions
                //   -- base MDX (role-free form)
                "select topcount([Product].[Product Name].members, 6,"
                + " Measures.[Unit Sales]) on 0 from sales"),

            new NamedMdx(
                "native-filter-product-names",
                // NativeSetEvaluationTest#testNativeHonorsRoleRestrictions
                //   -- base MDX (role-free form)
                "select filter([Product].[Product Name].members,"
                + " Measures.[Unit Sales] > 0) on 0 from sales"),

            new NamedMdx(
                "agg-distinct-count-product-family-weekly",
                // FastBatchingCellReaderTest#testAggregateMaxConstraints
                //   (without the MaxConstraints property override — base
                //   query shape: weekly-slicer with a multi-family row axis)
                "SELECT\n"
                + "  [Measures].[Unit Sales] on columns,\n"
                + "  [Product].[Product Family].Members on rows\n"
                + "FROM Sales\n"
                + "WHERE {\n"
                + "  [Time].[All Weeklys].[1997].[1].[15],\n"
                + "  [Time].[All Weeklys].[1997].[2].[1]}")
        );
    }
}
