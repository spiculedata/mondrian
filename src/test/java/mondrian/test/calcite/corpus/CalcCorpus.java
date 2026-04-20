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
 * Tier-4 calc-member corpus for the Calcite equivalence harness (Task S).
 * Queries exercise the pushable arithmetic calc-member shapes from the
 * Calcite backend rewrite design
 * ({@code docs/plans/2026-04-19-calcite-backend-rewrite-design.md} —
 * §Arithmetic calc-member push-down):
 *
 * <ul>
 *   <li>Arithmetic {@code + - * /}</li>
 *   <li>Unary minus</li>
 *   <li>Numeric literal operands</li>
 *   <li>{@code IIf(cond, then, else)} with numeric comparison</li>
 *   <li>{@code CoalesceEmpty(a, b)}</li>
 *   <li>Base-measure references</li>
 * </ul>
 *
 * <p>Two <b>control</b> queries (names prefixed {@code non-pushable-})
 * exercise dimensional navigation — {@code .Parent} and {@code YTD()} —
 * which the design explicitly classifies as non-pushable. They must stay
 * on the Java evaluator under Calcite and therefore are expected to be
 * green under both backends without pushdown work.
 *
 * <p>Under the <i>legacy</i> backend all entries are expected green once
 * goldens are recorded. Under <i>Calcite</i> (pre Task T) the pushable
 * arithmetic entries are expected to fail with
 * {@code UnsupportedTranslation: non-pushable calc member} — Task T will
 * implement pushdown to close the gap.
 */
public final class CalcCorpus {

    private CalcCorpus() {}

    public static List<NamedMdx> queries() {
        return Arrays.asList(
            new NamedMdx(
                "calc-arith-ratio",
                // base arithmetic: division of two base measures
                "with member [Measures].[Sales Per Unit] as"
                + " '[Measures].[Store Sales] / [Measures].[Unit Sales]'\n"
                + "select {[Measures].[Sales Per Unit]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-arith-sum",
                // base arithmetic: addition of two base measures
                "with member [Measures].[Sales Plus Cost] as"
                + " '[Measures].[Store Sales] + [Measures].[Store Cost]'\n"
                + "select {[Measures].[Sales Plus Cost]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-arith-unary-minus",
                // unary minus applied to a base measure
                "with member [Measures].[Neg Unit Sales] as"
                + " '-[Measures].[Unit Sales]'\n"
                + "select {[Measures].[Neg Unit Sales]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-arith-const-multiply",
                // numeric literal on RHS of multiplication
                "with member [Measures].[Unit Sales 10pct] as"
                + " '[Measures].[Unit Sales] * 1.1'\n"
                + "select {[Measures].[Unit Sales 10pct]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-iif-numeric",
                // IIf with numeric comparison and numeric branches
                "with member [Measures].[Sales If Big] as"
                + " 'IIf([Measures].[Unit Sales] > 100,"
                + " [Measures].[Store Sales], 0)'\n"
                + "select {[Measures].[Sales If Big]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-coalesce-empty",
                // CoalesceEmpty on base measure with numeric literal fallback
                "with member [Measures].[Sales Or Zero] as"
                + " 'CoalesceEmpty([Measures].[Store Sales], 0)'\n"
                + "select {[Measures].[Sales Or Zero]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-nested-arith",
                // nested arithmetic: (a - b) / c
                "with member [Measures].[Margin Per Unit] as"
                + " '([Measures].[Store Sales] - [Measures].[Store Cost])"
                + " / [Measures].[Unit Sales]'\n"
                + "select {[Measures].[Margin Per Unit]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-arith-with-filter",
                // pushable arithmetic combined with a WHERE slicer — ensures
                // slicer context flows into the pushed projection
                "with member [Measures].[Profit] as"
                + " '[Measures].[Store Sales] - [Measures].[Store Cost]'\n"
                + "select {[Measures].[Profit]} on columns,\n"
                + "  {[Product].[Product Family].members} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997].[Q1])"),

            // --- Non-pushable controls: dimensional navigation stays on the
            // Java evaluator. Expected green under both backends.
            new NamedMdx(
                "calc-non-pushable-parent",
                // .Parent navigation — design says non-pushable
                "with member [Measures].[Parent Sales] as"
                + " '([Measures].[Store Sales],"
                + " [Store].[Stores].CurrentMember.Parent)'\n"
                + "select {[Measures].[Store Sales],"
                + " [Measures].[Parent Sales]} on columns,\n"
                + "  {[Store].[All Stores].[USA].[CA].[Los Angeles]} on rows\n"
                + "from Sales\n"
                + "where ([Time].[1997])"),

            new NamedMdx(
                "calc-non-pushable-ytd",
                // YTD() navigation — design says non-pushable
                "with member [Measures].[YTD Store Sales] as"
                + " 'Sum(YTD(), [Measures].[Store Sales])',"
                + " format_string='#.00'\n"
                + "select {[Measures].[YTD Store Sales]} on columns,\n"
                + "  {[Time].[1997].[Q2].[5]} on rows\n"
                + "from Sales")
        );
    }
}

// End CalcCorpus.java
