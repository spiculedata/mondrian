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

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ArithmeticCalcAnalyzer}. Uses the FoodMart
 * connection to parse MDX expressions (cheaper and more faithful than
 * hand-constructing Exp trees) and exercises each pushable and
 * non-pushable shape from the design.
 */
public class ArithmeticCalcAnalyzerTest {

    private static Connection conn;

    @BeforeClass
    public static void boot() {
        FoodMartHsqldbBootstrap.ensureExtracted();
        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        conn = DriverManager.getConnection(props, null);
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    /** Parse+validate an MDX expression by embedding it as a calc
     *  member in a minimal {@code with member ... select ...} query
     *  against Sales, then lifting the resolved {@code Exp} out of the
     *  formula. {@link Connection#parseExpression} alone returns an
     *  <em>unresolved</em> tree (UnresolvedFunCall nodes, no member
     *  bindings) which the analyzer rightly rejects. */
    private static Exp parseExp(String expr) {
        String mdx =
            "with member [Measures].[__probe__] as '" + expr + "'"
            + " select {[Measures].[__probe__]} on columns from Sales";
        Query q = conn.parseQuery(mdx);
        q.resolve();
        for (Formula f : q.getFormulas()) {
            if (f.getMdxMember() != null
                && "__probe__".equals(
                    f.getMdxMember().getName()))
            {
                return f.getExpression();
            }
        }
        throw new IllegalStateException("probe member not found");
    }

    private static ArithmeticCalcAnalyzer.Classification classify(String e) {
        return ArithmeticCalcAnalyzer.classify(
            parseExp(e), Collections.emptySet());
    }

    @Test public void baseMeasureIsPushable() {
        assertTrue(classify("[Measures].[Unit Sales]").isPushable());
    }

    @Test public void numericLiteralIsPushable() {
        assertTrue(classify("1.5").isPushable());
    }

    @Test public void divisionIsPushable() {
        assertTrue(classify(
            "[Measures].[Store Sales] / [Measures].[Unit Sales]")
            .isPushable());
    }

    @Test public void additionIsPushable() {
        assertTrue(classify(
            "[Measures].[Store Sales] + [Measures].[Store Cost]")
            .isPushable());
    }

    @Test public void subtractionIsPushable() {
        assertTrue(classify(
            "[Measures].[Store Sales] - [Measures].[Store Cost]")
            .isPushable());
    }

    @Test public void multiplicationByLiteralIsPushable() {
        assertTrue(classify("[Measures].[Unit Sales] * 1.1").isPushable());
    }

    @Test public void unaryMinusIsPushable() {
        assertTrue(classify("-[Measures].[Unit Sales]").isPushable());
    }

    @Test public void parenthesesAreTransparent() {
        assertTrue(classify(
            "([Measures].[Store Sales] - [Measures].[Store Cost])"
            + " / [Measures].[Unit Sales]")
            .isPushable());
    }

    @Test public void iifNumericIsPushable() {
        assertTrue(classify(
            "IIf([Measures].[Unit Sales] > 100,"
            + " [Measures].[Store Sales], 0)")
            .isPushable());
    }

    @Test public void coalesceEmptyIsPushable() {
        assertTrue(classify(
            "CoalesceEmpty([Measures].[Store Sales], 0)").isPushable());
    }

    @Test public void parentNavIsNotPushable() {
        // Tuple with .Parent — dimensional navigation.
        ArithmeticCalcAnalyzer.Classification c = classify(
            "([Measures].[Store Sales],"
            + " [Store].[Stores].CurrentMember.Parent)");
        assertFalse("parent nav should not push", c.isPushable());
    }

    @Test public void ytdSumIsNotPushable() {
        ArithmeticCalcAnalyzer.Classification c = classify(
            "Sum(YTD(), [Measures].[Store Sales])");
        assertFalse("YTD/Sum should not push", c.isPushable());
    }

    @Test public void baseMeasureSetCapturedOnPushable() {
        ArithmeticCalcAnalyzer.Classification c = classify(
            "[Measures].[Store Sales] / [Measures].[Unit Sales]");
        assertTrue(c.isPushable());
        assertEquals("two base measures should be captured",
            2, c.baseMeasures.size());
    }
}

// End ArithmeticCalcAnalyzerTest.java
