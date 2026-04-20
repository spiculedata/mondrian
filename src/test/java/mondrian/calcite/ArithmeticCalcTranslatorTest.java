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
import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.TestContext;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ArithmeticCalcTranslator}. Builds a standalone
 * {@link RexBuilder}, wires a resolver that maps base-measure refs to
 * {@link RexInputRef}s, and asserts the shape of the returned
 * {@link RexNode} via {@code toString()}.
 */
public class ArithmeticCalcTranslatorTest {

    private static Connection conn;
    private static RexBuilder rexBuilder;
    private static RelDataType doubleType;

    @BeforeClass
    public static void boot() {
        FoodMartHsqldbBootstrap.ensureExtracted();
        Util.PropertyList props = Util.parseConnectString(
            TestContext.getDefaultConnectString());
        conn = DriverManager.getConnection(props, null);
        RelDataTypeFactory f = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(f);
        doubleType = f.createSqlType(SqlTypeName.DOUBLE);
    }

    @AfterClass
    public static void tearDown() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private static Exp parseExp(String expr) {
        String mdx =
            "with member [Measures].[__probe__] as '" + expr + "'"
            + " select {[Measures].[__probe__]} on columns from Sales";
        Query q = conn.parseQuery(mdx);
        q.resolve();
        for (Formula f : q.getFormulas()) {
            if (f.getMdxMember() != null
                && "__probe__".equals(f.getMdxMember().getName()))
            {
                return f.getExpression();
            }
        }
        throw new IllegalStateException("probe member not found");
    }

    /** Resolve base measures by walking the classifier's captured
     *  base-measure set and mapping each to a fresh
     *  {@link RexInputRef}. This avoids a SchemaReader lookup (which
     *  needs a cube context tied to each query). */
    private RexNode translate(String expr) {
        Exp parsed = parseExp(expr);
        ArithmeticCalcAnalyzer.Classification cls =
            ArithmeticCalcAnalyzer.classify(
                parsed, java.util.Collections.emptySet());
        assertTrue("expected pushable: " + expr + " -> " + cls.reason,
            cls.isPushable());
        Map<Member, RexNode> refs = new LinkedHashMap<>();
        int idx = 0;
        for (Member m : cls.baseMeasures) {
            refs.put(m, new RexInputRef(idx++, doubleType));
        }
        ArithmeticCalcTranslator tx = new ArithmeticCalcTranslator(
            rexBuilder,
            ArithmeticCalcTranslator.mapResolver(refs));
        return tx.translate(parsed);
    }

    @Test public void addition() {
        RexNode n = translate(
            "[Measures].[Store Sales] + [Measures].[Store Cost]");
        assertTrue("expected + in " + n, n.toString().contains("+"));
    }

    @Test public void subtraction() {
        RexNode n = translate(
            "[Measures].[Store Sales] - [Measures].[Store Cost]");
        assertTrue("expected - in " + n, n.toString().contains("-"));
    }

    @Test public void multiplication() {
        RexNode n = translate("[Measures].[Unit Sales] * 1.1");
        assertTrue("expected * in " + n, n.toString().contains("*"));
    }

    @Test public void divisionWrappedInCaseWhenZero() {
        RexNode n = translate(
            "[Measures].[Store Sales] / [Measures].[Unit Sales]");
        String s = n.toString();
        assertTrue("divide-by-zero guard missing: " + s,
            s.contains("CASE") && s.contains("/"));
    }

    @Test public void unaryMinus() {
        RexNode n = translate("-[Measures].[Unit Sales]");
        assertTrue("expected unary minus in " + n,
            n.toString().contains("-"));
    }

    @Test public void iifCompilesToCase() {
        RexNode n = translate(
            "IIf([Measures].[Unit Sales] > 100,"
            + " [Measures].[Store Sales], 0)");
        assertTrue("expected CASE for IIf: " + n,
            n.toString().contains("CASE"));
    }

    @Test public void coalesceEmptyCompilesToCoalesce() {
        RexNode n = translate(
            "CoalesceEmpty([Measures].[Store Sales], 0)");
        assertTrue("expected COALESCE: " + n,
            n.toString().toUpperCase().contains("COALESCE"));
    }

    @Test public void nestedArithmetic() {
        RexNode n = translate(
            "([Measures].[Store Sales] - [Measures].[Store Cost])"
            + " / [Measures].[Unit Sales]");
        String s = n.toString();
        assertTrue("expected - and / in " + s,
            s.contains("-") && s.contains("/"));
    }

    @Test(expected = UnsupportedTranslation.class)
    public void unresolvedBaseMeasureThrows() {
        // Build a translator with no resolved refs.
        ArithmeticCalcTranslator tx = new ArithmeticCalcTranslator(
            rexBuilder,
            ArithmeticCalcTranslator.mapResolver(
                new LinkedHashMap<>()));
        tx.translate(parseExp("[Measures].[Unit Sales]"));
    }

    @Test public void returnTypeIsDouble() {
        RexNode n = translate("[Measures].[Unit Sales] * 1.1");
        assertEquals(SqlTypeName.DOUBLE, n.getType().getSqlTypeName());
    }
}

// End ArithmeticCalcTranslatorTest.java
