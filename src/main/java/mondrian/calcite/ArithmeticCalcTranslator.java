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

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.fun.ParenthesesFunDef;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translates a pushable MDX expression tree (as classified by
 * {@link ArithmeticCalcAnalyzer}) into a Calcite {@link RexNode} on top
 * of a base-measure row. Callers supply:
 * <ul>
 *   <li>{@link RexBuilder} for constructing expressions</li>
 *   <li>a function mapping base {@link Member} → {@link RexNode} (a
 *       reference to the corresponding aggregate-output column)</li>
 * </ul>
 *
 * <p>Divide-by-zero is wrapped as
 * {@code CASE WHEN b = 0 THEN NULL ELSE a/b END} to preserve Mondrian's
 * empty-on-x/0 semantics.
 */
public final class ArithmeticCalcTranslator {

    /** Resolve a base measure reference to a RexNode against the
     *  current aggregate row. */
    public interface MeasureResolver {
        /** Returns null if the measure isn't available on the current
         *  aggregate row, in which case translation fails with
         *  {@link UnsupportedTranslation}. */
        RexNode resolve(Member baseMeasure);
    }

    private final RexBuilder builder;
    private final MeasureResolver resolver;

    public ArithmeticCalcTranslator(
        RexBuilder builder, MeasureResolver resolver)
    {
        if (builder == null) {
            throw new IllegalArgumentException("builder is null");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver is null");
        }
        this.builder = builder;
        this.resolver = resolver;
    }

    /** Convenience factory for a resolver backed by a map. */
    public static MeasureResolver mapResolver(
        final Map<Member, RexNode> refs)
    {
        return new MeasureResolver() {
            public RexNode resolve(Member m) { return refs.get(m); }
        };
    }

    public RexNode translate(Exp expression) {
        if (expression == null) {
            throw new UnsupportedTranslation(
                "ArithmeticCalcTranslator: null expression");
        }
        return cast(toDouble(walk(expression)));
    }

    private RexNode cast(RexNode n) {
        return builder.makeCast(
            builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
            n);
    }

    private RexNode toDouble(RexNode n) {
        // Coerce DECIMAL/INT/etc. to DOUBLE so cell-set parity with the
        // legacy Java evaluator (which returns double) holds regardless
        // of what HSQLDB decides the result type is.
        return builder.makeCast(
            builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
            n);
    }

    private RexNode walk(Exp e) {
        if (e instanceof Literal) {
            Object v = ((Literal) e).getValue();
            if (v instanceof BigDecimal) {
                return builder.makeExactLiteral((BigDecimal) v);
            }
            if (v instanceof Number) {
                return builder.makeExactLiteral(
                    new BigDecimal(v.toString()));
            }
            throw new UnsupportedTranslation(
                "ArithmeticCalcTranslator: non-numeric literal " + v);
        }
        if (e instanceof MemberExpr) {
            Member m = ((MemberExpr) e).getMember();
            if (m.isCalculated()) {
                return walk(m.getExpression());
            }
            RexNode ref = resolver.resolve(m);
            if (ref == null) {
                throw new UnsupportedTranslation(
                    "ArithmeticCalcTranslator: unresolved base measure "
                    + m.getUniqueName());
            }
            return ref;
        }
        if (e instanceof ResolvedFunCall) {
            return walkCall((ResolvedFunCall) e);
        }
        throw new UnsupportedTranslation(
            "ArithmeticCalcTranslator: unsupported node "
            + e.getClass().getSimpleName());
    }

    private RexNode walkCall(ResolvedFunCall c) {
        if (c.getFunDef() instanceof ParenthesesFunDef) {
            return walk(c.getArg(0));
        }
        String name = c.getFunName();
        Syntax syn = c.getSyntax();
        Exp[] args = c.getArgs();

        if (syn == Syntax.Infix && args.length == 2) {
            RexNode l = walk(args[0]);
            RexNode r = walk(args[1]);
            switch (name) {
            case "+":
                return builder.makeCall(SqlStdOperatorTable.PLUS, l, r);
            case "-":
                return builder.makeCall(SqlStdOperatorTable.MINUS, l, r);
            case "*":
                return builder.makeCall(
                    SqlStdOperatorTable.MULTIPLY, l, r);
            case "/":
                return safeDivide(l, r);
            default:
                // fall through
            }
        }

        if (syn == Syntax.Prefix && args.length == 1 && "-".equals(name)) {
            return builder.makeCall(
                SqlStdOperatorTable.UNARY_MINUS, walk(args[0]));
        }

        if ("IIf".equalsIgnoreCase(name) && args.length == 3) {
            RexNode cond = walkComparison((ResolvedFunCall) args[0]);
            RexNode thenV = walk(args[1]);
            RexNode elseV = walk(args[2]);
            return builder.makeCall(
                SqlStdOperatorTable.CASE, cond, thenV, elseV);
        }

        if ("CoalesceEmpty".equalsIgnoreCase(name) && args.length >= 2) {
            List<RexNode> ops = new ArrayList<>(args.length);
            for (Exp a : args) {
                ops.add(walk(a));
            }
            return builder.makeCall(SqlStdOperatorTable.COALESCE, ops);
        }

        throw new UnsupportedTranslation(
            "ArithmeticCalcTranslator: unsupported call "
            + name + "/" + syn + "/" + args.length);
    }

    /** {@code lhs / NULLIF(rhs, 0)} — matches Mondrian's empty-on-x/0
     *  semantics. Switched from a {@code CASE WHEN rhs = 0 THEN NULL
     *  ELSE lhs / rhs END} rendering to {@code NULLIF} because HSQLDB
     *  1.8 rejects the aggregate-in-CASE-WHEN form with "Not a
     *  condition". {@code NULLIF} is a SQL-92 standard and is accepted
     *  by every dialect in the harness, including HSQLDB 1.8. */
    private RexNode safeDivide(RexNode l, RexNode r) {
        RexNode zero = builder.makeExactLiteral(BigDecimal.ZERO);
        RexNode safeR = builder.makeCall(
            SqlStdOperatorTable.NULLIF, r, zero);
        return builder.makeCall(
            SqlStdOperatorTable.DIVIDE, l, safeR);
    }

    private RexNode walkComparison(ResolvedFunCall c) {
        String n = c.getFunName();
        RexNode l = walk(c.getArg(0));
        RexNode r = walk(c.getArg(1));
        switch (n) {
        case "=":
            return builder.makeCall(SqlStdOperatorTable.EQUALS, l, r);
        case "<>":
            return builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, l, r);
        case "<":
            return builder.makeCall(SqlStdOperatorTable.LESS_THAN, l, r);
        case ">":
            return builder.makeCall(SqlStdOperatorTable.GREATER_THAN, l, r);
        case "<=":
            return builder.makeCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL, l, r);
        case ">=":
            return builder.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, l, r);
        default:
            throw new UnsupportedTranslation(
                "ArithmeticCalcTranslator: unsupported comparison " + n);
        }
    }
}

// End ArithmeticCalcTranslator.java
