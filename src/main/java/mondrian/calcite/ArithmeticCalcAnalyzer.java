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
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.fun.ParenthesesFunDef;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Classifies an MDX calc-member expression tree as "pushable" (can be
 * translated to a RexNode on top of the base-measure aggregate) or
 * "not pushable" (must stay on the Java evaluator).
 *
 * <p>A node is pushable iff every transitive child is one of:
 * <ul>
 *   <li>Measure reference to a non-calculated base measure</li>
 *   <li>Numeric literal</li>
 *   <li>Binary arithmetic: {@code + - * /}</li>
 *   <li>Unary minus</li>
 *   <li>{@code IIf(cond, then, else)} where {@code cond} is a numeric
 *       comparison with pushable operands and both branches are
 *       pushable</li>
 *   <li>{@code CoalesceEmpty(a, b)} with both sides pushable</li>
 *   <li>Parentheses (associativity-only nodes)</li>
 * </ul>
 *
 * <p>Anything else (dimensional nav, set/tuple constructs, UDFs,
 * calc members that transitively depend on non-pushable calcs)
 * renders the expression non-pushable.
 *
 * <p>Design ref:
 * {@code docs/plans/2026-04-19-calcite-backend-rewrite-design.md}
 * §Arithmetic calc-member push-down.
 */
public final class ArithmeticCalcAnalyzer {

    private ArithmeticCalcAnalyzer() {}

    /** Verdict + the set of base measures the expression transitively
     *  depends on (populated only for {@link Verdict#PUSHABLE}). */
    public static final class Classification {
        public enum Verdict { PUSHABLE, NOT_PUSHABLE }

        public final Verdict verdict;
        public final Set<Member> baseMeasures;
        public final String reason;

        private Classification(
            Verdict v, Set<Member> baseMeasures, String reason)
        {
            this.verdict = v;
            this.baseMeasures = baseMeasures;
            this.reason = reason;
        }

        public static Classification pushable(Set<Member> bases) {
            return new Classification(Verdict.PUSHABLE, bases, null);
        }

        public static Classification notPushable(String reason) {
            return new Classification(
                Verdict.NOT_PUSHABLE, java.util.Collections.emptySet(),
                reason);
        }

        public boolean isPushable() {
            return verdict == Verdict.PUSHABLE;
        }
    }

    /**
     * Classify an expression tree. The {@code queryMeasures} set is
     * reserved for future dependency-on-query-measures checks; today the
     * analyzer accepts any non-calculated measure reference and returns
     * the transitive base-measure set for the caller to match against
     * the segment-load measures.
     */
    public static Classification classify(
        Exp expression, Set<Member> queryMeasures)
    {
        if (expression == null) {
            return Classification.notPushable("null expression");
        }
        Set<Member> bases = new LinkedHashSet<>();
        String rej = walk(expression, bases);
        if (rej != null) {
            return Classification.notPushable(rej);
        }
        return Classification.pushable(bases);
    }

    /** Walk the tree. Returns null on pushable, a rejection reason on
     *  not-pushable. Accumulates base-measure references into
     *  {@code bases}. */
    private static String walk(Exp e, Set<Member> bases) {
        if (e instanceof Literal) {
            Literal lit = (Literal) e;
            Object v = lit.getValue();
            if (v instanceof Number) {
                return null;
            }
            return "non-numeric literal: " + v;
        }
        if (e instanceof MemberExpr) {
            Member m = ((MemberExpr) e).getMember();
            if (m == null) {
                return "null member";
            }
            if (!m.isMeasure()) {
                return "non-measure member ref: " + m.getUniqueName();
            }
            if (m.isCalculated()) {
                // Transitive calc. Recurse into its expression.
                Exp inner = m.getExpression();
                if (inner == null) {
                    return "calc member has null expression: "
                        + m.getUniqueName();
                }
                return walk(inner, bases);
            }
            bases.add(m);
            return null;
        }
        if (e instanceof ResolvedFunCall) {
            return walkFunCall((ResolvedFunCall) e, bases);
        }
        if (e instanceof UnresolvedFunCall) {
            return "unresolved function call: "
                + ((UnresolvedFunCall) e).getFunName();
        }
        return "unsupported node: " + e.getClass().getSimpleName();
    }

    private static String walkFunCall(ResolvedFunCall call, Set<Member> bases) {
        String name = call.getFunName();
        Syntax syn = call.getSyntax();
        Exp[] args = call.getArgs();

        // Parentheses are transparent.
        if (call.getFunDef() instanceof ParenthesesFunDef) {
            for (Exp a : args) {
                String r = walk(a, bases);
                if (r != null) return r;
            }
            return null;
        }

        // Binary arithmetic + - * /
        if (syn == Syntax.Infix && args.length == 2) {
            if ("+".equals(name) || "-".equals(name)
                || "*".equals(name) || "/".equals(name))
            {
                String r0 = walk(args[0], bases);
                if (r0 != null) return r0;
                return walk(args[1], bases);
            }
        }

        // Unary minus
        if (syn == Syntax.Prefix && args.length == 1 && "-".equals(name)) {
            return walk(args[0], bases);
        }

        // IIf
        if ("IIf".equalsIgnoreCase(name) && args.length == 3) {
            String cond = walkNumericComparison(args[0], bases);
            if (cond != null) return cond;
            String t = walk(args[1], bases);
            if (t != null) return t;
            return walk(args[2], bases);
        }

        // CoalesceEmpty
        if ("CoalesceEmpty".equalsIgnoreCase(name) && args.length >= 2) {
            for (Exp a : args) {
                String r = walk(a, bases);
                if (r != null) return r;
            }
            return null;
        }

        return "unsupported function: " + name
            + "/" + syn + "/" + args.length;
    }

    /** Pushable condition: numeric comparison (=, &lt;&gt;, &lt;, &gt;,
     *  &lt;=, &gt;=) between two pushable numeric operands. */
    private static String walkNumericComparison(Exp cond, Set<Member> bases) {
        if (!(cond instanceof ResolvedFunCall)) {
            return "IIf condition is not a comparison call: " + cond;
        }
        ResolvedFunCall c = (ResolvedFunCall) cond;
        if (c.getSyntax() != Syntax.Infix) {
            return "IIf condition is not infix: " + c.getFunName();
        }
        String n = c.getFunName();
        boolean ok = "=".equals(n) || "<>".equals(n)
            || "<".equals(n) || ">".equals(n)
            || "<=".equals(n) || ">=".equals(n);
        if (!ok) {
            return "IIf condition operator not supported: " + n;
        }
        if (c.getArgCount() != 2) {
            return "IIf condition arity != 2";
        }
        String l = walk(c.getArg(0), bases);
        if (l != null) return l;
        return walk(c.getArg(1), bases);
    }

    /** Convenience accessor for the opaque {@link FunCall}. Used by the
     *  translator so this analyzer and translator share the same view of
     *  the Exp tree. */
    static boolean isInfix(Exp e) {
        return e instanceof FunCall
            && ((FunCall) e).getSyntax() == Syntax.Infix;
    }
}

// End ArithmeticCalcAnalyzer.java
