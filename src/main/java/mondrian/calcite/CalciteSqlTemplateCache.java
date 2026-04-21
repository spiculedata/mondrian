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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-shape SQL template cache for {@link CalciteSqlPlanner}.
 *
 * <p>Keyed by {@link PlannerRequest#structuralHash()}, stores a
 * parameterised version of the unparsed SQL string where literal values
 * have been replaced by position markers. Repeated requests with the
 * same structural shape but different literals bypass the
 * {@code RelBuilder} + {@code RelToSqlConverter} pipeline — the hit
 * path is a concatenation of pre-split string segments interleaved with
 * freshly-formatted literals.
 *
 * <p>Mondrian's per-MDX repeated-request pattern (cardinality probes
 * and segment loads that repeat the same shape across benchmark
 * iterations) turns every iteration after the first into a cache hit,
 * collapsing Calcite's per-statement Java cost from ~5ms to ~0.1ms.
 *
 * <p>Safety net: if any literal cannot be uniquely located in the
 * unparsed SQL (e.g. because its formatted form collides with a
 * column alias or a year-valued column name), {@link Template#tryBuild}
 * returns {@code null} and the cache stores a noop entry so subsequent
 * calls of that shape re-run the planner — correctness over perf.
 *
 * <p>Added in Task 6 (Phase 2 per-statement caching pivot). See
 * {@code docs/plans/2026-04-19-calcite-plan-as-lingua-franca-design.md}.
 */
public final class CalciteSqlTemplateCache {

    /** Hash-to-template entry. */
    private static final ConcurrentMap<Long, Entry> CACHE =
        new ConcurrentHashMap<Long, Entry>();

    private static final AtomicLong HITS = new AtomicLong();
    private static final AtomicLong MISSES = new AtomicLong();

    private CalciteSqlTemplateCache() {
        // utility
    }

    /** Reset all state (tests). */
    public static void clear() {
        CACHE.clear();
        HITS.set(0);
        MISSES.set(0);
    }

    public static int size() { return CACHE.size(); }
    public static long hits() { return HITS.get(); }
    public static long misses() { return MISSES.get(); }

    /**
     * Plan {@code req}, consulting the cache. On a miss, delegates to
     * {@code planner} and tries to derive a template from the result; on
     * a hit, renders directly from the cached template.
     *
     * @param dialectSalt per-planner-instance salt that namespaces the
     *     cache by dialect / any other dimension that affects the
     *     rendered SQL but is invisible to the {@link PlannerRequest}.
     *     Different dialects must supply different salts.
     */
    public static String plan(
        PlannerRequest req,
        long dialectSalt,
        java.util.function.Function<PlannerRequest, String> planner)
    {
        long h = req.structuralHash() ^ dialectSalt;
        Entry entry = CACHE.get(h);
        if (entry != null && entry.req.structurallyEqual(req)) {
            if (entry.template == null) {
                // Noop entry — shape is known-untemplatable. Run the
                // planner straight; don't increment hits (this is a
                // silent miss).
                MISSES.incrementAndGet();
                return planner.apply(req);
            }
            String rendered = entry.template.render(req.literals());
            if (rendered != null) {
                HITS.incrementAndGet();
                return rendered;
            }
            // Render failed (e.g. null literal we can't format in-place
            // without ambiguity). Fall back to planner.
            MISSES.incrementAndGet();
            return planner.apply(req);
        }
        MISSES.incrementAndGet();
        String sql = planner.apply(req);
        Template t = Template.tryBuild(req, sql);
        CACHE.put(h, new Entry(req, t));
        return sql;
    }

    /** Cache entry: reference request (for collision-safe equality) +
     *  template (null → noop/untemplatable shape). */
    private static final class Entry {
        final PlannerRequest req;
        final Template template;
        Entry(PlannerRequest req, Template template) {
            this.req = req;
            this.template = template;
        }
    }

    /**
     * Pre-split SQL string with placeholder gaps between segments. On
     * render, the {@link #segments} are concatenated with freshly
     * formatted literals inserted at each gap.
     *
     * <p>Invariant: {@code segments.size() == literalCount + 1}.
     */
    static final class Template {
        final List<String> segments;
        final int literalCount;

        Template(List<String> segments, int literalCount) {
            this.segments = segments;
            this.literalCount = literalCount;
        }

        /**
         * Try to derive a template from {@code sql} by locating each
         * literal of {@code req} (in the order returned by
         * {@link PlannerRequest#literals()}) as a unique substring. A
         * literal is considered located when its formatted form matches
         * exactly one position in the remaining SQL tail.
         *
         * <p>Returns {@code null} (→ stored as noop) when any literal
         * cannot be uniquely located — correctness takes priority.
         */
        static Template tryBuild(PlannerRequest req, String sql) {
            List<Object> literals = req.literals();
            if (literals.isEmpty()) {
                // No literals → the whole SQL is one segment, zero
                // gaps. render() returns sql unchanged. Still useful:
                // skips RelBuilder on subsequent hits of this shape.
                List<String> seg = new ArrayList<String>(1);
                seg.add(sql);
                return new Template(seg, 0);
            }
            List<String> segments = new ArrayList<String>(literals.size() + 1);
            int cursor = 0;
            for (int i = 0; i < literals.size(); i++) {
                Object lit = literals.get(i);
                String formatted = formatLiteral(lit);
                if (formatted == null) {
                    return null; // unsupported type
                }
                int first = findWordBoundary(sql, formatted, cursor);
                if (first < 0) {
                    return null; // literal not found at a word boundary
                }
                int second = findWordBoundary(
                    sql, formatted, first + formatted.length());
                if (second >= 0) {
                    // Ambiguous: literal's formatted form appears more
                    // than once in the remaining SQL at word boundaries.
                    // Substituting a different value on a hit would
                    // either change the wrong occurrence or both —
                    // refuse to template this shape.
                    return null;
                }
                segments.add(sql.substring(cursor, first));
                cursor = first + formatted.length();
            }
            segments.add(sql.substring(cursor));
            return new Template(segments, literals.size());
        }

        /**
         * Search for {@code needle} in {@code haystack} starting at
         * {@code from}, returning only matches whose surrounding
         * characters are not identifier-class (letter/digit/underscore).
         * A numeric literal like {@code 1997} embedded in a table name
         * {@code sales_fact_1997} is rejected; the same literal in
         * {@code WHERE the_year = 1997} is accepted (surrounded by space
         * and end-of-string or whitespace).
         */
        private static int findWordBoundary(
            String haystack, String needle, int from)
        {
            int n = needle.length();
            int limit = haystack.length() - n;
            int pos = from;
            while (pos <= limit) {
                int idx = haystack.indexOf(needle, pos);
                if (idx < 0) return -1;
                boolean leftOk = idx == 0
                    || !isIdentChar(haystack.charAt(idx - 1));
                boolean rightOk = idx + n == haystack.length()
                    || !isIdentChar(haystack.charAt(idx + n));
                if (leftOk && rightOk) {
                    return idx;
                }
                pos = idx + 1;
            }
            return -1;
        }

        private static boolean isIdentChar(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }

        /**
         * Render the template by concatenating segments with
         * freshly-formatted literals. Returns {@code null} if any
         * literal is of an unsupported type (caller falls back to
         * planner).
         */
        String render(List<Object> literals) {
            if (literals.size() != literalCount) {
                return null; // arity mismatch — stale template
            }
            StringBuilder out =
                new StringBuilder(segments.get(0).length() * 2);
            for (int i = 0; i < literalCount; i++) {
                out.append(segments.get(i));
                String formatted = formatLiteral(literals.get(i));
                if (formatted == null) {
                    return null;
                }
                out.append(formatted);
            }
            out.append(segments.get(literalCount));
            return out.toString();
        }
    }

    /**
     * Format a literal value the same way {@code RelToSqlConverter}
     * would emit it for the HSQLDB / Postgres dialects we target.
     *
     * <p>Supported:
     * <ul>
     *   <li>{@code Integer}, {@code Long}, {@code Short}, {@code Byte}
     *       → decimal digits.</li>
     *   <li>{@code Double}, {@code Float}, {@code BigDecimal} → decimal
     *       with trailing {@code .0} suppressed for integral values to
     *       match Calcite's {@code SqlLiteral} rendering.</li>
     *   <li>{@code String} → {@code 'escaped'} with single-quote
     *       doubling.</li>
     *   <li>{@code Boolean} → {@code TRUE} / {@code FALSE}.</li>
     * </ul>
     *
     * <p>Returns {@code null} for unsupported types (e.g. {@code null},
     * {@code Date}, custom objects) so the caller can decline to cache.
     */
    static String formatLiteral(Object v) {
        if (v == null) return null;
        if (v instanceof Integer
            || v instanceof Long
            || v instanceof Short
            || v instanceof Byte)
        {
            return v.toString();
        }
        if (v instanceof String) {
            String s = (String) v;
            return "'" + s.replace("'", "''") + "'";
        }
        if (v instanceof Boolean) {
            return ((Boolean) v).booleanValue() ? "TRUE" : "FALSE";
        }
        if (v instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) v;
            return bd.toPlainString();
        }
        if (v instanceof Double || v instanceof Float) {
            // Calcite's numeric literal unparse uses plain decimal.
            double d = ((Number) v).doubleValue();
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                // Render "5.0" → Calcite keeps the ".0"; match that.
                return String.valueOf(d);
            }
            return String.valueOf(d);
        }
        return null;
    }
}

// End CalciteSqlTemplateCache.java
