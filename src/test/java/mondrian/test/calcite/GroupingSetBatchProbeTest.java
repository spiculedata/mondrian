/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.test.calcite;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.AggregateCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase 2, Task 5 — diagnostic probe for the GROUPING SETS batching spike.
 *
 * <p>Disabled by default; opt in with
 * {@code -Dharness.probeGroupingSets=true}. A normal harness run skips it.
 *
 * <p>What this does: runs 5 representative MDX queries against the stock
 * HSQLDB FoodMart fixture, captures every JDBC statement via
 * {@link SqlCapture} through {@link FoodMartCapture#executeCold}, and
 * classifies segment-load statements by their normalised
 * {@code (factTable, group-by columns, WHERE signature, aggregate
 * measures)} shape. Statements sharing everything except group-by are
 * called a <b>batchable cohort</b>: in a world where Mondrian emitted one
 * {@code GROUP BY GROUPING SETS (...)} per cohort, we would send exactly
 * one SQL statement per cohort instead of one per member.
 *
 * <p>The probe also counts statements containing the literal token
 * {@code GROUPING SETS} / {@code GROUPING(}. That is the decisive
 * datapoint for the spike: if it is zero, then
 * {@link mondrian.rolap.agg.SegmentLoader#createExecuteSql} always
 * receives a {@link mondrian.rolap.agg.GroupingSetsList} of size 1 on this
 * fixture, so any batching optimisation must assemble the list upstream.
 *
 * <p>This test deliberately does not modify production code. The one piece
 * of instrumentation that would require it — confirming the arity of the
 * {@code GroupingSetsList} arriving at {@code createExecuteSql} — is
 * instead inferred from the emitted SQL shape, which is sufficient for
 * the spike's conclusion (and matches what a downstream JDBC trace would
 * show anyway).
 */
public class GroupingSetBatchProbeTest {

    private static final String RUN_PROP = "harness.probeGroupingSets";

    /** Five MDX queries per the Task 5 brief. */
    private static final List<String> PROBE_QUERIES = Arrays.asList(
        "crossjoin",
        "topcount",
        "aggregate-measure",
        "agg-distinct-count-customers-levels",
        "non-empty-rows");

    /**
     * Cohort-density extension — 4 MDX queries engineered to make
     * {@code CompositeBatch}/{@code GroupingSetsCollector} accrete
     * multiple group-by shapes that share fact+filters+measures. See the
     * extended probe report for rationale.
     */
    private static final List<NamedMdx> COHORT_PROBE_QUERIES = Arrays.asList(
        new NamedMdx(
            "cohort-time-drilldown",
            // Year + Quarter + Month unioned on rows — three levels of
            // the same Time hierarchy in one shot. CrossJoin across
            // levels of one hierarchy is illegal, so we Hierarchize the
            // union (the `CompositeBatch` primitive still sees three
            // batches that share fact/where/measures).
            "SELECT {[Measures].[Unit Sales]} ON 0,\n"
            + "  Hierarchize({\n"
            + "    [Time].[Year].Members,\n"
            + "    [Time].[Quarter].Members,\n"
            + "    Descendants([Time].[1997], [Time].[Month])\n"
            + "  }) ON 1\n"
            + "FROM Sales"),

        new NamedMdx(
            "cohort-multi-measure",
            // Three measures on same axis — sum/sum/sum against same fact.
            "SELECT {[Measures].[Unit Sales], "
            + "[Measures].[Store Sales], "
            + "[Measures].[Store Cost]} ON 0,\n"
            + "  [Product].[Product Family].Members ON 1\n"
            + "FROM Sales"),

        new NamedMdx(
            "cohort-agg-friendly",
            // Year x Store Country x Gender — agg-table friendly shape.
            "SELECT {[Measures].[Unit Sales]} ON 0,\n"
            + "  CrossJoin(\n"
            + "    [Time].[Year].Members,\n"
            + "    CrossJoin([Store].[Store Country].Members, "
            + "[Gender].Members)\n"
            + "  ) ON 1\n"
            + "FROM Sales"),

        new NamedMdx(
            "cohort-hierarchical-sum",
            // Three levels of Product dimension hierarchised on rows.
            "SELECT {[Measures].[Unit Sales]} ON 0,\n"
            + "  Hierarchize({\n"
            + "    [Product].[Product Family].Members,\n"
            + "    [Product].[Product Department].Members,\n"
            + "    [Product].[Product Category].Members\n"
            + "  }) ON 1\n"
            + "FROM Sales"));

    @Test
    public void probe() throws Exception {
        if (!Boolean.getBoolean(RUN_PROP)) {
            return; // silent no-op
        }
        if (HarnessBackend.current() == HarnessBackend.HSQLDB) {
            FoodMartHsqldbBootstrap.ensureExtracted();
        }

        // We want to know what the *default* segment-load pipeline does.
        // The toggle `EnableGroupingSets` defaults to false, but we print
        // both toggle + dialect support so the report can cite the
        // configuration.
        boolean enableProp =
            MondrianProperties.instance().EnableGroupingSets.get();
        System.out.println(
            "[probe] backend=" + HarnessBackend.current()
            + " EnableGroupingSets=" + enableProp);

        Map<String, NamedMdx> corpus = loadCorpus();

        // ---- original Task-5 corpus, default toggles ---------------------
        System.out.println(
            "[probe] --- per-MDX segment-load classification (T5 corpus) ---");
        System.out.printf(
            "[probe] %-42s %6s %8s %10s %12s %10s%n",
            "mdx", "total", "segload", "cohorts", "post-batch", "gSetSQL");
        List<PerMdx> results = new ArrayList<>();
        for (String name : PROBE_QUERIES) {
            NamedMdx mdx = corpus.get(name);
            if (mdx == null) {
                System.out.println(
                    "[probe] WARN: missing corpus entry: " + name);
                continue;
            }
            PerMdx row = runOne(mdx);
            results.add(row);
            System.out.printf(
                "[probe] %-42s %6d %8d %10d %12d %10d%n",
                name,
                row.totalStmts,
                row.segmentLoadStmts,
                row.cohortCount,
                row.postBatchStmtEstimate,
                row.containsGroupingSetsLiteral);
        }

        System.out.println("[probe] --- cohort breakdown ---");
        for (PerMdx row : results) {
            System.out.println("[probe] " + row.mdxName);
            for (Map.Entry<String, Integer> e : row.cohortToMembers
                .entrySet())
            {
                System.out.printf(
                    "[probe]     n=%d  %s%n", e.getValue(), e.getKey());
            }
        }

        // ---- cohort-density extension: engineered-to-cohort queries ------
        runCohortExtension();
    }

    /**
     * Runs {@link #COHORT_PROBE_QUERIES} twice: Pass A with default
     * properties, Pass B with
     * {@code UseAggregates=true, ReadAggregates=true,
     * EnableGroupingSets=true}. Prints a per-MDX x pass table plus the
     * distinct group-by-set list inside each cohort.
     */
    private void runCohortExtension() {
        System.out.println(
            "[probe] ================================================");
        System.out.println(
            "[probe] Cohort-density extension (4 engineered MDX)");
        System.out.println(
            "[probe] ================================================");

        MondrianProperties p = MondrianProperties.instance();
        boolean prevUse = p.UseAggregates.get();
        boolean prevRead = p.ReadAggregates.get();
        boolean prevEgs = p.EnableGroupingSets.get();

        List<PerMdx> passA = new ArrayList<>();
        List<PerMdx> passB = new ArrayList<>();

        try {
            // Pass A: defaults
            p.UseAggregates.set(false);
            p.ReadAggregates.set(false);
            p.EnableGroupingSets.set(false);
            System.out.println(
                "[probe] --- Pass A (UseAggregates=false, "
                + "EnableGroupingSets=false) ---");
            printHeader();
            for (NamedMdx q : COHORT_PROBE_QUERIES) {
                PerMdx row = runOne(q);
                passA.add(row);
                printRow(row);
            }

            // Pass B: UseAggregates + ReadAggregates + EnableGroupingSets
            p.UseAggregates.set(true);
            p.ReadAggregates.set(true);
            p.EnableGroupingSets.set(true);
            System.out.println(
                "[probe] --- Pass B (UseAggregates=true, "
                + "ReadAggregates=true, EnableGroupingSets=true) ---");
            printHeader();
            for (NamedMdx q : COHORT_PROBE_QUERIES) {
                PerMdx row = runOne(q);
                passB.add(row);
                printRow(row);
            }
        } finally {
            p.UseAggregates.set(prevUse);
            p.ReadAggregates.set(prevRead);
            p.EnableGroupingSets.set(prevEgs);
        }

        dumpCohorts("Pass A", passA);
        dumpCohorts("Pass B", passB);
    }

    private static void printHeader() {
        System.out.printf(
            "[probe] %-32s %6s %8s %10s %12s %10s %12s%n",
            "mdx", "total", "segload", "cohorts", "post-batch",
            "gSetSQL", "distGrpSets");
    }

    private static void printRow(PerMdx row) {
        System.out.printf(
            "[probe] %-32s %6d %8d %10d %12d %10d %12d%n",
            row.mdxName,
            row.totalStmts,
            row.segmentLoadStmts,
            row.cohortCount,
            row.postBatchStmtEstimate,
            row.containsGroupingSetsLiteral,
            row.distinctGroupBySets);
    }

    private static void dumpCohorts(String label, List<PerMdx> rows) {
        System.out.println("[probe] --- " + label + " cohort breakdown ---");
        for (PerMdx row : rows) {
            System.out.println("[probe] " + row.mdxName);
            for (Map.Entry<String, Integer> e : row.cohortToMembers
                .entrySet())
            {
                System.out.printf(
                    "[probe]     n=%d  %s%n", e.getValue(), e.getKey());
            }
            System.out.println(
                "[probe]     distinct group-by sets across cohorts: "
                + row.distinctGroupBySets);
        }
    }

    private static Map<String, NamedMdx> loadCorpus() {
        Map<String, NamedMdx> m = new LinkedHashMap<>();
        for (NamedMdx q : SmokeCorpus.queries()) {
            m.put(q.name, q);
        }
        for (NamedMdx q : AggregateCorpus.queries()) {
            m.put(q.name, q);
        }
        return m;
    }

    private PerMdx runOne(NamedMdx mdx) {
        FoodMartCapture.CapturedRun run =
            FoodMartCapture.executeCold(mdx, null);

        PerMdx out = new PerMdx();
        out.mdxName = mdx.name;
        out.totalStmts = run.executions.size();

        if (Boolean.getBoolean("harness.probeGroupingSets.dumpSql")) {
            System.out.println(
                "[probe.dump] --- " + mdx.name + " captured SQL ---");
            for (CapturedExecution ex : run.executions) {
                String preview =
                    ex.sql.replaceAll("\\s+", " ").trim();
                if (preview.length() > 200) {
                    preview = preview.substring(0, 200) + "...";
                }
                System.out.println(
                    "[probe.dump]   seq=" + ex.seq + " rows=" + ex.rowCount
                    + "  " + preview);
            }
        }

        // Cohort key = factTable || sorted(WHERE signature) || sorted(agg
        // measures). Members of the same key differ only in their group-by
        // list — i.e., they could be batched into a single
        // `GROUP BY GROUPING SETS (...)` statement.
        Map<String, Integer> cohort = new TreeMap<>();
        java.util.Set<String> groupBySets = new java.util.HashSet<>();
        for (CapturedExecution ex : run.executions) {
            String sql = ex.sql;
            if (!looksLikeSegmentLoad(sql)) {
                continue;
            }
            out.segmentLoadStmts++;
            if (containsGroupingSetsLiteral(sql)) {
                out.containsGroupingSetsLiteral++;
            }
            String key = cohortKey(sql);
            cohort.merge(key, 1, Integer::sum);
            groupBySets.add(groupBySignature(sql));
        }
        out.cohortToMembers = cohort;
        out.distinctGroupBySets = groupBySets.size();
        // A cohort contributes one post-batch SQL regardless of size.
        out.cohortCount = cohort.size();
        // Post-batch estimate: non-segment-load stmts remain as-is, plus
        // one statement per cohort.
        out.postBatchStmtEstimate =
            (out.totalStmts - out.segmentLoadStmts) + cohort.size();
        return out;
    }

    // ---- Segment-load classification via SQL-shape heuristics ----------
    //
    // These patterns tolerate cross-dialect quoting variants ("..", `..`,
    // [..]). We only need to distinguish segment-load SQL (fact-table
    // aggregate) from, e.g., dimension member lookups.

    private static final Pattern SELECT_FROM_PAT = Pattern.compile(
        "(?is)select\\s+(.*?)\\s+from\\s+(.*?)(?:\\s+where\\s+(.*?))?"
        + "(?:\\s+group\\s+by\\s+(.*?))?(?:\\s+order\\s+by\\s+.*)?$");

    private static final Pattern GROUP_BY_PAT = Pattern.compile(
        "(?is)\\bgroup\\s+by\\b");
    private static final Pattern AGG_PAT = Pattern.compile(
        "(?is)\\b(sum|count|min|max|avg)\\s*\\(");

    private static boolean looksLikeSegmentLoad(String sql) {
        // Segment-load statements always aggregate (sum/count/min/max/avg)
        // and always have a GROUP BY. Dimension member scans don't.
        return GROUP_BY_PAT.matcher(sql).find()
            && AGG_PAT.matcher(sql).find();
    }

    private static boolean containsGroupingSetsLiteral(String sql) {
        String lower = sql.toLowerCase();
        return lower.contains("grouping sets") || lower.contains("grouping(");
    }

    /**
     * Derives a cohort key: fact table + sorted WHERE-clause signature +
     * sorted aggregate-measure signature. Two segment-load statements with
     * identical cohort key but different GROUP BY lists are
     * {@code GROUPING SETS}-batchable.
     */
    private static String cohortKey(String sql) {
        Matcher m = SELECT_FROM_PAT.matcher(sql.trim());
        if (!m.matches()) {
            return "UNPARSED::" + hash(sql);
        }
        String selectList = m.group(1);
        String fromClause = m.group(2);
        String whereClause = m.group(3);
        // Measure signature: strip the non-aggregate columns from the
        // select list and keep only the aggregate expressions.
        List<String> measures = new ArrayList<>();
        for (String tok : selectList.split(",")) {
            String t = tok.trim().toLowerCase();
            if (t.matches("^(sum|count|min|max|avg)\\s*\\(.*")) {
                measures.add(t);
            }
        }
        java.util.Collections.sort(measures);
        String whereSig = whereClause == null
            ? "<none>" : normaliseWhere(whereClause);
        return "fact=" + stripAlias(fromClause.trim())
            + " | where=" + whereSig
            + " | measures=" + measures;
    }

    private static String stripAlias(String from) {
        // Pull the first table token — good enough for single-fact FoodMart.
        String[] tokens = from.split("\\s+|,");
        return tokens.length == 0 ? from : tokens[0];
    }

    private static String normaliseWhere(String where) {
        // Replace literal lists with '<list>' so filters-differing-only-in-
        // values don't falsely split cohorts.
        String n = where.toLowerCase()
            .replaceAll("in\\s*\\([^)]*\\)", "in(<list>)")
            .replaceAll("'[^']*'", "'<v>'")
            .replaceAll("\\s+", " ")
            .trim();
        // Sort AND-separated clauses so ordering doesn't matter.
        List<String> parts =
            new ArrayList<>(Arrays.asList(n.split("\\s+and\\s+")));
        java.util.Collections.sort(parts);
        return String.join(" AND ", parts);
    }

    /**
     * Returns a normalised signature for the GROUP BY clause of a
     * segment-load SQL. Two segment loads with the same cohort key but
     * *different* group-by signatures would be batchable into one
     * {@code GROUP BY GROUPING SETS (...)} statement.
     */
    private static String groupBySignature(String sql) {
        Matcher m = SELECT_FROM_PAT.matcher(sql.trim());
        if (!m.matches()) {
            return "UNPARSED::" + hash(sql);
        }
        String groupBy = m.group(4);
        if (groupBy == null) {
            return "<none>";
        }
        List<String> cols =
            new ArrayList<>(Arrays.asList(groupBy.toLowerCase().split(",")));
        for (int i = 0; i < cols.size(); i++) {
            cols.set(i, cols.get(i).trim());
        }
        java.util.Collections.sort(cols);
        return String.join(",", cols);
    }

    private static String hash(String s) {
        return Integer.toHexString(s.hashCode());
    }

    private static final class PerMdx {
        String mdxName;
        int totalStmts;
        int segmentLoadStmts;
        int cohortCount;
        int postBatchStmtEstimate;
        int containsGroupingSetsLiteral;
        int distinctGroupBySets;
        Map<String, Integer> cohortToMembers;
    }
}

// End GroupingSetBatchProbeTest.java
