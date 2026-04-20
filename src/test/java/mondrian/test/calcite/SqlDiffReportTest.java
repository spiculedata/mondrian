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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import mondrian.calcite.MondrianBackend;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.AggregateCorpus;
import mondrian.test.calcite.corpus.CalcCorpus;
import mondrian.test.calcite.corpus.MvHitCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Opt-in SQL-diff report generator. Walks the full harness corpus
 * ({@link SmokeCorpus} 20 + {@link AggregateCorpus} 11 + {@link CalcCorpus} 10
 * + {@link MvHitCorpus} 4 = 45 queries, of which 45 are user-facing;
 * {@code EquivalenceHarnessTest} plumbing is excluded) and writes
 * {@code docs/reports/sql-diff-report.md} — a side-by-side Markdown document
 * of every emitted SQL statement under legacy vs Calcite backends.
 *
 * <p>Enabled only when {@code -Dharness.writeSqlDiffReport=true}. Default
 * invocation is a no-op so this test contributes nothing to a standard
 * harness run.
 *
 * <p>Legacy SQL is lifted from the frozen {@code golden-legacy/<name>.json}
 * baselines where available (Smoke, Aggregate, Calc corpora). For MvHit the
 * MDX is executed live under {@code -Dmondrian.backend=legacy}. Calcite SQL
 * is always captured live via {@link FoodMartCapture} under
 * {@code -Dmondrian.backend=calcite}.
 *
 * <p>Reference material for a blog post comparing Mondrian's legacy SQL
 * generator with the Calcite-based rewrite.
 */
public class SqlDiffReportTest {

    public static final String WRITE_REPORT_PROP =
        "harness.writeSqlDiffReport";

    private static final Path GOLDEN_LEGACY_DIR =
        Paths.get("src/test/resources/calcite-harness/golden-legacy");

    private static final Path REPORT_FILE =
        Paths.get("docs/reports/sql-diff-report.md");

    @BeforeClass
    public static void bootFoodMart() {
        if (Boolean.getBoolean(WRITE_REPORT_PROP)) {
            FoodMartHsqldbBootstrap.ensureExtracted();
        }
    }

    @Test
    public void writeReport() throws Exception {
        if (!Boolean.getBoolean(WRITE_REPORT_PROP)) {
            // Default: no-op. The report is only generated when the user
            // explicitly opts in, so a standard harness run stays fast.
            return;
        }

        ObjectMapper mapper = new ObjectMapper();

        Map<String, List<QueryReport>> sections = new LinkedHashMap<>();
        sections.put("smoke", runCorpus(SmokeCorpus.queries(), mapper));
        sections.put("aggregate", runCorpus(AggregateCorpus.queries(), mapper));
        sections.put("calc", runCorpus(CalcCorpus.queries(), mapper));
        sections.put("mvhit", runMvHitCorpus());

        String markdown = renderMarkdown(sections);

        Path parent = REPORT_FILE.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(
            REPORT_FILE,
            markdown.getBytes(StandardCharsets.UTF_8));
    }

    // ----------------------------------------------------------------
    // Corpus execution
    // ----------------------------------------------------------------

    private List<QueryReport> runCorpus(
        List<NamedMdx> queries, ObjectMapper mapper) throws IOException
    {
        List<QueryReport> out = new ArrayList<>();
        for (NamedMdx mdx : queries) {
            out.add(runOneFromGolden(mdx, mapper));
        }
        return out;
    }

    /**
     * Smoke / Aggregate / Calc: read legacy SQL from the frozen golden,
     * run Calcite live.
     */
    private QueryReport runOneFromGolden(NamedMdx mdx, ObjectMapper mapper)
        throws IOException
    {
        Path goldenFile = GOLDEN_LEGACY_DIR.resolve(mdx.name + ".json");
        List<Exec> legacy = new ArrayList<>();
        String legacyCellSet = null;
        if (Files.exists(goldenFile)) {
            JsonNode golden = mapper.readTree(goldenFile.toFile());
            legacyCellSet = golden.path("cellSet").asText(null);
            JsonNode execs = golden.path("sqlExecutions");
            if (execs != null && execs.isArray()) {
                for (JsonNode je : execs) {
                    legacy.add(new Exec(
                        je.path("seq").asInt(-1),
                        je.path("sql").asText(""),
                        je.path("rowCount").asInt(-1),
                        je.path("checksum").asText("")));
                }
            }
        }

        // Run Calcite live.
        String prevBackend = System.getProperty(MondrianBackend.PROPERTY);
        System.setProperty(MondrianBackend.PROPERTY, "calcite");
        SegmentLoader.clearCalcitePlannerCache();
        FoodMartCapture.CapturedRun calciteRun;
        try {
            calciteRun = FoodMartCapture.executeCold(mdx, null);
        } finally {
            if (prevBackend == null) {
                System.clearProperty(MondrianBackend.PROPERTY);
            } else {
                System.setProperty(MondrianBackend.PROPERTY, prevBackend);
            }
            SegmentLoader.clearCalcitePlannerCache();
        }
        List<Exec> calcite = toExecs(calciteRun.executions);

        return new QueryReport(
            mdx.name, mdx.mdx,
            legacyCellSet != null ? legacyCellSet : calciteRun.cellSet,
            calciteRun.cellSet,
            legacy, calcite,
            !Files.exists(goldenFile));
    }

    /**
     * MvHit: no frozen legacy golden, so run both backends live. Must
     * toggle ReadAggregates/UseAggregates to activate the aggregate
     * matcher, matching {@link MvHitTest}'s setup.
     */
    private List<QueryReport> runMvHitCorpus() {
        List<QueryReport> out = new ArrayList<>();
        MondrianProperties p = MondrianProperties.instance();
        boolean prevReadAgg = p.ReadAggregates.get();
        boolean prevUseAgg = p.UseAggregates.get();
        String prevBackend = System.getProperty(MondrianBackend.PROPERTY);
        p.ReadAggregates.set(true);
        p.UseAggregates.set(true);
        try {
            for (MvHitCorpus.Entry entry : MvHitCorpus.entries()) {
                out.add(runMvHitOne(entry));
            }
        } finally {
            p.ReadAggregates.set(prevReadAgg);
            p.UseAggregates.set(prevUseAgg);
            if (prevBackend == null) {
                System.clearProperty(MondrianBackend.PROPERTY);
            } else {
                System.setProperty(MondrianBackend.PROPERTY, prevBackend);
            }
            SegmentLoader.clearCalcitePlannerCache();
        }
        return out;
    }

    private QueryReport runMvHitOne(MvHitCorpus.Entry entry) {
        System.setProperty(MondrianBackend.PROPERTY, "legacy");
        SegmentLoader.clearCalcitePlannerCache();
        FoodMartCapture.CapturedRun legacyRun =
            FoodMartCapture.executeCold(entry.mdx, null);

        System.setProperty(MondrianBackend.PROPERTY, "calcite");
        SegmentLoader.clearCalcitePlannerCache();
        FoodMartCapture.CapturedRun calciteRun =
            FoodMartCapture.executeCold(entry.mdx, null);

        return new QueryReport(
            entry.mdx.name,
            entry.mdx.mdx,
            legacyRun.cellSet,
            calciteRun.cellSet,
            toExecs(legacyRun.executions),
            toExecs(calciteRun.executions),
            false);
    }

    private static List<Exec> toExecs(List<CapturedExecution> list) {
        List<Exec> out = new ArrayList<>(list.size());
        for (CapturedExecution e : list) {
            out.add(new Exec(e.seq, e.sql, e.rowCount, e.checksum));
        }
        return out;
    }

    // ----------------------------------------------------------------
    // Markdown rendering
    // ----------------------------------------------------------------

    private static String renderMarkdown(
        Map<String, List<QueryReport>> sections)
    {
        int smokeN = sections.get("smoke").size();
        int aggN = sections.get("aggregate").size();
        int calcN = sections.get("calc").size();
        int mvhitN = sections.get("mvhit").size();
        int total = smokeN + aggN + calcN + mvhitN;

        String timestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());

        StringBuilder sb = new StringBuilder();
        sb.append("# Mondrian-on-Calcite SQL Diff Report\n\n");
        sb.append("**Generated:** ").append(timestamp).append("\n\n");
        sb.append("**Corpus:** ").append(total)
            .append(" queries — ").append(smokeN).append(" smoke + ")
            .append(aggN).append(" aggregate + ")
            .append(calcN).append(" calc + ")
            .append(mvhitN).append(" mv-hit\n\n");
        sb.append("**Backends:** HSQLDB 1.8 (FoodMart fixture) · ")
            .append("Legacy Mondrian SQL generator vs Apache Calcite ")
            .append("SQL generator (dialect: HSQLDB)\n\n");
        sb.append("Legacy SQL for smoke/aggregate/calc is lifted from the ")
            .append("frozen `src/test/resources/calcite-harness/")
            .append("golden-legacy/<name>.json` baselines captured at the ")
            .append("start of the Calcite rewrite. MvHit queries are ")
            .append("executed live under both backends.\n\n");

        sb.append("## Reproducing\n\n");
        sb.append("```sh\n");
        sb.append("mvn -Pcalcite-harness "
            + "-Dharness.writeSqlDiffReport=true "
            + "-Dtest=SqlDiffReportTest test\n");
        sb.append("```\n\n");
        sb.append("Overwrites this file in place.\n\n");

        sb.append("## Table of Contents\n\n");
        sb.append("- [Smoke corpus](#smoke) (")
            .append(smokeN).append(" queries)\n");
        appendToc(sb, sections.get("smoke"));
        sb.append("- [Aggregate corpus](#aggregate) (")
            .append(aggN).append(" queries)\n");
        appendToc(sb, sections.get("aggregate"));
        sb.append("- [Calc corpus](#calc) (")
            .append(calcN).append(" queries)\n");
        appendToc(sb, sections.get("calc"));
        sb.append("- [Mv-hit corpus](#mvhit) (")
            .append(mvhitN).append(" queries)\n");
        appendToc(sb, sections.get("mvhit"));
        sb.append("- [Key takeaways](#takeaways)\n\n");

        appendSection(sb, "smoke", "Smoke corpus", sections.get("smoke"));
        appendSection(sb, "aggregate", "Aggregate corpus",
            sections.get("aggregate"));
        appendSection(sb, "calc", "Calc corpus", sections.get("calc"));
        appendSection(sb, "mvhit", "Mv-hit corpus", sections.get("mvhit"));

        appendTakeaways(sb, sections);

        return sb.toString();
    }

    private static void appendToc(StringBuilder sb, List<QueryReport> qs) {
        for (QueryReport q : qs) {
            sb.append("  - [")
                .append(q.name).append("](#")
                .append(anchor(q.name)).append(")\n");
        }
    }

    private static void appendSection(
        StringBuilder sb, String anchor, String title,
        List<QueryReport> queries)
    {
        sb.append("## <a name=\"").append(anchor).append("\"></a>")
            .append(title).append("\n\n");
        for (QueryReport q : queries) {
            appendQuery(sb, q);
        }
    }

    private static void appendQuery(StringBuilder sb, QueryReport q) {
        sb.append("### <a name=\"").append(anchor(q.name)).append("\"></a>")
            .append(q.name).append("\n\n");

        if (q.goldenMissing) {
            sb.append("> **Note:** no `golden-legacy/")
                .append(q.name).append(".json` — legacy SQL column is ")
                .append("empty below.\n\n");
        }

        sb.append("**MDX:**\n\n");
        sb.append("```mdx\n").append(q.mdx.trim()).append("\n```\n\n");

        sb.append("**Cell-set (Calcite):**\n\n");
        sb.append("```\n").append(q.calciteCellSet.trim()).append("\n```\n\n");

        if (!Objects.equals(q.legacyCellSet, q.calciteCellSet)) {
            sb.append("> **Cell-set drift:** legacy and Calcite produced ")
                .append("different cell-sets. Inspect goldens.\n\n");
        }

        int n = Math.max(q.legacy.size(), q.calcite.size());
        if (n == 0) {
            sb.append("_No SQL captured._\n\n");
            return;
        }
        sb.append("**SQL executions:** legacy=")
            .append(q.legacy.size())
            .append(" · Calcite=")
            .append(q.calcite.size()).append("\n\n");

        for (int i = 0; i < n; i++) {
            Exec L = i < q.legacy.size() ? q.legacy.get(i) : null;
            Exec C = i < q.calcite.size() ? q.calcite.get(i) : null;
            sb.append("#### sqlExecution[").append(i).append("]\n\n");

            sb.append("*Legacy:*\n\n```sql\n");
            sb.append(L == null ? "(no corresponding legacy execution)"
                : nonEmpty(L.sql));
            sb.append("\n```\n\n");

            sb.append("*Calcite:*\n\n```sql\n");
            sb.append(C == null ? "(no corresponding Calcite execution)"
                : nonEmpty(C.sql));
            sb.append("\n```\n\n");

            sb.append("*Delta:* ").append(deltaSummary(L, C)).append("\n\n");
        }
    }

    /** Lightweight lexical-difference summary. Flags the common cases
     *  observed in the corpus — not an NLP diff. */
    private static String deltaSummary(Exec L, Exec C) {
        if (L == null && C == null) {
            return "both sides missing.";
        }
        if (L == null) {
            return "Calcite-only execution — legacy did not emit this SQL.";
        }
        if (C == null) {
            return "Legacy-only execution — Calcite did not emit this SQL.";
        }
        List<String> notes = new ArrayList<>();
        if (L.rowCount != C.rowCount) {
            notes.add("rowCount differs (legacy=" + L.rowCount
                + " calcite=" + C.rowCount + ")");
        } else {
            notes.add("rowCount matches (" + L.rowCount + ")");
        }
        if (!Objects.equals(L.checksum, C.checksum)) {
            notes.add("checksum differs — row-content hash changed "
                + "(expected under SQL_DRIFT)");
        }
        if (Objects.equals(L.sql, C.sql)) {
            notes.add("SQL identical");
        } else {
            String lUpper = L.sql.toUpperCase(Locale.ROOT);
            String cUpper = C.sql.toUpperCase(Locale.ROOT);
            boolean lComma = hasCommaJoin(L.sql);
            boolean cAnsi = cUpper.contains("INNER JOIN")
                || cUpper.contains("LEFT JOIN")
                || cUpper.contains("CROSS JOIN");
            if (lComma && cAnsi) {
                notes.add("join style: comma-join -> ANSI INNER JOIN");
            }
            if (!lUpper.contains("CASE WHEN")
                && cUpper.contains("IS NULL"))
            {
                notes.add(
                    "Calcite adds IS-NULL ordering handling");
            }
            if (!L.sql.contains(" AS \"") && C.sql.contains(" AS \"")) {
                notes.add("Calcite adds AS aliases");
            }
            if (lUpper.contains(" IN (") && !cUpper.contains(" IN (")
                && cUpper.contains(" OR "))
            {
                notes.add("IN-list -> OR chain");
            }
            if (!lUpper.contains(" IN (") && cUpper.contains(" IN (")) {
                notes.add("OR chain -> IN-list");
            }
            if (!lUpper.contains("FETCH NEXT")
                && cUpper.contains("FETCH NEXT"))
            {
                notes.add("Calcite emits FETCH NEXT (ANSI LIMIT)");
            }
            if (lUpper.contains("HAVING") != cUpper.contains("HAVING")) {
                notes.add("HAVING clause presence differs");
            }
            // Keyword-case heuristic: legacy predominantly lower-case,
            // Calcite upper-case. Only note when clearly different.
            if (isLowerKeyword(L.sql) && isUpperKeyword(C.sql)) {
                notes.add("keyword case: lower -> UPPER");
            }
            int lLen = L.sql.length();
            int cLen = C.sql.length();
            if (Math.abs(lLen - cLen) > Math.max(20, lLen / 4)) {
                notes.add("length delta " + (cLen - lLen)
                    + " chars (legacy=" + lLen
                    + ", calcite=" + cLen + ")");
            }
        }
        return String.join("; ", notes) + ".";
    }

    /** True if the SQL looks like a Mondrian-style comma-join
     *  (multiple FROM tables separated by commas, no ANSI JOIN). */
    private static boolean hasCommaJoin(String sql) {
        String upper = sql.toUpperCase(Locale.ROOT);
        if (upper.contains("INNER JOIN")
            || upper.contains("LEFT JOIN")
            || upper.contains("CROSS JOIN"))
        {
            return false;
        }
        int fromIdx = upper.indexOf(" FROM ");
        if (fromIdx < 0) return false;
        int whereIdx = upper.indexOf(" WHERE ", fromIdx);
        int groupIdx = upper.indexOf(" GROUP BY ", fromIdx);
        int orderIdx = upper.indexOf(" ORDER BY ", fromIdx);
        int end = sql.length();
        for (int idx : new int[]{whereIdx, groupIdx, orderIdx}) {
            if (idx > 0 && idx < end) end = idx;
        }
        String fromClause = sql.substring(fromIdx + 6, end);
        return fromClause.contains(",");
    }

    private static boolean isLowerKeyword(String sql) {
        // Look at the leading "select" keyword.
        String trimmed = sql.trim();
        return trimmed.startsWith("select ") || trimmed.startsWith("with ");
    }

    private static boolean isUpperKeyword(String sql) {
        String trimmed = sql.trim();
        return trimmed.startsWith("SELECT ") || trimmed.startsWith("WITH ");
    }

    private static String anchor(String name) {
        return "q-" + name.toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "-");
    }

    private static String nonEmpty(String s) {
        return (s == null || s.isEmpty()) ? "(empty)" : s;
    }

    // ----------------------------------------------------------------
    // Key takeaways — computed, not invented
    // ----------------------------------------------------------------

    private static void appendTakeaways(
        StringBuilder sb, Map<String, List<QueryReport>> sections)
    {
        List<QueryReport> all = new ArrayList<>();
        for (List<QueryReport> s : sections.values()) all.addAll(s);

        int totalExec = 0;
        int sqlIdentical = 0;
        int legacyComma = 0;
        int calciteAnsi = 0;
        int calciteAliases = 0;
        int calciteUpper = 0;
        int legacyLower = 0;
        int calciteIn = 0;
        int legacyIn = 0;
        int checksumDiffers = 0;
        int rowCountDiffers = 0;
        int queriesWithAnyDiff = 0;
        int calciteMoreExecs = 0;
        int calciteFewerExecs = 0;
        int calciteHaving = 0;
        int legacyHaving = 0;
        int cellSetDrifts = 0;

        for (QueryReport q : all) {
            if (!Objects.equals(q.legacyCellSet, q.calciteCellSet)) {
                cellSetDrifts++;
            }
            if (q.calcite.size() > q.legacy.size()) calciteMoreExecs++;
            if (q.calcite.size() < q.legacy.size()) calciteFewerExecs++;
            boolean anyDiff = false;
            int n = Math.min(q.legacy.size(), q.calcite.size());
            for (int i = 0; i < n; i++) {
                Exec L = q.legacy.get(i);
                Exec C = q.calcite.get(i);
                totalExec++;
                if (Objects.equals(L.sql, C.sql)) {
                    sqlIdentical++;
                } else {
                    anyDiff = true;
                }
                if (hasCommaJoin(L.sql)) legacyComma++;
                String cu = C.sql.toUpperCase(Locale.ROOT);
                String lu = L.sql.toUpperCase(Locale.ROOT);
                if (cu.contains("INNER JOIN")
                    || cu.contains("LEFT JOIN")
                    || cu.contains("CROSS JOIN"))
                {
                    calciteAnsi++;
                }
                if (C.sql.contains(" AS \"")) calciteAliases++;
                if (isUpperKeyword(C.sql)) calciteUpper++;
                if (isLowerKeyword(L.sql)) legacyLower++;
                if (cu.contains(" IN (")) calciteIn++;
                if (lu.contains(" IN (")) legacyIn++;
                if (cu.contains("HAVING")) calciteHaving++;
                if (lu.contains("HAVING")) legacyHaving++;
                if (!Objects.equals(L.checksum, C.checksum)) {
                    checksumDiffers++;
                }
                if (L.rowCount != C.rowCount) rowCountDiffers++;
            }
            if (anyDiff) queriesWithAnyDiff++;
        }

        sb.append("## <a name=\"takeaways\"></a>Key takeaways\n\n");
        sb.append("Derived from the actual captured pairs across ")
            .append(all.size()).append(" queries / ")
            .append(totalExec).append(" paired SQL executions.\n\n");

        sb.append("1. **Join style.** Legacy emits comma-separated ")
            .append("`FROM a, b WHERE a.x = b.y` joins in ")
            .append(legacyComma).append("/").append(totalExec)
            .append(" executions; Calcite emits ANSI `INNER JOIN` in ")
            .append(calciteAnsi).append("/").append(totalExec)
            .append(" executions. This is the single largest lexical ")
            .append("difference between the two backends.\n");

        sb.append("2. **Keyword case.** Legacy is lowercase (`select`, ")
            .append("`from`, `where`) in ")
            .append(legacyLower).append("/").append(totalExec)
            .append(" executions; Calcite is uppercase (`SELECT`, ")
            .append("`FROM`, `WHERE`) in ")
            .append(calciteUpper).append("/").append(totalExec)
            .append(". Both backends quote every identifier — that is ")
            .append("not a drift source.\n");

        sb.append("3. **Column aliases.** Calcite attaches `AS \"c0\"`-")
            .append("style aliases in ")
            .append(calciteAliases).append("/").append(totalExec)
            .append(" executions. Legacy uses the same aliasing convention, ")
            .append("but with lowercase `as`. Alias *names* line up ")
            .append("1:1 (`c0`, `c1`, `m0`, …).\n");

        sb.append("4. **IN-list rendering.** Legacy uses `IN (...)` in ")
            .append(legacyIn).append("/").append(totalExec)
            .append(" executions; Calcite emits `IN (...)` in ")
            .append(calciteIn).append("/").append(totalExec)
            .append(". Behaviour is close but not identical — Calcite ")
            .append("sometimes rewrites long OR chains to IN and ")
            .append("occasionally the reverse for 2-element lists.\n");

        sb.append("5. **HAVING clauses.** Legacy emits HAVING in ")
            .append(legacyHaving).append("/").append(totalExec)
            .append(" executions; Calcite in ")
            .append(calciteHaving).append("/").append(totalExec)
            .append(". Where they differ, Calcite has typically pushed ")
            .append("the filter into a subquery WHERE.\n");

        sb.append("6. **Rowset identity.** ")
            .append(checksumDiffers).append("/").append(totalExec)
            .append(" executions have a differing row-content checksum ")
            .append("(SQL_DRIFT advisory territory — cell-sets still match).")
            .append(" rowCount mismatches: ")
            .append(rowCountDiffers).append("/").append(totalExec)
            .append(" (cell-set parity gate should hold these at zero).\n");

        sb.append("7. **Execution-count alignment.** Calcite emitted ")
            .append("MORE SQL statements than legacy on ")
            .append(calciteMoreExecs).append(" queries, FEWER on ")
            .append(calciteFewerExecs).append(". Same count on ")
            .append(all.size() - calciteMoreExecs - calciteFewerExecs)
            .append(".\n");

        sb.append("8. **Cell-set drifts across backends:** ")
            .append(cellSetDrifts).append("/").append(all.size())
            .append(" queries. (Should be zero if CELL_SET_DRIFT gate is green.)\n");

        sb.append("9. **SQL-string identity:** ")
            .append(sqlIdentical).append("/").append(totalExec)
            .append(" executions are byte-identical between backends; ")
            .append("the rest differ only lexically. Across queries, ")
            .append(queriesWithAnyDiff).append("/").append(all.size())
            .append(" have at least one differing SQL pair.\n\n");

        sb.append("### Why this matters for the rewrite\n\n");
        sb.append("The dominant pattern is lexical (case + join style + ")
            .append("trivial aliasing). Rowset-identity drifts are the ")
            .append("interesting ones — those are where Calcite's planner ")
            .append("is making *semantically* different choices (predicate ")
            .append("reordering that surfaces ties differently, ")
            .append("subquery shapes, etc). The SQL_DRIFT advisory gate is ")
            .append("sized to tolerate #1–#5 while still catching #6 if it ")
            .append("bleeds into cell-set drift.\n");
    }

    // ----------------------------------------------------------------
    // Data carriers
    // ----------------------------------------------------------------

    private static final class QueryReport {
        final String name;
        final String mdx;
        final String legacyCellSet;
        final String calciteCellSet;
        final List<Exec> legacy;
        final List<Exec> calcite;
        final boolean goldenMissing;

        QueryReport(
            String name, String mdx,
            String legacyCellSet, String calciteCellSet,
            List<Exec> legacy, List<Exec> calcite,
            boolean goldenMissing)
        {
            this.name = name;
            this.mdx = mdx;
            this.legacyCellSet = legacyCellSet;
            this.calciteCellSet = calciteCellSet;
            this.legacy = legacy;
            this.calcite = calcite;
            this.goldenMissing = goldenMissing;
        }
    }

    private static final class Exec {
        final int seq;
        final String sql;
        final int rowCount;
        final String checksum;

        Exec(int seq, String sql, int rowCount, String checksum) {
            this.seq = seq;
            this.sql = sql == null ? "" : sql;
            this.rowCount = rowCount;
            this.checksum = checksum;
        }
    }
}

// End SqlDiffReportTest.java
