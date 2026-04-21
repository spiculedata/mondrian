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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Opt-in cross-cell SQL-diff report for the blog post narrative. Captures
 * one representative SQL batch per query per cell of the 2x2 matrix
 * (HSQLDB vs Postgres x legacy emitter vs Calcite emitter) and writes a
 * side-by-side Markdown document at
 * {@code docs/reports/sql-diff-2x2.md}.
 *
 * <p>Enabled only when {@code -Dharness.writeSqlDiff2x2=true}. Requires
 * a local Postgres {@code foodmart_calcite} database — if the connection
 * fails, Postgres cells are flagged as unavailable in the output rather
 * than failing the test.
 *
 * <p>Uses a curated 5-query subset rather than the full 45 so the
 * document stays readable.
 */
public class SqlDiffMatrixReportTest {

    public static final String WRITE_PROP = "harness.writeSqlDiff2x2";

    private static final Path REPORT_FILE =
        Paths.get("docs/reports/sql-diff-2x2.md");

    /** Curated subset for the blog (~5-8 queries). */
    private static final List<String> REPRESENTATIVE = Arrays.asList(
        "basic-select",
        "crossjoin",
        "aggregate-measure",
        "topcount",
        "filter",
        "agg-c-year-country",
        "calc-arith-ratio");

    @BeforeClass
    public static void bootFoodMart() {
        if (Boolean.getBoolean(WRITE_PROP)) {
            FoodMartHsqldbBootstrap.ensureExtracted();
        }
    }

    @Test
    public void writeReport() throws Exception {
        if (!Boolean.getBoolean(WRITE_PROP)) {
            return;
        }

        List<NamedMdx> picked = pickQueries();

        // Cell A — HSQLDB + legacy, Cell B — Postgres + legacy,
        // Cell C — HSQLDB + calcite, Cell D — Postgres + calcite.
        Map<String, CellResult> byName = new LinkedHashMap<>();

        boolean postgresUp = probePostgres();

        for (NamedMdx mdx : picked) {
            CellResult cr = new CellResult();
            cr.mdx = mdx;
            cr.a = runCell("hsqldb", "legacy", mdx);
            cr.c = runCell("hsqldb", "calcite", mdx);
            if (postgresUp) {
                cr.b = runCell("postgres", "legacy", mdx);
                cr.d = runCell("postgres", "calcite", mdx);
            }
            byName.put(mdx.name, cr);
        }

        String md = render(byName, postgresUp);
        Path parent = REPORT_FILE.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(REPORT_FILE, md.getBytes(StandardCharsets.UTF_8));
        System.out.println("[sql-diff-2x2] wrote "
            + REPORT_FILE.toAbsolutePath());
    }

    private static List<NamedMdx> pickQueries() {
        List<NamedMdx> all = new ArrayList<>();
        all.addAll(SmokeCorpus.queries());
        all.addAll(AggregateCorpus.queries());
        all.addAll(CalcCorpus.queries());
        for (MvHitCorpus.Entry e : MvHitCorpus.entries()) {
            all.add(e.mdx);
        }
        List<NamedMdx> picked = new ArrayList<>();
        for (String want : REPRESENTATIVE) {
            for (NamedMdx q : all) {
                if (q.name.equals(want)) {
                    picked.add(q);
                    break;
                }
            }
        }
        return picked;
    }

    private static boolean probePostgres() {
        try {
            DataSource ds = PostgresFoodMartDataSource.create();
            try (Connection c = ds.getConnection()) {
                return c.isValid(2);
            }
        } catch (SQLException | RuntimeException e) {
            System.out.println("[sql-diff-2x2] postgres unavailable: "
                + e.getMessage());
            return false;
        }
    }

    private static Cell runCell(
        String backend, String emitter, NamedMdx mdx)
    {
        String prevBackend = System.getProperty(HarnessBackend.SYS_PROP);
        String prevEmitter = System.getProperty(MondrianBackend.PROPERTY);
        System.setProperty(HarnessBackend.SYS_PROP, backend.toUpperCase());
        System.setProperty(MondrianBackend.PROPERTY, emitter);
        SegmentLoader.clearCalcitePlannerCache();

        boolean isMvHit = mdx.name.startsWith("agg-");
        MondrianProperties p = MondrianProperties.instance();
        boolean prevReadAgg = p.ReadAggregates.get();
        boolean prevUseAgg = p.UseAggregates.get();
        if (isMvHit) {
            p.ReadAggregates.set(true);
            p.UseAggregates.set(true);
        }
        try {
            FoodMartCapture.CapturedRun run =
                FoodMartCapture.executeCold(mdx, null);
            List<String> sqls = new ArrayList<>();
            for (CapturedExecution e : run.executions) {
                sqls.add(e.sql);
            }
            return new Cell(sqls, null);
        } catch (RuntimeException ex) {
            return new Cell(null, ex.getClass().getSimpleName()
                + ": " + ex.getMessage());
        } finally {
            p.ReadAggregates.set(prevReadAgg);
            p.UseAggregates.set(prevUseAgg);
            if (prevBackend == null) {
                System.clearProperty(HarnessBackend.SYS_PROP);
            } else {
                System.setProperty(HarnessBackend.SYS_PROP, prevBackend);
            }
            if (prevEmitter == null) {
                System.clearProperty(MondrianBackend.PROPERTY);
            } else {
                System.setProperty(MondrianBackend.PROPERTY, prevEmitter);
            }
            SegmentLoader.clearCalcitePlannerCache();
        }
    }

    // ----------------------------------------------------------------
    // Rendering
    // ----------------------------------------------------------------

    private static String render(
        Map<String, CellResult> byName, boolean postgresUp)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("# Mondrian-on-Calcite SQL diff — 2x2 matrix\n\n");
        sb.append("Side-by-side emitted SQL across the four cells of the ")
            .append("perf benchmark matrix:\n\n");
        sb.append("- **Cell A** — HSQLDB (~87k-row fact table) + legacy "
            + "Mondrian SQL emitter\n");
        sb.append("- **Cell B** — Postgres (86.8M-row fact table) + "
            + "legacy Mondrian SQL emitter\n");
        sb.append("- **Cell C** — HSQLDB + Calcite SQL emitter\n");
        sb.append("- **Cell D** — Postgres + Calcite SQL emitter\n\n");
        sb.append("This captures two dimensions of SQL difference at "
            + "once: **planner** (legacy vs Calcite) and **dialect** "
            + "(HSQLDB vs Postgres). The Calcite emitter selects its "
            + "dialect from the live JDBC connection, so Cells C and D "
            + "are emitting the same logical plan through different "
            + "dialect-specific SQL writers.\n\n");
        if (!postgresUp) {
            sb.append("> **Note:** Postgres was unreachable when this "
                + "report was generated. Cells B and D are marked "
                + "`unavailable` below. Start `foodmart_calcite` and "
                + "re-run to populate.\n\n");
        }
        sb.append("Corpus subset used (keeps the document readable): ")
            .append(REPRESENTATIVE).append(".\n\n");

        sb.append("## Reproducing\n\n");
        sb.append("```sh\n");
        sb.append("mvn -Pcalcite-harness "
            + "-Dharness.writeSqlDiff2x2=true "
            + "-Dtest=SqlDiffMatrixReportTest test\n");
        sb.append("```\n\n");

        for (Map.Entry<String, CellResult> e : byName.entrySet()) {
            appendQuery(sb, e.getKey(), e.getValue());
        }
        return sb.toString();
    }

    private static void appendQuery(
        StringBuilder sb, String name, CellResult r)
    {
        sb.append("## `").append(name).append("`\n\n");
        sb.append("**MDX:**\n\n```mdx\n")
            .append(r.mdx.mdx.trim()).append("\n```\n\n");

        appendCell(sb, "A (HSQLDB + legacy)", r.a);
        appendCell(sb, "B (Postgres + legacy)", r.b);
        appendCell(sb, "C (HSQLDB + Calcite)", r.c);
        appendCell(sb, "D (Postgres + Calcite)", r.d);

        sb.append("**Key differences:** ")
            .append(differenceSummary(r)).append("\n\n");
    }

    private static void appendCell(
        StringBuilder sb, String label, Cell cell)
    {
        sb.append("**Cell ").append(label).append(":**\n\n");
        if (cell == null) {
            sb.append("_unavailable_\n\n");
            return;
        }
        if (cell.error != null) {
            sb.append("_error: ").append(cell.error).append("_\n\n");
            return;
        }
        if (cell.sqls == null || cell.sqls.isEmpty()) {
            sb.append("_no SQL captured_\n\n");
            return;
        }
        // Pick the "representative" SQL — the last executed, which is
        // typically the fact-table query (dimension-member queries
        // come first).
        String sql = cell.sqls.get(cell.sqls.size() - 1);
        sb.append("```sql\n").append(sql).append("\n```\n\n");
        if (cell.sqls.size() > 1) {
            sb.append("_(")
                .append(cell.sqls.size())
                .append(" SQL statements total; last one shown)_\n\n");
        }
    }

    private static String differenceSummary(CellResult r) {
        List<String> notes = new ArrayList<>();
        // Planner differences (A vs C, B vs D): join style, keyword
        // case, alias quoting.
        if (r.a != null && r.c != null
            && r.a.sqls != null && r.c.sqls != null
            && !r.a.sqls.isEmpty() && !r.c.sqls.isEmpty())
        {
            String la = tail(r.a);
            String cc = tail(r.c);
            if (la != null && cc != null) {
                if (la.contains(",") && la.toLowerCase().contains(" from ")
                    && (cc.contains("INNER JOIN") || cc.contains("LEFT JOIN")))
                {
                    notes.add("Calcite switches the legacy comma-join "
                        + "to ANSI `INNER JOIN`");
                }
                if (la.trim().startsWith("select ")
                    && cc.trim().startsWith("SELECT "))
                {
                    notes.add("Calcite upper-cases SQL keywords");
                }
            }
        }
        // Dialect differences (A vs B, C vs D): identifier quoting,
        // limit syntax.
        if (r.c != null && r.d != null
            && r.c.sqls != null && r.d.sqls != null
            && !r.c.sqls.isEmpty() && !r.d.sqls.isEmpty())
        {
            String cc = tail(r.c);
            String dd = tail(r.d);
            if (cc != null && dd != null) {
                if (cc.contains("\"") && !dd.contains("\"") && dd.contains("`")) {
                    notes.add("Postgres dialect swaps identifier quoting");
                }
                if (cc.contains("FETCH NEXT") != dd.contains("FETCH NEXT")) {
                    notes.add("LIMIT/FETCH syntax differs between dialects");
                }
            }
        }
        if (notes.isEmpty()) {
            return "see SQL blocks above — differences are primarily "
                + "lexical (quoting, keyword case) unless the planner "
                + "changed predicate shape.";
        }
        return String.join("; ", notes) + ".";
    }

    private static String tail(Cell c) {
        if (c == null || c.sqls == null || c.sqls.isEmpty()) return null;
        return c.sqls.get(c.sqls.size() - 1);
    }

    // ----------------------------------------------------------------
    // Data carriers
    // ----------------------------------------------------------------

    private static final class CellResult {
        NamedMdx mdx;
        Cell a; // HSQLDB + legacy
        Cell b; // Postgres + legacy
        Cell c; // HSQLDB + calcite
        Cell d; // Postgres + calcite
    }

    private static final class Cell {
        final List<String> sqls;
        final String error;
        Cell(List<String> sqls, String error) {
            this.sqls = sqls;
            this.error = error;
        }
    }
}

// End SqlDiffMatrixReportTest.java
