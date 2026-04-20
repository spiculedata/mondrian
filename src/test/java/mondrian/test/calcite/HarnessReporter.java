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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Accumulates {@link HarnessResult} records keyed by query name and writes a
 * minimal side-by-side HTML report. Intended for human eyeballing, not pretty
 * presentation — plain tables, inline styles, zero external deps.
 *
 * <p>Task 9 of the Calcite Equivalence Harness plan.
 */
public final class HarnessReporter {

    private static final Map<String, HarnessResult> RESULTS =
        new TreeMap<String, HarnessResult>();

    private HarnessReporter() {
        // utility
    }

    /** Records a result. Thread-safe via synchronization on the map. */
    public static synchronized void record(String name, HarnessResult r) {
        RESULTS.put(name, r);
    }

    /** Clears accumulated state — useful from tests. */
    public static synchronized void reset() {
        RESULTS.clear();
    }

    /** Writes the accumulated report to {@code out}. */
    public static synchronized void writeHtml(Path out) throws IOException {
        Path parent = out.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (PrintWriter w = new PrintWriter(
            Files.newBufferedWriter(out, StandardCharsets.UTF_8)))
        {
            w.println("<!doctype html>");
            w.println("<html><head><meta charset=\"utf-8\">");
            w.println("<title>Calcite Harness Report</title>");
            w.println("<style>");
            w.println("body{font-family:sans-serif;margin:1em;}");
            w.println("table{border-collapse:collapse;width:100%;}");
            w.println("th,td{border:1px solid #999;padding:4px;"
                + "vertical-align:top;font-size:12px;}");
            w.println("tr.PASS{background:#e6ffe6;}");
            w.println("tr.FAIL{background:#ffe6e6;}");
            w.println("pre{white-space:pre-wrap;margin:0;font-size:11px;}");
            w.println("code{font-size:11px;}");
            w.println(".sql{width:50%;}");
            w.println("</style></head><body>");
            w.println("<h1>Calcite Harness Report</h1>");
            int total = RESULTS.size();
            int passed = 0;
            for (HarnessResult r : RESULTS.values()) {
                if (r.failureClass == FailureClass.PASS) {
                    passed++;
                }
            }
            w.printf("<p>%d / %d passed.</p>%n", passed, total);
            w.println("<table>");
            w.println("<tr><th>Name</th><th>Status</th><th>Detail</th>"
                + "<th class=\"sql\">Run A SQL</th>"
                + "<th class=\"sql\">Run B SQL</th></tr>");
            for (Map.Entry<String, HarnessResult> e : RESULTS.entrySet()) {
                writeRow(w, e.getKey(), e.getValue());
            }
            w.println("</table>");
            w.println("</body></html>");
        }
    }

    private static void writeRow(PrintWriter w, String name, HarnessResult r) {
        String cls = r.failureClass == FailureClass.PASS ? "PASS" : "FAIL";
        String detail = r.detail == null ? "" : r.detail;
        String shortDetail = detail.length() > 200
            ? detail.substring(0, 200) + "..." : detail;
        w.printf("<tr class=\"%s\">%n", cls);
        w.printf("<td><code>%s</code></td>%n", esc(name));
        w.printf("<td>%s</td>%n", esc(r.failureClass.name()));
        w.print("<td>");
        w.print(esc(shortDetail));
        if (detail.length() > 200) {
            w.print("<details><summary>full</summary><pre>");
            w.print(esc(detail));
            w.print("</pre></details>");
        }
        w.println("</td>");
        w.print("<td class=\"sql\">");
        writeSqlCell(w, r.runASql);
        w.println("</td>");
        w.print("<td class=\"sql\">");
        writeSqlCell(w, r.runBSql);
        w.println("</td>");
        w.println("</tr>");
    }

    private static void writeSqlCell(PrintWriter w, List<CapturedExecution> xs) {
        if (xs == null || xs.isEmpty()) {
            w.print("<em>none</em>");
            return;
        }
        for (CapturedExecution ce : xs) {
            w.printf("<details><summary>#%d rows=%d sum=%s</summary>"
                    + "<pre>%s</pre></details>",
                ce.seq, ce.rowCount,
                esc(ce.checksum == null ? "" : ce.checksum),
                esc(ce.sql == null ? "" : ce.sql));
        }
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '&': b.append("&amp;"); break;
            case '<': b.append("&lt;"); break;
            case '>': b.append("&gt;"); break;
            case '"': b.append("&quot;"); break;
            case '\'': b.append("&#39;"); break;
            default: b.append(c);
            }
        }
        return b.toString();
    }
}

// End HarnessReporter.java
