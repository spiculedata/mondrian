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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Opt-in performance benchmark harness for the 2×2 matrix:
 * {HSQLDB, Postgres} × {legacy SQL emitter, Calcite SQL emitter}.
 *
 * <p>Disabled by default. Enabled via {@code -Dharness.runPerfBench=true}.
 * When enabled, iterates every MDX in {@link SmokeCorpus},
 * {@link AggregateCorpus}, {@link CalcCorpus} and {@link MvHitCorpus}
 * (~45 queries), runs 3 warm-up iterations + 5 timed iterations per query
 * with the Mondrian schema cache flushed before each iteration (via
 * {@link FoodMartCapture#executeCold}), and writes the raw timings out to
 * {@code target/perf-bench-<backend>-<sqlemitter>.json} for later
 * cross-cell comparison by
 * {@code scripts/perf/render-bench-report.py}.
 *
 * <p>Intentionally file-only output (no asserts) — this is a reporting
 * tool, not a regression gate. The HSQLDB 44/44 harness pass stays green
 * because {@code @Test} returns immediately when the flag is off.
 */
public class PerfBenchmarkTest {

    public static final String RUN_PROP = "harness.runPerfBench";

    /** Warm-up iterations (discarded — JIT and connection-pool warmup).
     * Override with -Dharness.bench.warmup=N. Postgres at 1000× scale wants
     * fewer iterations to keep runtimes bounded. */
    private static final int WARMUP =
        Integer.getInteger("harness.bench.warmup", 3);

    /** Timed iterations reported in the JSON output.
     * Override with -Dharness.bench.timed=N. */
    private static final int TIMED =
        Integer.getInteger("harness.bench.timed", 5);

    @BeforeClass
    public static void bootFoodMart() {
        if (!Boolean.getBoolean(RUN_PROP)) {
            return;
        }
        if (HarnessBackend.current() == HarnessBackend.HSQLDB) {
            FoodMartHsqldbBootstrap.ensureExtracted();
        }
    }

    @Test
    public void benchmark() throws Exception {
        if (!Boolean.getBoolean(RUN_PROP)) {
            return;
        }

        HarnessBackend backend = HarnessBackend.current();
        MondrianBackend emitter = MondrianBackend.current();

        // Optional: -Dharness.bench.corpus=<smoke|aggregate|calc|mvhit>
        // (comma-separated) filters which corpora run. Default = all.
        String corpusFilter =
            System.getProperty("harness.bench.corpus", "").trim();
        java.util.Set<String> include;
        if (corpusFilter.isEmpty()) {
            include = null; // all
        } else {
            include = new java.util.HashSet<>();
            for (String s : corpusFilter.toLowerCase(Locale.ROOT).split(",")) {
                s = s.trim();
                if (!s.isEmpty()) {
                    include.add(s);
                }
            }
        }

        List<Target> targets = new ArrayList<>();
        if (include == null || include.contains("smoke")) {
            for (NamedMdx q : SmokeCorpus.queries()) {
                targets.add(new Target("smoke", q, false));
            }
        }
        if (include == null || include.contains("aggregate")) {
            for (NamedMdx q : AggregateCorpus.queries()) {
                targets.add(new Target("aggregate", q, false));
            }
        }
        if (include == null || include.contains("calc")) {
            for (NamedMdx q : CalcCorpus.queries()) {
                targets.add(new Target("calc", q, false));
            }
        }
        if (include == null || include.contains("mvhit")) {
            for (MvHitCorpus.Entry e : MvHitCorpus.entries()) {
                targets.add(new Target("mvhit", e.mdx, true));
            }
        }

        // Pin the emitter for the duration of this run so every corpus
        // query executes through the same code path.
        String prevEmitter = System.getProperty(MondrianBackend.PROPERTY);
        System.setProperty(
            MondrianBackend.PROPERTY,
            emitter.name().toLowerCase(Locale.ROOT));

        MondrianProperties p = MondrianProperties.instance();
        boolean prevReadAgg = p.ReadAggregates.get();
        boolean prevUseAgg = p.UseAggregates.get();

        List<QueryResult> results = new ArrayList<>();
        try {
            for (Target t : targets) {
                results.add(runQuery(t, p));
            }
        } finally {
            p.ReadAggregates.set(prevReadAgg);
            p.UseAggregates.set(prevUseAgg);
            if (prevEmitter == null) {
                System.clearProperty(MondrianBackend.PROPERTY);
            } else {
                System.setProperty(MondrianBackend.PROPERTY, prevEmitter);
            }
            SegmentLoader.clearCalcitePlannerCache();
        }

        String backendTag = backend.name().toLowerCase(Locale.ROOT);
        String emitterTag = emitter.name().toLowerCase(Locale.ROOT);
        Path out = Paths.get(
            "target",
            "perf-bench-" + backendTag + "-" + emitterTag + ".json");
        Path parent = out.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String json = renderJson(backendTag, emitterTag, results);
        Files.write(out, json.getBytes(StandardCharsets.UTF_8));
        System.out.println("[perf-bench] wrote " + out.toAbsolutePath());
    }

    private QueryResult runQuery(Target t, MondrianProperties p) {
        boolean prevReadAgg = p.ReadAggregates.get();
        boolean prevUseAgg = p.UseAggregates.get();
        if (t.mvHit) {
            p.ReadAggregates.set(true);
            p.UseAggregates.set(true);
        } else {
            p.ReadAggregates.set(false);
            p.UseAggregates.set(false);
        }
        try {
            // Warm-up (discarded). Also validates the query actually runs
            // on this cell before we start timing. After Y.2 the Calcite
            // planner cache is keyed on JDBC identity (not RolapStar), so
            // clearing it per iteration defeats the cache that Mondrian's
            // schema flush is supposed to survive. Clear exactly once at
            // the start of warm-up so the first iteration is truly cold.
            int sqlCount = 0;
            boolean nonEmpty = false;
            String topLeft = null;
            SegmentLoader.clearCalcitePlannerCache();
            for (int i = 0; i < WARMUP; i++) {
                FoodMartCapture.CapturedRun run =
                    FoodMartCapture.executeCold(t.mdx, null);
                if (i == 0) {
                    sqlCount = run.executions.size();
                    nonEmpty = run.cellSet != null
                        && !run.cellSet.trim().isEmpty();
                    topLeft = extractTopLeft(run.cellSet);
                }
            }

            // Timed runs.
            long[] ns = new long[TIMED];
            int sqlCountLastTimed = 0;
            for (int i = 0; i < TIMED; i++) {
                long t0 = System.nanoTime();
                FoodMartCapture.CapturedRun run =
                    FoodMartCapture.executeCold(t.mdx, null);
                long t1 = System.nanoTime();
                ns[i] = t1 - t0;
                sqlCountLastTimed = run.executions.size();
            }

            return new QueryResult(
                t.corpus, t.mdx.name, ns, sqlCountLastTimed == 0
                    ? sqlCount : sqlCountLastTimed, nonEmpty, topLeft,
                null);
        } catch (RuntimeException ex) {
            return new QueryResult(
                t.corpus, t.mdx.name, new long[0], 0, false, null,
                ex.getClass().getSimpleName() + ": " + ex.getMessage());
        } finally {
            p.ReadAggregates.set(prevReadAgg);
            p.UseAggregates.set(prevUseAgg);
        }
    }

    /** Grab the top-left numeric cell (or null) from a serialized
     *  cell-set. Used as a cross-cell sanity check — Postgres fact table
     *  is 1000× HSQLDB so matching queries should see a 1000× ratio. */
    private static String extractTopLeft(String cellSet) {
        if (cellSet == null) return null;
        // TestContext.toString(Result) formats like:
        //   Axis #0: ...
        //   Axis #1: ...
        //   Row #0: 12345.00
        // We pick the first "Row #0:" line.
        for (String line : cellSet.split("\\r?\\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("Row #0:")) {
                return trimmed.substring("Row #0:".length()).trim();
            }
        }
        return null;
    }

    // ----------------------------------------------------------------
    // JSON rendering (handwritten — no jackson dep needed beyond what
    // the harness already has; keeping this file self-contained is just
    // easier to audit).
    // ----------------------------------------------------------------

    private static String renderJson(
        String backendTag, String emitterTag, List<QueryResult> results)
    {
        long scale = backendTag.equals("postgres") ? 1000L : 1L;
        String timestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"matrix\": {\n");
        sb.append("    \"backend\": ").append(quote(backendTag))
            .append(",\n");
        sb.append("    \"sqlemitter\": ").append(quote(emitterTag))
            .append(",\n");
        sb.append("    \"scale\": ").append(scale).append(",\n");
        sb.append("    \"warmup\": ").append(WARMUP).append(",\n");
        sb.append("    \"timed\": ").append(TIMED).append(",\n");
        sb.append("    \"timestamp\": ").append(quote(timestamp))
            .append("\n");
        sb.append("  },\n");
        sb.append("  \"queries\": [\n");
        for (int i = 0; i < results.size(); i++) {
            QueryResult r = results.get(i);
            sb.append("    {\n");
            sb.append("      \"corpus\": ").append(quote(r.corpus))
                .append(",\n");
            sb.append("      \"name\": ").append(quote(r.name))
                .append(",\n");
            sb.append("      \"iterationsNs\": [");
            for (int k = 0; k < r.iterationsNs.length; k++) {
                if (k > 0) sb.append(", ");
                sb.append(r.iterationsNs[k]);
            }
            sb.append("],\n");
            long[] stats = stats(r.iterationsNs);
            sb.append("      \"stats\": {");
            if (stats != null) {
                sb.append("\"min\": ").append(stats[0])
                    .append(", \"median\": ").append(stats[1])
                    .append(", \"mean\": ").append(stats[2])
                    .append(", \"max\": ").append(stats[3]);
            }
            sb.append("},\n");
            sb.append("      \"sqlCount\": ").append(r.sqlCount)
                .append(",\n");
            sb.append("      \"nonEmpty\": ").append(r.nonEmpty)
                .append(",\n");
            sb.append("      \"topLeft\": ")
                .append(r.topLeft == null ? "null" : quote(r.topLeft))
                .append(",\n");
            sb.append("      \"error\": ")
                .append(r.error == null ? "null" : quote(r.error))
                .append("\n");
            sb.append("    }");
            if (i + 1 < results.size()) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    /** Returns {min, median, mean, max} in ns, or null if empty. */
    private static long[] stats(long[] xs) {
        if (xs == null || xs.length == 0) return null;
        long[] sorted = Arrays.copyOf(xs, xs.length);
        Arrays.sort(sorted);
        long min = sorted[0];
        long max = sorted[sorted.length - 1];
        long median;
        int n = sorted.length;
        if ((n & 1) == 1) {
            median = sorted[n / 2];
        } else {
            median = (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
        }
        long sum = 0;
        for (long x : sorted) sum += x;
        long mean = sum / n;
        return new long[] { min, median, mean, max };
    }

    private static String quote(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '"': sb.append("\\\""); break;
            case '\\': sb.append("\\\\"); break;
            case '\n': sb.append("\\n"); break;
            case '\r': sb.append("\\r"); break;
            case '\t': sb.append("\\t"); break;
            default:
                if (c < 0x20) {
                    sb.append(String.format("\\u%04x", (int) c));
                } else {
                    sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    // ----------------------------------------------------------------
    // Data carriers
    // ----------------------------------------------------------------

    private static final class Target {
        final String corpus;
        final NamedMdx mdx;
        final boolean mvHit;
        Target(String corpus, NamedMdx mdx, boolean mvHit) {
            this.corpus = corpus;
            this.mdx = mdx;
            this.mvHit = mvHit;
        }
    }

    private static final class QueryResult {
        final String corpus;
        final String name;
        final long[] iterationsNs;
        final int sqlCount;
        final boolean nonEmpty;
        final String topLeft;
        final String error;
        QueryResult(String corpus, String name, long[] iterationsNs,
                    int sqlCount, boolean nonEmpty, String topLeft,
                    String error)
        {
            this.corpus = corpus;
            this.name = name;
            this.iterationsNs = iterationsNs;
            this.sqlCount = sqlCount;
            this.nonEmpty = nonEmpty;
            this.topLeft = topLeft;
            this.error = error;
        }
    }

    @SuppressWarnings("unused")
    private static List<String> empty() {
        return Collections.unmodifiableList(new ArrayList<String>());
    }
}

// End PerfBenchmarkTest.java
