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

import mondrian.rolap.sql.SqlInterceptor;
import mondrian.spi.Dialect;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.SmokeCorpus;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Opt-in coverage probe: runs the smoke corpus with a
 * {@link CalcitePassThrough} interceptor active and counts how many SQL
 * emissions round-trip successfully through Calcite vs fall back to the
 * original SQL string (parse failure).
 *
 * <p>Writes goldens to a <em>throwaway temp directory</em> — never to the
 * checked-in {@code src/test/resources/calcite-harness/golden/} tree, which
 * is locked to classic Mondrian baselines.
 *
 * <p>Double-guarded:
 * <ul>
 *   <li>{@code @Ignore} keeps it out of normal {@code mvn test}.</li>
 *   <li>{@code Assume.assumeTrue(harness.calcite.coverage)} guards against
 *       accidental runs.</li>
 * </ul>
 *
 * <p>Run manually:
 * <pre>
 *   mvn test -Dtest=BaselineCalciteCoverageTest \
 *            -Dharness.calcite.coverage=true \
 *            -Dmondrian.sqlInterceptor=\
 *                mondrian.test.calcite.BaselineCalciteCoverageTest$CountingPassThrough
 * </pre>
 *
 * <p>The test also configures the counting interceptor programmatically via
 * {@link System#setProperty}, so the {@code -Dmondrian.sqlInterceptor} flag
 * is optional.
 */
@Ignore("run manually with -Dharness.calcite.coverage=true")
public class BaselineCalciteCoverageTest {

    public static final AtomicInteger PARSED = new AtomicInteger();
    public static final AtomicInteger FAILED = new AtomicInteger();
    public static final Map<String, Integer> FAILURE_SAMPLES =
        new ConcurrentHashMap<>();

    @Test
    public void measureCorpusParseCoverage() throws Exception {
        Assume.assumeTrue(
            "harness.calcite.coverage system property must be true",
            Boolean.getBoolean("harness.calcite.coverage"));
        FoodMartHsqldbBootstrap.ensureExtracted();

        // Reset counters in case of re-runs inside one JVM.
        PARSED.set(0);
        FAILED.set(0);
        FAILURE_SAMPLES.clear();

        String prevInterceptor =
            System.getProperty("mondrian.sqlInterceptor");
        System.setProperty(
            "mondrian.sqlInterceptor",
            CountingPassThrough.class.getName());
        Path tempDir = Files.createTempDirectory(
            "calcite-coverage-goldens-");
        try {
            List<SmokeCorpus.NamedMdx> queries = SmokeCorpus.queries();
            new BaselineRecorder(tempDir).record(queries);

            int parsed = PARSED.get();
            int failed = FAILED.get();
            int total = parsed + failed;
            System.out.println("=== CalcitePassThrough coverage ===");
            System.out.println("corpus queries: " + queries.size());
            System.out.println("SQL emissions total: " + total);
            System.out.println("parsed OK:           " + parsed);
            System.out.println("parse failures:      " + failed);
            if (!FAILURE_SAMPLES.isEmpty()) {
                System.out.println("--- up to 10 failure samples ---");
                FAILURE_SAMPLES.entrySet().stream()
                    .limit(10)
                    .forEach(e -> System.out.println(
                        "  [x" + e.getValue() + "] " + e.getKey()));
            }
            System.out.println("===================================");
        } finally {
            if (prevInterceptor == null) {
                System.clearProperty("mondrian.sqlInterceptor");
            } else {
                System.setProperty(
                    "mondrian.sqlInterceptor", prevInterceptor);
            }
            // Best-effort cleanup of throwaway goldens.
            deleteRecursively(tempDir);
        }
    }

    private static void deleteRecursively(Path dir) {
        try {
            if (!Files.exists(dir)) {
                return;
            }
            Files.walk(dir)
                .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignore) {
                        // best effort
                    }
                });
        } catch (Exception ignore) {
            // best effort
        }
    }

    /**
     * {@link CalcitePassThrough} subclass that increments static counters on
     * every parse attempt. Must have a public no-arg constructor so
     * {@link SqlInterceptor#loadFromSystemProperty()} can instantiate it.
     */
    /**
     * Measures parse coverage only. Runs the full Calcite parse-and-re-emit
     * via {@link CalcitePassThrough#onSqlEmitted} to count parse successes vs
     * failures, but <em>always returns the original SQL</em> so the surrounding
     * Mondrian + HSQLDB execution path is unaffected. This decouples "can
     * Calcite parse it?" (the Task 6 question) from "does Calcite re-emit it
     * in a form HSQLDB still executes?" (the Task 8 question).
     */
    public static class CountingPassThrough extends CalcitePassThrough {
        @Override
        public String onSqlEmitted(String sql, Dialect dialect) {
            String out = super.onSqlEmitted(sql, dialect);
            if (out == sql) {
                // fail-open path — identity returned, parse failed
                FAILED.incrementAndGet();
                String key = sql.length() > 160
                    ? sql.substring(0, 160) + "..."
                    : sql;
                FAILURE_SAMPLES.merge(key, 1, Integer::sum);
            } else {
                PARSED.incrementAndGet();
            }
            // Always return the ORIGINAL SQL — we are measuring parse
            // coverage, not testing re-emit fidelity here.
            return sql;
        }
    }
}

// End BaselineCalciteCoverageTest.java
