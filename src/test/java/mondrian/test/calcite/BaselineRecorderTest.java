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

import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.SmokeCorpus;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that {@link BaselineRecorder} produces byte-identical JSON goldens
 * across repeated runs of the same MDX corpus. Determinism is load-bearing:
 * if identical input produces different output, downstream Calcite drift
 * detection becomes meaningless.
 */
public class BaselineRecorderTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    @Test
    public void recordsAndReRecordsDeterministically() throws Exception {
        Path tempDir = Files.createTempDirectory("calcite-golden-test");
        BaselineRecorder rec = new BaselineRecorder(tempDir);
        rec.record(SmokeCorpus.queries());
        Map<String, String> firstRun = checksumAll(tempDir);

        assertEquals(
            "expected 20 goldens after first run",
            SmokeCorpus.queries().size(),
            firstRun.size());

        rec.record(SmokeCorpus.queries());
        Map<String, String> secondRun = checksumAll(tempDir);

        assertEquals(
            "rebaseline must be deterministic",
            firstRun, secondRun);
    }

    @Test
    public void goldenContainsCapturedSqlExecutions() throws Exception {
        Path tempDir = Files.createTempDirectory("calcite-golden-sanity");
        BaselineRecorder rec = new BaselineRecorder(tempDir);
        rec.record(SmokeCorpus.queries().subList(0, 1));
        Path golden =
            tempDir.resolve(SmokeCorpus.queries().get(0).name + ".json");
        String content =
            new String(Files.readAllBytes(golden), StandardCharsets.UTF_8);
        assertTrue(
            "golden must contain non-empty cellSet text:\n" + content,
            content.contains("\"cellSet\"")
                && content.contains("Axis #"));
        assertTrue(
            "golden must contain at least one sqlExecutions entry (empty "
            + "list would mean SqlCapture is not wired correctly): " + content,
            content.contains("\"sqlExecutions\"")
                && content.contains("\"checksum\" : \"sha256:"));
    }

    /**
     * SHA-256 every {@code *.json} under the golden dir. Sorted map so the
     * map equality check does not depend on filesystem iteration order.
     */
    private static Map<String, String> checksumAll(Path dir)
        throws IOException
    {
        Map<String, String> sums = new TreeMap<>();
        Files.list(dir)
            .filter(p -> p.getFileName().toString().endsWith(".json"))
            .forEach(p -> {
                try {
                    byte[] bytes = Files.readAllBytes(p);
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    byte[] digest = md.digest(bytes);
                    StringBuilder hex = new StringBuilder();
                    for (byte b : digest) {
                        hex.append(String.format("%02x", b & 0xff));
                    }
                    sums.put(p.getFileName().toString(), hex.toString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        return sums;
    }
}
