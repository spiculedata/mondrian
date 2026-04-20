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
import mondrian.test.calcite.corpus.AggregateCorpus;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Sibling of {@link BaselineRegenerationTest} that regenerates goldens for
 * the aggregate / native-evaluator corpus. Kept separate from the smoke
 * regeneration test so one corpus can be refreshed without touching the
 * other — changes to {@link AggregateCorpus} shouldn't force a smoke-
 * goldens rewrite, and vice-versa.
 *
 * <p>Double-guarded ({@code @Ignore} + {@code harness.rebaseline}); see
 * {@link BaselineRegenerationTest} javadoc. Run manually:
 * <pre>
 *   mvn test -Dharness.rebaseline=true
 *            -Dtest=BaselineAggregateRegenerationTest
 * </pre>
 */
@Ignore("run manually with -Dharness.rebaseline=true")
public class BaselineAggregateRegenerationTest {

    @Test
    public void regenerate() throws Exception {
        Assume.assumeTrue(
            "harness.rebaseline system property must be true",
            Boolean.getBoolean("harness.rebaseline"));
        FoodMartHsqldbBootstrap.ensureExtracted();
        Path dir = Paths.get("src/test/resources/calcite-harness/golden");
        Files.createDirectories(dir);
        new BaselineRecorder(dir).record(AggregateCorpus.queries());
    }
}
