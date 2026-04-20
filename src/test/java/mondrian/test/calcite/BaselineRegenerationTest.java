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

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Manual test that regenerates the checked-in Calcite harness goldens under
 * {@code src/test/resources/calcite-harness/golden/}. Double-guarded:
 *
 * <ul>
 *   <li>{@code @Ignore} keeps the test out of normal {@code mvn test}.</li>
 *   <li>{@code Assume.assumeTrue(harness.rebaseline)} protects against
 *       accidental runs if anyone removes the {@code @Ignore}.</li>
 * </ul>
 *
 * <p>Run manually:
 * <pre>
 *   mvn test -Dharness.rebaseline=true -Dtest=BaselineRegenerationTest
 * </pre>
 *
 * <p>Surefire 3.x honours {@code @Ignore} even when the test class is named
 * explicitly, so temporarily commenting out {@code @Ignore} is the reliable
 * path for initial generation.
 */
@Ignore("run manually with -Dharness.rebaseline=true")
public class BaselineRegenerationTest {

    @Test
    public void regenerate() throws Exception {
        Assume.assumeTrue(
            "harness.rebaseline system property must be true",
            Boolean.getBoolean("harness.rebaseline"));
        FoodMartHsqldbBootstrap.ensureExtracted();
        Path dir = Paths.get("src/test/resources/calcite-harness/golden");
        Files.createDirectories(dir);
        new BaselineRecorder(dir).record(SmokeCorpus.queries());
    }
}
