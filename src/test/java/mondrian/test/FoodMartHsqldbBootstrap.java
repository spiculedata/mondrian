/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule / Saiku community
// All Rights Reserved.
*/
package mondrian.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Extracts the pre-loaded HSQLDB 1.8 FoodMart database files shipped inside
 * the {@code mondrian-data-foodmart-hsql} test-scope artifact onto the local
 * filesystem under {@code target/foodmart/} so that tests using a
 * {@code jdbc:hsqldb:file:target/foodmart/foodmart} URL can open the DB.
 *
 * <p>Idempotent and thread-safe: the extraction runs at most once per JVM.
 */
public final class FoodMartHsqldbBootstrap {

    private static final String[] ENTRIES = {
        "foodmart/foodmart.properties",
        "foodmart/foodmart.script"
    };

    private static final Path TARGET_DIR = Paths.get("target", "foodmart");

    private static volatile boolean extracted = false;

    private FoodMartHsqldbBootstrap() {
    }

    /**
     * Ensure the HSQLDB files exist under {@code target/foodmart/}. Safe to
     * call from every test's static initializer — does real work only once.
     */
    public static synchronized void ensureExtracted() {
        if (extracted) {
            return;
        }
        Path marker = TARGET_DIR.resolve("foodmart.properties");
        try {
            if (!Files.exists(marker)) {
                Files.createDirectories(TARGET_DIR);
                ClassLoader cl = FoodMartHsqldbBootstrap.class.getClassLoader();
                for (String entry : ENTRIES) {
                    URL url = cl.getResource(entry);
                    if (url == null) {
                        throw new IOException(
                            "Could not locate " + entry + " on classpath. "
                            + "Is mondrian-data-foodmart-hsql:0.1 declared as a "
                            + "test dependency?");
                    }
                    // Strip the leading "foodmart/" — files land directly in target/foodmart/
                    String filename = entry.substring("foodmart/".length());
                    Path out = TARGET_DIR.resolve(filename);
                    try (InputStream in = url.openStream()) {
                        Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                System.out.println(
                    "[FoodMartHsqldbBootstrap] extracted HSQLDB FoodMart fixture to "
                    + TARGET_DIR.toAbsolutePath());
            }
            extracted = true;
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to extract HSQLDB FoodMart fixture to " + TARGET_DIR, e);
        }
    }
}
