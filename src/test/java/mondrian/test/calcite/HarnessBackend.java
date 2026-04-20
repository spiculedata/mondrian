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

import java.util.Locale;

/**
 * Identifies which JDBC backend the Calcite harness should target for
 * FoodMart. ORTHOGONAL to {@code MondrianBackend} (which selects the
 * legacy-vs-Calcite SQL emitter).
 *
 * <p>Resolution order for {@link #current()}:
 * <ol>
 *   <li>System property {@code calcite.harness.backend}</li>
 *   <li>Environment variable {@code CALCITE_HARNESS_BACKEND}</li>
 *   <li>Default: {@link #HSQLDB}</li>
 * </ol>
 *
 * <p>Introduced by Task Z to allow opt-in Postgres validation of the
 * Calcite harness without disturbing the default HSQLDB run.
 */
public enum HarnessBackend {
    HSQLDB,
    POSTGRES;

    public static final String SYS_PROP = "calcite.harness.backend";
    public static final String ENV_VAR = "CALCITE_HARNESS_BACKEND";

    public static HarnessBackend current() {
        String raw = System.getProperty(SYS_PROP);
        if (raw == null || raw.isEmpty()) {
            raw = System.getenv(ENV_VAR);
        }
        if (raw == null || raw.isEmpty()) {
            return HSQLDB;
        }
        String v = raw.trim().toUpperCase(Locale.ROOT);
        try {
            return HarnessBackend.valueOf(v);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Unrecognised " + ENV_VAR + "/" + SYS_PROP + " value '"
                + raw + "'. Expected one of: HSQLDB, POSTGRES.");
        }
    }
}

// End HarnessBackend.java
