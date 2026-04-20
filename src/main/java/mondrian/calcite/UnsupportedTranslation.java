/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.calcite;

/**
 * Thrown by {@link CalcitePlannerAdapters} when the requested
 * Mondrian-internal shape cannot (yet) be translated to a
 * {@link PlannerRequest}.
 *
 * <p>Callers are expected to catch this and fall back to the legacy
 * {@code SqlQuery}-built SQL string while the Calcite translator coverage
 * grows across worktrees.
 */
public class UnsupportedTranslation extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UnsupportedTranslation(String message) {
        super(message);
    }

    public UnsupportedTranslation(String message, Throwable cause) {
        super(message, cause);
    }
}

// End UnsupportedTranslation.java
