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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;

import javax.sql.DataSource;

/**
 * Calcite schema adapter over the JDBC {@link DataSource} that backs a
 * Mondrian star-schema cube.
 *
 * <p>Worktree #1 scope: reflects the DataSource via {@link JdbcSchema},
 * which gets fact + dimension tables (with JDBC-accurate column types) for
 * free without re-implementing type introspection. Snowflakes,
 * {@code <InlineTable>}, degenerate dimensions, and Mondrian schema-XML
 * driven type overrides land in later worktrees.
 *
 * <p>Used by {@code CalciteSqlPlanner} to obtain a {@link SchemaPlus} root
 * for {@code RelBuilder} / {@code FrameworkConfig} construction.
 */
public final class CalciteMondrianSchema {

    /** Opt-in profiling switch. Guarded by {@code -Dharness.calcite.profile=true}.
     *  When enabled, each constructor call records elapsed nanos under the
     *  {@code "CalciteMondrianSchema.ctor"} bucket in
     *  {@link CalciteProfile}. Off by default — zero overhead. */
    private static final boolean PROFILE =
        Boolean.getBoolean("harness.calcite.profile");

    private final SchemaPlus root;
    private final SchemaPlus schema;
    private final String schemaName;

    public CalciteMondrianSchema(DataSource dataSource, String schemaName) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource is null");
        }
        if (schemaName == null || schemaName.isEmpty()) {
            schemaName = "mondrian";
        }
        this.schemaName = schemaName;
        long t0 = PROFILE ? System.nanoTime() : 0L;
        this.root = Frameworks.createRootSchema(true);
        this.schema = root.add(
            schemaName,
            JdbcSchema.create(root, schemaName, dataSource, null, null));
        if (PROFILE) {
            CalciteProfile.record(
                "CalciteMondrianSchema.ctor", System.nanoTime() - t0);
        }
    }

    /** Root schema (use as entry point for RelBuilder / FrameworkConfig). */
    public SchemaPlus root() {
        return root;
    }

    /** The per-connection subschema holding the reflected JDBC tables. */
    public SchemaPlus schema() {
        return schema;
    }

    /** The name under which the JDBC subschema is registered in {@link #root}. */
    public String schemaName() {
        return schemaName;
    }
}

// End CalciteMondrianSchema.java
