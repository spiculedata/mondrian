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

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final Logger LOGGER =
        Logger.getLogger(CalciteMondrianSchema.class);

    /** Opt-in profiling switch. Guarded by {@code -Dharness.calcite.profile=true}.
     *  When enabled, each constructor call records elapsed nanos under the
     *  {@code "CalciteMondrianSchema.ctor"} bucket in
     *  {@link CalciteProfile}. Off by default — zero overhead. */
    private static final boolean PROFILE =
        Boolean.getBoolean("harness.calcite.profile");

    private final SchemaPlus root;
    private final SchemaPlus schema;
    private final String schemaName;
    private final DataSource dataSource;
    /** Lazily populated per-table row-count cache. Keyed on
     *  lower-cased table name. {@code null} means "not probed yet";
     *  a {@code Double} — potentially {@code 0.0} — means "probed". */
    private final ConcurrentHashMap<String, Double> rowCountCache =
        new ConcurrentHashMap<>();
    private volatile boolean rowCountProbeFailed;

    public CalciteMondrianSchema(DataSource dataSource, String schemaName) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource is null");
        }
        if (schemaName == null || schemaName.isEmpty()) {
            schemaName = "mondrian";
        }
        this.schemaName = schemaName;
        this.dataSource = dataSource;
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

    /**
     * Best-effort row-count lookup for a table in the backing
     * JDBC database. Probed lazily once per table name per
     * schema and cached.
     *
     * <p>Postgres: reads {@code pg_class.reltuples::bigint} — O(1),
     * approximate (last-analyze time) but within an order of
     * magnitude, which is all the Volcano cost model needs to
     * tell {@code sales_fact_1997} (86.8M) apart from
     * {@code agg_l_05_sales_fact_1997} (86k).
     *
     * <p>HSQLDB / other: falls back to {@code SELECT COUNT(*)} —
     * cheap on HSQLDB, expensive on Postgres (which is why we
     * prefer pg_class there).
     *
     * <p>Failure (table doesn't exist, driver throws,
     * schema-qualification mismatches) is swallowed and
     * {@code null} is returned; the caller should treat that
     * as "unknown, use defaults". A single probe failure is
     * remembered so subsequent calls are cheap rather than
     * re-hitting the same broken JDBC path.
     *
     * @return row count (never negative), or {@code null} if
     *   the probe couldn't produce a number.
     */
    public Double rowCount(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return null;
        }
        String key = tableName.toLowerCase(Locale.ROOT);
        Double cached = rowCountCache.get(key);
        if (cached != null) {
            // 0.0 is a valid cache entry; Double boxing keeps it
            // distinguishable from a miss.
            return cached;
        }
        if (rowCountProbeFailed) {
            return null;
        }
        Double probed = probeRowCount(tableName);
        if (probed != null) {
            rowCountCache.put(key, probed);
        }
        return probed;
    }

    /**
     * Snapshot of all row-count entries probed so far.
     * Unmodifiable; intended for diagnostic / test access.
     */
    public Map<String, Double> rowCountsSnapshot() {
        return Collections.unmodifiableMap(rowCountCache);
    }

    /** JDBC probe. Postgres-aware; falls back to {@code COUNT(*)}. */
    private Double probeRowCount(String tableName) {
        try (Connection c = dataSource.getConnection()) {
            String productName =
                c.getMetaData().getDatabaseProductName();
            if (productName != null
                && productName.toLowerCase(Locale.ROOT)
                    .contains("postgres"))
            {
                Double d = probePostgres(c, tableName);
                if (d != null) {
                    return d;
                }
                // fall through to COUNT(*) on miss
            }
            return probeCount(c, tableName);
        } catch (SQLException e) {
            rowCountProbeFailed = true;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "rowCount probe failed for " + tableName
                        + "; row-count stats disabled for this schema",
                    e);
            }
            return null;
        } catch (RuntimeException e) {
            rowCountProbeFailed = true;
            return null;
        }
    }

    private static Double probePostgres(Connection c, String tableName) {
        String sql =
            "SELECT reltuples::bigint FROM pg_class WHERE relname = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long n = rs.getLong(1);
                    if (n > 0) {
                        return (double) n;
                    }
                }
            }
        } catch (SQLException e) {
            // wrong catalog / permission issue — let the caller
            // fall through to COUNT(*).
            return null;
        }
        return null;
    }

    private static Double probeCount(Connection c, String tableName) {
        // Quote defensively — FoodMart's HSQLDB fixture stores
        // identifiers lower-case and the Calcite adapter quotes
        // them in emitted SQL; an unquoted identifier here works
        // on Postgres but fails on HSQLDB when the identifier
        // happens to collide with a reserved word. Double-quote
        // is SQL-standard and both dialects accept it.
        String sql =
            "SELECT COUNT(*) FROM \"" + tableName.replace("\"", "")
            + "\"";
        try (PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery())
        {
            if (rs.next()) {
                long n = rs.getLong(1);
                return n < 0 ? null : (double) n;
            }
        } catch (SQLException e) {
            return null;
        }
        return null;
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
