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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.sql.DataSource;

/**
 * Process-wide cache of {@link CalciteSqlPlanner} instances keyed on JDBC
 * connection identity — {@code (jdbcUrl, catalog, schema, user)} probed once
 * via {@link DatabaseMetaData}.
 *
 * <p>Motivation (see {@code docs/reports/perf-investigation-y1.md}):
 * the planner embeds a {@link CalciteMondrianSchema} whose Calcite
 * {@code JdbcSchema} reflects JDBC metadata lazily on first use. On Postgres
 * each {@code DatabaseMetaData.getColumns()} round-trip costs ~1.5 s.
 * Previous call sites keyed the planner on {@code RolapStar} (or on
 * {@code DataSource} identity); Mondrian's per-query schema-cache flush
 * invalidates the {@code RolapStar}, so the warm {@code JdbcSchema} was
 * thrown away on every query. Keying on the underlying JDBC connection
 * identity means JDBC metadata reflection happens once per JVM per physical
 * database, surviving {@code RolapStar} churn.
 *
 * <p>The key probe opens one connection, reads four metadata fields, and
 * closes. That probe cost is paid once per call (not cached); the
 * {@code CalciteSqlPlanner} behind the resulting key is cached and reused.
 */
public final class CalcitePlannerCache {

    private static final ConcurrentMap<Key, CalciteSqlPlanner> CACHE =
        new ConcurrentHashMap<>();

    private CalcitePlannerCache() {
        // utility
    }

    /**
     * Returns a planner for {@code ds}, building (and caching) one if this
     * is the first call for a given JDBC identity.
     */
    public static CalciteSqlPlanner plannerFor(DataSource ds) {
        Key key = Key.from(ds);
        return CACHE.computeIfAbsent(key, k -> build(ds));
    }

    private static CalciteSqlPlanner build(DataSource ds) {
        CalciteMondrianSchema schema =
            new CalciteMondrianSchema(ds, "mondrian");
        return new CalciteSqlPlanner(
            schema, CalciteDialectMap.forDataSource(ds));
    }

    /** Test seam: drop every cached planner. */
    public static void clear() {
        CACHE.clear();
    }

    /** Test seam: how many planners are currently cached. */
    public static int size() {
        return CACHE.size();
    }

    /**
     * JDBC identity — {@code (url, catalog, schema, user)} probed from
     * {@link DatabaseMetaData}. If any probe field is unavailable (driver
     * returns {@code null} or throws) we substitute an empty string, so the
     * key still hashes deterministically. The {@link DataSource} reference
     * itself is <strong>not</strong> part of the key — intentionally, so
     * that two {@code DataSource} wrappers pointing at the same physical
     * database share a planner and hence a warm schema.
     */
    static final class Key {
        final String url;
        final String catalog;
        final String schema;
        final String user;

        private Key(String url, String catalog, String schema, String user) {
            this.url = url == null ? "" : url;
            this.catalog = catalog == null ? "" : catalog;
            this.schema = schema == null ? "" : schema;
            this.user = user == null ? "" : user;
        }

        static Key from(DataSource ds) {
            String url = "";
            String catalog = "";
            String schema = "";
            String user = "";
            try (Connection c = ds.getConnection()) {
                DatabaseMetaData md = c.getMetaData();
                url = safeProbe(() -> md.getURL());
                user = safeProbe(() -> md.getUserName());
                catalog = safeProbe(() -> c.getCatalog());
                // Some drivers (and reflection-proxy wrappers used in the
                // test harness) don't implement getSchema() and surface an
                // AbstractMethodError through the proxy. Swallow that too.
                schema = safeProbe(() -> c.getSchema());
            } catch (SQLException e) {
                // Fallback: key on DataSource identity so we at least cache
                // within this DataSource lifetime. Should be vanishingly
                // rare — ds.getConnection() failing means nothing downstream
                // is going to work anyway.
                return new Key(
                    "ds@" + System.identityHashCode(ds), "", "", "");
            }
            return new Key(url, catalog, schema, user);
        }

        /** Functional interface for a metadata probe that may throw anything. */
        private interface Probe {
            String call() throws Throwable;
        }

        private static String safeProbe(Probe p) {
            try {
                String v = p.call();
                return v == null ? "" : v;
            } catch (Throwable t) {
                return "";
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return url.equals(k.url)
                && catalog.equals(k.catalog)
                && schema.equals(k.schema)
                && user.equals(k.user);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, catalog, schema, user);
        }

        @Override
        public String toString() {
            return "CalcitePlannerCache.Key{"
                + "url=" + url
                + ", catalog=" + catalog
                + ", schema=" + schema
                + ", user=" + user
                + "}";
        }
    }
}

// End CalcitePlannerCache.java
