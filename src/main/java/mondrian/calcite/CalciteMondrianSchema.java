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
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    /** Synthesised + probed primary-key columns per (lower-cased) table
     *  name. Populated at construction; read by the decorating schema to
     *  answer {@link Statistic#getKeys()} / {@link Statistic#isKey(ImmutableBitSet)}. */
    private final Map<String, List<List<String>>> primaryKeys =
        new HashMap<>();
    /** Synthesised + probed foreign-key edges from child (fact/snowflake)
     *  to parent (dim). Keyed on child table name, lower-cased; each entry
     *  is a list of (child-column, parent-table, parent-column) triples. */
    private final Map<String, List<ForeignKey>> foreignKeys =
        new HashMap<>();

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

        // 1) Reflect the JDBC DataSource as a JdbcSchema — keeps
        //    JdbcTableScan / JdbcTable-aware SQL generation intact.
        JdbcSchema jdbc =
            JdbcSchema.create(root, schemaName, dataSource, null, null);

        // 2) Probe DatabaseMetaData for declared PK/FK. Merge with the
        //    synthesised FoodMart map (HSQLDB script has no PK/FK DDL;
        //    Postgres fixture does). Keys stored as
        //    (table -> list of unique key column-lists).
        probeAndSynthesiseConstraints(dataSource, jdbc);

        // 3) Register a decorating schema that delegates to the JdbcSchema
        //    but overrides Table.getStatistic() so the MaterializedView
        //    rule's Goldstein-Larson duplication check can see PK/FK
        //    metadata via RelMetadataQuery / RelMdColumnUniqueness.
        this.schema =
            root.add(schemaName, new DecoratingJdbcSchema(jdbc, schemaName));
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

    /** Primary-key column lists for {@code tableName}, or an empty list if
     *  no keys are known. Exposed for diagnostics / tests. */
    public List<List<String>> primaryKeys(String tableName) {
        if (tableName == null) {
            return Collections.emptyList();
        }
        List<List<String>> pks =
            primaryKeys.get(tableName.toLowerCase(Locale.ROOT));
        return pks == null ? Collections.<List<String>>emptyList() : pks;
    }

    /** FK edges declared for {@code tableName} (child -> parent). */
    public List<ForeignKey> foreignKeys(String tableName) {
        if (tableName == null) {
            return Collections.emptyList();
        }
        List<ForeignKey> fks =
            foreignKeys.get(tableName.toLowerCase(Locale.ROOT));
        return fks == null ? Collections.<ForeignKey>emptyList() : fks;
    }

    /** Diagnostic snapshot of all constraints surfaced to Calcite. */
    public Map<String, List<List<String>>> primaryKeysSnapshot() {
        return Collections.unmodifiableMap(primaryKeys);
    }
    public Map<String, List<ForeignKey>> foreignKeysSnapshot() {
        return Collections.unmodifiableMap(foreignKeys);
    }

    // ------------------------------------------------------------------
    // Constraint probing + synthesis
    // ------------------------------------------------------------------

    /**
     * Populate {@link #primaryKeys} and {@link #foreignKeys} by:
     * (1) probing {@link DatabaseMetaData#getPrimaryKeys} /
     *     {@code getImportedKeys} for every reflected table, then
     * (2) falling back to a hard-coded FoodMart synthesis map for any
     *     table missing declared keys.
     *
     * <p>HSQLDB's FoodMart fixture declares no PK/FK DDL, so synthesis is
     * the primary path there. Postgres FoodMart has the FKs declared, so
     * JDBC probing produces identical output to synthesis and
     * confirms the canonical column names.
     */
    private void probeAndSynthesiseConstraints(
        DataSource ds,
        JdbcSchema jdbc)
    {
        Set<String> reflectedTables = Collections.emptySet();
        try {
            reflectedTables =
                jdbc.tables().getNames(LikePattern.any());
        } catch (RuntimeException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("tables() enumeration failed", e);
            }
        }
        // Probe via DatabaseMetaData.
        try (Connection c = ds.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            String cat = safeCatalog(c);
            String sch = safeSchema(c);
            for (String tbl : reflectedTables) {
                List<String> pkCols = probePrimaryKeyColumns(md, cat, sch, tbl);
                if (!pkCols.isEmpty()) {
                    primaryKeys
                        .computeIfAbsent(
                            tbl.toLowerCase(Locale.ROOT),
                            k -> new ArrayList<>())
                        .add(pkCols);
                }
                List<ForeignKey> fks =
                    probeImportedKeys(md, cat, sch, tbl);
                if (!fks.isEmpty()) {
                    foreignKeys.put(tbl.toLowerCase(Locale.ROOT), fks);
                }
            }
        } catch (SQLException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("DatabaseMetaData probe failed", e);
            }
        }
        // Synthesise any missing FoodMart constraints.
        synthesiseFoodMartConstraints();
    }

    /** JDBC 4.1-or-older-HSQLDB-safe catalog lookup. Old hsqldb jars
     *  don't implement {@link Connection#getCatalog()} /
     *  {@link Connection#getSchema()} and throw AbstractMethodError. */
    private static String safeCatalog(Connection c) {
        try {
            return c.getCatalog();
        } catch (SQLException | AbstractMethodError e) {
            return null;
        }
    }
    private static String safeSchema(Connection c) {
        try {
            return c.getSchema();
        } catch (SQLException | AbstractMethodError e) {
            return null;
        }
    }

    private static List<String> probePrimaryKeyColumns(
        DatabaseMetaData md, String cat, String sch, String tbl)
    {
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(cat, sch, tbl)) {
            // (key_seq, col_name) tuples.
            Map<Short, String> byPos = new LinkedHashMap<>();
            while (rs.next()) {
                byPos.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
            for (int i = 1; i <= byPos.size(); i++) {
                String col = byPos.get((short) i);
                if (col != null) {
                    cols.add(col);
                }
            }
        } catch (SQLException e) {
            // swallow — fall through to synthesis.
        }
        return cols;
    }

    private static List<ForeignKey> probeImportedKeys(
        DatabaseMetaData md, String cat, String sch, String tbl)
    {
        List<ForeignKey> out = new ArrayList<>();
        try (ResultSet rs = md.getImportedKeys(cat, sch, tbl)) {
            while (rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkCol = rs.getString("PKCOLUMN_NAME");
                String fkCol = rs.getString("FKCOLUMN_NAME");
                if (pkTable == null || pkCol == null || fkCol == null) {
                    continue;
                }
                out.add(new ForeignKey(fkCol, pkTable, pkCol));
            }
        } catch (SQLException e) {
            // swallow — fall through to synthesis.
        }
        return out;
    }

    /**
     * Hard-coded FoodMart PK/FK map. Only populates tables not already
     * surfaced by JDBC probing — on Postgres, declared FKs win;
     * on HSQLDB, this is the sole source of truth.
     *
     * <p>Derived from the FoodMart HSQLDB script + the canonical
     * Mondrian schema-XML — see the hard-coded expectations in the
     * task brief.
     */
    private void synthesiseFoodMartConstraints() {
        // (tableName, pkColumn-list) entries.
        Object[][] pks = new Object[][] {
            {"customer",           single("customer_id")},
            {"product",            single("product_id")},
            {"product_class",      single("product_class_id")},
            {"time_by_day",        single("time_id")},
            {"store",              single("store_id")},
            {"promotion",          single("promotion_id")},
            {"warehouse",          single("warehouse_id")},
            {"warehouse_class",    single("warehouse_class_id")},
            {"region",             single("region_id")},
            {"employee",           single("employee_id")},
            {"position",           single("position_id")},
            {"department",         single("department_id")},
            {"account",            single("account_id")},
            {"currency",           asList("currency_id", "date")},
            // Aggregate tables: the full group-key column list is
            // the natural (unique) key, by agg-loader invariants.
            {"agg_c_14_sales_fact_1997",
                asList("product_id", "customer_id", "store_id",
                    "promotion_id", "month_of_year",
                    "quarter", "the_year")},
            {"agg_c_10_sales_fact_1997",
                asList("month_of_year", "quarter", "the_year")},
            {"agg_g_ms_pcat_sales_fact_1997",
                asList("gender", "marital_status", "product_family",
                    "product_department", "product_category",
                    "product_subcategory", "the_year", "quarter",
                    "month_of_year")},
            {"agg_lc_100_sales_fact_1997",
                asList("product_id", "customer_id", "quarter", "the_year")},
            {"agg_l_03_sales_fact_1997",
                asList("time_id", "customer_id")},
            {"agg_l_04_sales_fact_1997", single("time_id")},
            {"agg_ll_01_sales_fact_1997",
                asList("product_id", "time_id", "customer_id")},
            {"agg_lc_06_sales_fact_1997",
                asList("time_id", "city", "state_province", "country")},
        };
        for (Object[] entry : pks) {
            String table = ((String) entry[0]).toLowerCase(Locale.ROOT);
            @SuppressWarnings("unchecked")
            List<String> cols = (List<String>) entry[1];
            if (!primaryKeys.containsKey(table)) {
                List<List<String>> keyList = new ArrayList<>();
                keyList.add(cols);
                primaryKeys.put(table, keyList);
            }
        }
        // Fact -> dim FKs.
        Object[][] factFks = new Object[][] {
            {"customer_id", "customer", "customer_id"},
            {"product_id", "product", "product_id"},
            {"time_id", "time_by_day", "time_id"},
            {"store_id", "store", "store_id"},
            {"promotion_id", "promotion", "promotion_id"},
        };
        String[] factTables = {
            "sales_fact_1997", "sales_fact_1998", "sales_fact_dec_1998",
        };
        for (String ft : factTables) {
            if (!foreignKeys.containsKey(ft)) {
                List<ForeignKey> fks = new ArrayList<>();
                for (Object[] fk : factFks) {
                    fks.add(new ForeignKey(
                        (String) fk[0], (String) fk[1], (String) fk[2]));
                }
                foreignKeys.put(ft, fks);
            }
        }
        // product.product_class_id -> product_class.product_class_id
        foreignKeys.computeIfAbsent("product", k -> new ArrayList<>());
        if (foreignKeys.get("product").isEmpty()) {
            foreignKeys.get("product").add(
                new ForeignKey(
                    "product_class_id", "product_class", "product_class_id"));
        }
    }

    private static List<String> single(String col) {
        return Collections.singletonList(col);
    }
    private static List<String> asList(String... cols) {
        return Arrays.asList(cols);
    }

    /** One foreign-key edge from a child table to a parent table.
     *  Simplified single-column case is all FoodMart uses. */
    public static final class ForeignKey {
        public final String childColumn;
        public final String parentTable;
        public final String parentColumn;
        public ForeignKey(
            String childColumn, String parentTable, String parentColumn)
        {
            this.childColumn = childColumn;
            this.parentTable = parentTable;
            this.parentColumn = parentColumn;
        }
    }

    // ------------------------------------------------------------------
    // Decorating Schema + Table: attaches PK/FK/rowCount to JdbcTable.
    // ------------------------------------------------------------------

    /**
     * Schema that delegates every operation to an inner {@link JdbcSchema}
     * but decorates each returned {@link Table} with a
     * {@link StatisticTable} wrapper whose {@link Table#getStatistic()}
     * surfaces probed / synthesised PK + FK metadata.
     *
     * <p>Keeps the JdbcSchema's convention / SQL generation path intact:
     * {@link StatisticTable#toRel} delegates to the inner {@link JdbcTable}.
     */
    private final class DecoratingJdbcSchema extends AbstractSchema
        implements Wrapper
    {
        private final JdbcSchema delegate;
        private final String registeredName;
        DecoratingJdbcSchema(JdbcSchema delegate, String registeredName) {
            this.delegate = delegate;
            this.registeredName = registeredName;
        }
        @Override public Lookup<Table> tables() {
            return delegate.tables()
                .map((t, name) -> decorate(t, name));
        }
        @Override public Lookup<? extends Schema> subSchemas() {
            return delegate.subSchemas();
        }
        @Override public boolean isMutable() {
            return delegate.isMutable();
        }
        @Override public Schema snapshot(SchemaVersion version) {
            return this;
        }
        @Override protected Map<String, Table> getTableMap() {
            // Only used by the deprecated getTable(name) / getTableNames()
            // accessors in AbstractSchema. The tables() Lookup is the
            // live path — precompute a small snapshot for correctness.
            Map<String, Table> out = new LinkedHashMap<>();
            for (String name
                : delegate.tables().getNames(LikePattern.any()))
            {
                Table t = delegate.tables().get(name);
                if (t != null) {
                    out.put(name, decorate(t, name));
                }
            }
            return out;
        }
        @Override public <T> T unwrap(Class<T> clazz) {
            if (clazz.isInstance(this)) {
                return clazz.cast(this);
            }
            if (clazz.isInstance(delegate)) {
                return clazz.cast(delegate);
            }
            if (clazz == DataSource.class) {
                return clazz.cast(delegate.getDataSource());
            }
            return null;
        }
        private Table decorate(Table t, String name) {
            if (t == null) {
                return null;
            }
            if (!(t instanceof JdbcTable)) {
                return t;
            }
            return new StatisticTable(
                (JdbcTable) t, name, registeredName);
        }
    }

    /** Table wrapper that forwards translation/scan to an inner
     *  {@link JdbcTable} but overrides {@link #getStatistic()}. */
    private final class StatisticTable
        implements TranslatableTable, Wrapper
    {
        private final JdbcTable inner;
        private final String tableName;
        private final String schemaRegisteredName;
        private volatile Statistic statisticCache;

        StatisticTable(
            JdbcTable inner, String tableName, String schemaRegisteredName)
        {
            this.inner = inner;
            this.tableName = tableName;
            this.schemaRegisteredName = schemaRegisteredName;
        }

        @Override public RelDataType getRowType(RelDataTypeFactory tf) {
            return inner.getRowType(tf);
        }
        @Override public Statistic getStatistic() {
            Statistic s = statisticCache;
            if (s != null) {
                return s;
            }
            s = buildStatistic();
            statisticCache = s;
            return s;
        }
        @Override public Schema.TableType getJdbcTableType() {
            return inner.getJdbcTableType();
        }
        @Override public boolean isRolledUp(String column) {
            return inner.isRolledUp(column);
        }
        @Override public boolean rolledUpColumnValidInsideAgg(
            String column,
            org.apache.calcite.sql.SqlCall call,
            org.apache.calcite.sql.SqlNode parent,
            org.apache.calcite.config.CalciteConnectionConfig config)
        {
            return inner.rolledUpColumnValidInsideAgg(
                column, call, parent, config);
        }
        @Override public RelNode toRel(
            RelOptTable.ToRelContext context, RelOptTable relOptTable)
        {
            // Delegate to JdbcTable.toRel — the resulting JdbcTableScan
            // carries the RelOptTable we were handed, and that RelOptTable
            // already uses our getStatistic() (RelOptTableImpl reads it
            // at construction).
            return inner.toRel(context, relOptTable);
        }
        @Override public <C> C unwrap(Class<C> aClass) {
            if (aClass.isInstance(this)) {
                return aClass.cast(this);
            }
            if (aClass.isInstance(inner)) {
                return aClass.cast(inner);
            }
            return inner.unwrap(aClass);
        }

        private Statistic buildStatistic() {
            String key = tableName.toLowerCase(Locale.ROOT);
            Double rows = rowCount(tableName);
            List<List<String>> pkColumnSets = primaryKeys
                .getOrDefault(key, Collections.<List<String>>emptyList());
            List<ForeignKey> fks = foreignKeys
                .getOrDefault(key, Collections.<ForeignKey>emptyList());
            if (pkColumnSets.isEmpty()
                && fks.isEmpty()
                && rows == null)
            {
                return Statistics.UNKNOWN;
            }
            // Resolve PK column names to field ordinals.
            RelDataType row = inner.getRowType(
                new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
                    org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT));
            List<String> fieldNames = row.getFieldNames();
            Map<String, Integer> ordinals = new HashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                ordinals.put(
                    fieldNames.get(i).toLowerCase(Locale.ROOT), i);
            }
            List<ImmutableBitSet> keys = new ArrayList<>();
            for (List<String> cols : pkColumnSets) {
                ImmutableBitSet.Builder b = ImmutableBitSet.builder();
                boolean allFound = true;
                for (String c : cols) {
                    Integer idx =
                        ordinals.get(c.toLowerCase(Locale.ROOT));
                    if (idx == null) {
                        allFound = false;
                        break;
                    }
                    b.set(idx);
                }
                if (allFound) {
                    keys.add(b.build());
                }
            }
            // Resolve FKs to RelReferentialConstraint (column ordinals on
            // both sides — child-side via our own row type, parent-side
            // via lookup against that table's row type).
            List<RelReferentialConstraint> refs = new ArrayList<>();
            for (ForeignKey fk : fks) {
                Integer childOrdinal =
                    ordinals.get(fk.childColumn.toLowerCase(Locale.ROOT));
                Integer parentOrdinal =
                    lookupColumnOrdinal(fk.parentTable, fk.parentColumn);
                if (childOrdinal == null || parentOrdinal == null) {
                    continue;
                }
                refs.add(RelReferentialConstraintImpl.of(
                    ImmutableList.of(schemaRegisteredName, tableName),
                    ImmutableList.of(schemaRegisteredName, fk.parentTable),
                    ImmutableList.of(
                        IntPair.of(childOrdinal, parentOrdinal))));
            }
            return Statistics.of(
                rows,
                keys.isEmpty() ? null : keys,
                refs.isEmpty() ? null : refs,
                null);
        }
    }

    /** Best-effort column-ordinal lookup against another table in the
     *  same JDBC schema. Used when building FK metadata. */
    private Integer lookupColumnOrdinal(String tableName, String columnName) {
        try {
            Table t = schema == null
                ? null
                : schema.tables().get(tableName);
            if (t == null) {
                return null;
            }
            RelDataType row = t.getRowType(
                new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
                    org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT));
            int i = 0;
            for (String n : row.getFieldNames()) {
                if (n.equalsIgnoreCase(columnName)) {
                    return i;
                }
                i++;
            }
        } catch (RuntimeException e) {
            // ignore — fall through.
        }
        return null;
    }
}

// End CalciteMondrianSchema.java
