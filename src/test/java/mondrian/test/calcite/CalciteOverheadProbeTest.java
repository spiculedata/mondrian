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

import mondrian.calcite.CalciteMondrianSchema;
import mondrian.calcite.CalciteProfile;
import mondrian.calcite.MondrianBackend;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.test.FoodMartHsqldbBootstrap;
import mondrian.test.calcite.corpus.MvHitCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus;
import mondrian.test.calcite.corpus.SmokeCorpus.NamedMdx;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.Test;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Opt-in diagnostic probe that isolates per-phase costs on the Calcite
 * per-query path. Disabled by default; enable with
 * {@code -Dharness.calcite.profile=true}. Also requires
 * {@code -Dharness.runCalciteProbe=true} to actually execute (so a normal
 * 44/44 harness run skips it entirely).
 *
 * <p>Split of work:
 * <ul>
 *   <li>Micro probe #1: time raw {@link CalciteMondrianSchema} construction
 *       + forced table-reflection via {@code getTable()}. This isolates
 *       the JDBC-metadata round-trips without any RelBuilder/unparse
 *       on top.</li>
 *   <li>Micro probe #2: end-to-end cold execute of a representative query
 *       via {@link FoodMartCapture#executeCold}, with the CalciteProfile
 *       buckets reset per iteration. Dumps per-phase totals after each
 *       iteration.</li>
 * </ul>
 *
 * <p>Picks {@code agg-c-year-country} (MvHit; small, pays 100% overhead) and
 * {@code crossjoin} (big; pays overhead but amortises it over large result).
 */
public class CalciteOverheadProbeTest {

    public static final String RUN_PROP = "harness.runCalciteProbe";

    @Test
    public void probe() throws Exception {
        if (!Boolean.getBoolean(RUN_PROP)) {
            return; // silent no-op on normal harness runs
        }
        if (!Boolean.getBoolean("harness.calcite.profile")) {
            throw new IllegalStateException(
                "CalciteOverheadProbeTest requires "
                + "-Dharness.calcite.profile=true so timings are recorded.");
        }
        if (HarnessBackend.current() == HarnessBackend.HSQLDB) {
            FoodMartHsqldbBootstrap.ensureExtracted();
        }

        HarnessBackend backend = HarnessBackend.current();
        MondrianBackend emitter = MondrianBackend.current();
        System.out.println(
            "[probe] backend=" + backend + " emitter=" + emitter);

        // ---------------- Micro probe #1: bare schema reflection ----------
        DataSource ds = FoodMartCapture.buildUnderlyingDataSource();
        System.out.println("[probe] --- schema-reflection micro-probe ---");
        for (int i = 0; i < 3; i++) {
            long t0 = System.nanoTime();
            CalciteMondrianSchema sch =
                new CalciteMondrianSchema(ds, "mondrian");
            long tCtor = System.nanoTime() - t0;

            long t1 = System.nanoTime();
            SchemaPlus sp = sch.schema();
            JdbcSchema jdbcSchema = sp.unwrap(JdbcSchema.class);
            // First call to tables().get(X) forces the LazyReference that
            // holds the table map — which runs DatabaseMetaData.getTables()
            // over the full schema. Subsequent gets are cheap map lookups.
            Table factTable = lookupTableLoose(
                jdbcSchema, "sales_fact_1997");
            RelDataType rowType = factTable != null
                ? factTable.getRowType(
                    new org.apache.calcite.jdbc.JavaTypeFactoryImpl())
                : null;
            long tReflectFact = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            int colCount = 0;
            for (String name : Arrays.asList(
                "product", "time_by_day", "store", "customer"))
            {
                Table tbl = lookupTableLoose(jdbcSchema, name);
                if (tbl != null) {
                    RelDataType rt = tbl.getRowType(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
                    colCount += rt.getFieldCount();
                }
            }
            long tReflectDims = System.nanoTime() - t2;

            System.out.printf(
                "[probe] iter=%d ctor=%.2fms factReflect=%.2fms "
                + "dimReflect(4tables)=%.2fms rowCols=%d dimCols=%d%n",
                i, tCtor / 1e6, tReflectFact / 1e6, tReflectDims / 1e6,
                rowType == null ? -1 : rowType.getFieldCount(), colCount);
        }

        // ---------------- Micro probe #2: end-to-end cold execute --------
        if (!emitter.isCalcite()) {
            System.out.println(
                "[probe] emitter is not calcite; skipping end-to-end "
                + "phase. Re-run with -Dmondrian.sqlemitter=calcite.");
            return;
        }

        NamedMdx smallMvHit = null;
        for (MvHitCorpus.Entry e : MvHitCorpus.entries()) {
            if ("agg-c-year-country".equals(e.mdx.name)) {
                smallMvHit = e.mdx;
                break;
            }
        }
        NamedMdx bigCrossjoin = null;
        for (NamedMdx q : SmokeCorpus.queries()) {
            if ("crossjoin".equals(q.name)) {
                bigCrossjoin = q;
                break;
            }
        }
        if (smallMvHit == null || bigCrossjoin == null) {
            throw new IllegalStateException(
                "could not locate probe queries in corpora");
        }

        MondrianProperties p = MondrianProperties.instance();
        System.setProperty(
            MondrianBackend.PROPERTY,
            emitter.name().toLowerCase(Locale.ROOT));

        // --- MvHit query: agg-c-year-country (UseAggregates=true) ---
        System.out.println(
            "[probe] --- end-to-end: agg-c-year-country (MvHit) ---");
        boolean prevRead = p.ReadAggregates.get();
        boolean prevUse = p.UseAggregates.get();
        p.ReadAggregates.set(true);
        p.UseAggregates.set(true);
        try {
            runEndToEnd(smallMvHit, 5);
        } finally {
            p.ReadAggregates.set(prevRead);
            p.UseAggregates.set(prevUse);
        }

        // --- Big query: crossjoin ---
        System.out.println(
            "[probe] --- end-to-end: crossjoin (big) ---");
        p.ReadAggregates.set(false);
        p.UseAggregates.set(false);
        runEndToEnd(bigCrossjoin, 5);
    }

    private static void runEndToEnd(NamedMdx query, int iterations) {
        // Clear the planner cache ONCE up-front, so iteration 0 pays the
        // metadata-reflection cost. After Y.2 the cache is keyed on JDBC
        // identity (not RolapStar), so a Mondrian schema flush no longer
        // evicts it — iteration 1+ should see steady-state. Clearing the
        // cache each iteration (as before) would defeat that measurement.
        SegmentLoader.clearCalcitePlannerCache();
        for (int i = 0; i < iterations; i++) {
            CalciteProfile.reset();
            long t0 = System.nanoTime();
            FoodMartCapture.CapturedRun run =
                FoodMartCapture.executeCold(query, null);
            long elapsed = System.nanoTime() - t0;
            Map<String, long[]> snap = CalciteProfile.snapshot();
            System.out.printf(
                "[probe] %s iter=%d total=%.2fms sqlCount=%d%n",
                query.name, i, elapsed / 1e6, run.executions.size());
            for (Map.Entry<String, long[]> e : snap.entrySet()) {
                long[] v = e.getValue();
                System.out.printf(
                    "[probe]     %-45s totalNs=%9d ns (%.3f ms) "
                    + "calls=%d avg=%.3fms%n",
                    e.getKey(),
                    v[0], v[0] / 1e6,
                    v[1],
                    v[1] == 0 ? 0.0 : (v[0] / 1e6) / v[1]);
            }
        }
    }

    /** Exact-then-case-insensitive table lookup via the Lookup API. */
    private static Table lookupTableLoose(
        JdbcSchema jdbcSchema, String name)
    {
        Table t = jdbcSchema.tables().get(name);
        if (t != null) {
            return t;
        }
        org.apache.calcite.schema.lookup.Named<Table> named =
            jdbcSchema.tables().getIgnoreCase(name);
        return named == null ? null : named.entity();
    }

    /** Silence the unused-import warning. */
    @SuppressWarnings("unused")
    private static List<String> x() { return java.util.Collections.emptyList(); }
}

// End CalciteOverheadProbeTest.java
