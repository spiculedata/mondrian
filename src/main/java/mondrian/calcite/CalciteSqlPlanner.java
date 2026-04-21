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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectJoinRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.apache.log4j.Logger;

import mondrian.olap.Exp;
import mondrian.olap.Member;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates a {@link PlannerRequest} into a dialect-specific SQL string by
 * building a Calcite {@link RelNode} via {@link RelBuilder} and unparsing it
 * through {@link RelToSqlConverter}.
 *
 * <p>Worktree #1 feature coverage: scan, inner equi-join, equality WHERE
 * filter, SUM/COUNT/MIN/MAX/AVG aggregation, GROUP BY, ORDER BY. Nested
 * subqueries, complex predicates, DISTINCT, window functions are deferred.
 */
public final class CalciteSqlPlanner {

    private static final Logger LOGGER =
        Logger.getLogger(CalciteSqlPlanner.class);

    /** Opt-in profiling switch ({@code -Dharness.calcite.profile=true}).
     *  When enabled each phase of {@link #plan} / {@link #planRel} records
     *  elapsed nanos under {@link CalciteProfile}. Off by default — the
     *  only overhead is a single final-boolean read per entry point. */
    private static final boolean PROFILE =
        Boolean.getBoolean("harness.calcite.profile");

    /**
     * Kill switch for the {@link VolcanoPlanner} stage. Default
     * {@code true} — the stage is a no-op when no MV registry is
     * attached, so enabling it globally is safe. Set
     * {@code -Dmondrian.calcite.volcano=false} to fall back to the
     * pre-Phase-3+ pipeline (Hep only) for a production rollback
     * without a code change.
     */
    private static final boolean VOLCANO_ENABLED =
        Boolean.parseBoolean(
            System.getProperty("mondrian.calcite.volcano", "true"));

    /**
     * Calc-consume opt-in
     * ({@code -Dmondrian.calcite.calcConsume=true}).
     *
     * <p>When false (default), the calc-bearing inner projection is
     * wrapped by an outer projection that re-projects only {groupBy,
     * measures}. Hep then folds both projections into the Aggregate
     * (PROJECT_REMOVE on the outer identity, AGGREGATE_PROJECT_MERGE
     * on the inner), so the calc is absent from the emitted SQL —
     * preserving row-checksum parity for the legacy SegmentLoader.
     *
     * <p>When true, the outer wrapping is skipped. The calc-bearing
     * Project survives Hep (non-identity shape; AGGREGATE_PROJECT_MERGE
     * won't push expressions involving aggregate outputs down into
     * the Aggregate), and {@link RelToSqlConverter} emits the
     * arithmetic inline in the SELECT list. The SegmentLoader path
     * to consume the SQL-computed column lives in Task 3'.
     */
    static boolean calcConsumeEnabled() {
        return Boolean.parseBoolean(
            System.getProperty(
                "mondrian.calcite.calcConsume", "false"));
    }

    /**
     * Curated rule set for the {@link VolcanoPlanner} stage.
     *
     * <p>Limited to the {@code MaterializedView*} family plus
     * {@link MaterializedViewFilterScanRule}. Deliberately excludes
     * {@code EnumerableRules.*} — the {@link RelToSqlConverter} only
     * unparses logical/JDBC conventions, so introducing Enumerable
     * nodes produces unparseable trees. Also excludes join-reorder
     * rules for the same reason the Hep stage does (no stats ⇒
     * Volcano guesses, which can pick bad plans).
     *
     * <p>The {@code MaterializedViewProject*} rules match a
     * {@code Project} above the target shape; the {@code
     * MaterializedViewOnly*} rules match the bare shape. Both are
     * registered so MV rewrites fire whether Hep left an outer
     * Project in place or not.
     *
     * <p>Rule classes locked to Calcite 1.41's exports under
     * {@code org.apache.calcite.rel.rules.materialize.*}.
     */
    private static final RelOptRule[] VOLCANO_RULES = {
        MaterializedViewFilterScanRule.Config.DEFAULT.toRule(),
        MaterializedViewProjectFilterRule.Config.DEFAULT.toRule(),
        MaterializedViewOnlyFilterRule.Config.DEFAULT.toRule(),
        MaterializedViewProjectJoinRule.Config.DEFAULT.toRule(),
        MaterializedViewOnlyJoinRule.Config.DEFAULT.toRule(),
        MaterializedViewProjectAggregateRule.Config.DEFAULT.toRule(),
        MaterializedViewOnlyAggregateRule.Config.DEFAULT.toRule(),
    };

    /**
     * Curated rewrite rules run through {@link HepPlanner} before unparse.
     *
     * <p>Feature-unlock work, not perf-chasing (Phase 3 Task 9 of
     * {@code docs/plans/2026-04-21-calcite-sql-quality-and-grouping-sets.md}).
     * The ruleset stays tight on purpose — every rule here must preserve
     * row semantics AND produce nodes the JDBC {@link RelToSqlConverter}
     * can unparse. Anything that introduces {@code EnumerableConvention}
     * or rewrites the tree into shapes the unparser doesn't handle stays
     * out.
     *
     * <p>Shipped set:
     * <ul>
     *   <li>{@code FILTER_INTO_JOIN} — pushes WHERE predicates into the
     *       Join's ON clause.</li>
     *   <li>{@code JOIN_CONDITION_PUSH} — further pushdown from the Join's
     *       ON into its inputs when safe.</li>
     *   <li>{@code FILTER_MERGE} — collapses adjacent Filters.</li>
     *   <li>{@code PROJECT_MERGE} — collapses adjacent Projects.</li>
     *   <li>{@code PROJECT_REMOVE} — drops tautological identity
     *       Projects.</li>
     *   <li>{@code AGGREGATE_PROJECT_MERGE} — merges an Aggregate with a
     *       Project beneath it when column refs line up.</li>
     * </ul>
     *
     * <p>Deliberately excluded: {@code EnumerableRules.*} (non-JDBC
     * convention), join-reorder rules (T11 wants cost-tuned ordering),
     * {@code AGGREGATE_UNION_TRANSPOSE} and similar semantics-changing
     * rewrites.
     */
    private static final RelOptRule[] CURATED_RULES = {
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.FILTER_MERGE,
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_REMOVE,
        CoreRules.AGGREGATE_PROJECT_MERGE,
    };

    private static HepProgram buildHepProgram() {
        HepProgramBuilder b = new HepProgramBuilder();
        for (RelOptRule r : CURATED_RULES) {
            b.addRuleInstance(r);
        }
        return b.build();
    }

    /** Built once; {@link HepPlanner} instances are constructed per
     *  optimise() call since HepPlanner is not thread-safe. */
    private static final HepProgram HEP_PROGRAM = buildHepProgram();

    private final CalciteMondrianSchema schema;
    private final SqlDialect dialect;
    private volatile MvRegistry mvRegistry;

    public CalciteSqlPlanner(
        CalciteMondrianSchema schema, SqlDialect dialect)
    {
        if (schema == null) {
            throw new IllegalArgumentException("schema is null");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("dialect is null");
        }
        this.schema = schema;
        this.dialect = dialect;
    }

    /**
     * Attach an {@link MvRegistry} to this planner. When non-empty,
     * its materializations are registered on each {@code plan()} call's
     * {@link VolcanoPlanner} stage so the {@code MaterializedView*}
     * rule family can rewrite matching queries onto the declared agg
     * table. The Hep stage is unchanged.
     *
     * <p>Attaching an empty or null registry is a no-op — the Volcano
     * stage short-circuits and the emitted SQL matches the Hep output.
     *
     * <p>The Volcano stage is kill-switched via
     * {@code -Dmondrian.calcite.volcano=false}, which makes this
     * attach a no-op regardless of the registry's contents.
     */
    public void attachMvRegistry(MvRegistry registry) {
        this.mvRegistry = registry;
    }

    /** @return the attached MV registry, or {@code null} if none. */
    public MvRegistry mvRegistry() {
        return mvRegistry;
    }

    /** @return the {@link CalciteMondrianSchema} used by this planner. */
    public CalciteMondrianSchema schema() {
        return schema;
    }

    // ------------------------------------------------------------------
    // Plan-snapshot capture (harness hook).
    //
    // When a thread calls beginCapture() every subsequent plan() on that
    // thread appends RelOptUtil.toString(rel) to a ThreadLocal list. The
    // EquivalenceHarness drains the list after each run to feed the
    // PLAN_DRIFT gate (see docs/plans/2026-04-19-calcite-plan-as-lingua-
    // franca-design.md §Harness evolution). Zero overhead when not
    // capturing: plan() only performs an int != null check.
    // ------------------------------------------------------------------

    private static final ThreadLocal<java.util.List<String>> CAPTURE =
        new ThreadLocal<java.util.List<String>>();

    /** Start accumulating plan snapshots on the current thread. */
    public static void beginCapture() {
        CAPTURE.set(new java.util.ArrayList<String>());
    }

    /**
     * Stop capturing on the current thread and return the snapshots that
     * were recorded, in call order. Returns an empty list if
     * {@link #beginCapture()} was not called.
     */
    public static java.util.List<String> endCapture() {
        java.util.List<String> out = CAPTURE.get();
        CAPTURE.remove();
        if (out == null) {
            return java.util.Collections.emptyList();
        }
        return out;
    }

    /** Render the request as a SQL string in the configured dialect. */
    public String plan(PlannerRequest req) {
        long tPlanStart = PROFILE ? System.nanoTime() : 0L;
        // Hand-rolled MV matcher (Option D —
        // docs/reports/perf-investigation-volcano-mv-win.md).
        // Runs before planRel so the downstream Hep/Volcano stages
        // see the simpler (already-rewritten) request. Bypasses
        // Calcite's SubstitutionVisitor entirely.
        //
        // ON BY DEFAULT post-VP-E. The row-order determinism fix
        // (MvMatcher emits ORDER BY groupBy when none was given)
        // eliminated the time-fn / parallelperiod LEGACY_DRIFT that
        // originally kept the matcher off. Kill switch:
        // -Dmondrian.calcite.mvMatch=false for emergency rollback.
        MvRegistry reg = mvRegistry;
        if (reg != null && reg.size() > 0
            && Boolean.parseBoolean(
                System.getProperty(
                    "mondrian.calcite.mvMatch", "true")))
        {
            try {
                PlannerRequest rewritten = MvMatcher.tryRewrite(req, reg);
                if (rewritten != req) {
                    req = rewritten;
                }
            } catch (RuntimeException re) {
                // Belt-and-braces: any matcher surprise falls back
                // to the original request. Never fail plan() due to
                // an MV-path issue.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "MvMatcher.tryRewrite threw; "
                        + "falling back to unrewritten request", re);
                }
            }
        }
        RelNode rel = planRel(req);
        long tOptStart = PROFILE ? System.nanoTime() : 0L;
        RelNode optimized = optimize(rel);
        if (PROFILE) {
            CalciteProfile.record(
                "CalciteSqlPlanner.hep", System.nanoTime() - tOptStart);
        }
        // Phase 3+ Volcano stage: MV rewrites when a registry is
        // attached. Graceful: on any failure, fall back to the Hep
        // output. On no-op conditions (kill switch off, empty
        // registry) returns the input untouched.
        long tVolcanoStart = PROFILE ? System.nanoTime() : 0L;
        optimized = runVolcano(optimized);
        if (PROFILE) {
            CalciteProfile.record(
                "CalciteSqlPlanner.volcano",
                System.nanoTime() - tVolcanoStart);
        }
        java.util.List<String> sink = CAPTURE.get();
        if (sink != null) {
            sink.add(normalisePlan(RelOptUtil.toString(optimized)));
        }
        long tUnparseStart = PROFILE ? System.nanoTime() : 0L;
        SqlNode sqlNode =
            new RelToSqlConverter(dialect).visitRoot(optimized).asStatement();
        String out = sqlNode.toSqlString(dialect).getSql();
        if (PROFILE) {
            long now = System.nanoTime();
            CalciteProfile.record(
                "CalciteSqlPlanner.unparse", now - tUnparseStart);
            CalciteProfile.record(
                "CalciteSqlPlanner.plan.total", now - tPlanStart);
        }
        return out;
    }

    /**
     * Run the curated {@link HepPlanner} program over {@code raw}.
     *
     * <p>A fresh {@link HepPlanner} is constructed per call — HepPlanner
     * instances hold mutable state (rule-match queue, DAG) and are not
     * safe to reuse across threads. The shared {@link HepProgram} is
     * immutable and cheap.
     *
     * <p>Returns {@code raw} unchanged if Hep fails to register / run.
     * Failure here should never happen given the rule set is JDBC-safe
     * by construction; the belt-and-braces fallback is only here so a
     * future rule surprise degrades to the pre-Phase-3 behaviour rather
     * than breaking every query.
     */
    RelNode optimize(RelNode raw) {
        try {
            HepPlanner hep = new HepPlanner(HEP_PROGRAM);
            hep.setRoot(raw);
            return hep.findBestExp();
        } catch (RuntimeException re) {
            // Intentional: never fail plan() because of an optimiser
            // surprise. Fall back to the unoptimised tree; the emitted
            // SQL stays correct, just less tidy.
            return raw;
        }
    }

    /**
     * Cost-based {@link VolcanoPlanner} stage run after Hep.
     *
     * <p>Primary job: feed the attached {@link MvRegistry}'s
     * materializations to Calcite's {@code MaterializedView*} rule
     * family so queries that match a declared aggregate
     * {@code MeasureGroup} can rewrite to scan the agg table
     * directly. This is Task U's original design goal — HepPlanner
     * is fixpoint-driven and cannot pick between alternatives on
     * cost, so the MV rule only fires meaningfully inside a
     * cost-based stage.
     *
     * <p>Guardrails:
     * <ul>
     *   <li>No-op when the kill switch
     *       ({@code -Dmondrian.calcite.volcano=false}) is set,
     *       when no MV registry is attached, or when the registry
     *       is empty. In those cases the method returns
     *       {@code input} unchanged with no planner construction.</li>
     *   <li>Only logical / transformation rules are registered —
     *       {@code EnumerableRules.*} are excluded because the
     *       subsequent {@link RelToSqlConverter} only unparses
     *       logical / JDBC conventions.</li>
     *   <li>Any {@link RuntimeException} from {@code findBestExp}
     *       is caught and logged; the method returns {@code input}
     *       (the Hep output) so plan() never fails due to a
     *       Volcano surprise. This is the belt-and-braces fallback
     *       called out in the Y.4 plan — if a rule mis-configures
     *       traits or the MV rule rejects the shape of our
     *       defining query, we degrade to the pre-Phase-3+ output
     *       rather than breaking 44 queries to enable MV on 4.</li>
     * </ul>
     *
     * <p><b>Caveat on trait wiring.</b> The {@code RelBuilder}
     * output is in {@link org.apache.calcite.plan.Convention#NONE},
     * and we preserve that trait set through the Volcano stage —
     * {@code RelToSqlConverter} is happy to unparse the
     * {@code Convention.NONE} logical tree. No explicit conversion
     * rules are added; if a registered MV rule fires, it produces
     * logical-convention nodes (TableScan / Project over the agg
     * table) that the unparser handles natively.
     */
    RelNode runVolcano(RelNode input) {
        if (!VOLCANO_ENABLED) {
            return input;
        }
        MvRegistry reg = mvRegistry;
        if (reg == null || reg.size() == 0) {
            return input;
        }
        // Fast-path MV substitution: Calcite's
        // {@link org.apache.calcite.plan.RelOptMaterializations#useMaterializedViews}
        // returns a list of (substituted-rel, used-MVs) pairs driven by
        // {@link org.apache.calcite.plan.SubstitutionVisitor}. When we
        // surface PK/FK metadata via CalciteMondrianSchema, the
        // shape-aware MvRegistry shapes unify for the MvHit corpus and
        // Volcano's subsequent cost comparison often keeps both paths
        // (same-order-of-magnitude rowcounts in the HSQLDB fixture
        // leave the MV path without a decisive cost advantage). We take
        // the first successful substitution as a definitive rewrite —
        // the MV registry is curated by shape, so any match is a valid
        // rewrite. Volcano then runs over the substituted tree for any
        // further cleanup.
        RelNode startingPoint = input;
        boolean mvRewritten = false;
        try {
            // Direct SubstitutionVisitor path — skip Calcite's wrapper
            // useMaterializedViews (which runs its own Hep program over
            // both sides, converting Project/Filter to Calc and
            // breaking the exact-shape match our MvRegistry was built
            // to satisfy). Per-MV SubstitutionVisitor.go(tableRel)
            // returns a list of substituted rels; we take the first
            // success, which guarantees a shape-matched rewrite onto
            // the MV's agg-table scan.
            for (RelOptMaterialization mv : reg.materializations()) {
                java.util.List<RelNode> subs =
                    new org.apache.calcite.plan.SubstitutionVisitor(
                        mv.queryRel, input)
                        .go(mv.tableRel);
                if (subs != null && !subs.isEmpty()) {
                    startingPoint = subs.get(0);
                    mvRewritten = true;
                    break;
                }
            }
        } catch (RuntimeException ex) {
            startingPoint = input;
        } catch (AssertionError ex) {
            startingPoint = input;
        }
        // If we got a definitive shape-aware rewrite, skip the
        // VolcanoPlanner stage — Volcano would re-explore the rewrite's
        // children and its cost model has no decisive preference
        // between 87k-row fact-join and 87k-row agg-scan on the HSQLDB
        // fixture; re-entering Volcano causes it to re-pick the
        // original tree. Running a minimal Hep cleanup is enough to
        // tidy residual Project/Calc nodes the substitution emits.
        if (mvRewritten) {
            try {
                HepPlanner hep = new HepPlanner(HEP_PROGRAM);
                hep.setRoot(startingPoint);
                return hep.findBestExp();
            } catch (RuntimeException ex) {
                return startingPoint;
            }
        }
        try {
            // MaterializedView rules only run when the planner's
            // context contains a CalciteConnectionConfig with
            // materializationsEnabled=true. The default no-arg
            // VolcanoPlanner has an empty context; without an
            // explicit config, registerMaterializations() short-
            // circuits and no MV rule ever fires.
            java.util.Properties props = new java.util.Properties();
            props.setProperty(
                org.apache.calcite.config.CalciteConnectionProperty
                    .MATERIALIZATIONS_ENABLED.camelName(), "true");
            org.apache.calcite.config.CalciteConnectionConfig connConfig =
                new org.apache.calcite.config.CalciteConnectionConfigImpl(
                    props);
            org.apache.calcite.plan.Context planCtx =
                org.apache.calcite.plan.Contexts.of(connConfig);
            VolcanoPlanner planner = new VolcanoPlanner(planCtx);
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            // Core abstract-relational equivalences; safe w.r.t.
            // JDBC unparse (no Enumerable conversion).
            planner.registerAbstractRelationalRules();
            for (RelOptRule r : VOLCANO_RULES) {
                planner.addRule(r);
            }
            for (RelOptMaterialization mv : reg.materializations()) {
                planner.addMaterialization(mv);
            }
            // Preserve the input's trait set: RelBuilder output is
            // Convention.NONE, which is exactly what RelToSqlConverter
            // consumes. Requesting a different convention would need
            // explicit converter rules we don't want in this stage.
            //
            // NOTE: {@link VolcanoPlanner#changeTraits} asserts that the
            // requested traits differ from the current ones, so we only
            // call it if a trait change is actually wanted. For the
            // same-trait case (our case), setRoot directly accepts the
            // input; Volcano registers it internally.
            RelTraitSet target = startingPoint.getTraitSet();
            RelNode rooted = startingPoint.getTraitSet().equals(target)
                ? startingPoint
                : planner.changeTraits(startingPoint, target);
            planner.setRoot(rooted);
            return planner.findBestExp();
        } catch (RuntimeException ex) {
            return handleVolcanoFailure(input, ex);
        } catch (Error ex) {
            // Calcite's internal invariants (trait consistency,
            // cross-cluster checks) use bare AssertionError rather
            // than a RuntimeException. Treat them the same way —
            // degrade to Hep output rather than surface the assert.
            if (ex instanceof AssertionError
                || ex instanceof StackOverflowError)
            {
                return handleVolcanoFailure(input, ex);
            }
            throw ex;
        }
    }

    private RelNode handleVolcanoFailure(RelNode input, Throwable ex) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "VolcanoPlanner failed; falling back to Hep output", ex);
        } else {
            LOGGER.warn(
                "VolcanoPlanner failed (" + ex.getClass().getSimpleName()
                + "); falling back to Hep output: " + ex.getMessage());
        }
        return input;
    }

    /**
     * Normalise a {@code RelOptUtil.toString} rendering so goldens are
     * deterministic across JVM runs. Calcite's toString includes a per-rel
     * numeric suffix on the outer rel name (e.g. {@code LogicalProject_5})
     * derived from a cluster-scoped counter; stripped here so a new plan
     * with the same shape produces identical text. Safe because structural
     * changes (different rels, different field lists, different
     * expressions) all alter the rendering elsewhere in the tree.
     */
    private static String normalisePlan(String raw) {
        // Strip "_<digits>" that appears directly after a rel class name —
        // only inside the first token on each line, i.e. between the first
        // run of letters and the opening parenthesis.
        StringBuilder out = new StringBuilder(raw.length());
        int i = 0;
        int n = raw.length();
        while (i < n) {
            char c = raw.charAt(i);
            if (c == '_' && i + 1 < n
                && Character.isDigit(raw.charAt(i + 1)))
            {
                // Look back: preceding char should be a letter (rel-name
                // suffix). Otherwise keep as-is (don't munge literal
                // identifiers like column names).
                int back = out.length() - 1;
                if (back >= 0 && Character.isLetter(out.charAt(back))) {
                    int j = i + 1;
                    while (j < n && Character.isDigit(raw.charAt(j))) {
                        j++;
                    }
                    // Only strip when followed by '(' (the rel's opening
                    // paren) — that's the positive signature of a rel
                    // name, not e.g. an alias token.
                    if (j < n && raw.charAt(j) == '(') {
                        i = j;
                        continue;
                    }
                }
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /** Build the Calcite {@link RelNode} (used by plan-snapshot tests). */
    public RelNode planRel(PlannerRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("request is null");
        }
        long tCfg = PROFILE ? System.nanoTime() : 0L;
        FrameworkConfig cfg = Frameworks.newConfigBuilder()
            .defaultSchema(schema.schema())
            .build();
        RelBuilder b = RelBuilder.create(cfg);
        long tBuild = PROFILE ? System.nanoTime() : 0L;
        RelNode out = build(b, req);
        if (PROFILE) {
            long now = System.nanoTime();
            CalciteProfile.record(
                "CalciteSqlPlanner.relBuilderCreate", tBuild - tCfg);
            CalciteProfile.record(
                "CalciteSqlPlanner.build", now - tBuild);
        }
        return out;
    }

    private RelNode build(RelBuilder b, PlannerRequest req) {
        b.scan(req.factTable);

        for (PlannerRequest.Join j : req.joins) {
            b.scan(j.dimTable);
            if (j.kind == PlannerRequest.JoinKind.CROSS) {
                // Unconditional cross-join: RelBuilder has no native
                // cartesian helper, so emit an INNER join on TRUE. Calcite's
                // unparser renders that as a CROSS JOIN in most dialects.
                b.join(JoinRelType.INNER, b.literal(true));
            } else {
                // For single-hop joins (leftTable == null), the LHS is the
                // fact table; unqualified field(2,0,name) resolves the FK
                // column in the flat LHS row-type.
                //
                // For snowflake multi-hop joins (leftTable != null), the
                // LHS is an already-joined chain of tables. Use the
                // alias-qualified field() overload so the FK column is
                // unambiguously resolved against the LHS's named input —
                // critical when the same column name (e.g. product_class_id)
                // appears on more than one table in the chain.
                org.apache.calcite.rex.RexNode lhs;
                if (j.leftTable == null) {
                    lhs = b.field(2, 0, j.factKey);
                } else {
                    lhs = b.field(2, j.leftTable, j.factKey);
                }
                org.apache.calcite.rex.RexNode rhs =
                    b.field(2, j.dimTable, j.dimKey);
                b.join(JoinRelType.INNER, b.equals(lhs, rhs));
            }
        }

        if (req.universalFalse) {
            b.filter(b.literal(false));
        } else {
            for (PlannerRequest.Filter f : req.filters) {
                b.filter(filterRex(b, f));
            }
            for (PlannerRequest.TupleFilter tf : req.tupleFilters) {
                b.filter(tupleFilterRex(b, tf));
            }
        }

        if (req.isAggregation()) {
            List<RexNode> keys = new ArrayList<>();
            for (PlannerRequest.Column c : req.groupBy) {
                keys.add(fieldRef(b, c));
            }
            List<RelBuilder.AggCall> aggs = new ArrayList<>();
            for (PlannerRequest.Measure m : req.measures) {
                aggs.add(aggCall(b, m));
            }
            // HAVING predicates ride along in the aggregate so their
            // measure alias is resolvable by the subsequent filter()
            // call. Distinct aliases are guaranteed by the builder —
            // user measures already have stable aliases, and HAVING
            // translation in CalcitePlannerAdapters emits h0..hN.
            // Measures that pre-exist the aggregate by matching
            // fn+column+distinct+alias are NOT deduped; the
            // post-aggregate reproject drops the HAVING-only ones
            // by name, preserving the user's SELECT layout.
            for (PlannerRequest.Having h : req.havings) {
                aggs.add(aggCall(b, h.measure));
            }
            b.aggregate(b.groupKey(keys), aggs);

            // HAVING: filter on the aggregate's output columns. Calcite
            // recognises a Filter immediately above an Aggregate whose
            // predicate references aggregate-output columns and unparses
            // it as a HAVING clause.
            for (PlannerRequest.Having h : req.havings) {
                b.filter(havingRex(b, h));
            }

            // Calcite's Aggregate normalises the group set to an
            // ImmutableBitSet, which re-orders group columns into the
            // input-row's column-ordinal order. That means the emitted
            // SELECT list's group-by prefix may not match the order we
            // passed to groupKey(). Mondrian's segment consumer positionally
            // maps SELECT columns onto GroupingSet.columns[i], so a
            // reordered SELECT assigns axis values to the wrong column and
            // cell lookups miss. Re-project to force the intended order:
            // groupBy columns in request order, followed by measures in
            // request order. HAVING-only measures (h0..hN) are dropped
            // from the final SELECT so they don't leak out.
            List<RexNode> restored = new ArrayList<>(
                req.groupBy.size() + req.measures.size());
            List<String> restoredAliases = new ArrayList<>(
                req.groupBy.size() + req.measures.size());
            for (PlannerRequest.Column c : req.groupBy) {
                restored.add(b.field(c.name));
                restoredAliases.add(c.name);
            }
            for (PlannerRequest.Measure m : req.measures) {
                restored.add(b.field(m.alias));
                restoredAliases.add(m.alias);
            }
            // Append pushed-calc projections as extra columns. They
            // reference the base-measure aliases (also in `restored`)
            // via the ComputedMeasure's baseMeasureAliases map.
            //
            // Task T.1: the calc is rendered alongside {groupBy,
            // measures} in the aggregate's output projection. A second
            // outer project then drops the calc aliases so the result
            // set seen by SegmentLoader stays shape-compatible with
            // the legacy path (row checksum parity in the equivalence
            // harness). We use "force=true" to stop RelBuilder from
            // collapsing the outer project into the inner one — the
            // inner projection's extra columns then survive into the
            // unparsed SQL as observational evidence that pushdown
            // fired, even though the outer select drops them.
            boolean hasComputed = !req.computedMeasures.isEmpty();
            int innerGroupAndMeasureCount = restored.size();
            for (PlannerRequest.ComputedMeasure cm : req.computedMeasures) {
                Map<Member, RexNode> refs = new HashMap<>();
                for (Map.Entry<Object, String> e
                    : cm.baseMeasureAliases.entrySet())
                {
                    refs.put((Member) e.getKey(), b.field(e.getValue()));
                }
                ArithmeticCalcTranslator tx =
                    new ArithmeticCalcTranslator(
                        b.getRexBuilder(),
                        ArithmeticCalcTranslator.mapResolver(refs));
                restored.add(tx.translate((Exp) cm.expression));
                restoredAliases.add(cm.alias);
            }
            b.project(restored, restoredAliases, true);
            if (hasComputed && !calcConsumeEnabled()) {
                // Default-off path: wrap the calc-bearing inner
                // projection in an outer project that re-projects only
                // {groupBy, measures} so SegmentLoader's row shape
                // matches the legacy path (row-checksum parity in the
                // equivalence harness).
                //
                // Under this path, Hep's PROJECT_REMOVE drops the
                // (identity) outer Project and AGGREGATE_PROJECT_MERGE
                // folds the inner Project into the Aggregate — the
                // calc column is erased from the SQL. That's
                // intentional here: the Java evaluator recomputes the
                // calc from the base-measure aggregates and cell-set
                // parity still holds.
                //
                // The -Dmondrian.calcite.calcConsume=true path skips
                // this wrapping entirely so the calc-bearing Project
                // survives Hep and RelToSqlConverter emits the
                // arithmetic in the SELECT list — see Task 1 baseline
                // and docs/plans/2026-04-21-calc-pushdown-to-sql.md.
                // RelBuilder eagerly folds adjacent Projects, which
                // would erase the computed-measure expressions from
                // the plan. Build the inner project as a fully-formed
                // RelNode and wrap it in a trivial-predicate
                // LogicalProject via direct construction so the outer
                // projection cannot collapse into the inner one.
                org.apache.calcite.rel.RelNode inner = b.build();
                List<RexNode> outer = new ArrayList<>(
                    innerGroupAndMeasureCount);
                List<String> outerAliases = new ArrayList<>(
                    innerGroupAndMeasureCount);
                org.apache.calcite.rel.type.RelDataType innerRow =
                    inner.getRowType();
                for (int i = 0; i < innerGroupAndMeasureCount; i++) {
                    String alias = restoredAliases.get(i);
                    outer.add(
                        b.getRexBuilder().makeInputRef(
                            innerRow.getFieldList().get(i).getType(),
                            i));
                    outerAliases.add(alias);
                }
                org.apache.calcite.rel.type.RelDataType outerRowType =
                    b.getTypeFactory().createStructType(
                        org.apache.calcite.util.Pair.right(
                            innerRow.getFieldList()
                                .subList(0,
                                    innerGroupAndMeasureCount)),
                        outerAliases);
                org.apache.calcite.rel.RelNode wrapped =
                    org.apache.calcite.rel.logical.LogicalProject.create(
                        inner,
                        java.util.Collections.<
                            org.apache.calcite.rel.hint.RelHint>emptyList(),
                        outer,
                        outerRowType,
                        java.util.Collections.<
                            org.apache.calcite.rel.core.CorrelationId>emptySet());
                b.push(wrapped);
            }
        } else {
            List<RexNode> projs = new ArrayList<>();
            for (PlannerRequest.Column c : req.projections) {
                projs.add(b.field(c.name));
            }
            b.project(projs);
            if (req.distinct) {
                b.distinct();
            }
        }

        if (!req.orderBy.isEmpty()) {
            List<RexNode> exprs = new ArrayList<>();
            for (PlannerRequest.OrderBy o : req.orderBy) {
                RexNode ref = b.field(o.column.name);
                exprs.add(
                    o.direction == PlannerRequest.Order.DESC
                        ? b.desc(ref)
                        : ref);
            }
            b.sort(exprs);
        }

        return b.build();
    }

    /** Table-qualified field ref when {@link PlannerRequest.Column#table}
     *  is non-null; unqualified otherwise. Qualified lookup is required
     *  when the same column name appears on more than one scan in the
     *  input (classic example: {@code product_id} on both
     *  {@code sales_fact_1997} and {@code product}). */
    private static RexNode fieldRef(
        RelBuilder b, PlannerRequest.Column c)
    {
        if (c.table == null) {
            return b.field(c.name);
        }
        return b.field(c.table, c.name);
    }

    private static RexNode filterRex(
        RelBuilder b, PlannerRequest.Filter f)
    {
        RexNode col = fieldRef(b, f.column);
        if (f.literals.size() == 1) {
            return b.equals(col, b.literal(f.literals.get(0)));
        }
        // Multi-literal → OR-chain of equalities (friendlier to dialects
        // than IN; avoids Calcite's SEARCH/SARG unparse surprises).
        List<RexNode> ors = new ArrayList<>(f.literals.size());
        for (Object lit : f.literals) {
            ors.add(b.equals(col, b.literal(lit)));
        }
        return b.or(ors);
    }

    private static RexNode tupleFilterRex(
        RelBuilder b, PlannerRequest.TupleFilter tf)
    {
        // Single-column tuple filter collapses to an OR-chain of equalities
        // (identical in shape to a multi-literal Filter) so single-column
        // OR reuses the same IN-list-like rendering.
        if (tf.columns.size() == 1) {
            RexNode col = b.field(tf.columns.get(0).name);
            List<RexNode> ors = new ArrayList<>(tf.rows.size());
            for (List<Object> row : tf.rows) {
                ors.add(b.equals(col, b.literal(row.get(0))));
            }
            return ors.size() == 1 ? ors.get(0) : b.or(ors);
        }
        // Multi-column: OR of ANDs.
        List<RexNode> ors = new ArrayList<>(tf.rows.size());
        for (List<Object> row : tf.rows) {
            List<RexNode> ands = new ArrayList<>(tf.columns.size());
            for (int i = 0; i < tf.columns.size(); i++) {
                RexNode col = b.field(tf.columns.get(i).name);
                ands.add(b.equals(col, b.literal(row.get(i))));
            }
            ors.add(ands.size() == 1 ? ands.get(0) : b.and(ands));
        }
        return ors.size() == 1 ? ors.get(0) : b.or(ors);
    }

    private static RexNode havingRex(
        RelBuilder b, PlannerRequest.Having h)
    {
        RexNode col = b.field(h.measure.alias);
        RexNode lit = b.literal(h.literal);
        switch (h.op) {
        case GT: return b.greaterThan(col, lit);
        case LT: return b.lessThan(col, lit);
        case GE: return b.greaterThanOrEqual(col, lit);
        case LE: return b.lessThanOrEqual(col, lit);
        case EQ: return b.equals(col, lit);
        case NE: return b.not(b.equals(col, lit));
        default:
            throw new IllegalStateException(
                "unhandled Having op: " + h.op);
        }
    }

    private static RelBuilder.AggCall aggCall(
        RelBuilder b, PlannerRequest.Measure m)
    {
        RexNode ref = b.field(m.column.name);
        switch (m.fn) {
        case SUM:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got SUM)");
            }
            return b.sum(ref).as(m.alias);
        case COUNT:
            return b.count(m.distinct, m.alias, ref);
        case MIN:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got MIN)");
            }
            return b.min(ref).as(m.alias);
        case MAX:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got MAX)");
            }
            return b.max(ref).as(m.alias);
        case AVG:
            if (m.distinct) {
                throw new UnsupportedTranslation(
                    "CalciteSqlPlanner.aggCall: DISTINCT only supported for "
                    + "COUNT (got AVG)");
            }
            return b.avg(ref).as(m.alias);
        default:
            throw new IllegalStateException("unhandled AggFn: " + m.fn);
        }
    }
}

// End CalciteSqlPlanner.java
