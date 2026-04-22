# Calc-Pushdown Fold Analysis

**Date:** 2026-04-21
**Plan:** docs/plans/2026-04-21-calc-pushdown-to-sql.md (Task 2)

## Question

Why does the calc arithmetic disappear from the SQL rendered by
`CalciteSqlPlanner`, even though `CalciteSqlPlanner#planRel` builds an
inner `LogicalProject` carrying the arithmetic expression?

## Method

Throwaway diagnostic (`CalcPushdownFoldDiagnosticTest`) ran the same
planner path used by `ComputedMeasureSqlTest`: an MDX calc
`[Ratio] = [Unit Sales] / [Store Sales]` translated to a
`PlannerRequest` with one `ComputedMeasure` and two base `SUM`
measures. The test dumped `RelOptUtil.toString(...)` at three points —
raw (pre-Hep), post-Hep, and the final SQL string.

## Observations

Raw (pre-Hep) — correct two-project shape:

```
LogicalProject(product_id=[$0], m0=[$1], m1=[$2])                               # outer: shape for legacy consumer
  LogicalProject(product_id=[$0], m0=[$1], m1=[$2],
                 ratio=[CAST(/($1, NULLIF($2, 0))):DOUBLE NOT NULL])            # inner: calc lives here
    LogicalAggregate(group=[{0}], m0=[SUM($7)], m1=[SUM($5)])
      JdbcTableScan(table=[[mondrian, sales_fact_1997]])
```

Post-Hep — both projects gone:

```
LogicalAggregate(group=[{0}], m0=[SUM($7)], m1=[SUM($5)])
  JdbcTableScan(table=[[mondrian, sales_fact_1997]])
```

Final SQL:

```sql
SELECT product_id, SUM(unit_sales) AS m0, SUM(store_sales) AS m1
FROM sales_fact_1997
GROUP BY product_id
```

## Conclusion

Hep strips the calc before the unparser ever runs. Two mechanisms in
the curated Hep program at `CalciteSqlPlanner.java:142-149` combine to
erase it:

1. `CoreRules.PROJECT_REMOVE` drops the **outer** `LogicalProject`
   because it is trivially identity over the first three columns of
   the inner project.
2. With the outer gone, `CoreRules.PROJECT_MERGE` (nothing to merge
   now) plus `CoreRules.AGGREGATE_PROJECT_MERGE` collapse the
   **inner** project into the `LogicalAggregate`: the calc's
   expression references two aggregate-output columns, so it looks
   mergeable. The calc field is dropped because no consumer above
   references it any more (the outer project, which did reference the
   first three columns, has already been removed).

The `force=true` flag on the outer `b.project(...)` call at
`CalciteSqlPlanner.java:712` prevents RelBuilder's *eager* fold during
tree construction but does not stop the downstream Hep rules — Hep
runs after the tree is built, and it happily removes/merges even
force-constructed Projects. The `LogicalProject.create` direct
construction at `CalciteSqlPlanner.java:742-751` has the same
limitation.

Note: the plan's original language named
`ProjectAggregateMergeRule` / `AggregateProjectMergeRule` as the
culprits. In this codebase (Calcite 1.41), the corresponding rules
that actually run are `PROJECT_REMOVE`, `PROJECT_MERGE`, and
`AGGREGATE_PROJECT_MERGE`. The spirit of Task 3 is unchanged:
fold-inducing rules in the Hep program must be prevented from
eliminating the calc-bearing Project.

## Implication for Tasks 3 and 4

Task 3 is necessary and sufficient *if* the RelNode tree given to
`RelToSqlConverter` still contains the calc Project. The diagnostic
shows Hep destroys it; the unparser is never the first offender here.
Task 4's alias-boundary wrapping would provide an additional safety
net against `RelToSqlConverter.visit(Project)` inlining a single
top-level Project during unparse, but verifying its necessity is
deferred until after Task 3 is measured.

The simplest surgical fix aligns with the plan:

- Tag the calc-bearing `LogicalProject` with a `RelHint` carrying
  `mondrian.calc.pushdown`.
- In Hep, skip `PROJECT_REMOVE`, `PROJECT_MERGE`, and
  `AGGREGATE_PROJECT_MERGE` matches whose target carries that hint
  (or whose parent carries it).
- Re-run the Task 1 baseline; if SQL now contains the arithmetic,
  Task 4 can be deferred.

## Reproducer

`CalcPushdownFoldDiagnosticTest` — temporary, no assertions. Remove
once Task 3 lands and the Task 1 baseline passes.
