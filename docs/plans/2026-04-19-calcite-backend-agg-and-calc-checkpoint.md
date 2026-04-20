# Calcite backend rewrite — worktree-#3 checkpoint (Tasks R + S)

Worktree: `.worktrees/calcite-backend-agg-and-calc`
Branch: `calcite-backend-agg-and-calc`
Base HEAD at start: `b900e48 feat(calcite): NECJ projects level properties`

## Task S — calc-equivalence corpus

### Files added

- `src/test/java/mondrian/test/calcite/corpus/CalcCorpus.java` — 10 named
  MDX queries targeting the pushable arithmetic calc-member shapes from
  the design (plus two non-pushable controls).
- `src/test/java/mondrian/test/calcite/corpus/CalcCorpusSanityTest.java`
  — sanity mirror of `AggregateCorpusSanityTest`: shape, uniqueness,
  name-collision, and live-execution checks.
- `src/test/java/mondrian/test/calcite/EquivalenceCalcTest.java` —
  parameterized harness driver. Exact mirror of
  `EquivalenceAggregateTest`.
- `src/test/java/mondrian/test/calcite/BaselineCalcRegenerationTest.java`
  — double-guarded (`@Ignore` + `-Dharness.rebaseline=true`) golden
  regenerator.
- `src/test/resources/calcite-harness/golden/calc-*.json` (10 files) —
  recorded cell-sets + SQL rowCount/checksum under unmodified Mondrian.
- `src/test/resources/calcite-harness/golden-legacy/calc-*.json` (10
  files) — byte-identical copy of the above, mirroring the existing
  smoke / aggregate dual-directory convention.
- `pom.xml` — added `EquivalenceCalcTest` to the default surefire
  excludes AND to the `calcite-harness` profile includes.

### The 10 MDX queries

| name | shape |
| --- | --- |
| `calc-arith-ratio` | `[Store Sales] / [Unit Sales]` — base arithmetic |
| `calc-arith-sum` | `[Store Sales] + [Store Cost]` — base arithmetic |
| `calc-arith-unary-minus` | `-[Unit Sales]` — unary minus |
| `calc-arith-const-multiply` | `[Unit Sales] * 1.1` — numeric literal operand |
| `calc-iif-numeric` | `IIf([Unit Sales] > 100, [Store Sales], 0)` |
| `calc-coalesce-empty` | `CoalesceEmpty([Store Sales], 0)` |
| `calc-nested-arith` | `([Store Sales] - [Store Cost]) / [Unit Sales]` |
| `calc-arith-with-filter` | `[Store Sales] - [Store Cost]` with WHERE slicer |
| `calc-non-pushable-parent` | control — `.CurrentMember.Parent` navigation |
| `calc-non-pushable-ytd` | control — `Sum(YTD(), [Store Sales])` |

### Harness results

- **Legacy** (default backend): `EquivalenceCalcTest` — **10/10 green**.
  Full `-Pcalcite-harness` profile: 41 tests, all green (Smoke 20 +
  Aggregate 11 + Calc 10). The Mutation drift summary still reports its
  expected 19/20 CELL_SET_DRIFT (unchanged pre-existing state).
- **Calcite** backend (`-Dmondrian.backend=calcite`):
  `EquivalenceCalcTest` — **10/10 green** as well. This is not what the
  task brief predicted (it expected failures with "UnsupportedTranslation:
  non-pushable calc member"). The reason: **pushdown is not yet
  implemented**. Today the Calcite backend only touches segment-loading
  and a subset of native flows; calc-member evaluation still flows
  through the Java evaluator regardless of `mondrian.backend`, so
  cell-sets are identical to the legacy goldens by construction.

### Implications for Task T

Task T — arithmetic calc-member push-down — cannot be validated against
the EquivalenceHarness with drift-on-failure semantics today, because
pushdown is simply not attempted. Options for Task T:

1. **Add a planner-assert mode**: a feature-flag (e.g.
   `-Dmondrian.calcite.requireCalcPushdown=true`) that forces the
   pushdown path and throws `UnsupportedTranslation` on the first calc
   it cannot push. Task T runs `EquivalenceCalcTest` under that flag;
   pushable entries must flip green, control entries must still emit
   `UnsupportedTranslation` → caught & routed back to the Java evaluator
   → still green.
2. **Emit a plan-snapshot**: capture `RelOptUtil.toString` and extend
   the harness PLAN_DRIFT gate (the scaffold is already in
   `EquivalenceHarness.comparePlanSnapshot`). Pushable queries must
   show arithmetic nodes fused into the Project above the Aggregate;
   non-pushable controls must stay untouched.

Either path makes the 10 queries a real shopping list. Suggested order
for Task T: ratio → sum → unary-minus → const-multiply → nested-arith →
arith-with-filter → iif-numeric → coalesce-empty. Controls should need
zero work.

### MDX-writing notes

- Every arithmetic calc in FoodMart resolves to a calc against the
  `[Measures]` dimension. To make the calc *reach the SQL emitter at
  all* the queries are projected across `[Product].[Product Family]`
  rows so each cell requires a real fact-table aggregate. Pure scalar
  selects short-circuit in the evaluator and never materialise.
- `calc-arith-with-filter` deliberately keeps the same arithmetic shape
  as `calc-nested-arith` but replaces the year slicer with a quarter
  slicer, so the pushdown has to flow a non-trivial WHERE into the
  pushed projection. The SQL rowsets are different between the two (Q1
  vs full-year Time filter) and were verified to produce distinct
  checksums in the goldens.
- `calc-non-pushable-parent` originally tried `.Parent.Name` (string
  result) and `Ancestor(...)` — both materialised but are not *cell*
  values; they land as axis decorations, which defeats the purpose of
  the test. The committed form uses a tuple-valued calc
  `([Store Sales], [Stores].CurrentMember.Parent)` so the cell value is
  a real aggregate with non-pushable dimensional navigation — exactly
  the shape the design classifies "stays on Java evaluator".

### Guardrails held

- Legacy harness count: 41/41 green (Smoke 20 + Aggregate 11 + Calc 10
  = 41, +1 MutationTest class). Task S's N=10 ⇒ 34+10 budget met on
  the queries the brief was tracking.
- HSQLDB only.
- No production code touched (no `src/main/java` changes).
