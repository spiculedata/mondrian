# Shape Catalog Power-Set Enumeration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `MvRegistry.buildShapeSpecs`'s hand-curated shape list with an automatic power-set enumeration over each declared aggregate MeasureGroup's `CopyLink` + `ForeignKeyLink` projections, so any query that slices on a subset of declared dimensions hits the right aggregate without developer intervention.

**Architecture:** For each aggregate MeasureGroup, derive the set of groupable columns from its `CopyLink`s (direct fact-column copies) and `ForeignKeyLink`s (denormalized joins). Enumerate all non-empty subsets up to size N (default 5) and generate a `ShapeSpec` per subset. Add year-prefixed variants automatically when `time_by_day.the_year` is copy-linked. Deduplicate across MeasureGroups (smallest aggregate wins on ties).

**Tech Stack:** Mondrian RolapMeasureGroup, RolapGalaxy, CopyLink, ForeignKeyLink metadata; existing ShapeSpec/MvRegistry.

**Prereq for Plan A:** This expands the candidate set Volcano sees — land this first.

---

### Task 1: Catalog the link metadata per MeasureGroup

**Files:**
- Read: `src/main/java/mondrian/rolap/RolapMeasureGroup.java`
- Create: `src/main/java/mondrian/calcite/MeasureGroupShapeInspector.java`

**Step 1:** Write `MeasureGroupShapeInspector.groupableColumns(RolapMeasureGroup mg)` returning `List<GroupCol>`:
- For each `CopyLink`: one `GroupCol(factCol=aggCol, name=copyLink.targetLevelName)`
- For each `ForeignKeyLink`: one `GroupCol(factCol=aggFkCol, name=fkLink.toHierarchyName, joinTable=fkLink.toTable)`

**Step 2:** Test `MeasureGroupShapeInspectorTest.agg_c_14_has_year_quarter_country()`:
- Load FoodMart schema
- Assert `groupableColumns(agg_c_14_sales_fact_1997)` contains `the_year`, `quarter`, `month_of_year`, `country`

**Step 3:** Run, commit.

```bash
git commit -m "feat(calcite): inspect MeasureGroup groupable columns"
```

---

### Task 2: Power-set enumeration with size cap

**Files:**
- Create: `src/main/java/mondrian/calcite/ShapeEnumerator.java`

**Step 1:** Implement `ShapeEnumerator.enumerate(RolapMeasureGroup mg, int maxSubsetSize)`:
- Get `groupableColumns` from Task 1
- Generate `Sets.powerSet(groupable)` filtered to `1 <= |subset| <= maxSubsetSize`
- For each subset, build a `ShapeSpec(aggTable=mg.getFactTable(), groups=subset, joins=requiredJoinsFor(subset))`

**Step 2:** Test `ShapeEnumeratorTest.enumeratesSubsetsUpToSize4()`:
- `agg_c_14` has 7 groupable cols → expect `C(7,1)+C(7,2)+C(7,3)+C(7,4) = 7+21+35+35 = 98` shapes
- Assert count matches

**Step 3:** Cap `maxSubsetSize=4` default (tunable via `-Dmondrian.calcite.mvMaxSubsetSize`).

**Step 4:** Commit.

```bash
git commit -m "feat(calcite): power-set shape enumeration with size cap"
```

---

### Task 3: Year-prefix auto-variants

**Files:**
- Modify: `src/main/java/mondrian/calcite/ShapeEnumerator.java`

**Why:** Mondrian's implicit slicer often adds `[Time].[Year]` even when the MDX query doesn't mention year. The hand-curated registry handled this via year-prefixed variants; the enumerator must do the same.

**Step 1:** After enumerating base subsets, for any subset that does *not* contain `the_year` but the MeasureGroup *does* copy-link `time_by_day.the_year`, add a variant `{the_year} ∪ subset`.

**Step 2:** Test `ShapeEnumeratorTest.addsYearPrefixedVariants()`:
- `agg_g_ms_pcat` has `the_year` + `product_family` + `gender` → expect both `{family,gender}` and `{year,family,gender}` in the output

**Step 3:** Commit.

```bash
git commit -m "feat(calcite): auto-generate year-prefixed shape variants"
```

---

### Task 4: Cross-MeasureGroup deduplication

**Files:**
- Modify: `src/main/java/mondrian/calcite/MvRegistry.java`

**Why:** Two aggregates can cover the same subset (`agg_c_14` and `agg_c_special` both cover `{year, country}`). Picking the smaller one saves scan cost.

**Step 1:** After all MeasureGroups enumerate, group shapes by `(groupCols, measureSet)` key. Within each group, keep the shape whose `aggTable.rowCount` is smallest.

**Step 2:** Test `MvRegistryDedupTest.picksSmallerAggregateOnTie()`:
- Inject two MeasureGroups with overlapping coverage + known row counts
- Assert the registry retains the smaller one's shape

**Step 3:** Commit.

```bash
git commit -m "feat(calcite): dedupe overlapping shapes, prefer smaller aggregate"
```

---

### Task 5: Replace hand-curated buildShapeSpecs

**Files:**
- Modify: `src/main/java/mondrian/calcite/MvRegistry.java` (`buildShapeSpecs` → call `ShapeEnumerator`)

**Step 1:** Replace the hardcoded shape list with:
```java
for (RolapMeasureGroup mg : schema.getAggregateMeasureGroups()) {
    shapes.addAll(ShapeEnumerator.enumerate(mg, maxSubsetSize));
}
dedupe(shapes);
```

**Step 2:** Run the existing MvHit 4-query corpus. Expected: all 4 still rewrite (year-prefixed variants auto-generated).

**Step 3:** Run the full 45-query equivalence corpus. Expected: no new false rewrites (exact-size matching in `MvMatcher` prevents over-match).

**Step 4:** If any corpus query now rewrites when it shouldn't (cell mismatch or unexpected plan), tighten the join-requirement validation in `ShapeEnumerator`.

**Step 5:** Commit.

```bash
git commit -m "feat(calcite): registry uses power-set enumeration"
```

---

### Task 6: Size budget + observability

**Files:**
- Modify: `src/main/java/mondrian/calcite/MvRegistry.java`

**Step 1:** Log registry size at startup: `[mv-registry] 4 MeasureGroups → 312 shapes (dedup: 287 kept, 25 dropped)`.

**Step 2:** Emit counter `mondrian.calcite.mvRegistry.size` and `mondrian.calcite.mvMatcher.shapeLookupsPerQuery` so we can see if the expanded catalog hurts matcher hot-path.

**Step 3:** Switch `MvMatcher`'s shape lookup from linear scan to `Map<GroupColKey, List<ShapeSpec>>` keyed on the groupBy set's canonical hash.

**Step 4:** Benchmark matcher hot path (microbench): 100 shapes → 1000 shapes. Expected: constant-time lookup, no regression.

**Step 5:** Commit.

```bash
git commit -m "perf(calcite): O(1) shape lookup for expanded registry"
```

---

### Task 7: Ship decision — default-on vs opt-in

**Files:**
- Create: `docs/reports/shape-enumeration-ship-decision.md`

**Step 1:** Run the full perf benchmark (45 queries, Postgres 1000×, HSQLDB) with:
- `maxSubsetSize=4` (default)
- `maxSubsetSize=5` (stretch)
- `maxSubsetSize=0` (disabled, fall back to hand-curated)

**Step 2:** Record geomean D/B and MvHit speedup per config. Pick the smallest `maxSubsetSize` that hits all real-world query shapes.

**Step 3:** Default `maxSubsetSize=4`. Document in `docs/reports/shape-enumeration-ship-decision.md`.

**Step 4:** Commit.

```bash
git commit -m "docs: shape enumeration ship decision"
```

---

## Risks

- **Catalog explosion:** 7-col aggregate × size-5 subsets = 120 shapes/MG; 10 MGs = 1200 shapes. Step 6's O(1) lookup mitigates; size cap bounds it.
- **False rewrites from aggressive enumeration:** Covered by existing exact-size matching + full equivalence corpus.
- **Cold-start cost:** Enumeration runs once per schema load. Measure in Task 6; if > 500ms, cache to disk keyed on schema hash.
- **Join-requirement correctness:** A shape's `joins` must include every dim-join needed to reach a `GroupCol`. If enumerator is sloppy, matcher produces wrong SQL. Task 1's inspector must be the single source of truth for reachability.

## Done When

- `MvRegistry.buildShapeSpecs` no longer contains hardcoded shape literals
- All 4 MvHit queries still rewrite with matcher-only path
- Full 45-query equivalence corpus green
- Registry size logged at startup; matcher lookup is O(1)
- Ship decision doc captures chosen `maxSubsetSize`
