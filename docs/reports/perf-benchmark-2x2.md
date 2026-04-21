# Mondrian-on-Calcite 2x2 performance benchmark

Matrix: {HSQLDB ~87k rows, Postgres 86.8M rows} x {legacy Mondrian SQL emitter, Calcite SQL emitter}. Each cell runs every corpus MDX 3 warm-up + 5 timed iterations with a cold Mondrian schema cache before each iteration. Median of the 5 timed runs is reported.

Scale factor: Postgres fact table is **1000x** the HSQLDB fact table (86.8M vs 87k rows). Dimensions are identical.

Hardware:

```
    Darwin Toms-MacBook-Pro.local 25.3.0 Darwin Kernel Version 25.3.0: Wed Jan 28 20:54:55 PST 2026; root:xnu-12377.91.3~2/RELEASE_ARM64_T6031 arm64
    ProductName:		macOS / ProductVersion:		26.3.1 / ProductVersionExtra:	(a) / BuildVersion:		25D771280a
```

## Headline table (median ms per query)

| corpus | query | A (hsqldb/legacy) | B (pg/legacy) | C (hsqldb/calcite) | D (pg/calcite) | C/A | D/B | D/A |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| smoke | `basic-select` | 1.10s | 4.21s | 211ms | 4.99s | 0.19x | 1.18x | 4.54x |
| smoke | `crossjoin` | 1.12s | 8.89s | 220ms | 10.42s | 0.20x | 1.17x | 9.33x |
| smoke | `non-empty-rows` | 1.12s | 21.58s | 214ms | 23.84s | 0.19x | 1.10x | 21.4x |
| smoke | `calc-member` | 1.30s | 9.87s | 338ms | 11.62s | 0.26x | 1.18x | 8.90x |
| smoke | `named-set` | 2.43s | 30.15s | 518ms | 26.59s | 0.21x | 0.88x | 10.9x |
| smoke | `time-fn` | 1.11s | 7.09s | 166ms | 7.14s | 0.15x | 1.01x | 6.46x |
| smoke | `slicer-where` | 1.77s | 4.52s | 347ms | 4.71s | 0.20x | 1.04x | 2.66x |
| smoke | `topcount` | 2.50s | 30.82s | 523ms | 27.20s | 0.21x | 0.88x | 10.9x |
| smoke | `filter` | 2.48s | 19.96s | 515ms | 18.82s | 0.21x | 0.94x | 7.59x |
| smoke | `order` | 1.28s | 10.67s | 317ms | 10.74s | 0.25x | 1.01x | 8.39x |
| smoke | `aggregate-measure` | 1.06s | 3.62s | 163ms | 3.62s | 0.15x | 1.00x | 3.42x |
| smoke | `distinct-count` | 1.11s | 28.42s | 185ms | 27.68s | 0.17x | 0.97x | 25.0x |
| smoke | `hierarchy-children` | 1.08s | 8.81s | 195ms | 8.68s | 0.18x | 0.99x | 8.04x |
| smoke | `hierarchy-parent` | 394ms | 3.82s | 160ms | 3.72s | 0.41x | 0.97x | 9.42x |
| smoke | `descendants` | 1.07s | 6.15s | 156ms | 6.14s | 0.15x | 1.00x | 5.73x |
| smoke | `ancestor` | 112ms | 1.20s | 127ms | 1.18s | 1.13x | 0.99x | 10.5x |
| smoke | `ytd` | 1.07s | 3.62s | 163ms | 3.64s | 0.15x | 1.00x | 3.41x |
| smoke | `parallelperiod` | 1.07s | 7.26s | 157ms | 7.38s | 0.15x | 1.02x | 6.92x |
| smoke | `iif` | 1.92s | 1.52s | 344ms | 1.55s | 0.18x | 1.02x | 0.81x |
| smoke | `format-string` | 1.31s | 14.81s | 296ms | 9.41s | 0.23x | 0.64x | 7.18x |
| aggregate | `agg-distinct-count-set-of-members` | 1.11s | 20.63s | 152ms | 12.86s | 0.14x | 0.62x | 11.6x |
| aggregate | `agg-distinct-count-two-states` | 1.12s | 19.54s | 185ms | 11.94s | 0.17x | 0.61x | 10.7x |
| aggregate | `agg-crossjoin-gender-states` | 1.11s | 9.05s | 188ms | 5.44s | 0.17x | 0.60x | 4.88x |
| aggregate | `agg-distinct-count-measure-tuple` | 419ms | 8.13s | 179ms | 5.10s | 0.43x | 0.63x | 12.2x |
| aggregate | `agg-distinct-count-particular-tuple` | 1.13s | 7.55s | 243ms | 4.64s | 0.22x | 0.61x | 4.09x |
| aggregate | `agg-distinct-count-quarters` | 424ms | 13.91s | 190ms | 8.51s | 0.45x | 0.61x | 20.1x |
| aggregate | `native-cj-usa-product-names` | 1.41s | 33.70s | 416ms | 21.65s | 0.30x | 0.64x | 15.4x |
| aggregate | `native-topcount-product-names` | 2.43s | 35.47s | 495ms | 30.37s | 0.20x | 0.86x | 12.5x |
| aggregate | `native-filter-product-names` | 2.42s | 19.54s | 350ms | 18.82s | 0.14x | 0.96x | 7.77x |
| aggregate | `agg-distinct-count-product-family-weekly` | 1.31s | 1.10s | 319ms | 1.28s | 0.24x | 1.16x | 0.98x |
| aggregate | `agg-distinct-count-customers-levels` | 2.20s | 24.54s | 314ms | 29.58s | 0.14x | 1.21x | 13.4x |
| calc | `calc-arith-ratio` | 1.33s | 8.81s | 301ms | 9.34s | 0.23x | 1.06x | 7.02x |
| calc | `calc-arith-sum` | 1.34s | 9.36s | 295ms | 9.29s | 0.22x | 0.99x | 6.94x |
| calc | `calc-arith-unary-minus` | 1.33s | 8.69s | 299ms | 8.68s | 0.23x | 1.00x | 6.54x |
| calc | `calc-arith-const-multiply` | 1.32s | 8.67s | 298ms | 8.89s | 0.23x | 1.02x | 6.75x |
| calc | `calc-iif-numeric` | 2.53s | 16.06s | 455ms | 16.38s | 0.18x | 1.02x | 6.46x |
| calc | `calc-coalesce-empty` | 1.32s | 8.59s | 294ms | 8.63s | 0.22x | 1.00x | 6.54x |
| calc | `calc-nested-arith` | 1.34s | 9.96s | 298ms | 10.09s | 0.22x | 1.01x | 7.54x |
| calc | `calc-arith-with-filter` | 1.79s | 4.22s | 328ms | 4.46s | 0.18x | 1.06x | 2.50x |
| calc | `calc-non-pushable-parent` | 512ms | 3.95s | 181ms | 3.92s | 0.35x | 0.99x | 7.65x |
| calc | `calc-non-pushable-ytd` | 1.11s | 3.88s | 164ms | 3.92s | 0.15x | 1.01x | 3.54x |
| mvhit | `agg-g-ms-pcat-family-gender` | 118ms | 1.21s | 132ms | 1.15s | 1.11x | 0.96x | 9.71x |
| mvhit | `agg-c-year-country` | 134ms | 1.17s | 156ms | 1.21s | 1.16x | 1.03x | 9.01x |
| mvhit | `agg-c-quarter-country` | 149ms | 1.17s | 167ms | 1.22s | 1.12x | 1.04x | 8.14x |
| mvhit | `agg-g-ms-pcat-family-gender-marital` | 121ms | 1.20s | 139ms | 1.17s | 1.15x | 0.97x | 9.64x |

## Ratios of interest

Geomean of the **per-query median-ms** ratios across the corpus. Higher than 1 means numerator is slower.

| ratio | geomean | interpretation |
|---|---:|---|
| C/A | 0.25x | Calcite overhead at toy scale (HSQLDB) |
| D/B | 0.93x | Calcite speedup on real planner (Postgres) |
| D/A | 7.11x | Full-rewrite net (scale + planner + emitter) |
| D/C | 28.55x | Dataset-scale effect under Calcite |

## Per-corpus summary

| corpus | cell | n | median-of-medians | geomean |
|---|---|---:|---:|---:|
| smoke | A | 20 | 1.11s | 1.13s |
| smoke | B | 20 | 8.03s | 7.84s |
| smoke | C | 20 | 212ms | 240ms |
| smoke | D | 20 | 8.03s | 7.77s |
| aggregate | A | 11 | 1.13s | 1.19s |
| aggregate | B | 11 | 19.54s | 13.11s |
| aggregate | C | 11 | 243ms | 257ms |
| aggregate | D | 11 | 11.94s | 9.79s |
| calc | A | 10 | 1.33s | 1.30s |
| calc | B | 10 | 8.68s | 7.49s |
| calc | C | 10 | 298ms | 281ms |
| calc | D | 10 | 8.78s | 7.61s |
| mvhit | A | 4 | 127ms | 130ms |
| mvhit | B | 4 | 1.19s | 1.19s |
| mvhit | C | 4 | 148ms | 148ms |
| mvhit | D | 4 | 1.19s | 1.19s |

## Callouts (>10x spread across cells)

| corpus | query | A | B | C | D |
|---|---|---:|---:|---:|---:|
| smoke | `basic-select` | 1.10s | 4.21s | 211ms | 4.99s |
| smoke | `crossjoin` | 1.12s | 8.89s | 220ms | 10.42s |
| smoke | `non-empty-rows` | 1.12s | 21.58s | 214ms | 23.84s |
| smoke | `calc-member` | 1.30s | 9.87s | 338ms | 11.62s |
| smoke | `named-set` | 2.43s | 30.15s | 518ms | 26.59s |
| smoke | `time-fn` | 1.11s | 7.09s | 166ms | 7.14s |
| smoke | `slicer-where` | 1.77s | 4.52s | 347ms | 4.71s |
| smoke | `topcount` | 2.50s | 30.82s | 523ms | 27.20s |
| smoke | `filter` | 2.48s | 19.96s | 515ms | 18.82s |
| smoke | `order` | 1.28s | 10.67s | 317ms | 10.74s |
| smoke | `aggregate-measure` | 1.06s | 3.62s | 163ms | 3.62s |
| smoke | `distinct-count` | 1.11s | 28.42s | 185ms | 27.68s |
| smoke | `hierarchy-children` | 1.08s | 8.81s | 195ms | 8.68s |
| smoke | `hierarchy-parent` | 394ms | 3.82s | 160ms | 3.72s |
| smoke | `descendants` | 1.07s | 6.15s | 156ms | 6.14s |
| smoke | `ancestor` | 112ms | 1.20s | 127ms | 1.18s |
| smoke | `ytd` | 1.07s | 3.62s | 163ms | 3.64s |
| smoke | `parallelperiod` | 1.07s | 7.26s | 157ms | 7.38s |
| smoke | `format-string` | 1.31s | 14.81s | 296ms | 9.41s |
| aggregate | `agg-distinct-count-set-of-members` | 1.11s | 20.63s | 152ms | 12.86s |
| aggregate | `agg-distinct-count-two-states` | 1.12s | 19.54s | 185ms | 11.94s |
| aggregate | `agg-crossjoin-gender-states` | 1.11s | 9.05s | 188ms | 5.44s |
| aggregate | `agg-distinct-count-measure-tuple` | 419ms | 8.13s | 179ms | 5.10s |
| aggregate | `agg-distinct-count-particular-tuple` | 1.13s | 7.55s | 243ms | 4.64s |
| aggregate | `agg-distinct-count-quarters` | 424ms | 13.91s | 190ms | 8.51s |
| aggregate | `native-cj-usa-product-names` | 1.41s | 33.70s | 416ms | 21.65s |
| aggregate | `native-topcount-product-names` | 2.43s | 35.47s | 495ms | 30.37s |
| aggregate | `native-filter-product-names` | 2.42s | 19.54s | 350ms | 18.82s |
| aggregate | `agg-distinct-count-customers-levels` | 2.20s | 24.54s | 314ms | 29.58s |
| calc | `calc-arith-ratio` | 1.33s | 8.81s | 301ms | 9.34s |
| calc | `calc-arith-sum` | 1.34s | 9.36s | 295ms | 9.29s |
| calc | `calc-arith-unary-minus` | 1.33s | 8.69s | 299ms | 8.68s |
| calc | `calc-arith-const-multiply` | 1.32s | 8.67s | 298ms | 8.89s |
| calc | `calc-iif-numeric` | 2.53s | 16.06s | 455ms | 16.38s |
| calc | `calc-coalesce-empty` | 1.32s | 8.59s | 294ms | 8.63s |
| calc | `calc-nested-arith` | 1.34s | 9.96s | 298ms | 10.09s |
| calc | `calc-arith-with-filter` | 1.79s | 4.22s | 328ms | 4.46s |
| calc | `calc-non-pushable-parent` | 512ms | 3.95s | 181ms | 3.92s |
| calc | `calc-non-pushable-ytd` | 1.11s | 3.88s | 164ms | 3.92s |
| mvhit | `agg-g-ms-pcat-family-gender` | 118ms | 1.21s | 132ms | 1.15s |

## Reproducing

```sh
scripts/perf/run-bench-matrix.sh
```

Or one cell at a time, e.g. cell A (HSQLDB + legacy):

```sh
mvn -Pcalcite-harness -Dharness.runPerfBench=true \
    -Dmondrian.backend=legacy \
    -Dtest=PerfBenchmarkTest test
python3 scripts/perf/render-bench-report.py
```

## Raw data

- Cell A (hsqldb/legacy): `target/perf-bench-hsqldb-legacy.json` — present
- Cell B (postgres/legacy): `target/perf-bench-postgres-legacy.json` — present
- Cell C (hsqldb/calcite): `target/perf-bench-hsqldb-calcite.json` — present
- Cell D (postgres/calcite): `target/perf-bench-postgres-calcite.json` — present

