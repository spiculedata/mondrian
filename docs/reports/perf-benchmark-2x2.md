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
| smoke | `basic-select` | 1.10s | 4.85s | 211ms | 6.28s | 0.19x | 1.30x | 5.72x |
| smoke | `crossjoin` | 1.12s | 10.17s | 220ms | 11.54s | 0.20x | 1.14x | 10.3x |
| smoke | `non-empty-rows` | 1.12s | 23.67s | 214ms | 25.53s | 0.19x | 1.08x | 22.9x |
| smoke | `calc-member` | 1.30s | 11.41s | 338ms | 12.89s | 0.26x | 1.13x | 9.88x |
| smoke | `named-set` | 2.43s | 30.39s | 518ms | 28.14s | 0.21x | 0.93x | 11.6x |
| smoke | `time-fn` | 1.11s | 7.10s | 166ms | 8.37s | 0.15x | 1.18x | 7.57x |
| smoke | `slicer-where` | 1.77s | 4.63s | 347ms | 5.92s | 0.20x | 1.28x | 3.34x |
| smoke | `topcount` | 2.50s | 30.89s | 523ms | 28.78s | 0.21x | 0.93x | 11.5x |
| smoke | `filter` | 2.48s | 20.02s | 515ms | 20.02s | 0.21x | 1.00x | 8.07x |
| smoke | `order` | 1.28s | 10.62s | 317ms | 11.96s | 0.25x | 1.13x | 9.34x |
| smoke | `aggregate-measure` | 1.06s | 3.53s | 163ms | 4.97s | 0.15x | 1.41x | 4.69x |
| smoke | `distinct-count` | 1.11s | 28.21s | 185ms | 29.71s | 0.17x | 1.05x | 26.8x |
| smoke | `hierarchy-children` | 1.08s | 8.72s | 195ms | 9.98s | 0.18x | 1.14x | 9.24x |
| smoke | `hierarchy-parent` | 394ms | 3.75s | 160ms | 5.05s | 0.41x | 1.35x | 12.8x |
| smoke | `descendants` | 1.07s | 6.10s | 156ms | 7.31s | 0.15x | 1.20x | 6.83x |
| smoke | `ancestor` | 112ms | 1.15s | 127ms | 1.77s | 1.13x | 1.54x | 15.8x |
| smoke | `ytd` | 1.07s | 3.58s | 163ms | 4.94s | 0.15x | 1.38x | 4.63x |
| smoke | `parallelperiod` | 1.07s | 7.08s | 157ms | 8.45s | 0.15x | 1.19x | 7.91x |
| smoke | `iif` | 1.92s | 1.54s | 344ms | 3.21s | 0.18x | 2.09x | 1.67x |
| smoke | `format-string` | 1.31s | 9.31s | 296ms | 10.62s | 0.23x | 1.14x | 8.11x |
| aggregate | `agg-distinct-count-set-of-members` | 1.11s | 12.86s | 152ms | 14.28s | 0.14x | 1.11x | 12.9x |
| aggregate | `agg-distinct-count-two-states` | 1.12s | 12.26s | 185ms | 13.28s | 0.17x | 1.08x | 11.9x |
| aggregate | `agg-crossjoin-gender-states` | 1.11s | 5.49s | 188ms | 6.74s | 0.17x | 1.23x | 6.05x |
| aggregate | `agg-distinct-count-measure-tuple` | 419ms | 5.16s | 179ms | 6.51s | 0.43x | 1.26x | 15.5x |
| aggregate | `agg-distinct-count-particular-tuple` | 1.13s | 4.72s | 243ms | 5.96s | 0.22x | 1.26x | 5.26x |
| aggregate | `agg-distinct-count-quarters` | 424ms | 8.41s | 190ms | 9.64s | 0.45x | 1.15x | 22.7x |
| aggregate | `native-cj-usa-product-names` | 1.41s | 21.53s | 416ms | 22.81s | 0.30x | 1.06x | 16.2x |
| aggregate | `native-topcount-product-names` | 2.43s | 32.88s | 495ms | 31.10s | 0.20x | 0.95x | 12.8x |
| aggregate | `native-filter-product-names` | 2.42s | 19.93s | 350ms | 19.81s | 0.14x | 0.99x | 8.18x |
| aggregate | `agg-distinct-count-product-family-weekly` | 1.31s | 1.21s | 319ms | 2.84s | 0.24x | 2.34x | 2.16x |
| aggregate | `agg-distinct-count-customers-levels` | 2.20s | 29.21s | 314ms | 31.31s | 0.14x | 1.07x | 14.2x |
| calc | `calc-arith-ratio` | 1.33s | 9.46s | 301ms | 10.79s | 0.23x | 1.14x | 8.11x |
| calc | `calc-arith-sum` | 1.34s | 9.32s | 295ms | 10.58s | 0.22x | 1.13x | 7.90x |
| calc | `calc-arith-unary-minus` | 1.33s | 8.67s | 299ms | 9.99s | 0.23x | 1.15x | 7.53x |
| calc | `calc-arith-const-multiply` | 1.32s | 8.69s | 298ms | 9.94s | 0.23x | 1.14x | 7.55x |
| calc | `calc-iif-numeric` | 2.53s | 16.09s | 455ms | 17.38s | 0.18x | 1.08x | 6.86x |
| calc | `calc-coalesce-empty` | 1.32s | 8.61s | 294ms | 9.83s | 0.22x | 1.14x | 7.45x |
| calc | `calc-nested-arith` | 1.34s | 10.06s | 298ms | 11.32s | 0.22x | 1.13x | 8.46x |
| calc | `calc-arith-with-filter` | 1.79s | 4.26s | 328ms | 5.58s | 0.18x | 1.31x | 3.12x |
| calc | `calc-non-pushable-parent` | 512ms | 3.95s | 181ms | 5.19s | 0.35x | 1.31x | 10.1x |
| calc | `calc-non-pushable-ytd` | 1.11s | 3.91s | 164ms | 5.13s | 0.15x | 1.31x | 4.64x |
| mvhit | `agg-g-ms-pcat-family-gender` | 118ms | 1.24s | 132ms | 2.70s | 1.11x | 2.16x | 22.7x |
| mvhit | `agg-c-year-country` | 134ms | 1.22s | 156ms | 2.74s | 1.16x | 2.24x | 20.4x |
| mvhit | `agg-c-quarter-country` | 149ms | 1.23s | 167ms | 2.99s | 1.12x | 2.43x | 19.9x |
| mvhit | `agg-g-ms-pcat-family-gender-marital` | 121ms | 1.21s | 139ms | 2.79s | 1.15x | 2.30x | 23.0x |

## Ratios of interest

Geomean of the **per-query median-ms** ratios across the corpus. Higher than 1 means numerator is slower.

| ratio | geomean | interpretation |
|---|---:|---|
| C/A | 0.25x | Calcite overhead at toy scale (HSQLDB) |
| D/B | 1.27x | Calcite speedup on real planner (Postgres) |
| D/A | 9.06x | Full-rewrite net (scale + planner + emitter) |
| D/C | 36.40x | Dataset-scale effect under Calcite |

## Per-corpus summary

| corpus | cell | n | median-of-medians | geomean |
|---|---|---:|---:|---:|
| smoke | A | 20 | 1.11s | 1.13s |
| smoke | B | 20 | 7.91s | 7.81s |
| smoke | C | 20 | 212ms | 240ms |
| smoke | D | 20 | 9.21s | 9.44s |
| aggregate | A | 11 | 1.13s | 1.19s |
| aggregate | B | 11 | 12.26s | 9.89s |
| aggregate | C | 11 | 243ms | 257ms |
| aggregate | D | 11 | 13.28s | 11.76s |
| calc | A | 10 | 1.33s | 1.30s |
| calc | B | 10 | 8.68s | 7.56s |
| calc | C | 10 | 298ms | 281ms |
| calc | D | 10 | 9.96s | 8.94s |
| mvhit | A | 4 | 127ms | 130ms |
| mvhit | B | 4 | 1.22s | 1.23s |
| mvhit | C | 4 | 148ms | 148ms |
| mvhit | D | 4 | 2.77s | 2.80s |

## Callouts (>10x spread across cells)

| corpus | query | A | B | C | D |
|---|---|---:|---:|---:|---:|
| smoke | `basic-select` | 1.10s | 4.85s | 211ms | 6.28s |
| smoke | `crossjoin` | 1.12s | 10.17s | 220ms | 11.54s |
| smoke | `non-empty-rows` | 1.12s | 23.67s | 214ms | 25.53s |
| smoke | `calc-member` | 1.30s | 11.41s | 338ms | 12.89s |
| smoke | `named-set` | 2.43s | 30.39s | 518ms | 28.14s |
| smoke | `time-fn` | 1.11s | 7.10s | 166ms | 8.37s |
| smoke | `slicer-where` | 1.77s | 4.63s | 347ms | 5.92s |
| smoke | `topcount` | 2.50s | 30.89s | 523ms | 28.78s |
| smoke | `filter` | 2.48s | 20.02s | 515ms | 20.02s |
| smoke | `order` | 1.28s | 10.62s | 317ms | 11.96s |
| smoke | `aggregate-measure` | 1.06s | 3.53s | 163ms | 4.97s |
| smoke | `distinct-count` | 1.11s | 28.21s | 185ms | 29.71s |
| smoke | `hierarchy-children` | 1.08s | 8.72s | 195ms | 9.98s |
| smoke | `hierarchy-parent` | 394ms | 3.75s | 160ms | 5.05s |
| smoke | `descendants` | 1.07s | 6.10s | 156ms | 7.31s |
| smoke | `ancestor` | 112ms | 1.15s | 127ms | 1.77s |
| smoke | `ytd` | 1.07s | 3.58s | 163ms | 4.94s |
| smoke | `parallelperiod` | 1.07s | 7.08s | 157ms | 8.45s |
| smoke | `format-string` | 1.31s | 9.31s | 296ms | 10.62s |
| aggregate | `agg-distinct-count-set-of-members` | 1.11s | 12.86s | 152ms | 14.28s |
| aggregate | `agg-distinct-count-two-states` | 1.12s | 12.26s | 185ms | 13.28s |
| aggregate | `agg-crossjoin-gender-states` | 1.11s | 5.49s | 188ms | 6.74s |
| aggregate | `agg-distinct-count-measure-tuple` | 419ms | 5.16s | 179ms | 6.51s |
| aggregate | `agg-distinct-count-particular-tuple` | 1.13s | 4.72s | 243ms | 5.96s |
| aggregate | `agg-distinct-count-quarters` | 424ms | 8.41s | 190ms | 9.64s |
| aggregate | `native-cj-usa-product-names` | 1.41s | 21.53s | 416ms | 22.81s |
| aggregate | `native-topcount-product-names` | 2.43s | 32.88s | 495ms | 31.10s |
| aggregate | `native-filter-product-names` | 2.42s | 19.93s | 350ms | 19.81s |
| aggregate | `agg-distinct-count-customers-levels` | 2.20s | 29.21s | 314ms | 31.31s |
| calc | `calc-arith-ratio` | 1.33s | 9.46s | 301ms | 10.79s |
| calc | `calc-arith-sum` | 1.34s | 9.32s | 295ms | 10.58s |
| calc | `calc-arith-unary-minus` | 1.33s | 8.67s | 299ms | 9.99s |
| calc | `calc-arith-const-multiply` | 1.32s | 8.69s | 298ms | 9.94s |
| calc | `calc-iif-numeric` | 2.53s | 16.09s | 455ms | 17.38s |
| calc | `calc-coalesce-empty` | 1.32s | 8.61s | 294ms | 9.83s |
| calc | `calc-nested-arith` | 1.34s | 10.06s | 298ms | 11.32s |
| calc | `calc-arith-with-filter` | 1.79s | 4.26s | 328ms | 5.58s |
| calc | `calc-non-pushable-parent` | 512ms | 3.95s | 181ms | 5.19s |
| calc | `calc-non-pushable-ytd` | 1.11s | 3.91s | 164ms | 5.13s |
| mvhit | `agg-g-ms-pcat-family-gender` | 118ms | 1.24s | 132ms | 2.70s |
| mvhit | `agg-c-year-country` | 134ms | 1.22s | 156ms | 2.74s |
| mvhit | `agg-c-quarter-country` | 149ms | 1.23s | 167ms | 2.99s |
| mvhit | `agg-g-ms-pcat-family-gender-marital` | 121ms | 1.21s | 139ms | 2.79s |

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

