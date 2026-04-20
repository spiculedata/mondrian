# Postgres FoodMart bootstrap (Task AA)

Produces a local Postgres database (`foodmart_calcite`) containing the FoodMart
schema with dimension tables at base cardinality and `sales_fact_1997` fanned
out by an arbitrary scale factor (default 1000). The four agg tables that the
Calcite harness exercises are recomputed from the scaled fact so AggGen-style
tests still see consistent measure totals.

This is the data fixture for `CALCITE_HARNESS_BACKEND=POSTGRES` harness runs
(Task Y performance work).

## Prerequisites

- Postgres 14+ running on `localhost:5432`, peer auth OK. Tested against 18.1.
- Database `foodmart_calcite` created and empty (`createdb foodmart_calcite`).
- `psql` on `$PATH`.
- Python 3 (>=3.8) on `$PATH` for one-off extraction.
- HSQLDB source script at `target/foodmart/foodmart.script` (shipped with the
  Mondrian test fixtures).
- ~25 GB free disk for 1000x (fact table ~7.5 GB + indexes ~3 GB + WAL).
- ~30–45 min wall time for the full 1000x load on a modern laptop SSD.

## One-time: extract CSV + DDL from the HSQLDB script

```bash
python3 scripts/postgres/extract_foodmart.py \
  target/foodmart/foodmart.script \
  scripts/postgres/foodmart-ddl.sql \
  target/foodmart-postgres-csv
```

This parses the HSQLDB DDL + INSERT statements and writes:

- `scripts/postgres/foodmart-ddl.sql` — Postgres-dialect DDL for all 37 tables
  (HSQLDB types translated: `VARCHAR_IGNORECASE→VARCHAR`, `DECIMAL→NUMERIC`,
  `DOUBLE→DOUBLE PRECISION`, `TINYINT→SMALLINT`, `LONGVARCHAR→TEXT`,
  `BINARY→BYTEA`, `GLOBAL TEMPORARY/MEMORY/CACHED` hints dropped). Identifiers
  are quoted with `"..."` to preserve original lower-case.
- `target/foodmart-postgres-csv/<table>.csv` — one CSV per table, ready for
  `\COPY`. `TRUE/FALSE` become `t/f`, `NULL`s become empty fields, strings are
  RFC4180-quoted where needed.

Runs in ~5 seconds.

## Bootstrap

```bash
# Default 1000x fact-table scale
scripts/postgres/load-foodmart.sh

# Smaller scales for quick validation (1x reproduces the original HSQLDB rows)
scripts/postgres/load-foodmart.sh 1
scripts/postgres/load-foodmart.sh 10

# Huge scale (you do you)
scripts/postgres/load-foodmart.sh 5000
```

The script:

1. Runs the DDL (drops + recreates every table).
2. `\COPY`s every CSV into the matching table.
3. If `$SCALE > 1`: `CREATE TEMP TABLE _base AS SELECT * FROM sales_fact_1997;
   TRUNCATE sales_fact_1997; INSERT INTO sales_fact_1997 SELECT b.* FROM _base
   b CROSS JOIN generate_series(1,$SCALE);` Same `time_id` values are reused so
   aggregation by time still maps to 1997.
4. Recomputes the four harness-referenced agg tables from the scaled fact:
   `agg_c_special_sales_fact_1997`, `agg_l_05_sales_fact_1997`,
   `agg_c_14_sales_fact_1997`, `agg_g_ms_pcat_sales_fact_1997`. Group-by keys
   and measure columns mirror the `aggColumn` mappings in
   `demo/FoodMart.mondrian.xml:461-540`.
5. Adds FK indexes on `sales_fact_1997` (`time_id`, `store_id`, `customer_id`,
   `product_id`, `promotion_id`).
6. Runs `ANALYZE`.

Env overrides: `PG_DB`, `PG_USER`, `PG_HOST`, `PG_PORT`, `CSV_DIR`, `DDL_FILE`.

## Expected post-load row counts (scale=1000)

| Table                              | Rows          |
| ---------------------------------- | ------------- |
| `sales_fact_1997`                  | 86 837 000    |
| `customer`                         | 10 281        |
| `product`                          | 1 560         |
| `time_by_day`                      | 730           |
| `store`                            | 25            |
| `agg_c_special_sales_fact_1997`    | 86 805        |
| `agg_l_05_sales_fact_1997`         | 86 154        |
| `agg_c_14_sales_fact_1997`         | 86 805        |
| `agg_g_ms_pcat_sales_fact_1997`    | 2 637         |

Because the fan-out reuses the same `(product_id, customer_id, store_id,
promotion_id, time_id)` combinations, agg-table cardinality stays constant
across scales — measure totals grow linearly (1000x base) but the number of
distinct groupings does not. This is what we want for AggGen / agg-table
rewrite tests: the same keys, 1000x the measure volume.

Base reference (scale=1): `sales_fact_1997` has 86 837 rows, total
`SUM(unit_sales) = 266 773`. At scale=N, `SUM(unit_sales) = N * 266 773`.

## Verify

```bash
psql -U "$USER" -d foodmart_calcite -c \
  "SELECT count(*) FROM sales_fact_1997;"
#  count
# ---------
#  86837000   (at scale=1000)

psql -U "$USER" -d foodmart_calcite -c \
  "SELECT SUM(unit_sales) FROM sales_fact_1997;"
#   sum
# ----------------
#  266773000.0000
```

## Re-running at different scales

The bootstrap is idempotent: it drops every table and re-loads. You can go from
1000x to 10x by re-running `scripts/postgres/load-foodmart.sh 10` — no manual
cleanup needed.

## Phase timings (indicative, Apple M-series laptop, SSD)

| Phase                        | scale=1  | scale=10 | scale=1000 (measured) |
| ---------------------------- | -------- | -------- | --------------------- |
| DDL                          | <1 s     | <1 s     | <1 s                  |
| Dim + base-fact CSV loads    | ~5 s     | ~5 s     | ~5 s                  |
| Fact fan-out                 | skipped  | ~3 s     | **~12 min**           |
| `agg_c_special` recompute    | ~1 s     | ~2 s     | 1:18                  |
| `agg_l_05` recompute         | ~1 s     | ~1 s     | 0:27                  |
| `agg_c_14` recompute         | ~1 s     | ~1 s     | 1:17                  |
| `agg_g_ms_pcat` recompute    | ~1 s     | ~2 s     | 4:27 (4 joins + DISTINCT) |
| Fact-table indexes           | <1 s     | <1 s     | (in DDL; no post-load) |
| `ANALYZE`                    | ~1 s     | ~1 s     | ~1 s                  |
| **Total**                    | ~8 s     | ~17 s    | **~20 min**           |

The fact fan-out is the dominant cost because the 5 `sales_fact_1997`
FK indexes (from the HSQLDB-derived DDL) are maintained inline as rows
are inserted. If a future bootstrap needs to be faster, drop those
indexes before the fan-out and recreate them after.

### NUMERIC precision

The HSQLDB schema declares aggregate-table measure columns as
`NUMERIC(10,4)` (max abs value 999 999.9999). At 1000x scale, some
groups in `agg_g_ms_pcat` overflow this — its `store_sales` column was
observed hitting ~1.4M per row. The DDL in this repo widens every
aggregate-table measure column to `NUMERIC(18,4)` to accommodate any
scale up to ~10 000x before precision becomes the gating factor. The
base `sales_fact_1997` measures stay at `NUMERIC(10,4)` since
individual rows are small.

## Harness validation

After the load:

```bash
# 1) Connectivity
CALCITE_HARNESS_BACKEND=POSTGRES \
  mvn -Pcalcite-harness -Dtest=PostgresConnectivityTest test

# 2) Smoke corpus
CALCITE_HARNESS_BACKEND=POSTGRES \
  mvn -Pcalcite-harness -Dtest=EquivalenceSmokeTest test
```

The corpus goldens live under `src/test/resources/calcite-harness/golden` and
were captured against HSQLDB. Under Postgres the legacy-SQL strings will
typically drift (identifier quoting, NULL ordering, etc.), so the
`LEGACY_DRIFT` gate may trip. Full corpus-on-Postgres greening is out of scope
for Task AA — this task's deliverable is the data. See the commit report /
follow-up notes for the current pass/fail distribution.

## Data-conversion notes

- `VARCHAR_IGNORECASE` is mapped to plain `VARCHAR`. Case-insensitive
  semantics are not preserved in Postgres. Tests that rely on
  case-insensitive equality need explicit `LOWER()` or a `citext`/`ICU`
  collation — revisit if it surfaces.
- HSQLDB `DOUBLE` literals are written with scientific notation (`8.39E0`).
  The extractor preserves them as-is in CSV; Postgres `DOUBLE PRECISION`
  parses these correctly.
- Boolean literals (`TRUE`/`FALSE`) become `t`/`f` to satisfy Postgres
  `COPY`'s canonical boolean encoding.
- `NULL` becomes an empty field (`,,`); the `\COPY ... NULL ''` option in the
  script interprets this correctly.
- Strings containing embedded single-quotes (e.g. `O'Brien`) are tokenized
  via doubled-quote escape (`'O''Brien'`) and re-emitted as standard CSV
  double-quoted fields with doubled double-quote escapes.
- Foreign-key constraints are **not** re-added. Base FoodMart tests don't
  enforce them, and fact-table bulk inserts are ~10x faster without them.
  Add manually if a test requires enforcement.
