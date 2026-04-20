#!/usr/bin/env bash
# Bootstrap a local Postgres `foodmart_calcite` database from the HSQLDB-derived
# CSVs in target/foodmart-postgres-csv. Fan out sales_fact_1997 by $SCALE.
#
# Usage:
#   scripts/postgres/load-foodmart.sh           # default scale=1000
#   scripts/postgres/load-foodmart.sh 10        # 10x scale (quick validation)
#   scripts/postgres/load-foodmart.sh 1         # 1x (base rows)
#
# Env overrides:
#   PG_DB       (default foodmart_calcite)
#   PG_USER     (default $USER)
#   PG_HOST     (default localhost)
#   PG_PORT     (default 5432)
#   CSV_DIR     (default target/foodmart-postgres-csv)
#   DDL_FILE    (default scripts/postgres/foodmart-ddl.sql)
#
set -euo pipefail

SCALE="${1:-1000}"
PG_DB="${PG_DB:-foodmart_calcite}"
PG_USER="${PG_USER:-$USER}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CSV_DIR="${CSV_DIR:-$REPO_ROOT/target/foodmart-postgres-csv}"
DDL_FILE="${DDL_FILE:-$REPO_ROOT/scripts/postgres/foodmart-ddl.sql}"

PSQL="psql -X -v ON_ERROR_STOP=1 -U $PG_USER -h $PG_HOST -p $PG_PORT -d $PG_DB"

echo "==> Loading FoodMart into $PG_DB at scale=$SCALE"
echo "    DDL:   $DDL_FILE"
echo "    CSV:   $CSV_DIR"
echo

if [[ ! -d "$CSV_DIR" ]]; then
  echo "ERROR: CSV dir $CSV_DIR does not exist. Run scripts/postgres/extract_foodmart.py first."
  exit 1
fi
if [[ ! -f "$DDL_FILE" ]]; then
  echo "ERROR: DDL file $DDL_FILE does not exist."
  exit 1
fi

phase_start() { phase_t0=$(date +%s); echo "---- $1"; }
phase_end()   { local t1; t1=$(date +%s); echo "     (${1} took $((t1-phase_t0))s)"; }

# Tables to load from CSV (all except the four we recompute at scale).
ALL_TABLES=(
  account
  agg_c_10_sales_fact_1997
  agg_c_14_sales_fact_1997
  agg_c_special_sales_fact_1997
  agg_g_ms_pcat_sales_fact_1997
  agg_l_03_sales_fact_1997
  agg_l_04_sales_fact_1997
  agg_l_05_sales_fact_1997
  agg_lc_06_sales_fact_1997
  agg_lc_100_sales_fact_1997
  agg_ll_01_sales_fact_1997
  agg_pl_01_sales_fact_1997
  category
  currency
  customer
  days
  department
  employee
  employee_closure
  expense_fact
  inventory_fact_1997
  inventory_fact_1998
  position
  product
  product_class
  promotion
  region
  reserve_employee
  salary
  sales_fact_1997
  sales_fact_1998
  sales_fact_dec_1998
  store
  store_ragged
  time_by_day
  warehouse
  warehouse_class
)

# Tables we'll recompute post-fan-out (these still get loaded with base CSV
# first, then TRUNCATE + INSERT-SELECT after the fact fan-out).
RECOMPUTE_TABLES=(
  agg_c_special_sales_fact_1997
  agg_l_05_sales_fact_1997
  agg_c_14_sales_fact_1997
  agg_g_ms_pcat_sales_fact_1997
)

phase_start "DDL (drop + re-create all tables)"
$PSQL -f "$DDL_FILE" >/dev/null
phase_end "DDL"

phase_start "Dimension + base-fact CSV loads"
# Bump maintenance memory for this session
for t in "${ALL_TABLES[@]}"; do
  f="$CSV_DIR/$t.csv"
  if [[ ! -s "$f" ]]; then
    echo "  (skip $t: $f missing or empty)"
    continue
  fi
  # Use \COPY so file is client-side. NULL '' matches our CSV convention.
  n=$($PSQL -q -v ON_ERROR_STOP=1 -c "\\COPY \"$t\" FROM '$f' WITH (FORMAT csv, NULL '', QUOTE '\"', ESCAPE '\"')" -c "SELECT count(*) FROM \"$t\"" -tA | tail -1)
  printf "  %-40s %10s rows\n" "$t" "$n"
done
phase_end "CSV loads"

if [[ "$SCALE" -gt 1 ]]; then
  phase_start "Fact fan-out (sales_fact_1997 x $SCALE)"
  $PSQL <<SQL
SET maintenance_work_mem = '1GB';
SET synchronous_commit = OFF;
CREATE TEMP TABLE _sales_fact_1997_base AS SELECT * FROM "sales_fact_1997";
TRUNCATE "sales_fact_1997";
INSERT INTO "sales_fact_1997"
SELECT b.*
  FROM _sales_fact_1997_base b
  CROSS JOIN generate_series(1, $SCALE) AS g(n);
DROP TABLE _sales_fact_1997_base;
SQL
  phase_end "Fact fan-out"
fi

phase_start "Recompute aggregates from scaled fact"
$PSQL <<'SQL'
SET maintenance_work_mem = '1GB';
SET synchronous_commit = OFF;

-- agg_c_special_sales_fact_1997
TRUNCATE "agg_c_special_sales_fact_1997";
INSERT INTO "agg_c_special_sales_fact_1997"
  ("product_id","promotion_id","customer_id","store_id",
   "time_month","time_quarter","time_year",
   "store_sales_sum","store_cost_sum","unit_sales_sum","fact_count")
SELECT
  f.product_id, f.promotion_id, f.customer_id, f.store_id,
  t.month_of_year, t.quarter, t.the_year,
  SUM(f.store_sales), SUM(f.store_cost), SUM(f.unit_sales), COUNT(*)
FROM "sales_fact_1997" f
JOIN "time_by_day" t ON f.time_id = t.time_id
GROUP BY f.product_id, f.promotion_id, f.customer_id, f.store_id,
         t.month_of_year, t.quarter, t.the_year;

-- agg_l_05_sales_fact_1997 (NoLink to Time -> group without time cols)
TRUNCATE "agg_l_05_sales_fact_1997";
INSERT INTO "agg_l_05_sales_fact_1997"
  ("product_id","customer_id","promotion_id","store_id",
   "store_sales","store_cost","unit_sales","fact_count")
SELECT
  f.product_id, f.customer_id, f.promotion_id, f.store_id,
  SUM(f.store_sales), SUM(f.store_cost), SUM(f.unit_sales), COUNT(*)
FROM "sales_fact_1997" f
GROUP BY f.product_id, f.customer_id, f.promotion_id, f.store_id;

-- agg_c_14_sales_fact_1997
TRUNCATE "agg_c_14_sales_fact_1997";
INSERT INTO "agg_c_14_sales_fact_1997"
  ("product_id","customer_id","store_id","promotion_id",
   "month_of_year","quarter","the_year",
   "store_sales","store_cost","unit_sales","fact_count")
SELECT
  f.product_id, f.customer_id, f.store_id, f.promotion_id,
  t.month_of_year, t.quarter, t.the_year,
  SUM(f.store_sales), SUM(f.store_cost), SUM(f.unit_sales), COUNT(*)
FROM "sales_fact_1997" f
JOIN "time_by_day" t ON f.time_id = t.time_id
GROUP BY f.product_id, f.customer_id, f.store_id, f.promotion_id,
         t.month_of_year, t.quarter, t.the_year;

-- agg_g_ms_pcat_sales_fact_1997
TRUNCATE "agg_g_ms_pcat_sales_fact_1997";
INSERT INTO "agg_g_ms_pcat_sales_fact_1997"
  ("gender","marital_status","product_family","product_department","product_category",
   "month_of_year","quarter","the_year",
   "store_sales","store_cost","unit_sales","customer_count","fact_count")
SELECT
  c.gender, c.marital_status,
  pc.product_family, pc.product_department, pc.product_category,
  t.month_of_year, t.quarter, t.the_year,
  SUM(f.store_sales), SUM(f.store_cost), SUM(f.unit_sales),
  COUNT(DISTINCT f.customer_id),
  COUNT(*)
FROM "sales_fact_1997" f
JOIN "time_by_day"   t  ON f.time_id     = t.time_id
JOIN "customer"      c  ON f.customer_id = c.customer_id
JOIN "product"       p  ON f.product_id  = p.product_id
JOIN "product_class" pc ON p.product_class_id = pc.product_class_id
GROUP BY c.gender, c.marital_status,
         pc.product_family, pc.product_department, pc.product_category,
         t.month_of_year, t.quarter, t.the_year;
SQL
phase_end "Agg recompute"

phase_start "Fact-table indexes"
# The extracted DDL only ships base-table indexes from HSQLDB which didn't
# include per-FK indexes on sales_fact_1997. Add them here so Postgres has
# reasonable plans on the scaled fact.
$PSQL <<'SQL'
SET maintenance_work_mem = '1GB';
CREATE INDEX IF NOT EXISTS "i_sls97_time"      ON "sales_fact_1997" ("time_id");
CREATE INDEX IF NOT EXISTS "i_sls97_store"     ON "sales_fact_1997" ("store_id");
CREATE INDEX IF NOT EXISTS "i_sls97_customer"  ON "sales_fact_1997" ("customer_id");
CREATE INDEX IF NOT EXISTS "i_sls97_product"   ON "sales_fact_1997" ("product_id");
CREATE INDEX IF NOT EXISTS "i_sls97_promotion" ON "sales_fact_1997" ("promotion_id");
SQL
phase_end "Fact-table indexes"

phase_start "ANALYZE"
$PSQL -c "ANALYZE;" >/dev/null
phase_end "ANALYZE"

phase_start "Summary row counts"
$PSQL <<'SQL'
SELECT 'sales_fact_1997'              AS table, count(*) FROM "sales_fact_1997"
UNION ALL SELECT 'customer',                    count(*) FROM "customer"
UNION ALL SELECT 'product',                     count(*) FROM "product"
UNION ALL SELECT 'time_by_day',                 count(*) FROM "time_by_day"
UNION ALL SELECT 'store',                       count(*) FROM "store"
UNION ALL SELECT 'agg_c_special_sales_fact_1997', count(*) FROM "agg_c_special_sales_fact_1997"
UNION ALL SELECT 'agg_l_05_sales_fact_1997',      count(*) FROM "agg_l_05_sales_fact_1997"
UNION ALL SELECT 'agg_c_14_sales_fact_1997',      count(*) FROM "agg_c_14_sales_fact_1997"
UNION ALL SELECT 'agg_g_ms_pcat_sales_fact_1997', count(*) FROM "agg_g_ms_pcat_sales_fact_1997"
ORDER BY 1;

SELECT SUM("unit_sales") AS total_unit_sales FROM "sales_fact_1997";
SQL
phase_end "Summary"

echo
echo "==> DONE. foodmart_calcite populated at scale=$SCALE."
