# Mondrian-on-Calcite SQL diff — 2x2 matrix

Side-by-side emitted SQL across the four cells of the perf benchmark matrix:

- **Cell A** — HSQLDB (~87k-row fact table) + legacy Mondrian SQL emitter
- **Cell B** — Postgres (86.8M-row fact table) + legacy Mondrian SQL emitter
- **Cell C** — HSQLDB + Calcite SQL emitter
- **Cell D** — Postgres + Calcite SQL emitter

This captures two dimensions of SQL difference at once: **planner** (legacy vs Calcite) and **dialect** (HSQLDB vs Postgres). The Calcite emitter selects its dialect from the live JDBC connection, so Cells C and D are emitting the same logical plan through different dialect-specific SQL writers.

Corpus subset used (keeps the document readable): [basic-select, crossjoin, aggregate-measure, topcount, filter, agg-c-year-country, calc-arith-ratio].

## Reproducing

```sh
mvn -Pcalcite-harness -Dharness.writeSqlDiff2x2=true -Dtest=SqlDiffMatrixReportTest test
```

## `basic-select`

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns from Sales
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year"
```

_(2 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year"
```

_(2 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "time_by_day"."the_year", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year"
```

_(2 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "time_by_day"."the_year", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year"
```

_(2 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `crossjoin`

**MDX:**

```mdx
select CrossJoin(
    {[Gender].[All Gender].children},
    {[Marital Status].[All Marital Status].children}) on columns,
  {[Measures].[Unit Sales]} on rows
from Sales
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "customer"."gender" as "c1", "customer"."marital_status" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."gender", "customer"."marital_status"
```

_(11 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "customer"."gender" as "c1", "customer"."marital_status" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."gender", "customer"."marital_status"
```

_(11 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "time_by_day"."the_year", "customer"."gender", "customer"."marital_status", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "customer"."marital_status", "customer"."gender"
```

_(11 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "time_by_day"."the_year", "customer"."gender", "customer"."marital_status", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "customer"."marital_status", "customer"."gender"
```

_(11 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `aggregate-measure`

**MDX:**

```mdx
with member [Measures].[Total Store Sales] as 'Sum(YTD(), [Measures].[Store Sales])', format_string='#.00'
select {[Measures].[Total Store Sales]} on columns,
  {[Time].[1997].[Q2].[4]} on rows
from Sales
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."month_of_year" as "c1", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "time_by_day"."month_of_year" in (1, 2, 3, 4) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."month_of_year"
```

_(8 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."month_of_year" as "c1", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "time_by_day"."month_of_year" in (1, 2, 3, 4) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."month_of_year"
```

_(8 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4)
GROUP BY "the_year", "month_of_year"
```

_(8 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4)
GROUP BY "the_year", "month_of_year"
```

_(8 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `topcount`

**MDX:**

```mdx
select {[Measures].[Store Sales]} on columns,
  TopCount([Product].[Product Department].members, 5, [Measures].[Store Sales]) on rows
from Sales
where ([Time].[1997])
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Canned Foods', 'Frozen Foods', 'Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

_(5 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Canned Foods', 'Frozen Foods', 'Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

_(5 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "the_year", "product_family", "product_department", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_department" IN ('Canned Foods', 'Frozen Foods', 'Household', 'Produce', 'Snack Foods')
GROUP BY "the_year", "product_department", "product_family"
```

_(5 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "the_year", "product_family", "product_department", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_department" IN ('Canned Foods', 'Frozen Foods', 'Household', 'Produce', 'Snack Foods')
GROUP BY "the_year", "product_department", "product_family"
```

_(5 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `filter`

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  Filter([Product].[Product Department].members, [Measures].[Unit Sales] > 10000) on rows
from Sales
where ([Time].[1997])
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Baking Goods', 'Beverages', 'Canned Foods', 'Dairy', 'Deli', 'Frozen Foods', 'Health and Hygiene', 'Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

_(5 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

_(5 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "the_year", "product_family", "product_department", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_department" IN ('Baking Goods', 'Beverages', 'Canned Foods', 'Dairy', 'Deli', 'Frozen Foods', 'Health and Hygiene', 'Household', 'Produce', 'Snack Foods')
GROUP BY "the_year", "product_department", "product_family"
```

_(5 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_department", "product_class"."product_family"
```

_(5 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `agg-c-year-country`

**MDX:**

```mdx
SELECT {[Measures].[Unit Sales]} ON COLUMNS,
  CROSSJOIN([Time].[Year].Members, [Store].[Store Country].Members) ON ROWS
FROM Sales
```

**Cell A (HSQLDB + legacy):**

```sql
select "store"."store_country" as "c0", "agg_c_14_sales_fact_1997"."the_year" as "c1", sum("agg_c_14_sales_fact_1997"."unit_sales") as "m0" from "agg_c_14_sales_fact_1997" as "agg_c_14_sales_fact_1997", "store" as "store" where "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id" group by "store"."store_country", "agg_c_14_sales_fact_1997"."the_year"
```

_(4 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "store"."store_country" as "c0", "agg_c_14_sales_fact_1997"."the_year" as "c1", sum("agg_c_14_sales_fact_1997"."unit_sales") as "m0" from "agg_c_14_sales_fact_1997" as "agg_c_14_sales_fact_1997", "store" as "store" where "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id" group by "store"."store_country", "agg_c_14_sales_fact_1997"."the_year"
```

_(4 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "store"."store_country", "agg_c_14_sales_fact_1997"."the_year", SUM("agg_c_14_sales_fact_1997"."unit_sales") AS "m0"
FROM "agg_c_14_sales_fact_1997"
INNER JOIN "store" ON "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id"
GROUP BY "agg_c_14_sales_fact_1997"."the_year", "store"."store_country"
```

_(4 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "store"."store_country", "agg_c_14_sales_fact_1997"."the_year", SUM("agg_c_14_sales_fact_1997"."unit_sales") AS "m0"
FROM "agg_c_14_sales_fact_1997"
INNER JOIN "store" ON "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id"
GROUP BY "agg_c_14_sales_fact_1997"."the_year", "store"."store_country"
```

_(4 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

## `calc-arith-ratio`

**MDX:**

```mdx
with member [Measures].[Sales Per Unit] as '[Measures].[Store Sales] / [Measures].[Unit Sales]'
select {[Measures].[Sales Per Unit]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell A (HSQLDB + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", sum("sales_fact_1997"."unit_sales") as "m0", sum("sales_fact_1997"."store_sales") as "m1" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family"
```

_(4 SQL statements total; last one shown)_

**Cell B (Postgres + legacy):**

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", sum("sales_fact_1997"."unit_sales") as "m0", sum("sales_fact_1997"."store_sales") as "m1" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family"
```

_(4 SQL statements total; last one shown)_

**Cell C (HSQLDB + Calcite):**

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

_(4 SQL statements total; last one shown)_

**Cell D (Postgres + Calcite):**

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

_(4 SQL statements total; last one shown)_

**Key differences:** Calcite switches the legacy comma-join to ANSI `INNER JOIN`; Calcite upper-cases SQL keywords.

