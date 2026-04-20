# Mondrian-on-Calcite SQL Diff Report

**Generated:** 2026-04-20T22:36:51.48065Z

**Corpus:** 45 queries — 20 smoke + 11 aggregate + 10 calc + 4 mv-hit

**Backends:** HSQLDB 1.8 (FoodMart fixture) · Legacy Mondrian SQL generator vs Apache Calcite SQL generator (dialect: HSQLDB)

Legacy SQL for smoke/aggregate/calc is lifted from the frozen `src/test/resources/calcite-harness/golden-legacy/<name>.json` baselines captured at the start of the Calcite rewrite. MvHit queries are executed live under both backends.

## Reproducing

```sh
mvn -Pcalcite-harness -Dharness.writeSqlDiffReport=true -Dtest=SqlDiffReportTest test
```

Overwrites this file in place.

## Table of Contents

- [Smoke corpus](#smoke) (20 queries)
  - [basic-select](#q-basic-select)
  - [crossjoin](#q-crossjoin)
  - [non-empty-rows](#q-non-empty-rows)
  - [calc-member](#q-calc-member)
  - [named-set](#q-named-set)
  - [time-fn](#q-time-fn)
  - [slicer-where](#q-slicer-where)
  - [topcount](#q-topcount)
  - [filter](#q-filter)
  - [order](#q-order)
  - [aggregate-measure](#q-aggregate-measure)
  - [distinct-count](#q-distinct-count)
  - [hierarchy-children](#q-hierarchy-children)
  - [hierarchy-parent](#q-hierarchy-parent)
  - [descendants](#q-descendants)
  - [ancestor](#q-ancestor)
  - [ytd](#q-ytd)
  - [parallelperiod](#q-parallelperiod)
  - [iif](#q-iif)
  - [format-string](#q-format-string)
- [Aggregate corpus](#aggregate) (11 queries)
  - [agg-distinct-count-set-of-members](#q-agg-distinct-count-set-of-members)
  - [agg-distinct-count-two-states](#q-agg-distinct-count-two-states)
  - [agg-crossjoin-gender-states](#q-agg-crossjoin-gender-states)
  - [agg-distinct-count-measure-tuple](#q-agg-distinct-count-measure-tuple)
  - [agg-distinct-count-particular-tuple](#q-agg-distinct-count-particular-tuple)
  - [agg-distinct-count-quarters](#q-agg-distinct-count-quarters)
  - [native-cj-usa-product-names](#q-native-cj-usa-product-names)
  - [native-topcount-product-names](#q-native-topcount-product-names)
  - [native-filter-product-names](#q-native-filter-product-names)
  - [agg-distinct-count-product-family-weekly](#q-agg-distinct-count-product-family-weekly)
  - [agg-distinct-count-customers-levels](#q-agg-distinct-count-customers-levels)
- [Calc corpus](#calc) (10 queries)
  - [calc-arith-ratio](#q-calc-arith-ratio)
  - [calc-arith-sum](#q-calc-arith-sum)
  - [calc-arith-unary-minus](#q-calc-arith-unary-minus)
  - [calc-arith-const-multiply](#q-calc-arith-const-multiply)
  - [calc-iif-numeric](#q-calc-iif-numeric)
  - [calc-coalesce-empty](#q-calc-coalesce-empty)
  - [calc-nested-arith](#q-calc-nested-arith)
  - [calc-arith-with-filter](#q-calc-arith-with-filter)
  - [calc-non-pushable-parent](#q-calc-non-pushable-parent)
  - [calc-non-pushable-ytd](#q-calc-non-pushable-ytd)
- [Mv-hit corpus](#mvhit) (4 queries)
  - [agg-g-ms-pcat-family-gender](#q-agg-g-ms-pcat-family-gender)
  - [agg-c-year-country](#q-agg-c-year-country)
  - [agg-c-quarter-country](#q-agg-c-quarter-country)
  - [agg-g-ms-pcat-family-gender-marital](#q-agg-g-ms-pcat-family-gender-marital)
- [Key takeaways](#takeaways)

## <a name="smoke"></a>Smoke corpus

### <a name="q-basic-select"></a>basic-select

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Row #0: 266,773
```

**SQL executions:** legacy=2 · Calcite=2

#### sqlExecution[0]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-crossjoin"></a>crossjoin

**MDX:**

```mdx
select CrossJoin(
    {[Gender].[All Gender].children},
    {[Marital Status].[All Marital Status].children}) on columns,
  {[Measures].[Unit Sales]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}
{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}
{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}
{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}
Axis #2:
{[Measures].[Unit Sales]}
Row #0: 65,336
Row #0: 66,222
Row #0: 66,460
Row #0: 68,755
```

**SQL executions:** legacy=11 · Calcite=11

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Marital Status') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Calcite:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Marital Status') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Marital Status') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Calcite:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Marital Status') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Marital Status') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Marital Status') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Marital Status') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Calcite:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Marital Status') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Marital Status') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Calcite:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Marital Status') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[6]

*Legacy:*

```sql
select "customer"."marital_status" as "c0" from "customer" as "customer" group by "customer"."marital_status" order by CASE WHEN "customer"."marital_status" IS NULL THEN 0 ELSE 1 END, "customer"."marital_status" ASC
```

*Calcite:*

```sql
select "customer"."marital_status" as "c0" from "customer" as "customer" group by "customer"."marital_status" order by CASE WHEN "customer"."marital_status" IS NULL THEN 0 ELSE 1 END, "customer"."marital_status" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[7]

*Legacy:*

```sql
select count(distinct "gender") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "gender") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[8]

*Legacy:*

```sql
select count(distinct "marital_status") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "marital_status") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[9]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[10]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "customer"."gender" as "c1", "customer"."marital_status" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."gender", "customer"."marital_status"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "customer"."gender", "customer"."marital_status", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "customer"."marital_status", "customer"."gender"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-non-empty-rows"></a>non-empty-rows

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  NON EMPTY [Store].[Store Name].members on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}
{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}
{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}
{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}
{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}
{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}
{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}
{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}
{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}
{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}
{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}
{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}
{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}
Row #0: 21,333
Row #1: 25,663
Row #2: 25,635
Row #3: 2,117
Row #4: 26,079
Row #5: 41,580
Row #6: 2,237
Row #7: 24,576
Row #8: 25,011
Row #9: 23,591
Row #10: 35,257
Row #11: 2,203
Row #12: 11,491
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0", "store"."store_state" as "c1", "store"."store_city" as "c2", "store"."store_name" as "c3", "store"."store_type" as "c4", "store"."store_manager" as "c5", "store"."store_sqft" as "c6", "store"."grocery_sqft" as "c7", "store"."frozen_sqft" as "c8", "store"."meat_sqft" as "c9", "store"."coffee_bar" as "c10", "store"."store_street_address" as "c11" from "sales_fact_1997" as "sales_fact_1997", "store" as "store" where "sales_fact_1997"."store_id" = "store"."store_id" group by "store"."store_country", "store"."store_state", "store"."store_city", "store"."store_name", "store"."store_type", "store"."store_manager", "store"."store_sqft", "store"."grocery_sqft", "store"."frozen_sqft", "store"."meat_sqft", "store"."coffee_bar", "store"."store_street_address" order by CASE WHEN "store"."store_country" IS NULL THEN 1 ELSE 0 END, "store"."store_country" ASC, CASE WHEN "store"."store_state" IS NULL THEN 1 ELSE 0 END, "store"."store_state" ASC, CASE WHEN "store"."store_city" IS NULL THEN 1 ELSE 0 END, "store"."store_city" ASC, CASE WHEN "store"."store_name" IS NULL THEN 1 ELSE 0 END, "store"."store_name" ASC
```

*Calcite:*

```sql
SELECT "store"."store_country", "store"."store_state", "store"."store_city", "store"."store_name", "store"."store_type", "store"."store_manager", "store"."store_sqft", "store"."grocery_sqft", "store"."frozen_sqft", "store"."meat_sqft", "store"."coffee_bar", "store"."store_street_address"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
GROUP BY "store"."store_type", "store"."store_name", "store"."store_street_address", "store"."store_city", "store"."store_state", "store"."store_country", "store"."store_manager", "store"."store_sqft", "store"."grocery_sqft", "store"."frozen_sqft", "store"."meat_sqft", "store"."coffee_bar"
ORDER BY "store"."store_country", "store"."store_state", "store"."store_city", "store"."store_name"
```

*Delta:* rowCount matches (13); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -375 chars (legacy=1149, calcite=774).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "store_name") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_name") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "store"."store_name" as "c0", "time_by_day"."the_year" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "store"."store_name", "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "store"."store_name", "time_by_day"."the_year", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "store"."store_name", "time_by_day"."the_year"
```

*Delta:* rowCount matches (13); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-calc-member"></a>calc-member

**MDX:**

```mdx
with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'
select
  {[Measures].[Store Cost],   [Measures].[Store Sales],   [Measures].[Store Profit Rate]} on columns,
  Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Store Cost]}
{[Measures].[Store Sales]}
{[Measures].[Store Profit Rate]}
Axis #2:
{[Product].[Products].[Food].[Breakfast Foods]}
{[Product].[Products].[Non-Consumable].[Carousel]}
{[Product].[Products].[Food].[Canned Products]}
{[Product].[Products].[Food].[Baking Goods]}
{[Product].[Products].[Drink].[Alcoholic Beverages]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Food].[Baked Goods]}
{[Product].[Products].[Drink].[Beverages]}
{[Product].[Products].[Food].[Frozen Foods]}
{[Product].[Products].[Non-Consumable].[Periodicals]}
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Seafood]}
{[Product].[Products].[Food].[Deli]}
{[Product].[Products].[Food].[Meat]}
{[Product].[Products].[Food].[Canned Foods]}
{[Product].[Products].[Non-Consumable].[Household]}
{[Product].[Products].[Food].[Starchy Foods]}
{[Product].[Products].[Food].[Eggs]}
{[Product].[Products].[Food].[Snacks]}
{[Product].[Products].[Food].[Dairy]}
{[Product].[Products].[Drink].[Dairy]}
{[Product].[Products].[Non-Consumable].[Checkout]}
Row #0: 2,756.80
Row #0: 6,941.46
Row #0: 151.79%
Row #1: 595.97
Row #1: 1,500.11
Row #1: 151.71%
Row #2: 1,317.13
Row #2: 3,314.52
Row #2: 151.65%
Row #3: 15,370.61
Row #3: 38,670.41
Row #3: 151.59%
Row #4: 5,576.79
Row #4: 14,029.08
Row #4: 151.56%
Row #5: 12,972.99
Row #5: 32,571.86
Row #5: 151.07%
Row #6: 26,963.34
Row #6: 67,609.82
Row #6: 150.75%
Row #7: 6,564.09
Row #7: 16,455.43
Row #7: 150.69%
Row #8: 11,069.53
Row #8: 27,748.53
Row #8: 150.67%
Row #9: 22,030.66
Row #9: 55,207.50
Row #9: 150.59%
Row #10: 3,614.55
Row #10: 9,056.76
Row #10: 150.56%
Row #11: 32,831.33
Row #11: 82,248.42
Row #11: 150.52%
Row #12: 1,520.70
Row #12: 3,809.14
Row #12: 150.49%
Row #13: 10,108.87
Row #13: 25,318.93
Row #13: 150.46%
Row #14: 1,465.42
Row #14: 3,669.89
Row #14: 150.43%
Row #15: 15,894.53
Row #15: 39,774.34
Row #15: 150.24%
Row #16: 24,170.73
Row #16: 60,469.89
Row #16: 150.18%
Row #17: 4,705.91
Row #17: 11,756.07
Row #17: 149.82%
Row #18: 3,684.90
Row #18: 9,200.76
Row #18: 149.69%
Row #19: 5,827.58
Row #19: 14,550.05
Row #19: 149.68%
Row #20: 12,228.85
Row #20: 30,508.85
Row #20: 149.48%
Row #21: 2,830.92
Row #21: 7,058.60
Row #21: 149.34%
Row #22: 1,525.04
Row #22: 3,767.71
Row #22: 147.06%
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family", "product_class"."product_department" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family", "product_class"."product_department"
ORDER BY "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -193 chars (legacy=541, calcite=348).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."store_cost") as "m0", sum("sales_fact_1997"."store_sales") as "m1" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department", SUM("sales_fact_1997"."store_cost") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_department", "product_class"."product_family"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-named-set"></a>named-set

**MDX:**

```mdx
with set [Top Food Departments] as
  'TopCount([Product].[Product Department].members, 3, [Measures].[Unit Sales])'
select {[Measures].[Unit Sales]} on columns,
  [Top Food Departments] on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Non-Consumable].[Household]}
Row #0: 37,792
Row #1: 30,545
Row #2: 27,038
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1", sum("sales_fact_1997"."unit_sales") as "c2" from "sales_fact_1997" as "sales_fact_1997", "product" as "product", "product_class" as "product_class", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "product_class"."product_family", "product_class"."product_department" order by CASE WHEN sum("sales_fact_1997"."unit_sales") IS NULL THEN 1 ELSE 0 END, sum("sales_fact_1997"."unit_sales") DESC, CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product_class"."product_department", "product_class"."product_family"
ORDER BY 3 DESC, "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta -438 chars (legacy=927, calcite=489).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

```sql
SELECT "the_year", "product_family", "product_department", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_department" IN ('Household', 'Produce', 'Snack Foods')
GROUP BY "the_year", "product_department", "product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1576 chars (legacy=724, calcite=2300).

### <a name="q-time-fn"></a>time-fn

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  {[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], [Time].[1997].[Q4]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Time].[Time].[1997].[Q1]}
{[Time].[Time].[1997].[Q2]}
{[Time].[Time].[1997].[Q3]}
{[Time].[Time].[1997].[Q4]}
Row #0: 66,291
Row #1: 62,610
Row #2: 65,848
Row #3: 72,024
```

**SQL executions:** legacy=7 · Calcite=7

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q3') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q3') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q4') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q4') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "quarter") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."quarter" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."quarter"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "time_by_day"."quarter", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "time_by_day"."quarter"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-slicer-where"></a>slicer-where

**MDX:**

```mdx
select {[Measures].[Store Sales]} on columns,
  {[Product].[Product Department].members} on rows
from Sales
where ([Time].[1997].[Q1])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997].[Q1]}
Axis #1:
{[Measures].[Store Sales]}
Axis #2:
{[Product].[Products].[Drink].[Alcoholic Beverages]}
{[Product].[Products].[Drink].[Beverages]}
{[Product].[Products].[Drink].[Dairy]}
{[Product].[Products].[Food].[Baked Goods]}
{[Product].[Products].[Food].[Baking Goods]}
{[Product].[Products].[Food].[Breakfast Foods]}
{[Product].[Products].[Food].[Canned Foods]}
{[Product].[Products].[Food].[Canned Products]}
{[Product].[Products].[Food].[Dairy]}
{[Product].[Products].[Food].[Deli]}
{[Product].[Products].[Food].[Eggs]}
{[Product].[Products].[Food].[Frozen Foods]}
{[Product].[Products].[Food].[Meat]}
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Seafood]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Food].[Snacks]}
{[Product].[Products].[Food].[Starchy Foods]}
{[Product].[Products].[Non-Consumable].[Carousel]}
{[Product].[Products].[Non-Consumable].[Checkout]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene]}
{[Product].[Products].[Non-Consumable].[Household]}
{[Product].[Products].[Non-Consumable].[Periodicals]}
Row #0: 3,082.00
Row #1: 6,770.79
Row #2: 1,733.01
Row #3: 4,024.78
Row #4: 9,578.15
Row #5: 1,711.14
Row #6: 9,826.91
Row #7: 724.70
Row #8: 7,708.75
Row #9: 6,265.33
Row #10: 1,949.79
Row #11: 13,626.27
Row #12: 818.44
Row #13: 20,554.12
Row #14: 842.81
Row #15: 17,089.74
Row #16: 3,578.41
Row #17: 2,961.98
Row #18: 343.03
Row #19: 1,136.60
Row #20: 7,915.71
Row #21: 15,125.85
Row #22: 2,260.04
```

**SQL executions:** legacy=7 · Calcite=7

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family", "product_class"."product_department" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family", "product_class"."product_department"
ORDER BY "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -193 chars (legacy=541, calcite=348).

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "quarter") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."quarter" as "c1", "product_class"."product_family" as "c2", "product_class"."product_department" as "c3", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1' and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "time_by_day"."quarter", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

```sql
SELECT "the_year", "quarter", "product_family", "product_department", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."quarter" = 'Q1'
GROUP BY "the_year", "quarter", "product_department", "product_family"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1542 chars (legacy=734, calcite=2276).

### <a name="q-topcount"></a>topcount

**MDX:**

```mdx
select {[Measures].[Store Sales]} on columns,
  TopCount([Product].[Product Department].members, 5, [Measures].[Store Sales]) on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Store Sales]}
Axis #2:
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Non-Consumable].[Household]}
{[Product].[Products].[Food].[Frozen Foods]}
{[Product].[Products].[Food].[Canned Foods]}
Row #0: 82,248.42
Row #1: 67,609.82
Row #2: 60,469.89
Row #3: 55,207.50
Row #4: 39,774.34
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1", sum("sales_fact_1997"."store_sales") as "c2" from "sales_fact_1997" as "sales_fact_1997", "product" as "product", "product_class" as "product_class", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "product_class"."product_family", "product_class"."product_department" order by CASE WHEN sum("sales_fact_1997"."store_sales") IS NULL THEN 1 ELSE 0 END, sum("sales_fact_1997"."store_sales") DESC, CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product_class"."product_department", "product_class"."product_family"
ORDER BY 3 DESC, "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (5); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta -440 chars (legacy=930, calcite=490).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Canned Foods', 'Frozen Foods', 'Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

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

*Delta:* rowCount matches (5); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1576 chars (legacy=757, calcite=2333).

### <a name="q-filter"></a>filter

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  Filter([Product].[Product Department].members, [Measures].[Unit Sales] > 10000) on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Product].[Products].[Drink].[Beverages]}
{[Product].[Products].[Food].[Baking Goods]}
{[Product].[Products].[Food].[Canned Foods]}
{[Product].[Products].[Food].[Dairy]}
{[Product].[Products].[Food].[Deli]}
{[Product].[Products].[Food].[Frozen Foods]}
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene]}
{[Product].[Products].[Non-Consumable].[Household]}
Row #0: 13,573
Row #1: 20,245
Row #2: 19,026
Row #3: 12,885
Row #4: 12,037
Row #5: 26,655
Row #6: 37,792
Row #7: 30,545
Row #8: 16,284
Row #9: 27,038
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1" from "sales_fact_1997" as "sales_fact_1997", "product" as "product", "product_class" as "product_class", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "product_class"."product_family", "product_class"."product_department" having (sum("sales_fact_1997"."unit_sales") > 10000) order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department"
FROM "sales_fact_1997"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product_class"."product_department", "product_class"."product_family"
HAVING SUM("sales_fact_1997"."unit_sales") > 10000
ORDER BY "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (10); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -333 chars (legacy=820, calcite=487).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_department" in ('Baking Goods', 'Beverages', 'Canned Foods', 'Dairy', 'Deli', 'Frozen Foods', 'Health and Hygiene', 'Household', 'Produce', 'Snack Foods') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

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

*Delta:* rowCount matches (11); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1576 chars (legacy=824, calcite=2400).

### <a name="q-order"></a>order

**MDX:**

```mdx
select {[Measures].[Store Sales]} on columns,
  Order([Product].[Product Department].members, [Measures].[Store Sales], BDESC) on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Store Sales]}
Axis #2:
{[Product].[Products].[Food].[Produce]}
{[Product].[Products].[Food].[Snack Foods]}
{[Product].[Products].[Non-Consumable].[Household]}
{[Product].[Products].[Food].[Frozen Foods]}
{[Product].[Products].[Food].[Canned Foods]}
{[Product].[Products].[Food].[Baking Goods]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene]}
{[Product].[Products].[Food].[Dairy]}
{[Product].[Products].[Drink].[Beverages]}
{[Product].[Products].[Food].[Deli]}
{[Product].[Products].[Food].[Baked Goods]}
{[Product].[Products].[Food].[Snacks]}
{[Product].[Products].[Drink].[Alcoholic Beverages]}
{[Product].[Products].[Food].[Starchy Foods]}
{[Product].[Products].[Food].[Eggs]}
{[Product].[Products].[Non-Consumable].[Periodicals]}
{[Product].[Products].[Drink].[Dairy]}
{[Product].[Products].[Food].[Breakfast Foods]}
{[Product].[Products].[Food].[Seafood]}
{[Product].[Products].[Non-Consumable].[Checkout]}
{[Product].[Products].[Food].[Meat]}
{[Product].[Products].[Food].[Canned Products]}
{[Product].[Products].[Non-Consumable].[Carousel]}
Row #0: 82,248.42
Row #1: 67,609.82
Row #2: 60,469.89
Row #3: 55,207.50
Row #4: 39,774.34
Row #5: 38,670.41
Row #6: 32,571.86
Row #7: 30,508.85
Row #8: 27,748.53
Row #9: 25,318.93
Row #10: 16,455.43
Row #11: 14,550.05
Row #12: 14,029.08
Row #13: 11,756.07
Row #14: 9,200.76
Row #15: 9,056.76
Row #16: 7,058.60
Row #17: 6,941.46
Row #18: 3,809.14
Row #19: 3,767.71
Row #20: 3,669.89
Row #21: 3,314.52
Row #22: 1,500.11
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family", "product_class"."product_department" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family", "product_class"."product_department"
ORDER BY "product_class"."product_family", "product_class"."product_department"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -193 chars (legacy=541, calcite=348).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_department", "product_class"."product_family"
```

*Delta:* rowCount matches (23); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-aggregate-measure"></a>aggregate-measure

**MDX:**

```mdx
with member [Measures].[Total Store Sales] as 'Sum(YTD(), [Measures].[Store Sales])', format_string='#.00'
select {[Measures].[Total Store Sales]} on columns,
  {[Time].[1997].[Q2].[4]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Total Store Sales]}
Axis #2:
{[Time].[Time].[1997].[Q2].[4]}
Row #0: 182506.60
```

**SQL executions:** legacy=8 · Calcite=8

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 4 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 4 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "month_of_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "month_of_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[7]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."month_of_year" as "c1", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "time_by_day"."month_of_year" in (1, 2, 3, 4) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."month_of_year"
```

*Calcite:*

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4)
GROUP BY "the_year", "month_of_year"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 708 chars (legacy=411, calcite=1119).

### <a name="q-distinct-count"></a>distinct-count

**MDX:**

```mdx
select {[Measures].[Customer Count]} on columns,
  {[Gender].[All Gender].children} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Customer Count]}
Axis #2:
{[Customer].[Gender].[F]}
{[Customer].[Gender].[M]}
Row #0: 2,755
Row #1: 2,826
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "gender") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "gender") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "customer"."gender" as "c1", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."gender"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "customer"."gender", COUNT(DISTINCT "sales_fact_1997"."customer_id") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "customer"."gender"
```

*Delta:* rowCount matches (2); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-hierarchy-children"></a>hierarchy-children

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  [Store].[USA].children on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Store].[Stores].[USA].[CA]}
{[Store].[Stores].[USA].[OR]}
{[Store].[Stores].[USA].[WA]}
Row #0: 74,748
Row #1: 67,659
Row #2: 124,366
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Calcite:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "store_state") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "store"."store_state" as "c0", "time_by_day"."the_year" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day" where "store"."store_state" in ('CA', 'OR', 'WA') and "time_by_day"."the_year" = 1997 and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "store"."store_state", "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "store_state", "the_year", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" IN ('CA', 'OR', 'WA')) AS "t"
WHERE "t"."the_year" = 1997
GROUP BY "store_state", "the_year"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1709 chars (legacy=466, calcite=2175).

### <a name="q-hierarchy-parent"></a>hierarchy-parent

**MDX:**

```mdx
select {[Measures].[Unit Sales]} on columns,
  {[Store].[USA].[CA].[Los Angeles].Parent} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Store].[Stores].[USA].[CA]}
Row #0: 74,748
```

**SQL executions:** legacy=3 · Calcite=3

#### sqlExecution[0]

*Legacy:*

```sql
select count(distinct "store_state") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select "store"."store_state" as "c0", "time_by_day"."the_year" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day" where "store"."store_state" = 'CA' and "time_by_day"."the_year" = 1997 and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "store"."store_state", "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "store_state", "the_year", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997
GROUP BY "store_state", "the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1709 chars (legacy=451, calcite=2160).

### <a name="q-descendants"></a>descendants

**MDX:**

```mdx
select {[Measures].[Store Sales]} on columns,
  Descendants([Time].[1997], [Time].[Month]) on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Store Sales]}
Axis #2:
{[Time].[Time].[1997].[Q1].[1]}
{[Time].[Time].[1997].[Q1].[2]}
{[Time].[Time].[1997].[Q1].[3]}
{[Time].[Time].[1997].[Q2].[4]}
{[Time].[Time].[1997].[Q2].[5]}
{[Time].[Time].[1997].[Q2].[6]}
{[Time].[Time].[1997].[Q3].[7]}
{[Time].[Time].[1997].[Q3].[8]}
{[Time].[Time].[1997].[Q3].[9]}
{[Time].[Time].[1997].[Q4].[10]}
{[Time].[Time].[1997].[Q4].[11]}
{[Time].[Time].[1997].[Q4].[12]}
Row #0: 45,539.69
Row #1: 44,058.79
Row #2: 50,029.87
Row #3: 42,878.25
Row #4: 44,456.29
Row #5: 45,331.73
Row #6: 50,246.88
Row #7: 46,199.04
Row #8: 43,825.97
Row #9: 42,342.27
Row #10: 53,363.71
Row #11: 56,965.64
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."quarter" as "c1", "time_by_day"."month_of_year" as "c2" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1' or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2' or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q3' or "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q4') group by "time_by_day"."the_year", "time_by_day"."quarter", "time_by_day"."month_of_year" order by CASE WHEN "time_by_day"."the_year" IS NULL THEN 1 ELSE 0 END, "time_by_day"."the_year" ASC, CASE WHEN "time_by_day"."quarter" IS NULL THEN 1 ELSE 0 END, "time_by_day"."quarter" ASC, CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 1 ELSE 0 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
SELECT "the_year", "quarter", "month_of_year"
FROM "time_by_day"
WHERE "the_year" = 1997 AND "quarter" = 'Q1' OR "the_year" = 1997 AND "quarter" = 'Q2' OR "the_year" = 1997 AND "quarter" = 'Q3' OR "the_year" = 1997 AND "quarter" = 'Q4'
GROUP BY "the_year", "quarter", "month_of_year"
ORDER BY "the_year", "quarter", "month_of_year"
```

*Delta:* rowCount matches (12); keyword case: lower -> UPPER; length delta -483 chars (legacy=814, calcite=331).

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "month_of_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "month_of_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."month_of_year" as "c1", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."month_of_year"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "time_by_day"."month_of_year", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "time_by_day"."month_of_year"
```

*Delta:* rowCount matches (12); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-ancestor"></a>ancestor

**MDX:**

```mdx
with member [Measures].[Store Country] as 'Ancestor([Store].[Stores].CurrentMember, [Store].[Stores].[Store Country]).Name'
select {[Measures].[Store Country]} on columns,
  {[Store].[USA].[CA].[Los Angeles]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Store Country]}
Axis #2:
{[Store].[Stores].[USA].[CA].[Los Angeles]}
Row #0: USA
```

_No SQL captured._

### <a name="q-ytd"></a>ytd

**MDX:**

```mdx
with member [Measures].[YTD Sales] as 'Sum(YTD(), [Measures].[Store Sales])', format_string='#.00'
select {[Measures].[YTD Sales]} on columns,
  {[Time].[1997].[Q2].[4]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[YTD Sales]}
Axis #2:
{[Time].[Time].[1997].[Q2].[4]}
Row #0: 182506.60
```

**SQL executions:** legacy=8 · Calcite=8

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 4 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 4 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "month_of_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "month_of_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[7]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."month_of_year" as "c1", sum("sales_fact_1997"."store_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "time_by_day"."month_of_year" in (1, 2, 3, 4) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."month_of_year"
```

*Calcite:*

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4)
GROUP BY "the_year", "month_of_year"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 708 chars (legacy=411, calcite=1119).

### <a name="q-parallelperiod"></a>parallelperiod

**MDX:**

```mdx
with member [Measures].[Prev Qtr Sales] as '([Measures].[Unit Sales], ParallelPeriod([Time].[Time].[Quarter], 1, [Time].[Time].CurrentMember))'
select {[Measures].[Unit Sales], [Measures].[Prev Qtr Sales]} on columns,
  {[Time].[1997].[Q2], [Time].[1997].[Q3]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
{[Measures].[Prev Qtr Sales]}
Axis #2:
{[Time].[Time].[1997].[Q2]}
{[Time].[Time].[1997].[Q3]}
Row #0: 62,610
Row #0: 66,291
Row #1: 65,848
Row #1: 62,610
```

**SQL executions:** legacy=6 · Calcite=6

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q3') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q3') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "quarter") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."quarter" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year", "time_by_day"."quarter"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "time_by_day"."quarter", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "time_by_day"."quarter"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-iif"></a>iif

**MDX:**

```mdx
with member [Measures].[Beer Flag] as
  'IIf([Measures].[Unit Sales] > 100, "Yes", "No")'
select {[Measures].[Unit Sales], [Measures].[Beer Flag]} on columns,
  {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Unit Sales]}
{[Measures].[Beer Flag]}
Axis #2:
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine]}
Row #0: 6,838
Row #0: Yes
```

**SQL executions:** legacy=8 · Calcite=8

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Drink') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Drink') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "product_class"."product_department" as "c0", "product_class"."product_family" as "c1" from "product" as "product", "product_class" as "product_class" where ("product_class"."product_family" = 'Drink') and UPPER("product_class"."product_department") = UPPER('Alcoholic Beverages') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_department", "product_class"."product_family" order by CASE WHEN "product_class"."product_department" IS NULL THEN 0 ELSE 1 END, "product_class"."product_department" ASC
```

*Calcite:*

```sql
select "product_class"."product_department" as "c0", "product_class"."product_family" as "c1" from "product" as "product", "product_class" as "product_class" where ("product_class"."product_family" = 'Drink') and UPPER("product_class"."product_department") = UPPER('Alcoholic Beverages') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_department", "product_class"."product_family" order by CASE WHEN "product_class"."product_department" IS NULL THEN 0 ELSE 1 END, "product_class"."product_department" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "product_class"."product_category" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2" from "product" as "product", "product_class" as "product_class" where ("product_class"."product_family" = 'Drink' and "product_class"."product_department" = 'Alcoholic Beverages') and UPPER("product_class"."product_category") = UPPER('Beer and Wine') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_category", "product_class"."product_family", "product_class"."product_department" order by CASE WHEN "product_class"."product_category" IS NULL THEN 0 ELSE 1 END, "product_class"."product_category" ASC
```

*Calcite:*

```sql
select "product_class"."product_category" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2" from "product" as "product", "product_class" as "product_class" where ("product_class"."product_family" = 'Drink' and "product_class"."product_department" = 'Alcoholic Beverages') and UPPER("product_class"."product_category") = UPPER('Beer and Wine') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_category", "product_class"."product_family", "product_class"."product_department" order by CASE WHEN "product_class"."product_category" IS NULL THEN 0 ELSE 1 END, "product_class"."product_category" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "product_department") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_department") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select count(distinct "product_category") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_category") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[7]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", "product_class"."product_category" as "c3", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "product_class"."product_family" = 'Drink' and "product_class"."product_department" = 'Alcoholic Beverages' and "product_class"."product_category" = 'Beer and Wine' and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family", "product_class"."product_department", "product_class"."product_category"
```

*Calcite:*

```sql
SELECT "the_year", "product_family", "product_department", "product_category", SUM("unit_sales") AS "m0"
FROM (SELECT *
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_family" = 'Drink') AS "t0"
WHERE "product_department" = 'Alcoholic Beverages') AS "t1"
WHERE "product_category" = 'Beer and Wine'
GROUP BY "the_year", "product_category", "product_department", "product_family"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1556 chars (legacy=889, calcite=2445).

### <a name="q-format-string"></a>format-string

**MDX:**

```mdx
with member [Measures].[Sales Pct] as '[Measures].[Store Sales] / [Measures].[Store Cost]', format_string = '#.00%'
select {[Measures].[Sales Pct]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Sales Pct]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 250.73%
Row #1: 250.53%
Row #2: 250.39%
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -107 chars (legacy=341, calcite=234).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product_class"."product_family" as "c1", sum("sales_fact_1997"."store_cost") as "m0", sum("sales_fact_1997"."store_sales") as "m1" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_cost") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

## <a name="aggregate"></a>Aggregate corpus

### <a name="q-agg-distinct-count-set-of-members"></a>agg-distinct-count-set-of-members

**MDX:**

```mdx
WITH MEMBER Gender.X AS 'Aggregate({[Gender].[Gender].Members})'
SELECT Gender.X ON 0, [Measures].[Customer Count] ON 1 FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Customer].[Gender].[X]}
Axis #2:
{[Measures].[Customer Count]}
Row #0: 5,581
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 1 ELSE 0 END, "customer"."gender" ASC
```

*Calcite:*

```sql
SELECT "gender"
FROM "customer"
GROUP BY "gender"
ORDER BY "gender"
```

*Delta:* rowCount matches (2); keyword case: lower -> UPPER; length delta -116 chars (legacy=183, calcite=67).

#### sqlExecution[1]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", COUNT(DISTINCT "sales_fact_1997"."customer_id") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-agg-distinct-count-two-states"></a>agg-distinct-count-two-states

**MDX:**

```mdx
WITH MEMBER [Store].[Stores].[X] as 'Aggregate({[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[WA]})'
SELECT [Store].[Stores].[X] ON ROWS, {[Measures].[Customer Count]} ON COLUMNS
FROM [Sales]
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Customer Count]}
Axis #2:
{[Store].[Stores].[X]}
Row #0: 4,544
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') and UPPER("store"."store_state") = UPPER('WA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Calcite:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') and UPPER("store"."store_state") = UPPER('WA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Calcite:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "store" as "store" where "time_by_day"."the_year" = 1997 and (("store"."store_state" in ('CA', 'WA'))) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."store_id" = "store"."store_id" group by "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "the_year", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."store_state" IN ('CA', 'WA')
GROUP BY "the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1735 chars (legacy=422, calcite=2157).

### <a name="q-agg-crossjoin-gender-states"></a>agg-crossjoin-gender-states

**MDX:**

```mdx
WITH MEMBER Gender.X AS 'Aggregate({[Gender].[Gender].Members} * {[Store].[All Stores].[USA].[CA]})', solve_order=100
SELECT Gender.X ON 0, [Measures].[Customer Count] ON 1 FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Customer].[Gender].[X]}
Axis #2:
{[Measures].[Customer Count]}
Row #0: 2,716
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 1 ELSE 0 END, "customer"."gender" ASC
```

*Calcite:*

```sql
SELECT "gender"
FROM "customer"
GROUP BY "gender"
ORDER BY "gender"
```

*Delta:* rowCount matches (2); keyword case: lower -> UPPER; length delta -116 chars (legacy=183, calcite=67).

#### sqlExecution[1]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Calcite:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "store" as "store" where "time_by_day"."the_year" = 1997 and "store"."store_state" = 'CA' and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."store_id" = "store"."store_id" group by "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "the_year", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."store_state" = 'CA'
GROUP BY "the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1739 chars (legacy=409, calcite=2148).

### <a name="q-agg-distinct-count-measure-tuple"></a>agg-distinct-count-measure-tuple

**MDX:**

```mdx
SELECT [Store].[All Stores].[USA].[CA] ON 0, ([Measures].[Customer Count], [Gender].[M]) ON 1 FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Store].[Stores].[USA].[CA]}
Axis #2:
{[Measures].[Customer Count], [Customer].[Gender].[M]}
Row #0: 1,389
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" where UPPER("customer"."gender") = UPPER('M') group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" where UPPER("customer"."gender") = UPPER('M') group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "store_state") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "gender") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "gender") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "store"."store_state" as "c0", "time_by_day"."the_year" as "c1", "customer"."gender" as "c2", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day", "customer" as "customer" where "store"."store_state" = 'CA' and "time_by_day"."the_year" = 1997 and "customer"."gender" = 'M' and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "store"."store_state", "time_by_day"."the_year", "customer"."gender"
```

*Calcite:*

```sql
SELECT "store_state", "the_year", "gender", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "customer"."customer_id" AS "customer_id0", "customer"."account_num" AS "account_num", "customer"."lname" AS "lname", "customer"."fname" AS "fname", "customer"."mi" AS "mi", "customer"."address1" AS "address1", "customer"."address2" AS "address2", "customer"."address3" AS "address3", "customer"."address4" AS "address4", "customer"."city" AS "city", "customer"."state_province" AS "state_province", "customer"."postal_code" AS "postal_code", "customer"."country" AS "country", "customer"."customer_region_id" AS "customer_region_id", "customer"."phone1" AS "phone1", "customer"."phone2" AS "phone2", "customer"."birthdate" AS "birthdate", "customer"."marital_status" AS "marital_status", "customer"."yearly_income" AS "yearly_income", "customer"."gender" AS "gender", "customer"."total_children" AS "total_children", "customer"."num_children_at_home" AS "num_children_at_home", "customer"."education" AS "education", "customer"."date_accnt_opened" AS "date_accnt_opened", "customer"."member_card" AS "member_card", "customer"."occupation" AS "occupation", "customer"."houseowner" AS "houseowner", "customer"."num_cars_owned" AS "num_cars_owned", "customer"."fullname" AS "fullname"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997) AS "t0"
WHERE "gender" = 'M'
GROUP BY "store_state", "the_year", "gender"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 2873 chars (legacy=632, calcite=3505).

### <a name="q-agg-distinct-count-particular-tuple"></a>agg-distinct-count-particular-tuple

**MDX:**

```mdx
WITH MEMBER Gender.X AS 'Aggregate({[Gender].[M]} * {[Store].[All Stores].[USA].[CA]})', solve_order=100
SELECT Gender.X ON 0, [Measures].[Customer Count] ON 1 FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Customer].[Gender].[X]}
Axis #2:
{[Measures].[Customer Count]}
Row #0: 1,389
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" where UPPER("customer"."gender") = UPPER('M') group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" where UPPER("customer"."gender") = UPPER('M') group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Calcite:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 0 ELSE 1 END, "customer"."gender" ASC
```

*Delta:* rowCount matches (2); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Calcite:*

```sql
select "store"."store_state" as "c0" from "store" as "store" where ("store"."store_country" = 'USA') group by "store"."store_state" order by CASE WHEN "store"."store_state" IS NULL THEN 0 ELSE 1 END, "store"."store_state" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "store" as "store", "customer" as "customer" where "time_by_day"."the_year" = 1997 and ("customer"."gender" = 'M' and "store"."store_state" = 'CA') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year"
```

*Calcite:*

```sql
SELECT "the_year", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "customer"."customer_id" AS "customer_id0", "customer"."account_num" AS "account_num", "customer"."lname" AS "lname", "customer"."fname" AS "fname", "customer"."mi" AS "mi", "customer"."address1" AS "address1", "customer"."address2" AS "address2", "customer"."address3" AS "address3", "customer"."address4" AS "address4", "customer"."city" AS "city", "customer"."state_province" AS "state_province", "customer"."postal_code" AS "postal_code", "customer"."country" AS "country", "customer"."customer_region_id" AS "customer_region_id", "customer"."phone1" AS "phone1", "customer"."phone2" AS "phone2", "customer"."birthdate" AS "birthdate", "customer"."marital_status" AS "marital_status", "customer"."yearly_income" AS "yearly_income", "customer"."gender" AS "gender", "customer"."total_children" AS "total_children", "customer"."num_children_at_home" AS "num_children_at_home", "customer"."education" AS "education", "customer"."date_accnt_opened" AS "date_accnt_opened", "customer"."member_card" AS "member_card", "customer"."occupation" AS "occupation", "customer"."houseowner" AS "houseowner", "customer"."num_cars_owned" AS "num_cars_owned", "customer"."fullname" AS "fullname"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."gender" = 'M') AS "t0"
WHERE "store_state" = 'CA'
GROUP BY "the_year"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 2931 chars (legacy=530, calcite=3461).

### <a name="q-agg-distinct-count-quarters"></a>agg-distinct-count-quarters

**MDX:**

```mdx
WITH MEMBER [Time].[Time].[1997 Q1 plus Q2] AS 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})', solve_order=1
SELECT {[Measures].[Customer Count]} ON COLUMNS,
  {[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997 Q1 plus Q2]} ON ROWS
FROM Sales
WHERE ([Store].[USA].[CA])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Store].[Stores].[USA].[CA]}
Axis #1:
{[Measures].[Customer Count]}
Axis #2:
{[Time].[Time].[1997].[Q1]}
{[Time].[Time].[1997].[Q2]}
{[Time].[Time].[1997 Q1 plus Q2]}
Row #0: 1,110
Row #1: 1,173
Row #2: 1,854
```

**SQL executions:** legacy=8 · Calcite=8

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "store_state") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "quarter") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[6]

*Legacy:*

```sql
select "store"."store_state" as "c0", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day" where "store"."store_state" = 'CA' and (("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') or ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2')) and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "store"."store_state"
```

*Calcite:*

```sql
SELECT "store_state", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997 AND "t"."quarter" = 'Q1' OR "t"."the_year" = 1997 AND "t"."quarter" = 'Q2'
GROUP BY "store_state"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1709 chars (legacy=514, calcite=2223).

#### sqlExecution[7]

*Legacy:*

```sql
select "store"."store_state" as "c0", "time_by_day"."the_year" as "c1", "time_by_day"."quarter" as "c2", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day" where "store"."store_state" = 'CA' and "time_by_day"."the_year" = 1997 and "time_by_day"."quarter" in ('Q1', 'Q2') and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "store"."store_state", "time_by_day"."the_year", "time_by_day"."quarter"
```

*Calcite:*

```sql
SELECT "store_state", "the_year", "quarter", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997) AS "t0"
WHERE "quarter" IN ('Q1', 'Q2')
GROUP BY "store_state", "the_year", "quarter"
```

*Delta:* rowCount matches (2); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1685 chars (legacy=565, calcite=2250).

### <a name="q-native-cj-usa-product-names"></a>native-cj-usa-product-names

**MDX:**

```mdx
select non empty crossjoin({[Store].[USA]}, [Product].[Product Name].members) on 0 from sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Light Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Light Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Imported Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Light Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Imported Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Light Beer]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Light Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Merlot Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good White Zinfandel Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Light Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Merlot Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl White Zinfandel Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Light Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Merlot Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth White Zinfandel Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Light Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Merlot Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure White Zinfandel Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Light Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Merlot Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus White Zinfandel Wine]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Cream Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Diet Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Diet Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Cream Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Diet Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Diet Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Cream Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Diet Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Diet Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Cream Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Diet Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Diet Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Cream Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Cola]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Soda]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Apple Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Mango Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Strawberry Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Apple Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Mango Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Strawberry Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Apple Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Mango Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Strawberry Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Apple Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Mango Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Strawberry Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Apple Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Mango Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Strawberry Drink]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best].[BBB Best Hot Chocolate]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[CDR].[CDR Hot Chocolate]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Landslide].[Landslide Hot Chocolate]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Plato].[Plato Hot Chocolate]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Super].[Super Hot Chocolate]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Columbian Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Decaf Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best French Roast Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Regular Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Columbian Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Decaf Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR French Roast Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Regular Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Columbian Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Decaf Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide French Roast Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Regular Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Columbian Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Decaf Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato French Roast Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Regular Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Columbian Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Decaf Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super French Roast Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Regular Coffee]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Apple Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Berry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Cranberry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Orange Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Apple Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Berry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Cranberry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Orange Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Apple Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Berry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Cranberry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Orange Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Apple Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Berry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Cranberry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Orange Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Apple Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Berry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Cranberry Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Orange Juice]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 1% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 2% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Buttermilk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Chocolate Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Whole Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 1% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 2% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Buttermilk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Chocolate Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Whole Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 1% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 2% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Buttermilk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Chocolate Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Whole Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 1% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 2% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Buttermilk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Chocolate Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Whole Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 1% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 2% Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Buttermilk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Chocolate Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony].[Colony Bagels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Fantastic].[Fantastic Bagels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Great].[Great Bagels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Modell].[Modell Bagels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Sphinx].[Sphinx Bagels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Blueberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Cranberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony English Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Blueberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Cranberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic English Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Blueberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Cranberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great English Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Blueberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Cranberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell English Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Blueberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Cranberry Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx English Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Muffins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Pumpernickel Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Rye Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Wheat Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony White Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Pumpernickel Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Rye Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Wheat Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic White Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Pumpernickel Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Rye Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Wheat Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great White Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Pumpernickel Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Rye Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Wheat Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell White Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Pumpernickel Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Rye Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Wheat Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx White Bread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Canola Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Corn Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Sesame Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Vegetable Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Canola Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Corn Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Sesame Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Vegetable Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Canola Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Corn Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Sesame Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Vegetable Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Canola Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Corn Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Sesame Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Vegetable Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Canola Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Corn Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Sesame Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Vegetable Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[BBB Best].[BBB Best Tomato Sauce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[CDR].[CDR Tomato Sauce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Landslide].[Landslide Tomato Sauce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Plato].[Plato Tomato Sauce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Super].[Super Tomato Sauce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Oregano]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Salt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Oregano]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Salt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Oregano]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Salt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Oregano]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Salt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Oregano]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Salt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[BBB Best].[BBB Best Brown Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[BBB Best].[BBB Best White Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[CDR].[CDR Brown Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[CDR].[CDR White Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Landslide].[Landslide Brown Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Landslide].[Landslide White Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Plato].[Plato Brown Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Plato].[Plato White Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Super].[Super Brown Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Super].[Super White Sugar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Apple Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Grape Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Strawberry Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Apple Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Grape Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Strawberry Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Apple Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Grape Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Strawberry Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Apple Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Grape Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Strawberry Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Apple Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Grape Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Strawberry Jam]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Apple Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Grape Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Strawberry Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[CDR].[CDR Apple Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[CDR].[CDR Strawberry Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Apple Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Grape Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Strawberry Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Apple Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Grape Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Strawberry Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Apple Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Grape Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Strawberry Jelly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Creamy Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Extra Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Creamy Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Extra Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Creamy Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Extra Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Creamy Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Extra Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Creamy Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Extra Chunky Peanut Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Apple Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Grape Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Low Fat Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Strawberry Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Apple Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Grape Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Low Fat Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Strawberry Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Apple Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Grape Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Low Fat Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Strawberry Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Apple Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Grape Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Low Fat Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Strawberry Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Apple Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Grape Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Low Fat Apple Butter]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Strawberry Preserves]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Corn Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Grits]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Oatmeal]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Wheat Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Corn Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Grits]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Oatmeal]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Wheat Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Corn Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Grits]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Oatmeal]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Wheat Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Corn Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Grits]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Oatmeal]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Wheat Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Corn Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Grits]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Oatmeal]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Wheat Puffs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Better].[Better Fancy Canned Anchovies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Blue Label].[Blue Label Fancy Canned Anchovies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Bravo].[Bravo Fancy Canned Anchovies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Just Right].[Just Right Fancy Canned Anchovies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Pleasant].[Pleasant Fancy Canned Anchovies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Better].[Better Fancy Canned Clams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Blue Label].[Blue Label Fancy Canned Clams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Bravo].[Bravo Fancy Canned Clams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Just Right].[Just Right Fancy Canned Clams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Pleasant].[Pleasant Fancy Canned Clams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Better].[Better Fancy Canned Oysters]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Blue Label].[Blue Label Fancy Canned Oysters]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Bravo].[Bravo Fancy Canned Oysters]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Just Right].[Just Right Fancy Canned Oysters]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Pleasant].[Pleasant Fancy Canned Oysters]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Better].[Better Fancy Canned Sardines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Blue Label].[Blue Label Fancy Canned Sardines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Bravo].[Bravo Fancy Canned Sardines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Just Right].[Just Right Fancy Canned Sardines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Pleasant].[Pleasant Fancy Canned Sardines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Better].[Better Large Canned Shrimp]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Blue Label].[Blue Label Large Canned Shrimp]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Bravo].[Bravo Large Canned Shrimp]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Just Right].[Just Right Large Canned Shrimp]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Pleasant].[Pleasant Large Canned Shrimp]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Beef Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Regular Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Rice Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Turkey Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Vegetable Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Beef Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Regular Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Rice Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Turkey Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Vegetable Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Beef Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Regular Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Rice Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Turkey Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Vegetable Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Beef Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Regular Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Rice Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Turkey Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Vegetable Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Beef Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Regular Ramen Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Rice Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Turkey Noodle Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Vegetable Soup]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Water]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Blue Label].[Blue Label Canned Tuna in Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Blue Label].[Blue Label Canned Tuna in Water]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Bravo].[Bravo Canned Tuna in Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Bravo].[Bravo Canned Tuna in Water]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Just Right].[Just Right Canned Tuna in Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Just Right].[Just Right Canned Tuna in Water]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Pleasant].[Pleasant Canned Tuna in Oil]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Pleasant].[Pleasant Canned Tuna in Water]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned String Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Yams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Creamed Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned String Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Yams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Creamed Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned String Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Yams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Creamed Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned String Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Yams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Creamed Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned String Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Yams]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Creamed Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Applause].[Applause Canned Mixed Fruit]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Applause].[Applause Canned Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Big City].[Big City Canned Mixed Fruit]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Big City].[Big City Canned Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Green Ribbon].[Green Ribbon Canned Mixed Fruit]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Green Ribbon].[Green Ribbon Canned Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Swell].[Swell Canned Mixed Fruit]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Swell].[Swell Canned Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Toucan].[Toucan Canned Mixed Fruit]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Toucan].[Toucan Canned Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Cheese Spread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Havarti Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Head Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Jack Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Low Fat String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Mild Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Muenster Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Sharp Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Cheese Spread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Havarti Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Head Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Jack Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Low Fat String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Mild Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Muenster Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Sharp Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Cheese Spread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Havarti Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Head Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Jack Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Low Fat String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Mild Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Muenster Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Sharp Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Cheese Spread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Havarti Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Head Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Jack Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Low Fat String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Mild Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Muenster Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Sharp Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Cheese Spread]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Havarti Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Head Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Jack Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Low Fat String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Mild Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Muenster Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Sharp Cheddar Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla String Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Booker].[Booker Large Curd Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Booker].[Booker Low Fat Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Carlson].[Carlson Large Curd Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Carlson].[Carlson Low Fat Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Club].[Club Large Curd Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Club].[Club Low Fat Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Even Better].[Even Better Large Curd Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Even Better].[Even Better Low Fat Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Gorilla].[Gorilla Large Curd Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Gorilla].[Gorilla Low Fat Cottage Cheese]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Booker].[Booker Low Fat Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Booker].[Booker Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Carlson].[Carlson Low Fat Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Carlson].[Carlson Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Club].[Club Low Fat Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Club].[Club Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Even Better].[Even Better Low Fat Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Even Better].[Even Better Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Gorilla].[Gorilla Low Fat Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Gorilla].[Gorilla Sour Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Booker].[Booker Blueberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Booker].[Booker Strawberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Carlson].[Carlson Blueberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Carlson].[Carlson Strawberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Club].[Club Blueberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Club].[Club Strawberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Even Better].[Even Better Blueberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Even Better].[Even Better Strawberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Gorilla].[Gorilla Blueberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Gorilla].[Gorilla Strawberry Yogurt]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Beef Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Low Fat Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Pimento Loaf]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Beef Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Low Fat Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Pimento Loaf]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Beef Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Low Fat Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Pimento Loaf]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Beef Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Low Fat Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Pimento Loaf]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Beef Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Low Fat Bologna]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Pimento Loaf]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Corned Beef]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Ham]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Turkey]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Corned Beef]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Ham]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Turkey]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Corned Beef]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Ham]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Turkey]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Corned Beef]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Ham]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Turkey]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Corned Beef]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Ham]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Turkey]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[American].[American Roasted Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Cutting Edge].[Cutting Edge Roasted Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Lake].[Lake Roasted Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Moms].[Moms Roasted Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Red Spade].[Red Spade Roasted Chicken]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Chicken Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Foot-Long Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Turkey Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Chicken Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Foot-Long Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Turkey Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Chicken Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Foot-Long Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Turkey Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Chicken Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Foot-Long Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Turkey Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Chicken Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Foot-Long Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Turkey Hot Dogs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Low Fat Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Potato Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Low Fat Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Potato Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Low Fat Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Potato Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Low Fat Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Potato Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Low Fat Cole Slaw]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Potato Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Egg Substitute]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Large Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Large Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Small Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Small Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Egg Substitute]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Large Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Large Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Small Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Small Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Egg Substitute]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Large Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Large Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Small Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Small Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Egg Substitute]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Large Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Large Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Small Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Small Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Egg Substitute]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Large Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Large Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Brown Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Eggs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time].[Big Time Pancake Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Carrington].[Carrington Pancake Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden].[Golden Pancake Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Imagine].[Imagine Pancake Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[PigTail].[PigTail Pancake Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Big Time].[Big Time Frozen Pancakes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Carrington].[Carrington Frozen Pancakes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Golden].[Golden Frozen Pancakes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Imagine].[Imagine Frozen Pancakes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[PigTail].[PigTail Frozen Pancakes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Apple Cinnamon Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Blueberry Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Low Fat Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Apple Cinnamon Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Blueberry Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Low Fat Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Apple Cinnamon Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Blueberry Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Low Fat Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Apple Cinnamon Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Blueberry Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Low Fat Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Apple Cinnamon Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Blueberry Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Low Fat Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Waffles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Ice Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Ice Cream Sandwich]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Ice Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Ice Cream Sandwich]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Ice Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Ice Cream Sandwich]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Ice Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Ice Cream Sandwich]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Ice Cream]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Ice Cream Sandwich]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Grape Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Lemon Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Lime Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Orange Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Grape Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Lemon Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Lime Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Orange Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Grape Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Lemon Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Lime Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Orange Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Grape Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Lemon Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Lime Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Orange Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Grape Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Lemon Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Lime Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Orange Popsicles]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Beef TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Chicken TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Turkey TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Beef TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Chicken TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Turkey TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Beef TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Chicken TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Turkey TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Beef TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Chicken TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Turkey TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Beef TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Chicken TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Turkey TV Dinner]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Breast]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Thighs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Wings]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Breast]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Thighs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Wings]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Breast]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Thighs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Wings]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Breast]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Thighs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Wings]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Breast]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Thighs]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Wings]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Cheese Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Mushroom Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Pepperoni Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Sausage Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Cheese Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Mushroom Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Pepperoni Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Sausage Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Cheese Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Mushroom Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Pepperoni Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Sausage Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Cheese Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Mushroom Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Pepperoni Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Sausage Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Cheese Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Mushroom Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Pepperoni Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Sausage Pizza]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Fajita French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Home Style French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Low Fat French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Fajita French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Home Style French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Low Fat French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Fajita French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Home Style French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Low Fat French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Fajita French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Home Style French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Low Fat French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Fajita French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Home Style French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Low Fat French Fries]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Carrots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Carrots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Carrots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Carrots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Carrots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Corn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Footnote].[Footnote Extra Lean Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Footnote].[Footnote Seasoned Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Genteel].[Genteel Extra Lean Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Genteel].[Genteel Seasoned Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Gerolli].[Gerolli Extra Lean Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Gerolli].[Gerolli Seasoned Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Quick].[Quick Extra Lean Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Quick].[Quick Seasoned Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Ship Shape].[Ship Shape Extra Lean Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Ship Shape].[Ship Shape Seasoned Hamburger]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Cantelope]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Fancy Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Fuji Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Golden Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Honey Dew]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Lemons]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Limes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Macintosh Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Mandarin Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Red Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Tangerines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Cantelope]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fuji Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Golden Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Honey Dew]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Limes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Macintosh Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Mandarin Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Red Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Tangerines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Cantelope]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Fancy Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Fuji Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Golden Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Honey Dew]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Lemons]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Limes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Macintosh Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Mandarin Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Red Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Tangerines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Cantelope]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Fancy Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Fuji Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Golden Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Honey Dew]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Lemons]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Limes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Macintosh Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Mandarin Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Red Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Tangerines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Cantelope]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Fancy Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Fuji Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Golden Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Honey Dew]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Lemons]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Limes]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Macintosh Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Mandarin Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Oranges]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Peaches]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Plums]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Red Delcious Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Tangerines]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Ebony].[Ebony Firm Tofu]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Hermanos].[Hermanos Firm Tofu]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[High Top].[High Top Firm Tofu]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Tell Tale].[Tell Tale Firm Tofu]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Tri-State].[Tri-State Firm Tofu]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Almonds]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Canned Peanuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Mixed Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Party Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Walnuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Almonds]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Canned Peanuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Mixed Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Party Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Walnuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Almonds]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Canned Peanuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Mixed Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Party Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Walnuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Almonds]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Canned Peanuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Mixed Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Party Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Walnuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Almonds]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Canned Peanuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Mixed Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Party Nuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Walnuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Asparagus]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Baby Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Corn on the Cob]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Dried Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Elephant Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Fresh Lima Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Green Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Lettuce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony New Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Onions]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Prepared Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Red Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Shitake Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Summer Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Sweet Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Sweet Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Asparagus]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Baby Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Corn on the Cob]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Dried Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Elephant Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Fresh Lima Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Lettuce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos New Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Onions]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Prepared Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Red Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Shitake Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Summer Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Sweet Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Sweet Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Asparagus]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Baby Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Corn on the Cob]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Dried Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Elephant Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Fresh Lima Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Green Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Lettuce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top New Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Onions]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Prepared Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Red Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Shitake Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Summer Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Sweet Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Sweet Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Asparagus]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Baby Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Corn on the Cob]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Dried Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Elephant Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Fresh Lima Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Green Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Lettuce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale New Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Onions]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Prepared Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Red Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Shitake Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Summer Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Sweet Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Sweet Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Asparagus]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Baby Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Beets]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Broccoli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Cauliflower]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Corn on the Cob]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Dried Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Elephant Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Fresh Lima Beans]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Garlic]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Green Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Lettuce]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State New Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Onions]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Potatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Prepared Salad]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Red Pepper]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Shitake Mushrooms]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Summer Squash]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Sweet Onion]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Sweet Peas]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Tomatos]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Amigo].[Amigo Lox]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Curlew].[Curlew Lox]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Dual City].[Dual City Lox]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Kiwi].[Kiwi Lox]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Tip Top].[Tip Top Lox]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Amigo].[Amigo Scallops]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Curlew].[Curlew Scallops]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Dual City].[Dual City Scallops]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Kiwi].[Kiwi Scallops]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Tip Top].[Tip Top Scallops]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice BBQ Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Corn Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Low Fat BBQ Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Low Fat Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast BBQ Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Corn Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Low Fat BBQ Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Low Fat Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West BBQ Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Corn Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Low Fat BBQ Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Low Fat Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio BBQ Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Corn Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Low Fat BBQ Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Low Fat Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel BBQ Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Corn Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Low Fat BBQ Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Low Fat Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Potato Chips]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Chocolate Chip Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Frosted Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Fudge Brownies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Fudge Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Graham Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Lemon Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Low Fat Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Sugar Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Chocolate Chip Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Frosted Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Fudge Brownies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Fudge Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Graham Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Lemon Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Low Fat Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Sugar Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Chocolate Chip Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Frosted Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Fudge Brownies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Fudge Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Graham Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Lemon Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Low Fat Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Sugar Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Chocolate Chip Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Frosted Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Fudge Brownies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Fudge Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Graham Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Lemon Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Low Fat Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Sugar Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Chocolate Chip Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Frosted Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Fudge Brownies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Fudge Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Graham Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Lemon Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Low Fat Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Sugar Cookies]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Best Choice].[Best Choice Cheese Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Best Choice].[Best Choice Sesame Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fast].[Fast Cheese Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fast].[Fast Sesame Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fort West].[Fort West Cheese Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fort West].[Fort West Sesame Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Horatio].[Horatio Cheese Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Horatio].[Horatio Sesame Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Nationeel].[Nationeel Cheese Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Nationeel].[Nationeel Sesame Crackers]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Avocado Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Cheese Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Fondue Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Salsa Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Avocado Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Cheese Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Fondue Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Salsa Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Avocado Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Cheese Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Fondue Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Salsa Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Avocado Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Cheese Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Fondue Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Salsa Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Avocado Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Cheese Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Fondue Mix]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Salsa Dip]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Chocolate Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Frosted Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Mini Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Chocolate Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Frosted Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Mini Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Chocolate Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Frosted Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Mini Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Chocolate Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Frosted Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Mini Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Chocolate Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Frosted Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Mini Donuts]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Apple Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Apricots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Dates]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Golden Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Grape Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Raspberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Strawberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Apple Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Apricots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Dates]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Golden Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Grape Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raspberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Strawberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Apple Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Apricots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Dates]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Golden Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Grape Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raspberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Strawberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Apple Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Apricots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Dates]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Golden Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Grape Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Raspberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Strawberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Apple Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Apples]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Apricots]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Dates]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Golden Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Grape Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Raisins]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Raspberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Strawberry Fruit Roll]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Best Choice].[Best Choice Beef Jerky]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Fast].[Fast Beef Jerky]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Fort West].[Fort West Beef Jerky]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Horatio].[Horatio Beef Jerky]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Nationeel].[Nationeel Beef Jerky]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice Buttered Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice Low Fat Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice No Salt Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast Buttered Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast Low Fat Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast No Salt Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West Buttered Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West Low Fat Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West No Salt Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Buttered Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Low Fat Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio No Salt Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel Buttered Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel Low Fat Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel No Salt Popcorn]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Best Choice].[Best Choice Salted Pretzels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Fast].[Fast Salted Pretzels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Fort West].[Fort West Salted Pretzels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Horatio].[Horatio Salted Pretzels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Nationeel].[Nationeel Salted Pretzels]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Malted Milk Balls]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Mint Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Semi-Sweet Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Tasty Candy Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic White Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Malted Milk Balls]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Mint Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Semi-Sweet Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Tasty Candy Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice White Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Malted Milk Balls]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Mint Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Semi-Sweet Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Tasty Candy Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast White Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Malted Milk Balls]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Mint Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Semi-Sweet Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Tasty Candy Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial White Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Malted Milk Balls]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Mint Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Semi-Sweet Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Tasty Candy Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher White Chocolate Bar]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Gum].[Atomic].[Atomic Bubble Gum]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Gum].[Choice].[Choice Bubble Gum]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Gum].[Gulf Coast].[Gulf Coast Bubble Gum]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Gum].[Musial].[Musial Bubble Gum]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Gum].[Thresher].[Thresher Bubble Gum]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Atomic].[Atomic Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Atomic].[Atomic Spicy Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Choice].[Choice Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Choice].[Choice Spicy Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Gulf Coast].[Gulf Coast Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Gulf Coast].[Gulf Coast Spicy Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Musial].[Musial Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Musial].[Musial Spicy Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Thresher].[Thresher Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Thresher].[Thresher Spicy Mints]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Manicotti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Ravioli]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Spaghetti]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Colossal].[Colossal Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Colossal].[Colossal Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Discover].[Discover Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Discover].[Discover Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Jardon].[Jardon Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Jardon].[Jardon Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Medalist].[Medalist Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Medalist].[Medalist Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Monarch].[Monarch Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Monarch].[Monarch Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Shady Lake].[Shady Lake Rice Medly]}
{[Store].[Stores].[USA], [Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Shady Lake].[Shady Lake Thai Rice]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[ADJ].[ADJ Rosy Sunglasses]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[King].[King Rosy Sunglasses]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Prelude].[Prelude Rosy Sunglasses]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Symphony].[Symphony Rosy Sunglasses]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Toretti].[Toretti Rosy Sunglasses]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Akron].[Akron Eyeglass Screwdriver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Black Tie].[Black Tie Eyeglass Screwdriver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Framton].[Framton Eyeglass Screwdriver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[James Bay].[James Bay Eyeglass Screwdriver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Queen].[Queen Eyeglass Screwdriver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Akron].[Akron City Map]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Black Tie].[Black Tie City Map]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Framton].[Framton City Map]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[James Bay].[James Bay City Map]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Queen].[Queen City Map]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Bird Call].[Bird Call Silky Smooth Hair Conditioner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Consolidated].[Consolidated Silky Smooth Hair Conditioner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Faux Products].[Faux Products Silky Smooth Hair Conditioner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Hilltop].[Hilltop Silky Smooth Hair Conditioner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Steady].[Steady Silky Smooth Hair Conditioner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Bird Call].[Bird Call Laundry Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Bird Call].[Bird Call Mint Mouthwash]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Consolidated].[Consolidated Laundry Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Consolidated].[Consolidated Mint Mouthwash]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Faux Products].[Faux Products Laundry Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Faux Products].[Faux Products Mint Mouthwash]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Laundry Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Steady].[Steady Laundry Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Steady].[Steady Mint Mouthwash]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Apricot Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Conditioning Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Extra Moisture Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Apricot Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Conditioning Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Extra Moisture Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Apricot Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Conditioning Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Extra Moisture Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Apricot Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Conditioning Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Extra Moisture Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Apricot Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Conditioning Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Extra Moisture Shampoo]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Bird Call].[Bird Call Angled Toothbrush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Consolidated].[Consolidated Angled Toothbrush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Faux Products].[Faux Products Angled Toothbrush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Hilltop].[Hilltop Angled Toothbrush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Steady].[Steady Angled Toothbrush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Bird Call].[Bird Call Childrens Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Bird Call].[Bird Call Multi-Symptom Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Consolidated].[Consolidated Childrens Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Consolidated].[Consolidated Multi-Symptom Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Faux Products].[Faux Products Childrens Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Faux Products].[Faux Products Multi-Symptom Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Hilltop].[Hilltop Childrens Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Hilltop].[Hilltop Multi-Symptom Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Steady].[Steady Childrens Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Steady].[Steady Multi-Symptom Cold Remedy]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Bird Call].[Bird Call Dishwasher Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Bird Call].[Bird Call HCL Nasal Spray]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Consolidated].[Consolidated Dishwasher Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Consolidated].[Consolidated HCL Nasal Spray]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Faux Products].[Faux Products Dishwasher Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Faux Products].[Faux Products HCL Nasal Spray]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Hilltop].[Hilltop Dishwasher Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Hilltop].[Hilltop HCL Nasal Spray]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Steady].[Steady Dishwasher Detergent]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Steady].[Steady HCL Nasal Spray]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Deodorant]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Tartar Control Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Whitening Toothpast]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Deodorant]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Tartar Control Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Whitening Toothpast]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Deodorant]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Tartar Control Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Whitening Toothpast]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Deodorant]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Tartar Control Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Whitening Toothpast]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Deodorant]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Tartar Control Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Toothpaste]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Whitening Toothpast]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Bird Call].[Bird Call 200 MG Acetominifen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Consolidated].[Consolidated 200 MG Acetominifen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Faux Products].[Faux Products 200 MG Acetominifen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Hilltop].[Hilltop 200 MG Acetominifen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Steady].[Steady 200 MG Acetominifen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Bird Call].[Bird Call Buffered Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Bird Call].[Bird Call Childrens Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Consolidated].[Consolidated Buffered Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Consolidated].[Consolidated Childrens Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Faux Products].[Faux Products Buffered Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Faux Products].[Faux Products Childrens Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Hilltop].[Hilltop Buffered Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Hilltop].[Hilltop Childrens Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Steady].[Steady Buffered Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Steady].[Steady Childrens Aspirin]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Bird Call].[Bird Call 200 MG Ibuprofen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Consolidated].[Consolidated 200 MG Ibuprofen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Faux Products].[Faux Products 200 MG Ibuprofen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Hilltop].[Hilltop 200 MG Ibuprofen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Steady].[Steady 200 MG Ibuprofen]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Cormorant].[Cormorant Economy Toilet Brush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Denny].[Denny Economy Toilet Brush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[High Quality].[High Quality Economy Toilet Brush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Red Wing].[Red Wing Economy Toilet Brush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Sunset].[Sunset Economy Toilet Brush]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Cormorant].[Cormorant Bees Wax Candles]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Denny].[Denny Bees Wax Candles]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[High Quality].[High Quality Bees Wax Candles]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Red Wing].[Red Wing Bees Wax Candles]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Sunset].[Sunset Bees Wax Candles]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Counter Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Glass Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Toilet Bowl Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Counter Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Glass Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Toilet Bowl Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Counter Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Glass Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Toilet Bowl Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Counter Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Glass Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Toilet Bowl Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Counter Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Glass Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Toilet Bowl Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Cormorant].[Cormorant Room Freshener]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Denny].[Denny Room Freshener]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[High Quality].[High Quality Room Freshener]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Red Wing].[Red Wing Room Freshener]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Sunset].[Sunset Room Freshener]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AAA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant C-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant D-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AAA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny C-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny D-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality AA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality AAA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality C-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality D-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing AA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing AAA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing C-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing D-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset AA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset AAA-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset C-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset D-Size Batteries]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 100 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 25 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 60 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 75 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 100 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 25 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 60 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 75 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 100 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 25 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 60 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 75 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 100 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 25 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 60 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 75 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 100 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 25 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 60 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 75 Watt Lightbulb]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Cormorant].[Cormorant Scissors]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Cormorant].[Cormorant Screw Driver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Denny].[Denny Scissors]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Denny].[Denny Screw Driver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[High Quality].[High Quality Scissors]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[High Quality].[High Quality Screw Driver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Red Wing].[Red Wing Scissors]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Red Wing].[Red Wing Screw Driver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Sunset].[Sunset Scissors]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Sunset].[Sunset Screw Driver]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Cormorant].[Cormorant Copper Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Cormorant].[Cormorant Silver Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Denny].[Denny Copper Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Denny].[Denny Silver Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[High Quality].[High Quality Copper Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[High Quality].[High Quality Silver Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Red Wing].[Red Wing Copper Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Red Wing].[Red Wing Silver Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Sunset].[Sunset Copper Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Sunset].[Sunset Silver Cleaner]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant].[Cormorant Copper Pot Scrubber]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny].[Denny Copper Pot Scrubber]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality].[High Quality Copper Pot Scrubber]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing].[Red Wing Copper Pot Scrubber]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Sunset].[Sunset Copper Pot Scrubber]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant].[Cormorant Frying Pan]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny].[Denny Frying Pan]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality].[High Quality Frying Pan]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing].[Red Wing Frying Pan]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset].[Sunset Frying Pan]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Cormorant].[Cormorant Large Sponge]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Denny].[Denny Large Sponge]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[High Quality].[High Quality Large Sponge]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Red Wing].[Red Wing Large Sponge]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Sunset].[Sunset Large Sponge]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Cormorant].[Cormorant Paper Cups]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Cormorant].[Cormorant Paper Plates]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Denny].[Denny Paper Cups]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Denny].[Denny Paper Plates]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[High Quality].[High Quality Paper Cups]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[High Quality].[High Quality Paper Plates]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Red Wing].[Red Wing Paper Cups]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Red Wing].[Red Wing Paper Plates]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Sunset].[Sunset Paper Cups]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Sunset].[Sunset Paper Plates]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Paper Towels]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Scented Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Scented Toilet Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Soft Napkins]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Tissues]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Toilet Paper]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Paper Towels]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Scented Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Scented Toilet Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Soft Napkins]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Tissues]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Toilet Paper]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Paper Towels]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Scented Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Scented Toilet Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Soft Napkins]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Tissues]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Toilet Paper]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Paper Towels]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Scented Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Scented Toilet Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Soft Napkins]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Tissues]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Toilet Paper]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Paper Towels]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Scented Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Scented Toilet Tissue]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Soft Napkins]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Tissues]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Toilet Paper]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Forks]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Knives]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Spoons]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Forks]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Knives]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Spoons]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Forks]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Knives]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Spoons]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Forks]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Knives]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Spoons]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Forks]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Knives]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Spoons]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Dollar].[Dollar Monthly Auto Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Excel].[Excel Monthly Auto Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Gauss].[Gauss Monthly Auto Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Mighty Good].[Mighty Good Monthly Auto Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Robust].[Robust Monthly Auto Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Dollar].[Dollar Monthly Computer Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Excel].[Excel Monthly Computer Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Gauss].[Gauss Monthly Computer Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Mighty Good].[Mighty Good Monthly Computer Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Robust].[Robust Monthly Computer Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Dollar].[Dollar Monthly Fashion Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Excel].[Excel Monthly Fashion Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Gauss].[Gauss Monthly Fashion Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Mighty Good].[Mighty Good Monthly Fashion Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Robust].[Robust Monthly Fashion Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Dollar].[Dollar Monthly Home Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Excel].[Excel Monthly Home Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Gauss].[Gauss Monthly Home Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Mighty Good].[Mighty Good Monthly Home Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Robust].[Robust Monthly Home Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Dollar].[Dollar Monthly Sports Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Excel].[Excel Monthly Sports Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Gauss].[Gauss Monthly Sports Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Mighty Good].[Mighty Good Monthly Sports Magazine]}
{[Store].[Stores].[USA], [Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]}
Row #0: 154
Row #0: 115
Row #0: 175
Row #0: 210
Row #0: 187
Row #0: 175
Row #0: 145
Row #0: 161
Row #0: 174
Row #0: 187
Row #0: 163
Row #0: 192
Row #0: 146
Row #0: 148
Row #0: 137
Row #0: 159
Row #0: 209
Row #0: 189
Row #0: 210
Row #0: 198
Row #0: 228
Row #0: 172
Row #0: 136
Row #0: 170
Row #0: 214
Row #0: 152
Row #0: 184
Row #0: 159
Row #0: 140
Row #0: 164
Row #0: 129
Row #0: 172
Row #0: 186
Row #0: 160
Row #0: 185
Row #0: 173
Row #0: 175
Row #0: 140
Row #0: 185
Row #0: 180
Row #0: 193
Row #0: 189
Row #0: 184
Row #0: 172
Row #0: 148
Row #0: 151
Row #0: 153
Row #0: 180
Row #0: 160
Row #0: 142
Row #0: 187
Row #0: 166
Row #0: 162
Row #0: 159
Row #0: 234
Row #0: 180
Row #0: 178
Row #0: 155
Row #0: 180
Row #0: 134
Row #0: 167
Row #0: 176
Row #0: 125
Row #0: 143
Row #0: 147
Row #0: 179
Row #0: 160
Row #0: 178
Row #0: 168
Row #0: 176
Row #0: 137
Row #0: 153
Row #0: 238
Row #0: 171
Row #0: 151
Row #0: 171
Row #0: 167
Row #0: 155
Row #0: 175
Row #0: 134
Row #0: 158
Row #0: 189
Row #0: 233
Row #0: 194
Row #0: 187
Row #0: 194
Row #0: 150
Row #0: 183
Row #0: 159
Row #0: 189
Row #0: 168
Row #0: 167
Row #0: 179
Row #0: 190
Row #0: 154
Row #0: 195
Row #0: 141
Row #0: 152
Row #0: 160
Row #0: 157
Row #0: 164
Row #0: 209
Row #0: 160
Row #0: 119
Row #0: 252
Row #0: 215
Row #0: 202
Row #0: 192
Row #0: 133
Row #0: 170
Row #0: 137
Row #0: 169
Row #0: 161
Row #0: 208
Row #0: 156
Row #0: 181
Row #0: 162
Row #0: 83
Row #0: 130
Row #0: 193
Row #0: 189
Row #0: 177
Row #0: 110
Row #0: 133
Row #0: 163
Row #0: 212
Row #0: 131
Row #0: 175
Row #0: 175
Row #0: 234
Row #0: 155
Row #0: 145
Row #0: 140
Row #0: 159
Row #0: 168
Row #0: 190
Row #0: 177
Row #0: 227
Row #0: 197
Row #0: 168
Row #0: 160
Row #0: 133
Row #0: 174
Row #0: 151
Row #0: 143
Row #0: 163
Row #0: 160
Row #0: 145
Row #0: 165
Row #0: 182
Row #0: 193
Row #0: 182
Row #0: 161
Row #0: 204
Row #0: 215
Row #0: 227
Row #0: 189
Row #0: 167
Row #0: 127
Row #0: 165
Row #0: 164
Row #0: 149
Row #0: 186
Row #0: 166
Row #0: 196
Row #0: 171
Row #0: 166
Row #0: 131
Row #0: 157
Row #0: 181
Row #0: 177
Row #0: 177
Row #0: 228
Row #0: 155
Row #0: 194
Row #0: 207
Row #0: 226
Row #0: 188
Row #0: 178
Row #0: 140
Row #0: 154
Row #0: 166
Row #0: 167
Row #0: 165
Row #0: 169
Row #0: 152
Row #0: 200
Row #0: 154
Row #0: 163
Row #0: 198
Row #0: 175
Row #0: 162
Row #0: 187
Row #0: 166
Row #0: 167
Row #0: 156
Row #0: 138
Row #0: 158
Row #0: 168
Row #0: 199
Row #0: 184
Row #0: 143
Row #0: 182
Row #0: 159
Row #0: 162
Row #0: 151
Row #0: 114
Row #0: 199
Row #0: 168
Row #0: 139
Row #0: 158
Row #0: 129
Row #0: 165
Row #0: 169
Row #0: 160
Row #0: 199
Row #0: 222
Row #0: 177
Row #0: 97
Row #0: 191
Row #0: 166
Row #0: 185
Row #0: 245
Row #0: 197
Row #0: 141
Row #0: 152
Row #0: 135
Row #0: 172
Row #0: 146
Row #0: 149
Row #0: 150
Row #0: 229
Row #0: 150
Row #0: 196
Row #0: 201
Row #0: 167
Row #0: 195
Row #0: 142
Row #0: 139
Row #0: 156
Row #0: 120
Row #0: 193
Row #0: 187
Row #0: 220
Row #0: 166
Row #0: 163
Row #0: 156
Row #0: 169
Row #0: 208
Row #0: 196
Row #0: 142
Row #0: 130
Row #0: 160
Row #0: 191
Row #0: 155
Row #0: 190
Row #0: 227
Row #0: 185
Row #0: 166
Row #0: 188
Row #0: 194
Row #0: 213
Row #0: 189
Row #0: 156
Row #0: 177
Row #0: 188
Row #0: 127
Row #0: 179
Row #0: 186
Row #0: 188
Row #0: 201
Row #0: 167
Row #0: 182
Row #0: 178
Row #0: 185
Row #0: 189
Row #0: 175
Row #0: 167
Row #0: 192
Row #0: 173
Row #0: 155
Row #0: 143
Row #0: 190
Row #0: 182
Row #0: 240
Row #0: 207
Row #0: 168
Row #0: 193
Row #0: 180
Row #0: 167
Row #0: 79
Row #0: 136
Row #0: 153
Row #0: 171
Row #0: 176
Row #0: 154
Row #0: 193
Row #0: 168
Row #0: 137
Row #0: 163
Row #0: 158
Row #0: 155
Row #0: 169
Row #0: 175
Row #0: 157
Row #0: 136
Row #0: 162
Row #0: 155
Row #0: 148
Row #0: 115
Row #0: 158
Row #0: 159
Row #0: 137
Row #0: 159
Row #0: 139
Row #0: 194
Row #0: 129
Row #0: 173
Row #0: 135
Row #0: 154
Row #0: 155
Row #0: 147
Row #0: 151
Row #0: 205
Row #0: 179
Row #0: 196
Row #0: 194
Row #0: 171
Row #0: 267
Row #0: 198
Row #0: 136
Row #0: 147
Row #0: 205
Row #0: 214
Row #0: 189
Row #0: 165
Row #0: 184
Row #0: 177
Row #0: 167
Row #0: 124
Row #0: 127
Row #0: 119
Row #0: 160
Row #0: 178
Row #0: 166
Row #0: 191
Row #0: 158
Row #0: 164
Row #0: 140
Row #0: 159
Row #0: 138
Row #0: 137
Row #0: 188
Row #0: 182
Row #0: 162
Row #0: 203
Row #0: 175
Row #0: 158
Row #0: 203
Row #0: 166
Row #0: 166
Row #0: 217
Row #0: 163
Row #0: 177
Row #0: 182
Row #0: 190
Row #0: 176
Row #0: 147
Row #0: 175
Row #0: 175
Row #0: 158
Row #0: 209
Row #0: 166
Row #0: 163
Row #0: 157
Row #0: 154
Row #0: 236
Row #0: 130
Row #0: 135
Row #0: 205
Row #0: 189
Row #0: 208
Row #0: 175
Row #0: 198
Row #0: 148
Row #0: 191
Row #0: 165
Row #0: 227
Row #0: 217
Row #0: 198
Row #0: 186
Row #0: 155
Row #0: 189
Row #0: 193
Row #0: 151
Row #0: 157
Row #0: 150
Row #0: 204
Row #0: 157
Row #0: 167
Row #0: 174
Row #0: 142
Row #0: 194
Row #0: 156
Row #0: 170
Row #0: 212
Row #0: 217
Row #0: 130
Row #0: 148
Row #0: 145
Row #0: 184
Row #0: 166
Row #0: 168
Row #0: 173
Row #0: 191
Row #0: 132
Row #0: 175
Row #0: 129
Row #0: 150
Row #0: 151
Row #0: 174
Row #0: 197
Row #0: 148
Row #0: 163
Row #0: 191
Row #0: 176
Row #0: 199
Row #0: 166
Row #0: 195
Row #0: 223
Row #0: 199
Row #0: 188
Row #0: 161
Row #0: 189
Row #0: 163
Row #0: 168
Row #0: 172
Row #0: 167
Row #0: 194
Row #0: 205
Row #0: 150
Row #0: 204
Row #0: 179
Row #0: 142
Row #0: 173
Row #0: 204
Row #0: 175
Row #0: 187
Row #0: 193
Row #0: 188
Row #0: 192
Row #0: 181
Row #0: 197
Row #0: 209
Row #0: 132
Row #0: 166
Row #0: 198
Row #0: 169
Row #0: 146
Row #0: 163
Row #0: 207
Row #0: 202
Row #0: 190
Row #0: 164
Row #0: 190
Row #0: 184
Row #0: 159
Row #0: 153
Row #0: 164
Row #0: 116
Row #0: 174
Row #0: 160
Row #0: 168
Row #0: 155
Row #0: 180
Row #0: 206
Row #0: 182
Row #0: 161
Row #0: 164
Row #0: 200
Row #0: 185
Row #0: 179
Row #0: 159
Row #0: 200
Row #0: 187
Row #0: 181
Row #0: 139
Row #0: 161
Row #0: 174
Row #0: 192
Row #0: 159
Row #0: 153
Row #0: 171
Row #0: 158
Row #0: 133
Row #0: 176
Row #0: 178
Row #0: 158
Row #0: 129
Row #0: 206
Row #0: 194
Row #0: 150
Row #0: 168
Row #0: 140
Row #0: 149
Row #0: 159
Row #0: 121
Row #0: 161
Row #0: 200
Row #0: 173
Row #0: 216
Row #0: 174
Row #0: 175
Row #0: 124
Row #0: 177
Row #0: 140
Row #0: 185
Row #0: 187
Row #0: 190
Row #0: 184
Row #0: 198
Row #0: 176
Row #0: 163
Row #0: 183
Row #0: 168
Row #0: 196
Row #0: 195
Row #0: 166
Row #0: 167
Row #0: 161
Row #0: 174
Row #0: 150
Row #0: 174
Row #0: 183
Row #0: 164
Row #0: 182
Row #0: 162
Row #0: 205
Row #0: 141
Row #0: 148
Row #0: 176
Row #0: 211
Row #0: 184
Row #0: 123
Row #0: 144
Row #0: 178
Row #0: 167
Row #0: 121
Row #0: 171
Row #0: 135
Row #0: 178
Row #0: 171
Row #0: 156
Row #0: 206
Row #0: 231
Row #0: 141
Row #0: 173
Row #0: 165
Row #0: 160
Row #0: 168
Row #0: 160
Row #0: 204
Row #0: 196
Row #0: 150
Row #0: 163
Row #0: 174
Row #0: 170
Row #0: 177
Row #0: 172
Row #0: 170
Row #0: 153
Row #0: 183
Row #0: 188
Row #0: 177
Row #0: 204
Row #0: 210
Row #0: 163
Row #0: 153
Row #0: 171
Row #0: 157
Row #0: 186
Row #0: 158
Row #0: 214
Row #0: 194
Row #0: 147
Row #0: 172
Row #0: 137
Row #0: 183
Row #0: 169
Row #0: 186
Row #0: 170
Row #0: 207
Row #0: 154
Row #0: 170
Row #0: 174
Row #0: 168
Row #0: 161
Row #0: 148
Row #0: 157
Row #0: 179
Row #0: 136
Row #0: 162
Row #0: 160
Row #0: 152
Row #0: 144
Row #0: 126
Row #0: 130
Row #0: 137
Row #0: 133
Row #0: 203
Row #0: 184
Row #0: 220
Row #0: 166
Row #0: 168
Row #0: 169
Row #0: 176
Row #0: 178
Row #0: 175
Row #0: 226
Row #0: 201
Row #0: 163
Row #0: 110
Row #0: 173
Row #0: 167
Row #0: 160
Row #0: 140
Row #0: 165
Row #0: 149
Row #0: 169
Row #0: 167
Row #0: 199
Row #0: 180
Row #0: 134
Row #0: 192
Row #0: 200
Row #0: 151
Row #0: 193
Row #0: 142
Row #0: 152
Row #0: 184
Row #0: 199
Row #0: 126
Row #0: 172
Row #0: 148
Row #0: 209
Row #0: 154
Row #0: 150
Row #0: 180
Row #0: 175
Row #0: 242
Row #0: 218
Row #0: 205
Row #0: 163
Row #0: 192
Row #0: 146
Row #0: 141
Row #0: 163
Row #0: 180
Row #0: 211
Row #0: 149
Row #0: 166
Row #0: 190
Row #0: 182
Row #0: 184
Row #0: 197
Row #0: 173
Row #0: 232
Row #0: 224
Row #0: 182
Row #0: 146
Row #0: 165
Row #0: 178
Row #0: 176
Row #0: 192
Row #0: 152
Row #0: 161
Row #0: 157
Row #0: 147
Row #0: 171
Row #0: 157
Row #0: 129
Row #0: 175
Row #0: 166
Row #0: 180
Row #0: 185
Row #0: 194
Row #0: 222
Row #0: 170
Row #0: 161
Row #0: 147
Row #0: 180
Row #0: 158
Row #0: 217
Row #0: 107
Row #0: 153
Row #0: 213
Row #0: 196
Row #0: 119
Row #0: 163
Row #0: 169
Row #0: 196
Row #0: 202
Row #0: 152
Row #0: 161
Row #0: 169
Row #0: 174
Row #0: 134
Row #0: 168
Row #0: 189
Row #0: 150
Row #0: 186
Row #0: 179
Row #0: 202
Row #0: 149
Row #0: 186
Row #0: 246
Row #0: 185
Row #0: 186
Row #0: 184
Row #0: 192
Row #0: 157
Row #0: 203
Row #0: 136
Row #0: 146
Row #0: 139
Row #0: 134
Row #0: 155
Row #0: 135
Row #0: 120
Row #0: 141
Row #0: 173
Row #0: 199
Row #0: 149
Row #0: 144
Row #0: 231
Row #0: 204
Row #0: 183
Row #0: 140
Row #0: 168
Row #0: 157
Row #0: 180
Row #0: 134
Row #0: 218
Row #0: 136
Row #0: 155
Row #0: 150
Row #0: 149
Row #0: 169
Row #0: 195
Row #0: 231
Row #0: 200
Row #0: 201
Row #0: 183
Row #0: 202
Row #0: 166
Row #0: 192
Row #0: 158
Row #0: 186
Row #0: 194
Row #0: 156
Row #0: 174
Row #0: 137
Row #0: 192
Row #0: 160
Row #0: 168
Row #0: 133
Row #0: 177
Row #0: 162
Row #0: 160
Row #0: 205
Row #0: 155
Row #0: 156
Row #0: 157
Row #0: 210
Row #0: 185
Row #0: 174
Row #0: 153
Row #0: 169
Row #0: 190
Row #0: 184
Row #0: 182
Row #0: 180
Row #0: 170
Row #0: 127
Row #0: 208
Row #0: 188
Row #0: 174
Row #0: 195
Row #0: 166
Row #0: 151
Row #0: 170
Row #0: 142
Row #0: 184
Row #0: 162
Row #0: 134
Row #0: 168
Row #0: 190
Row #0: 167
Row #0: 170
Row #0: 157
Row #0: 150
Row #0: 235
Row #0: 216
Row #0: 207
Row #0: 162
Row #0: 185
Row #0: 230
Row #0: 173
Row #0: 176
Row #0: 173
Row #0: 212
Row #0: 168
Row #0: 120
Row #0: 131
Row #0: 107
Row #0: 150
Row #0: 94
Row #0: 143
Row #0: 151
Row #0: 164
Row #0: 144
Row #0: 156
Row #0: 179
Row #0: 157
Row #0: 159
Row #0: 142
Row #0: 213
Row #0: 203
Row #0: 182
Row #0: 167
Row #0: 162
Row #0: 154
Row #0: 191
Row #0: 162
Row #0: 165
Row #0: 194
Row #0: 172
Row #0: 151
Row #0: 127
Row #0: 161
Row #0: 148
Row #0: 185
Row #0: 202
Row #0: 151
Row #0: 154
Row #0: 150
Row #0: 152
Row #0: 206
Row #0: 162
Row #0: 199
Row #0: 146
Row #0: 152
Row #0: 203
Row #0: 143
Row #0: 167
Row #0: 211
Row #0: 172
Row #0: 188
Row #0: 148
Row #0: 138
Row #0: 137
Row #0: 210
Row #0: 216
Row #0: 172
Row #0: 160
Row #0: 184
Row #0: 213
Row #0: 202
Row #0: 184
Row #0: 185
Row #0: 167
Row #0: 168
Row #0: 168
Row #0: 165
Row #0: 180
Row #0: 160
Row #0: 161
Row #0: 204
Row #0: 201
Row #0: 156
Row #0: 169
Row #0: 143
Row #0: 179
Row #0: 178
Row #0: 171
Row #0: 123
Row #0: 168
Row #0: 180
Row #0: 169
Row #0: 175
Row #0: 192
Row #0: 177
Row #0: 207
Row #0: 168
Row #0: 127
Row #0: 173
Row #0: 161
Row #0: 207
Row #0: 172
Row #0: 182
Row #0: 154
Row #0: 176
Row #0: 126
Row #0: 210
Row #0: 133
Row #0: 157
Row #0: 153
Row #0: 138
Row #0: 195
Row #0: 205
Row #0: 180
Row #0: 257
Row #0: 202
Row #0: 195
Row #0: 184
Row #0: 211
Row #0: 165
Row #0: 190
Row #0: 239
Row #0: 148
Row #0: 173
Row #0: 180
Row #0: 143
Row #0: 224
Row #0: 192
Row #0: 187
Row #0: 199
Row #0: 221
Row #0: 200
Row #0: 195
Row #0: 220
Row #0: 196
Row #0: 160
Row #0: 139
Row #0: 159
Row #0: 139
Row #0: 148
Row #0: 138
Row #0: 159
Row #0: 186
Row #0: 163
Row #0: 156
Row #0: 176
Row #0: 152
Row #0: 168
Row #0: 162
Row #0: 137
Row #0: 154
Row #0: 143
Row #0: 156
Row #0: 185
Row #0: 151
Row #0: 122
Row #0: 157
Row #0: 161
Row #0: 145
Row #0: 190
Row #0: 174
Row #0: 172
Row #0: 182
Row #0: 182
Row #0: 192
Row #0: 211
Row #0: 186
Row #0: 231
Row #0: 177
Row #0: 197
Row #0: 194
Row #0: 170
Row #0: 188
Row #0: 183
Row #0: 143
Row #0: 148
Row #0: 151
Row #0: 142
Row #0: 184
Row #0: 201
Row #0: 188
Row #0: 207
Row #0: 186
Row #0: 166
Row #0: 158
Row #0: 173
Row #0: 165
Row #0: 209
Row #0: 131
Row #0: 160
Row #0: 193
Row #0: 175
Row #0: 189
Row #0: 148
Row #0: 171
Row #0: 197
Row #0: 158
Row #0: 104
Row #0: 214
Row #0: 128
Row #0: 191
Row #0: 157
Row #0: 149
Row #0: 141
Row #0: 153
Row #0: 160
Row #0: 154
Row #0: 175
Row #0: 186
Row #0: 174
Row #0: 181
Row #0: 119
Row #0: 186
Row #0: 178
Row #0: 198
Row #0: 195
Row #0: 172
Row #0: 131
Row #0: 124
Row #0: 140
Row #0: 106
Row #0: 159
Row #0: 234
Row #0: 159
Row #0: 139
Row #0: 198
Row #0: 215
Row #0: 197
Row #0: 160
Row #0: 163
Row #0: 172
Row #0: 134
Row #0: 184
Row #0: 146
Row #0: 159
Row #0: 132
Row #0: 155
Row #0: 228
Row #0: 174
Row #0: 174
Row #0: 218
Row #0: 177
Row #0: 170
Row #0: 151
Row #0: 142
Row #0: 174
Row #0: 189
Row #0: 158
Row #0: 155
Row #0: 142
Row #0: 141
Row #0: 175
Row #0: 197
Row #0: 144
Row #0: 159
Row #0: 167
Row #0: 183
Row #0: 159
Row #0: 207
Row #0: 214
Row #0: 196
Row #0: 180
Row #0: 163
Row #0: 174
Row #0: 154
Row #0: 188
Row #0: 162
Row #0: 148
Row #0: 213
Row #0: 180
Row #0: 176
Row #0: 164
Row #0: 178
Row #0: 140
Row #0: 194
Row #0: 143
Row #0: 121
Row #0: 162
Row #0: 218
Row #0: 204
Row #0: 173
Row #0: 142
Row #0: 170
Row #0: 130
Row #0: 173
Row #0: 181
Row #0: 197
Row #0: 168
Row #0: 198
Row #0: 172
Row #0: 183
Row #0: 184
Row #0: 186
Row #0: 118
Row #0: 107
Row #0: 152
Row #0: 195
Row #0: 189
Row #0: 189
Row #0: 184
Row #0: 172
Row #0: 135
Row #0: 180
Row #0: 130
Row #0: 220
Row #0: 169
Row #0: 153
Row #0: 131
Row #0: 210
Row #0: 145
Row #0: 171
Row #0: 196
Row #0: 117
Row #0: 158
Row #0: 121
Row #0: 197
Row #0: 198
Row #0: 175
Row #0: 164
Row #0: 195
Row #0: 142
Row #0: 181
Row #0: 186
Row #0: 155
Row #0: 175
Row #0: 149
Row #0: 179
Row #0: 174
Row #0: 158
Row #0: 158
Row #0: 162
Row #0: 205
Row #0: 114
Row #0: 167
Row #0: 110
Row #0: 150
Row #0: 149
Row #0: 175
Row #0: 187
Row #0: 170
Row #0: 154
Row #0: 173
Row #0: 152
Row #0: 163
Row #0: 154
Row #0: 181
Row #0: 179
Row #0: 199
Row #0: 156
Row #0: 188
Row #0: 178
Row #0: 175
Row #0: 210
Row #0: 189
Row #0: 177
Row #0: 129
Row #0: 141
Row #0: 149
Row #0: 157
Row #0: 191
Row #0: 166
Row #0: 149
Row #0: 169
Row #0: 185
Row #0: 193
Row #0: 224
Row #0: 130
Row #0: 193
Row #0: 216
Row #0: 188
Row #0: 167
Row #0: 138
Row #0: 150
Row #0: 258
Row #0: 179
Row #0: 201
Row #0: 219
Row #0: 139
Row #0: 197
Row #0: 152
Row #0: 168
Row #0: 189
Row #0: 215
Row #0: 155
Row #0: 175
Row #0: 192
Row #0: 158
Row #0: 163
Row #0: 131
Row #0: 164
Row #0: 128
Row #0: 184
Row #0: 165
Row #0: 156
Row #0: 177
Row #0: 191
Row #0: 189
Row #0: 188
Row #0: 176
Row #0: 203
Row #0: 167
Row #0: 166
Row #0: 176
Row #0: 203
Row #0: 161
Row #0: 150
Row #0: 156
Row #0: 192
Row #0: 174
Row #0: 177
Row #0: 158
Row #0: 161
Row #0: 157
Row #0: 184
Row #0: 125
Row #0: 155
Row #0: 191
Row #0: 164
Row #0: 174
Row #0: 169
Row #0: 184
Row #0: 220
Row #0: 221
Row #0: 104
Row #0: 192
Row #0: 157
Row #0: 176
Row #0: 186
Row #0: 182
Row #0: 170
Row #0: 175
Row #0: 145
Row #0: 160
Row #0: 153
Row #0: 159
Row #0: 198
Row #0: 175
Row #0: 183
Row #0: 204
Row #0: 152
Row #0: 165
Row #0: 168
Row #0: 136
Row #0: 192
Row #0: 175
Row #0: 146
Row #0: 197
Row #0: 204
Row #0: 154
Row #0: 199
Row #0: 193
Row #0: 185
Row #0: 172
Row #0: 189
Row #0: 184
Row #0: 184
Row #0: 158
Row #0: 183
Row #0: 156
Row #0: 160
Row #0: 127
Row #0: 159
Row #0: 210
Row #0: 205
Row #0: 135
Row #0: 204
Row #0: 183
Row #0: 164
Row #0: 165
Row #0: 183
Row #0: 167
Row #0: 162
Row #0: 174
Row #0: 111
Row #0: 160
Row #0: 196
Row #0: 169
Row #0: 177
Row #0: 203
Row #0: 213
Row #0: 177
Row #0: 199
Row #0: 109
Row #0: 171
Row #0: 148
Row #0: 218
Row #0: 167
Row #0: 195
Row #0: 167
Row #0: 161
Row #0: 141
Row #0: 165
Row #0: 163
Row #0: 151
Row #0: 219
Row #0: 214
Row #0: 199
Row #0: 183
Row #0: 142
Row #0: 188
Row #0: 124
Row #0: 160
Row #0: 125
Row #0: 164
Row #0: 186
Row #0: 188
Row #0: 160
Row #0: 148
Row #0: 185
Row #0: 179
Row #0: 166
Row #0: 163
Row #0: 190
Row #0: 136
Row #0: 174
Row #0: 174
Row #0: 162
Row #0: 149
Row #0: 184
Row #0: 189
Row #0: 162
Row #0: 149
Row #0: 136
Row #0: 208
Row #0: 202
Row #0: 205
Row #0: 192
Row #0: 164
Row #0: 187
Row #0: 171
Row #0: 191
Row #0: 180
Row #0: 199
Row #0: 189
Row #0: 141
Row #0: 181
Row #0: 210
Row #0: 191
Row #0: 140
Row #0: 172
Row #0: 197
Row #0: 154
Row #0: 122
Row #0: 176
Row #0: 159
Row #0: 144
Row #0: 150
Row #0: 194
Row #0: 168
Row #0: 197
Row #0: 182
Row #0: 190
Row #0: 155
Row #0: 234
Row #0: 190
Row #0: 213
Row #0: 228
Row #0: 203
Row #0: 144
Row #0: 142
Row #0: 158
Row #0: 195
Row #0: 158
Row #0: 159
Row #0: 143
Row #0: 123
Row #0: 143
Row #0: 135
Row #0: 183
Row #0: 164
Row #0: 208
Row #0: 183
Row #0: 150
Row #0: 174
Row #0: 130
Row #0: 183
Row #0: 176
Row #0: 166
Row #0: 151
Row #0: 137
Row #0: 153
Row #0: 226
Row #0: 163
Row #0: 133
Row #0: 149
Row #0: 195
Row #0: 175
Row #0: 178
Row #0: 138
Row #0: 141
Row #0: 176
Row #0: 135
Row #0: 147
Row #0: 151
Row #0: 178
Row #0: 158
Row #0: 201
Row #0: 243
Row #0: 154
Row #0: 220
Row #0: 161
Row #0: 133
Row #0: 153
Row #0: 162
Row #0: 207
Row #0: 210
Row #0: 195
Row #0: 151
Row #0: 171
Row #0: 136
Row #0: 166
Row #0: 163
Row #0: 194
Row #0: 138
Row #0: 141
Row #0: 192
Row #0: 143
Row #0: 143
Row #0: 157
Row #0: 165
Row #0: 155
Row #0: 225
Row #0: 171
Row #0: 156
Row #0: 205
Row #0: 139
Row #0: 209
Row #0: 150
Row #0: 147
Row #0: 163
Row #0: 153
Row #0: 182
Row #0: 132
Row #0: 218
Row #0: 137
Row #0: 143
Row #0: 150
Row #0: 198
Row #0: 195
Row #0: 182
Row #0: 221
Row #0: 231
Row #0: 168
Row #0: 161
Row #0: 200
Row #0: 152
Row #0: 179
Row #0: 148
Row #0: 144
Row #0: 172
Row #0: 184
Row #0: 157
Row #0: 143
Row #0: 205
Row #0: 140
Row #0: 203
Row #0: 186
Row #0: 186
Row #0: 160
Row #0: 155
Row #0: 159
Row #0: 126
Row #0: 170
Row #0: 178
Row #0: 188
Row #0: 190
Row #0: 198
Row #0: 118
Row #0: 129
Row #0: 164
Row #0: 159
Row #0: 164
Row #0: 179
Row #0: 142
Row #0: 178
Row #0: 216
Row #0: 164
Row #0: 157
Row #0: 167
Row #0: 189
Row #0: 163
Row #0: 206
Row #0: 156
Row #0: 163
Row #0: 122
Row #0: 176
Row #0: 174
Row #0: 188
Row #0: 195
Row #0: 137
Row #0: 208
Row #0: 164
Row #0: 174
Row #0: 190
Row #0: 146
Row #0: 130
Row #0: 171
Row #0: 145
Row #0: 141
Row #0: 142
Row #0: 201
Row #0: 140
Row #0: 128
Row #0: 131
Row #0: 221
Row #0: 187
Row #0: 197
Row #0: 195
Row #0: 173
Row #0: 185
Row #0: 122
Row #0: 173
Row #0: 169
Row #0: 198
Row #0: 215
Row #0: 186
Row #0: 167
Row #0: 139
Row #0: 168
Row #0: 229
Row #0: 177
Row #0: 180
Row #0: 155
Row #0: 131
Row #0: 121
Row #0: 179
Row #0: 144
Row #0: 135
Row #0: 143
Row #0: 177
Row #0: 171
Row #0: 164
Row #0: 203
Row #0: 181
Row #0: 191
Row #0: 180
Row #0: 202
Row #0: 184
Row #0: 154
Row #0: 170
Row #0: 170
Row #0: 131
Row #0: 171
Row #0: 134
Row #0: 202
Row #0: 170
Row #0: 169
Row #0: 202
Row #0: 156
Row #0: 174
Row #0: 120
Row #0: 146
Row #0: 220
Row #0: 211
Row #0: 178
Row #0: 176
Row #0: 147
Row #0: 217
Row #0: 170
Row #0: 200
Row #0: 170
Row #0: 152
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0", "product_class"."product_family" as "c1", "product_class"."product_department" as "c2", "product_class"."product_category" as "c3", "product_class"."product_subcategory" as "c4", "product"."brand_name" as "c5", "product"."product_name" as "c6", "product"."product_id" as "c7" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "product" as "product", "product_class" as "product_class" where ("store"."store_country" = 'USA') and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "store"."store_country", "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id" order by CASE WHEN "store"."store_country" IS NULL THEN 1 ELSE 0 END, "store"."store_country" ASC, CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC, CASE WHEN "product_class"."product_category" IS NULL THEN 1 ELSE 0 END, "product_class"."product_category" ASC, CASE WHEN "product_class"."product_subcategory" IS NULL THEN 1 ELSE 0 END, "product_class"."product_subcategory" ASC, CASE WHEN "product"."brand_name" IS NULL THEN 1 ELSE 0 END, "product"."brand_name" ASC, CASE WHEN "product"."product_name" IS NULL THEN 1 ELSE 0 END, "product"."product_name" ASC, CASE WHEN "product"."product_id" IS NULL THEN 1 ELSE 0 END, "product"."product_id" ASC
```

*Calcite:*

```sql
SELECT "store"."store_country", "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "store"."store_country" = 'USA'
GROUP BY "store"."store_country", "product"."product_id", "product"."brand_name", "product"."product_name", "product_class"."product_subcategory", "product_class"."product_category", "product_class"."product_department", "product_class"."product_family"
ORDER BY "store"."store_country", "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id"
```

*Delta:* rowCount matches (1559); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -669 chars (legacy=1737, calcite=1068).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "store_country") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_country") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "product_id") from "product"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_id") AS "c"
FROM "product"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select "store"."store_country" as "c0", "time_by_day"."the_year" as "c1", "product"."product_id" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "store" as "store", "time_by_day" as "time_by_day", "product" as "product" where "store"."store_country" = 'USA' and "time_by_day"."the_year" = 1997 and "sales_fact_1997"."store_id" = "store"."store_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" group by "store"."store_country", "time_by_day"."the_year", "product"."product_id"
```

*Calcite:*

```sql
SELECT "store_country", "the_year", "product_id0" AS "product_id", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
WHERE "store"."store_country" = 'USA') AS "t"
WHERE "t"."the_year" = 1997
GROUP BY "store_country", "the_year", "product_id0"
```

*Delta:* rowCount matches (1559); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 2326 chars (legacy=598, calcite=2924).

### <a name="q-native-topcount-product-names"></a>native-topcount-product-names

**MDX:**

```mdx
select topcount([Product].[Product Name].members, 6, Measures.[Unit Sales]) on 0 from sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Wheat Puffs]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Fast].[Fast Beef Jerky]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Broccoli]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Apple Juice]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Pepper]}
Row #0: 267
Row #0: 258
Row #0: 257
Row #0: 252
Row #0: 246
Row #0: 245
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1", "product_class"."product_category" as "c2", "product_class"."product_subcategory" as "c3", "product"."brand_name" as "c4", "product"."product_name" as "c5", "product"."product_id" as "c6", sum("sales_fact_1997"."unit_sales") as "c7" from "sales_fact_1997" as "sales_fact_1997", "product" as "product", "product_class" as "product_class", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id" order by CASE WHEN sum("sales_fact_1997"."unit_sales") IS NULL THEN 1 ELSE 0 END, sum("sales_fact_1997"."unit_sales") DESC, CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC, CASE WHEN "product_class"."product_category" IS NULL THEN 1 ELSE 0 END, "product_class"."product_category" ASC, CASE WHEN "product_class"."product_subcategory" IS NULL THEN 1 ELSE 0 END, "product_class"."product_subcategory" ASC, CASE WHEN "product"."brand_name" IS NULL THEN 1 ELSE 0 END, "product"."brand_name" ASC, CASE WHEN "product"."product_name" IS NULL THEN 1 ELSE 0 END, "product"."product_name" ASC, CASE WHEN "product"."product_id" IS NULL THEN 1 ELSE 0 END, "product"."product_id" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product"."product_id", "product"."brand_name", "product"."product_name", "product_class"."product_subcategory", "product_class"."product_category", "product_class"."product_department", "product_class"."product_family"
ORDER BY 8 DESC, "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id"
```

*Delta:* rowCount matches (6); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta -827 chars (legacy=1763, calcite=936).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "product_id") from "product"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_id") AS "c"
FROM "product"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product"."product_id" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product" where "time_by_day"."the_year" = 1997 and "product"."product_id" in (404, 549, 609, 948, 952, 1452) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" group by "time_by_day"."the_year", "product"."product_id"
```

*Calcite:*

```sql
SELECT "the_year", "product_id0" AS "product_id", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."product_id0" IN (404, 549, 609, 948, 952, 1452)
GROUP BY "the_year", "product_id0"
```

*Delta:* rowCount matches (6); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1366 chars (legacy=492, calcite=1858).

### <a name="q-native-filter-product-names"></a>native-filter-product-names

**MDX:**

```mdx
select filter([Product].[Product Name].members, Measures.[Unit Sales] > 0) on 0 from sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Light Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Light Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Imported Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure].[Top Measure Light Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Imported Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus].[Walrus Light Beer]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chablis Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Chardonnay Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Light Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good Merlot Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Good].[Good White Zinfandel Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chablis Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Chardonnay Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Light Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl Merlot Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].[Pearl White Zinfandel Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chablis Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Chardonnay Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Light Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Merlot Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth White Zinfandel Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chablis Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Chardonnay Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Light Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure Merlot Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Top Measure].[Top Measure White Zinfandel Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chablis Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Chardonnay Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Light Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus Merlot Wine]}
{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Walrus].[Walrus White Zinfandel Wine]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Cream Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Diet Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent].[Excellent Diet Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Cream Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Diet Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous].[Fabulous Diet Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Cream Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Diet Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner].[Skinner Diet Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Cream Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Diet Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token].[Token Diet Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Cream Soda]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Cola]}
{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Soda]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Apple Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Mango Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent].[Excellent Strawberry Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Apple Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Mango Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous].[Fabulous Strawberry Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Apple Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Mango Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner].[Skinner Strawberry Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Apple Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Mango Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token].[Token Strawberry Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Apple Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Mango Drink]}
{[Product].[Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington].[Washington Strawberry Drink]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[BBB Best].[BBB Best Hot Chocolate]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[CDR].[CDR Hot Chocolate]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Landslide].[Landslide Hot Chocolate]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Plato].[Plato Hot Chocolate]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Chocolate].[Super].[Super Hot Chocolate]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Columbian Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Decaf Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best French Roast Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[BBB Best].[BBB Best Regular Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Columbian Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Decaf Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR French Roast Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[CDR].[CDR Regular Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Columbian Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Decaf Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide French Roast Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Landslide].[Landslide Regular Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Columbian Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Decaf Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato French Roast Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Plato].[Plato Regular Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Columbian Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Decaf Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super French Roast Coffee]}
{[Product].[Products].[Drink].[Beverages].[Hot Beverages].[Coffee].[Super].[Super Regular Coffee]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Apple Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Berry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Cranberry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Excellent].[Excellent Orange Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Apple Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Berry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Cranberry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Fabulous].[Fabulous Orange Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Apple Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Berry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Cranberry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Skinner].[Skinner Orange Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Apple Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Berry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Cranberry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Token].[Token Orange Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Apple Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Berry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Cranberry Juice]}
{[Product].[Products].[Drink].[Beverages].[Pure Juice Beverages].[Juice].[Washington].[Washington Orange Juice]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 1% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker 2% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Buttermilk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Chocolate Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Booker].[Booker Whole Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 1% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson 2% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Buttermilk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Chocolate Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Carlson].[Carlson Whole Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 1% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 2% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Buttermilk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Chocolate Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Whole Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 1% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better 2% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Buttermilk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Chocolate Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Even Better].[Even Better Whole Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 1% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla 2% Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Buttermilk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Chocolate Milk]}
{[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony].[Colony Bagels]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Fantastic].[Fantastic Bagels]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Great].[Great Bagels]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Modell].[Modell Bagels]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Sphinx].[Sphinx Bagels]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Blueberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Cranberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony English Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Colony].[Colony Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Blueberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Cranberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic English Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic].[Fantastic Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Blueberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Cranberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great English Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Great].[Great Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Blueberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Cranberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell English Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Modell].[Modell Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Blueberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Cranberry Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx English Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx].[Sphinx Muffins]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Pumpernickel Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Rye Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony Wheat Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony].[Colony White Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Pumpernickel Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Rye Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic Wheat Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic].[Fantastic White Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Pumpernickel Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Rye Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great Wheat Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great].[Great White Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Pumpernickel Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Rye Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell Wheat Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell].[Modell White Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Pumpernickel Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Rye Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx Wheat Bread]}
{[Product].[Products].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx].[Sphinx White Bread]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Canola Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Corn Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Sesame Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[BBB Best].[BBB Best Vegetable Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Canola Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Corn Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Sesame Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[CDR].[CDR Vegetable Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Canola Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Corn Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Sesame Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Landslide].[Landslide Vegetable Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Canola Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Corn Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Sesame Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Plato].[Plato Vegetable Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Canola Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Corn Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Sesame Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Cooking Oil].[Super].[Super Vegetable Oil]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[BBB Best].[BBB Best Tomato Sauce]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[CDR].[CDR Tomato Sauce]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Landslide].[Landslide Tomato Sauce]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Plato].[Plato Tomato Sauce]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sauces].[Super].[Super Tomato Sauce]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Oregano]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Pepper]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[BBB Best].[BBB Best Salt]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Oregano]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Pepper]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[CDR].[CDR Salt]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Oregano]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Pepper]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Landslide].[Landslide Salt]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Oregano]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Pepper]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Plato].[Plato Salt]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Oregano]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Pepper]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Spices].[Super].[Super Salt]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[BBB Best].[BBB Best Brown Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[BBB Best].[BBB Best White Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[CDR].[CDR Brown Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[CDR].[CDR White Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Landslide].[Landslide Brown Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Landslide].[Landslide White Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Plato].[Plato Brown Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Plato].[Plato White Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Super].[Super Brown Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Baking Goods].[Sugar].[Super].[Super White Sugar]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Apple Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Grape Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[BBB Best].[BBB Best Strawberry Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Apple Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Grape Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[CDR].[CDR Strawberry Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Apple Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Grape Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Landslide].[Landslide Strawberry Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Apple Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Grape Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Plato].[Plato Strawberry Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Apple Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Grape Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jam].[Super].[Super Strawberry Jam]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Apple Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Grape Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[BBB Best].[BBB Best Strawberry Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[CDR].[CDR Apple Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[CDR].[CDR Strawberry Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Apple Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Grape Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Landslide].[Landslide Strawberry Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Apple Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Grape Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Plato].[Plato Strawberry Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Apple Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Grape Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Jelly].[Super].[Super Strawberry Jelly]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Creamy Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[BBB Best].[BBB Best Extra Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Creamy Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Extra Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Creamy Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Landslide].[Landslide Extra Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Creamy Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Extra Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Creamy Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Super].[Super Extra Chunky Peanut Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Apple Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Grape Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Low Fat Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[BBB Best].[BBB Best Strawberry Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Apple Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Grape Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Low Fat Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[CDR].[CDR Strawberry Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Apple Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Grape Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Low Fat Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Landslide].[Landslide Strawberry Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Apple Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Grape Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Low Fat Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Plato].[Plato Strawberry Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Apple Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Grape Preserves]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Low Fat Apple Butter]}
{[Product].[Products].[Food].[Baking Goods].[Jams and Jellies].[Preserves].[Super].[Super Strawberry Preserves]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Corn Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Grits]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Oatmeal]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Best].[Best Wheat Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Corn Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Grits]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Oatmeal]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Jeffers].[Jeffers Wheat Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Corn Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Grits]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Oatmeal]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Wheat Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Corn Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Grits]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Oatmeal]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Radius].[Radius Wheat Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Corn Puffs]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Grits]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Oatmeal]}
{[Product].[Products].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Special].[Special Wheat Puffs]}
{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Better].[Better Fancy Canned Anchovies]}
{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Blue Label].[Blue Label Fancy Canned Anchovies]}
{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Bravo].[Bravo Fancy Canned Anchovies]}
{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Just Right].[Just Right Fancy Canned Anchovies]}
{[Product].[Products].[Food].[Canned Foods].[Canned Anchovies].[Anchovies].[Pleasant].[Pleasant Fancy Canned Anchovies]}
{[Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Better].[Better Fancy Canned Clams]}
{[Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Blue Label].[Blue Label Fancy Canned Clams]}
{[Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Bravo].[Bravo Fancy Canned Clams]}
{[Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Just Right].[Just Right Fancy Canned Clams]}
{[Product].[Products].[Food].[Canned Foods].[Canned Clams].[Clams].[Pleasant].[Pleasant Fancy Canned Clams]}
{[Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Better].[Better Fancy Canned Oysters]}
{[Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Blue Label].[Blue Label Fancy Canned Oysters]}
{[Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Bravo].[Bravo Fancy Canned Oysters]}
{[Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Just Right].[Just Right Fancy Canned Oysters]}
{[Product].[Products].[Food].[Canned Foods].[Canned Oysters].[Oysters].[Pleasant].[Pleasant Fancy Canned Oysters]}
{[Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Better].[Better Fancy Canned Sardines]}
{[Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Blue Label].[Blue Label Fancy Canned Sardines]}
{[Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Bravo].[Bravo Fancy Canned Sardines]}
{[Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Just Right].[Just Right Fancy Canned Sardines]}
{[Product].[Products].[Food].[Canned Foods].[Canned Sardines].[Sardines].[Pleasant].[Pleasant Fancy Canned Sardines]}
{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Better].[Better Large Canned Shrimp]}
{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Blue Label].[Blue Label Large Canned Shrimp]}
{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Bravo].[Bravo Large Canned Shrimp]}
{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Just Right].[Just Right Large Canned Shrimp]}
{[Product].[Products].[Food].[Canned Foods].[Canned Shrimp].[Shrimp].[Pleasant].[Pleasant Large Canned Shrimp]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Beef Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Chicken Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Regular Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Rice Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Turkey Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Better].[Better Vegetable Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Beef Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Chicken Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Regular Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Rice Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Turkey Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Blue Label].[Blue Label Vegetable Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Beef Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Chicken Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Regular Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Rice Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Turkey Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Vegetable Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Beef Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Chicken Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Regular Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Rice Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Turkey Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Just Right].[Just Right Vegetable Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Beef Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Chicken Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Regular Ramen Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Rice Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Turkey Noodle Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Soup].[Soup].[Pleasant].[Pleasant Vegetable Soup]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Oil]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Water]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Blue Label].[Blue Label Canned Tuna in Oil]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Blue Label].[Blue Label Canned Tuna in Water]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Bravo].[Bravo Canned Tuna in Oil]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Bravo].[Bravo Canned Tuna in Water]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Just Right].[Just Right Canned Tuna in Oil]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Just Right].[Just Right Canned Tuna in Water]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Pleasant].[Pleasant Canned Tuna in Oil]}
{[Product].[Products].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Pleasant].[Pleasant Canned Tuna in Water]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Beets]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Peas]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned String Beans]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Tomatos]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Canned Yams]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Better].[Better Creamed Corn]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Beets]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Peas]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned String Beans]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Tomatos]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Canned Yams]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Blue Label].[Blue Label Creamed Corn]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Beets]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Peas]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned String Beans]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Tomatos]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Canned Yams]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Bravo].[Bravo Creamed Corn]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Beets]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Peas]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned String Beans]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Tomatos]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Canned Yams]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Just Right].[Just Right Creamed Corn]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Beets]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Peas]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned String Beans]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Tomatos]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Canned Yams]}
{[Product].[Products].[Food].[Canned Foods].[Vegetables].[Canned Vegetables].[Pleasant].[Pleasant Creamed Corn]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Applause].[Applause Canned Mixed Fruit]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Applause].[Applause Canned Peaches]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Big City].[Big City Canned Mixed Fruit]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Big City].[Big City Canned Peaches]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Green Ribbon].[Green Ribbon Canned Mixed Fruit]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Green Ribbon].[Green Ribbon Canned Peaches]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Swell].[Swell Canned Mixed Fruit]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Swell].[Swell Canned Peaches]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Toucan].[Toucan Canned Mixed Fruit]}
{[Product].[Products].[Food].[Canned Products].[Fruit].[Canned Fruit].[Toucan].[Toucan Canned Peaches]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Cheese Spread]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Havarti Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Head Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Jack Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Low Fat String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Mild Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Muenster Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker Sharp Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Booker].[Booker String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Cheese Spread]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Havarti Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Head Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Jack Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Low Fat String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Mild Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Muenster Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson Sharp Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Carlson].[Carlson String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Cheese Spread]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Havarti Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Head Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Jack Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Low Fat String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Mild Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Muenster Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club Sharp Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Club].[Club String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Cheese Spread]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Havarti Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Head Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Jack Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Low Fat String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Mild Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Muenster Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better Sharp Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Even Better].[Even Better String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Cheese Spread]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Havarti Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Head Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Jack Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Low Fat String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Mild Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Muenster Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla Sharp Cheddar Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cheese].[Gorilla].[Gorilla String Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Booker].[Booker Large Curd Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Booker].[Booker Low Fat Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Carlson].[Carlson Large Curd Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Carlson].[Carlson Low Fat Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Club].[Club Large Curd Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Club].[Club Low Fat Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Even Better].[Even Better Large Curd Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Even Better].[Even Better Low Fat Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Gorilla].[Gorilla Large Curd Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Cottage Cheese].[Gorilla].[Gorilla Low Fat Cottage Cheese]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Booker].[Booker Low Fat Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Booker].[Booker Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Carlson].[Carlson Low Fat Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Carlson].[Carlson Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Club].[Club Low Fat Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Club].[Club Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Even Better].[Even Better Low Fat Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Even Better].[Even Better Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Gorilla].[Gorilla Low Fat Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Sour Cream].[Gorilla].[Gorilla Sour Cream]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Booker].[Booker Blueberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Booker].[Booker Strawberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Carlson].[Carlson Blueberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Carlson].[Carlson Strawberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Club].[Club Blueberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Club].[Club Strawberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Even Better].[Even Better Blueberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Even Better].[Even Better Strawberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Gorilla].[Gorilla Blueberry Yogurt]}
{[Product].[Products].[Food].[Dairy].[Dairy].[Yogurt].[Gorilla].[Gorilla Strawberry Yogurt]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Beef Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Low Fat Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[American].[American Pimento Loaf]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Beef Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Low Fat Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Cutting Edge].[Cutting Edge Pimento Loaf]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Beef Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Low Fat Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Lake].[Lake Pimento Loaf]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Beef Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Low Fat Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Moms].[Moms Pimento Loaf]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Beef Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Low Fat Bologna]}
{[Product].[Products].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Pimento Loaf]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Corned Beef]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Ham]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[American].[American Sliced Turkey]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Corned Beef]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Ham]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Cutting Edge].[Cutting Edge Sliced Turkey]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Corned Beef]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Ham]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Lake].[Lake Sliced Turkey]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Corned Beef]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Ham]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Moms].[Moms Sliced Turkey]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Corned Beef]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Ham]}
{[Product].[Products].[Food].[Deli].[Meat].[Deli Meats].[Red Spade].[Red Spade Sliced Turkey]}
{[Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[American].[American Roasted Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Cutting Edge].[Cutting Edge Roasted Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Lake].[Lake Roasted Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Moms].[Moms Roasted Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Fresh Chicken].[Red Spade].[Red Spade Roasted Chicken]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Chicken Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Foot-Long Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[American].[American Turkey Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Chicken Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Foot-Long Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Cutting Edge].[Cutting Edge Turkey Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Chicken Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Foot-Long Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Lake].[Lake Turkey Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Chicken Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Foot-Long Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Moms].[Moms Turkey Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Chicken Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Foot-Long Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Meat].[Hot Dogs].[Red Spade].[Red Spade Turkey Hot Dogs]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Low Fat Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[American].[American Potato Salad]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Low Fat Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Cutting Edge].[Cutting Edge Potato Salad]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Low Fat Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Lake].[Lake Potato Salad]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Low Fat Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Moms].[Moms Potato Salad]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Low Fat Cole Slaw]}
{[Product].[Products].[Food].[Deli].[Side Dishes].[Deli Salads].[Red Spade].[Red Spade Potato Salad]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Egg Substitute]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Large Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Large Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Small Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Blue Medal].[Blue Medal Small Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Egg Substitute]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Large Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Large Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Small Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Giant].[Giant Small Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Egg Substitute]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Large Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Large Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Small Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Jumbo].[Jumbo Small Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Egg Substitute]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Large Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Large Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Small Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[National].[National Small Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Egg Substitute]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Large Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Large Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Brown Eggs]}
{[Product].[Products].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Eggs]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time].[Big Time Pancake Mix]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Carrington].[Carrington Pancake Mix]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden].[Golden Pancake Mix]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Imagine].[Imagine Pancake Mix]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[PigTail].[PigTail Pancake Mix]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Big Time].[Big Time Frozen Pancakes]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Carrington].[Carrington Frozen Pancakes]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Golden].[Golden Frozen Pancakes]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[Imagine].[Imagine Frozen Pancakes]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancakes].[PigTail].[PigTail Frozen Pancakes]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Apple Cinnamon Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Blueberry Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Low Fat Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Big Time].[Big Time Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Apple Cinnamon Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Blueberry Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Low Fat Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Carrington].[Carrington Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Apple Cinnamon Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Blueberry Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Low Fat Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Golden].[Golden Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Apple Cinnamon Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Blueberry Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Low Fat Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[Imagine].[Imagine Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Apple Cinnamon Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Blueberry Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Low Fat Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Waffles].[PigTail].[PigTail Waffles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Ice Cream]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Ice Cream Sandwich]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Big Time].[Big Time Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Ice Cream]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Ice Cream Sandwich]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Carrington].[Carrington Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Ice Cream]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Ice Cream Sandwich]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Golden].[Golden Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Ice Cream]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Ice Cream Sandwich]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[Imagine].[Imagine Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Ice Cream]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Ice Cream Sandwich]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Ice Cream].[PigTail].[PigTail Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Grape Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Lemon Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Lime Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Big Time].[Big Time Orange Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Grape Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Lemon Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Lime Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Carrington].[Carrington Orange Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Grape Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Lemon Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Lime Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Golden].[Golden Orange Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Grape Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Lemon Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Lime Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[Imagine].[Imagine Orange Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Grape Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Lemon Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Lime Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Desserts].[Popsicles].[PigTail].[PigTail Orange Popsicles]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Beef TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Chicken TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Big Time].[Big Time Turkey TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Beef TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Chicken TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Carrington].[Carrington Turkey TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Beef TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Chicken TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Golden].[Golden Turkey TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Beef TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Chicken TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[Imagine].[Imagine Turkey TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Beef TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Chicken TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Frozen Entrees].[TV Dinner].[PigTail].[PigTail Turkey TV Dinner]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Breast]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Thighs]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Big Time].[Big Time Frozen Chicken Wings]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Breast]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Thighs]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Carrington].[Carrington Frozen Chicken Wings]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Breast]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Thighs]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Golden].[Golden Frozen Chicken Wings]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Breast]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Thighs]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[Imagine].[Imagine Frozen Chicken Wings]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Breast]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Thighs]}
{[Product].[Products].[Food].[Frozen Foods].[Meat].[Frozen Chicken].[PigTail].[PigTail Frozen Chicken Wings]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Cheese Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Pepperoni Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Big Time].[Big Time Frozen Sausage Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Cheese Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Pepperoni Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Carrington].[Carrington Frozen Sausage Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Cheese Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Pepperoni Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Golden].[Golden Frozen Sausage Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Cheese Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Pepperoni Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[Imagine].[Imagine Frozen Sausage Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Cheese Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Mushroom Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Pepperoni Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Pizza].[Pizza].[PigTail].[PigTail Frozen Sausage Pizza]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Fajita French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Home Style French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Big Time].[Big Time Low Fat French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Fajita French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Home Style French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Carrington].[Carrington Low Fat French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Fajita French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Home Style French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Golden].[Golden Low Fat French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Fajita French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Home Style French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[Imagine].[Imagine Low Fat French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Fajita French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Home Style French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[French Fries].[PigTail].[PigTail Low Fat French Fries]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Broccoli]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Carrots]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Cauliflower]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Corn]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Big Time].[Big Time Frozen Peas]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Broccoli]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Carrots]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Cauliflower]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Corn]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Carrington].[Carrington Frozen Peas]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Broccoli]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Carrots]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Cauliflower]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Corn]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Golden].[Golden Frozen Peas]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Broccoli]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Carrots]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Cauliflower]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Corn]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[Imagine].[Imagine Frozen Peas]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Broccoli]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Carrots]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Cauliflower]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Corn]}
{[Product].[Products].[Food].[Frozen Foods].[Vegetables].[Frozen Vegetables].[PigTail].[PigTail Frozen Peas]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Footnote].[Footnote Extra Lean Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Footnote].[Footnote Seasoned Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Genteel].[Genteel Extra Lean Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Genteel].[Genteel Seasoned Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Gerolli].[Gerolli Extra Lean Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Gerolli].[Gerolli Seasoned Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Quick].[Quick Extra Lean Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Quick].[Quick Seasoned Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Ship Shape].[Ship Shape Extra Lean Hamburger]}
{[Product].[Products].[Food].[Meat].[Meat].[Hamburger].[Ship Shape].[Ship Shape Seasoned Hamburger]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Cantelope]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Fancy Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Fuji Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Golden Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Honey Dew]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Lemons]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Limes]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Macintosh Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Mandarin Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Peaches]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Red Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Ebony].[Ebony Tangerines]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Cantelope]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fancy Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Fuji Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Golden Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Honey Dew]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Lemons]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Limes]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Macintosh Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Mandarin Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Peaches]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Red Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Hermanos].[Hermanos Tangerines]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Cantelope]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Fancy Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Fuji Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Golden Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Honey Dew]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Lemons]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Limes]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Macintosh Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Mandarin Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Peaches]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Red Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Tangerines]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Cantelope]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Fancy Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Fuji Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Golden Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Honey Dew]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Lemons]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Limes]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Macintosh Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Mandarin Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Peaches]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Red Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tell Tale].[Tell Tale Tangerines]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Cantelope]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Fancy Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Fuji Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Golden Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Honey Dew]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Lemons]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Limes]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Macintosh Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Mandarin Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Oranges]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Peaches]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Plums]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Red Delcious Apples]}
{[Product].[Products].[Food].[Produce].[Fruit].[Fresh Fruit].[Tri-State].[Tri-State Tangerines]}
{[Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Ebony].[Ebony Firm Tofu]}
{[Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Hermanos].[Hermanos Firm Tofu]}
{[Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[High Top].[High Top Firm Tofu]}
{[Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Tell Tale].[Tell Tale Firm Tofu]}
{[Product].[Products].[Food].[Produce].[Packaged Vegetables].[Tofu].[Tri-State].[Tri-State Firm Tofu]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Almonds]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Canned Peanuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Mixed Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Party Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Ebony].[Ebony Walnuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Almonds]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Canned Peanuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Mixed Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Party Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Hermanos].[Hermanos Walnuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Almonds]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Canned Peanuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Mixed Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Party Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[High Top].[High Top Walnuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Almonds]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Canned Peanuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Mixed Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Party Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tell Tale].[Tell Tale Walnuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Almonds]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Canned Peanuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Mixed Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Party Nuts]}
{[Product].[Products].[Food].[Produce].[Specialty].[Nuts].[Tri-State].[Tri-State Walnuts]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Asparagus]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Baby Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Beets]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Broccoli]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Cauliflower]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Corn on the Cob]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Dried Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Elephant Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Fresh Lima Beans]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Green Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Lettuce]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony New Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Onions]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Prepared Salad]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Red Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Shitake Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Summer Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Sweet Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Sweet Peas]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Tomatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Asparagus]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Baby Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Beets]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Broccoli]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Cauliflower]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Corn on the Cob]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Dried Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Elephant Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Fresh Lima Beans]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Lettuce]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos New Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Onions]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Prepared Salad]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Red Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Shitake Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Summer Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Sweet Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Sweet Peas]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Tomatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Asparagus]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Baby Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Beets]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Broccoli]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Cauliflower]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Corn on the Cob]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Dried Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Elephant Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Fresh Lima Beans]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Green Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Lettuce]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top New Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Onions]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Prepared Salad]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Red Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Shitake Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Summer Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Sweet Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Sweet Peas]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top].[High Top Tomatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Asparagus]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Baby Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Beets]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Broccoli]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Cauliflower]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Corn on the Cob]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Dried Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Elephant Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Fresh Lima Beans]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Green Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Lettuce]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale New Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Onions]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Prepared Salad]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Red Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Shitake Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Summer Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Sweet Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Sweet Peas]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Tomatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Asparagus]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Baby Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Beets]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Broccoli]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Cauliflower]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Corn on the Cob]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Dried Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Elephant Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Fresh Lima Beans]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Garlic]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Green Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Lettuce]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State New Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Onions]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Potatos]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Prepared Salad]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Red Pepper]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Shitake Mushrooms]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Summer Squash]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Sweet Onion]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Sweet Peas]}
{[Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tri-State].[Tri-State Tomatos]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Amigo].[Amigo Lox]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Curlew].[Curlew Lox]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Dual City].[Dual City Lox]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Kiwi].[Kiwi Lox]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Fresh Fish].[Tip Top].[Tip Top Lox]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Amigo].[Amigo Scallops]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Curlew].[Curlew Scallops]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Dual City].[Dual City Scallops]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Kiwi].[Kiwi Scallops]}
{[Product].[Products].[Food].[Seafood].[Seafood].[Shellfish].[Tip Top].[Tip Top Scallops]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice BBQ Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Corn Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Low Fat BBQ Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Low Fat Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Best Choice].[Best Choice Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast BBQ Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Corn Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Low Fat BBQ Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Low Fat Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fast].[Fast Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West BBQ Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Corn Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Low Fat BBQ Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Low Fat Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Fort West].[Fort West Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio BBQ Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Corn Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Low Fat BBQ Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Low Fat Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Horatio].[Horatio Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel BBQ Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Corn Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Low Fat BBQ Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Low Fat Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Chips].[Nationeel].[Nationeel Potato Chips]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Chocolate Chip Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Frosted Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Fudge Brownies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Fudge Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Graham Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Lemon Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Low Fat Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Best Choice].[Best Choice Sugar Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Chocolate Chip Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Frosted Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Fudge Brownies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Fudge Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Graham Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Lemon Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Low Fat Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fast].[Fast Sugar Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Chocolate Chip Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Frosted Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Fudge Brownies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Fudge Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Graham Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Lemon Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Low Fat Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Fort West].[Fort West Sugar Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Chocolate Chip Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Frosted Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Fudge Brownies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Fudge Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Graham Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Lemon Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Low Fat Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Horatio].[Horatio Sugar Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Chocolate Chip Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Frosted Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Fudge Brownies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Fudge Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Graham Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Lemon Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Low Fat Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Cookies].[Nationeel].[Nationeel Sugar Cookies]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Best Choice].[Best Choice Cheese Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Best Choice].[Best Choice Sesame Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fast].[Fast Cheese Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fast].[Fast Sesame Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fort West].[Fort West Cheese Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Fort West].[Fort West Sesame Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Horatio].[Horatio Cheese Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Horatio].[Horatio Sesame Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Nationeel].[Nationeel Cheese Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Crackers].[Nationeel].[Nationeel Sesame Crackers]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Avocado Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Cheese Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Fondue Mix]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Best Choice].[Best Choice Salsa Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Avocado Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Cheese Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Fondue Mix]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fast].[Fast Salsa Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Avocado Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Cheese Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Fondue Mix]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Fort West].[Fort West Salsa Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Avocado Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Cheese Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Fondue Mix]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Horatio].[Horatio Salsa Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Avocado Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Cheese Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Fondue Mix]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dips].[Nationeel].[Nationeel Salsa Dip]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Chocolate Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Frosted Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Best Choice].[Best Choice Mini Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Chocolate Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Frosted Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fast].[Fast Mini Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Chocolate Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Frosted Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Fort West].[Fort West Mini Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Chocolate Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Frosted Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Horatio].[Horatio Mini Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Chocolate Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Frosted Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Donuts].[Nationeel].[Nationeel Mini Donuts]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Apple Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Apples]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Apricots]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Dried Dates]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Golden Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Grape Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Raspberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Strawberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Apple Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Apples]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Apricots]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Dried Dates]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Golden Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Grape Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raspberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Strawberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Apple Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Apples]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Apricots]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Dried Dates]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Golden Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Grape Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raspberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Strawberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Apple Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Apples]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Apricots]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Dried Dates]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Golden Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Grape Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Raspberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Strawberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Apple Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Apples]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Apricots]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Dried Dates]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Golden Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Grape Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Raisins]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Raspberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Strawberry Fruit Roll]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Best Choice].[Best Choice Beef Jerky]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Fast].[Fast Beef Jerky]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Fort West].[Fort West Beef Jerky]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Horatio].[Horatio Beef Jerky]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Dried Meat].[Nationeel].[Nationeel Beef Jerky]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice Buttered Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice Low Fat Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Best Choice].[Best Choice No Salt Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast Buttered Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast Low Fat Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fast].[Fast No Salt Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West Buttered Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West Low Fat Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Fort West].[Fort West No Salt Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Buttered Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Low Fat Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio No Salt Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel Buttered Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel Low Fat Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Nationeel].[Nationeel No Salt Popcorn]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Best Choice].[Best Choice Salted Pretzels]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Fast].[Fast Salted Pretzels]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Fort West].[Fort West Salted Pretzels]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Horatio].[Horatio Salted Pretzels]}
{[Product].[Products].[Food].[Snack Foods].[Snack Foods].[Pretzels].[Nationeel].[Nationeel Salted Pretzels]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Malted Milk Balls]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Mint Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Semi-Sweet Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic Tasty Candy Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Atomic].[Atomic White Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Malted Milk Balls]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Mint Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Semi-Sweet Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice Tasty Candy Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Choice].[Choice White Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Malted Milk Balls]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Mint Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Semi-Sweet Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast Tasty Candy Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Gulf Coast].[Gulf Coast White Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Malted Milk Balls]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Mint Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Semi-Sweet Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial Tasty Candy Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Musial].[Musial White Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Malted Milk Balls]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Mint Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Semi-Sweet Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher Tasty Candy Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Chocolate Candy].[Thresher].[Thresher White Chocolate Bar]}
{[Product].[Products].[Food].[Snacks].[Candy].[Gum].[Atomic].[Atomic Bubble Gum]}
{[Product].[Products].[Food].[Snacks].[Candy].[Gum].[Choice].[Choice Bubble Gum]}
{[Product].[Products].[Food].[Snacks].[Candy].[Gum].[Gulf Coast].[Gulf Coast Bubble Gum]}
{[Product].[Products].[Food].[Snacks].[Candy].[Gum].[Musial].[Musial Bubble Gum]}
{[Product].[Products].[Food].[Snacks].[Candy].[Gum].[Thresher].[Thresher Bubble Gum]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Atomic].[Atomic Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Atomic].[Atomic Spicy Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Choice].[Choice Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Choice].[Choice Spicy Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Gulf Coast].[Gulf Coast Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Gulf Coast].[Gulf Coast Spicy Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Musial].[Musial Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Musial].[Musial Spicy Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Thresher].[Thresher Mints]}
{[Product].[Products].[Food].[Snacks].[Candy].[Hard Candy].[Thresher].[Thresher Spicy Mints]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Colossal].[Colossal Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Discover].[Discover Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Jardon].[Jardon Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Medalist].[Medalist Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Monarch].[Monarch Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Manicotti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Ravioli]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Pasta].[Shady Lake].[Shady Lake Spaghetti]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Colossal].[Colossal Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Colossal].[Colossal Thai Rice]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Discover].[Discover Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Discover].[Discover Thai Rice]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Jardon].[Jardon Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Jardon].[Jardon Thai Rice]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Medalist].[Medalist Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Medalist].[Medalist Thai Rice]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Monarch].[Monarch Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Monarch].[Monarch Thai Rice]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Shady Lake].[Shady Lake Rice Medly]}
{[Product].[Products].[Food].[Starchy Foods].[Starchy Foods].[Rice].[Shady Lake].[Shady Lake Thai Rice]}
{[Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[ADJ].[ADJ Rosy Sunglasses]}
{[Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[King].[King Rosy Sunglasses]}
{[Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Prelude].[Prelude Rosy Sunglasses]}
{[Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Symphony].[Symphony Rosy Sunglasses]}
{[Product].[Products].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[Toretti].[Toretti Rosy Sunglasses]}
{[Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Akron].[Akron Eyeglass Screwdriver]}
{[Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Black Tie].[Black Tie Eyeglass Screwdriver]}
{[Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Framton].[Framton Eyeglass Screwdriver]}
{[Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[James Bay].[James Bay Eyeglass Screwdriver]}
{[Product].[Products].[Non-Consumable].[Checkout].[Hardware].[Screwdrivers].[Queen].[Queen Eyeglass Screwdriver]}
{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Akron].[Akron City Map]}
{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Black Tie].[Black Tie City Map]}
{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Framton].[Framton City Map]}
{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[James Bay].[James Bay City Map]}
{[Product].[Products].[Non-Consumable].[Checkout].[Miscellaneous].[Maps].[Queen].[Queen City Map]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Bird Call].[Bird Call Silky Smooth Hair Conditioner]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Consolidated].[Consolidated Silky Smooth Hair Conditioner]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Faux Products].[Faux Products Silky Smooth Hair Conditioner]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Hilltop].[Hilltop Silky Smooth Hair Conditioner]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Conditioner].[Steady].[Steady Silky Smooth Hair Conditioner]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Bird Call].[Bird Call Laundry Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Bird Call].[Bird Call Mint Mouthwash]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Consolidated].[Consolidated Laundry Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Consolidated].[Consolidated Mint Mouthwash]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Faux Products].[Faux Products Laundry Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Faux Products].[Faux Products Mint Mouthwash]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Laundry Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Steady].[Steady Laundry Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Steady].[Steady Mint Mouthwash]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Apricot Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Conditioning Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Bird Call].[Bird Call Extra Moisture Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Apricot Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Conditioning Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Consolidated].[Consolidated Extra Moisture Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Apricot Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Conditioning Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Faux Products].[Faux Products Extra Moisture Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Apricot Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Conditioning Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Hilltop].[Hilltop Extra Moisture Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Apricot Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Conditioning Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Shampoo].[Steady].[Steady Extra Moisture Shampoo]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Bird Call].[Bird Call Angled Toothbrush]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Consolidated].[Consolidated Angled Toothbrush]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Faux Products].[Faux Products Angled Toothbrush]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Hilltop].[Hilltop Angled Toothbrush]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Toothbrushes].[Steady].[Steady Angled Toothbrush]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Bird Call].[Bird Call Childrens Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Bird Call].[Bird Call Multi-Symptom Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Consolidated].[Consolidated Childrens Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Consolidated].[Consolidated Multi-Symptom Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Faux Products].[Faux Products Childrens Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Faux Products].[Faux Products Multi-Symptom Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Hilltop].[Hilltop Childrens Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Hilltop].[Hilltop Multi-Symptom Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Steady].[Steady Childrens Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Cold Remedies].[Cold Remedies].[Steady].[Steady Multi-Symptom Cold Remedy]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Bird Call].[Bird Call Dishwasher Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Bird Call].[Bird Call HCL Nasal Spray]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Consolidated].[Consolidated Dishwasher Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Consolidated].[Consolidated HCL Nasal Spray]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Faux Products].[Faux Products Dishwasher Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Faux Products].[Faux Products HCL Nasal Spray]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Hilltop].[Hilltop Dishwasher Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Hilltop].[Hilltop HCL Nasal Spray]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Steady].[Steady Dishwasher Detergent]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Decongestants].[Nasal Sprays].[Steady].[Steady HCL Nasal Spray]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Deodorant]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Tartar Control Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Bird Call].[Bird Call Whitening Toothpast]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Deodorant]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Tartar Control Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Consolidated].[Consolidated Whitening Toothpast]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Deodorant]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Tartar Control Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Faux Products].[Faux Products Whitening Toothpast]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Deodorant]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Tartar Control Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Hilltop].[Hilltop Whitening Toothpast]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Deodorant]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Tartar Control Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Toothpaste]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Hygiene].[Personal Hygiene].[Steady].[Steady Whitening Toothpast]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Bird Call].[Bird Call 200 MG Acetominifen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Consolidated].[Consolidated 200 MG Acetominifen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Faux Products].[Faux Products 200 MG Acetominifen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Hilltop].[Hilltop 200 MG Acetominifen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Acetominifen].[Steady].[Steady 200 MG Acetominifen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Bird Call].[Bird Call Buffered Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Bird Call].[Bird Call Childrens Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Consolidated].[Consolidated Buffered Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Consolidated].[Consolidated Childrens Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Faux Products].[Faux Products Buffered Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Faux Products].[Faux Products Childrens Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Hilltop].[Hilltop Buffered Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Hilltop].[Hilltop Childrens Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Steady].[Steady Buffered Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Aspirin].[Steady].[Steady Childrens Aspirin]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Bird Call].[Bird Call 200 MG Ibuprofen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Consolidated].[Consolidated 200 MG Ibuprofen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Faux Products].[Faux Products 200 MG Ibuprofen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Hilltop].[Hilltop 200 MG Ibuprofen]}
{[Product].[Products].[Non-Consumable].[Health and Hygiene].[Pain Relievers].[Ibuprofen].[Steady].[Steady 200 MG Ibuprofen]}
{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Cormorant].[Cormorant Economy Toilet Brush]}
{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Denny].[Denny Economy Toilet Brush]}
{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[High Quality].[High Quality Economy Toilet Brush]}
{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Red Wing].[Red Wing Economy Toilet Brush]}
{[Product].[Products].[Non-Consumable].[Household].[Bathroom Products].[Toilet Brushes].[Sunset].[Sunset Economy Toilet Brush]}
{[Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Cormorant].[Cormorant Bees Wax Candles]}
{[Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Denny].[Denny Bees Wax Candles]}
{[Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[High Quality].[High Quality Bees Wax Candles]}
{[Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Red Wing].[Red Wing Bees Wax Candles]}
{[Product].[Products].[Non-Consumable].[Household].[Candles].[Candles].[Sunset].[Sunset Bees Wax Candles]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Counter Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Glass Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Cormorant].[Cormorant Toilet Bowl Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Counter Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Glass Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Denny].[Denny Toilet Bowl Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Counter Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Glass Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[High Quality].[High Quality Toilet Bowl Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Counter Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Glass Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Red Wing].[Red Wing Toilet Bowl Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Counter Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Glass Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Cleaners].[Sunset].[Sunset Toilet Bowl Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Cormorant].[Cormorant Room Freshener]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Denny].[Denny Room Freshener]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[High Quality].[High Quality Room Freshener]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Red Wing].[Red Wing Room Freshener]}
{[Product].[Products].[Non-Consumable].[Household].[Cleaning Supplies].[Deodorizers].[Sunset].[Sunset Room Freshener]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AAA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant C-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant D-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AAA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny C-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny D-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality AA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality AAA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality C-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[High Quality].[High Quality D-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing AA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing AAA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing C-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Red Wing].[Red Wing D-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset AA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset AAA-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset C-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Batteries].[Sunset].[Sunset D-Size Batteries]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 100 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 25 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 60 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Cormorant].[Cormorant 75 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 100 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 25 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 60 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 75 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 100 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 25 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 60 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[High Quality].[High Quality 75 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 100 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 25 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 60 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Red Wing].[Red Wing 75 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 100 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 25 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 60 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Sunset].[Sunset 75 Watt Lightbulb]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Cormorant].[Cormorant Scissors]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Cormorant].[Cormorant Screw Driver]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Denny].[Denny Scissors]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Denny].[Denny Screw Driver]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[High Quality].[High Quality Scissors]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[High Quality].[High Quality Screw Driver]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Red Wing].[Red Wing Scissors]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Red Wing].[Red Wing Screw Driver]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Sunset].[Sunset Scissors]}
{[Product].[Products].[Non-Consumable].[Household].[Hardware].[Tools].[Sunset].[Sunset Screw Driver]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Cormorant].[Cormorant Copper Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Cormorant].[Cormorant Silver Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Denny].[Denny Copper Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Denny].[Denny Silver Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[High Quality].[High Quality Copper Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[High Quality].[High Quality Silver Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Red Wing].[Red Wing Copper Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Red Wing].[Red Wing Silver Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Sunset].[Sunset Copper Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Cleaners].[Sunset].[Sunset Silver Cleaner]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Cormorant].[Cormorant Copper Pot Scrubber]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Denny].[Denny Copper Pot Scrubber]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[High Quality].[High Quality Copper Pot Scrubber]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Red Wing].[Red Wing Copper Pot Scrubber]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pot Scrubbers].[Sunset].[Sunset Copper Pot Scrubber]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Cormorant].[Cormorant Frying Pan]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Denny].[Denny Frying Pan]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[High Quality].[High Quality Frying Pan]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Red Wing].[Red Wing Frying Pan]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Pots and Pans].[Sunset].[Sunset Frying Pan]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Cormorant].[Cormorant Large Sponge]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Denny].[Denny Large Sponge]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[High Quality].[High Quality Large Sponge]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Red Wing].[Red Wing Large Sponge]}
{[Product].[Products].[Non-Consumable].[Household].[Kitchen Products].[Sponges].[Sunset].[Sunset Large Sponge]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Cormorant].[Cormorant Paper Cups]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Cormorant].[Cormorant Paper Plates]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Denny].[Denny Paper Cups]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Denny].[Denny Paper Plates]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[High Quality].[High Quality Paper Cups]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[High Quality].[High Quality Paper Plates]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Red Wing].[Red Wing Paper Cups]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Red Wing].[Red Wing Paper Plates]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Sunset].[Sunset Paper Cups]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Dishes].[Sunset].[Sunset Paper Plates]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Paper Towels]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Scented Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Scented Toilet Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Soft Napkins]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Tissues]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Cormorant].[Cormorant Toilet Paper]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Paper Towels]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Scented Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Scented Toilet Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Soft Napkins]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Tissues]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Denny].[Denny Toilet Paper]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Paper Towels]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Scented Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Scented Toilet Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Soft Napkins]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Tissues]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[High Quality].[High Quality Toilet Paper]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Paper Towels]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Scented Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Scented Toilet Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Soft Napkins]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Tissues]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Red Wing].[Red Wing Toilet Paper]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Paper Towels]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Scented Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Scented Toilet Tissue]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Soft Napkins]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Tissues]}
{[Product].[Products].[Non-Consumable].[Household].[Paper Products].[Paper Wipes].[Sunset].[Sunset Toilet Paper]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Forks]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Knives]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Cormorant].[Cormorant Plastic Spoons]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Forks]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Knives]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Denny].[Denny Plastic Spoons]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Forks]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Knives]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[High Quality].[High Quality Plastic Spoons]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Forks]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Knives]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Red Wing].[Red Wing Plastic Spoons]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Forks]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Knives]}
{[Product].[Products].[Non-Consumable].[Household].[Plastic Products].[Plastic Utensils].[Sunset].[Sunset Plastic Spoons]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Dollar].[Dollar Monthly Auto Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Excel].[Excel Monthly Auto Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Gauss].[Gauss Monthly Auto Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Mighty Good].[Mighty Good Monthly Auto Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Auto Magazines].[Robust].[Robust Monthly Auto Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Dollar].[Dollar Monthly Computer Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Excel].[Excel Monthly Computer Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Gauss].[Gauss Monthly Computer Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Mighty Good].[Mighty Good Monthly Computer Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Computer Magazines].[Robust].[Robust Monthly Computer Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Dollar].[Dollar Monthly Fashion Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Excel].[Excel Monthly Fashion Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Gauss].[Gauss Monthly Fashion Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Mighty Good].[Mighty Good Monthly Fashion Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Fashion Magazines].[Robust].[Robust Monthly Fashion Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Dollar].[Dollar Monthly Home Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Excel].[Excel Monthly Home Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Gauss].[Gauss Monthly Home Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Mighty Good].[Mighty Good Monthly Home Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Home Magazines].[Robust].[Robust Monthly Home Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Dollar].[Dollar Monthly Sports Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Excel].[Excel Monthly Sports Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Gauss].[Gauss Monthly Sports Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Mighty Good].[Mighty Good Monthly Sports Magazine]}
{[Product].[Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]}
Row #0: 154
Row #0: 115
Row #0: 175
Row #0: 210
Row #0: 187
Row #0: 175
Row #0: 145
Row #0: 161
Row #0: 174
Row #0: 187
Row #0: 163
Row #0: 192
Row #0: 146
Row #0: 148
Row #0: 137
Row #0: 159
Row #0: 209
Row #0: 189
Row #0: 210
Row #0: 198
Row #0: 228
Row #0: 172
Row #0: 136
Row #0: 170
Row #0: 214
Row #0: 152
Row #0: 184
Row #0: 159
Row #0: 140
Row #0: 164
Row #0: 129
Row #0: 172
Row #0: 186
Row #0: 160
Row #0: 185
Row #0: 173
Row #0: 175
Row #0: 140
Row #0: 185
Row #0: 180
Row #0: 193
Row #0: 189
Row #0: 184
Row #0: 172
Row #0: 148
Row #0: 151
Row #0: 153
Row #0: 180
Row #0: 160
Row #0: 142
Row #0: 187
Row #0: 166
Row #0: 162
Row #0: 159
Row #0: 234
Row #0: 180
Row #0: 178
Row #0: 155
Row #0: 180
Row #0: 134
Row #0: 167
Row #0: 176
Row #0: 125
Row #0: 143
Row #0: 147
Row #0: 179
Row #0: 160
Row #0: 178
Row #0: 168
Row #0: 176
Row #0: 137
Row #0: 153
Row #0: 238
Row #0: 171
Row #0: 151
Row #0: 171
Row #0: 167
Row #0: 155
Row #0: 175
Row #0: 134
Row #0: 158
Row #0: 189
Row #0: 233
Row #0: 194
Row #0: 187
Row #0: 194
Row #0: 150
Row #0: 183
Row #0: 159
Row #0: 189
Row #0: 168
Row #0: 167
Row #0: 179
Row #0: 190
Row #0: 154
Row #0: 195
Row #0: 141
Row #0: 152
Row #0: 160
Row #0: 157
Row #0: 164
Row #0: 209
Row #0: 160
Row #0: 119
Row #0: 252
Row #0: 215
Row #0: 202
Row #0: 192
Row #0: 133
Row #0: 170
Row #0: 137
Row #0: 169
Row #0: 161
Row #0: 208
Row #0: 156
Row #0: 181
Row #0: 162
Row #0: 83
Row #0: 130
Row #0: 193
Row #0: 189
Row #0: 177
Row #0: 110
Row #0: 133
Row #0: 163
Row #0: 212
Row #0: 131
Row #0: 175
Row #0: 175
Row #0: 234
Row #0: 155
Row #0: 145
Row #0: 140
Row #0: 159
Row #0: 168
Row #0: 190
Row #0: 177
Row #0: 227
Row #0: 197
Row #0: 168
Row #0: 160
Row #0: 133
Row #0: 174
Row #0: 151
Row #0: 143
Row #0: 163
Row #0: 160
Row #0: 145
Row #0: 165
Row #0: 182
Row #0: 193
Row #0: 182
Row #0: 161
Row #0: 204
Row #0: 215
Row #0: 227
Row #0: 189
Row #0: 167
Row #0: 127
Row #0: 165
Row #0: 164
Row #0: 149
Row #0: 186
Row #0: 166
Row #0: 196
Row #0: 171
Row #0: 166
Row #0: 131
Row #0: 157
Row #0: 181
Row #0: 177
Row #0: 177
Row #0: 228
Row #0: 155
Row #0: 194
Row #0: 207
Row #0: 226
Row #0: 188
Row #0: 178
Row #0: 140
Row #0: 154
Row #0: 166
Row #0: 167
Row #0: 165
Row #0: 169
Row #0: 152
Row #0: 200
Row #0: 154
Row #0: 163
Row #0: 198
Row #0: 175
Row #0: 162
Row #0: 187
Row #0: 166
Row #0: 167
Row #0: 156
Row #0: 138
Row #0: 158
Row #0: 168
Row #0: 199
Row #0: 184
Row #0: 143
Row #0: 182
Row #0: 159
Row #0: 162
Row #0: 151
Row #0: 114
Row #0: 199
Row #0: 168
Row #0: 139
Row #0: 158
Row #0: 129
Row #0: 165
Row #0: 169
Row #0: 160
Row #0: 199
Row #0: 222
Row #0: 177
Row #0: 97
Row #0: 191
Row #0: 166
Row #0: 185
Row #0: 245
Row #0: 197
Row #0: 141
Row #0: 152
Row #0: 135
Row #0: 172
Row #0: 146
Row #0: 149
Row #0: 150
Row #0: 229
Row #0: 150
Row #0: 196
Row #0: 201
Row #0: 167
Row #0: 195
Row #0: 142
Row #0: 139
Row #0: 156
Row #0: 120
Row #0: 193
Row #0: 187
Row #0: 220
Row #0: 166
Row #0: 163
Row #0: 156
Row #0: 169
Row #0: 208
Row #0: 196
Row #0: 142
Row #0: 130
Row #0: 160
Row #0: 191
Row #0: 155
Row #0: 190
Row #0: 227
Row #0: 185
Row #0: 166
Row #0: 188
Row #0: 194
Row #0: 213
Row #0: 189
Row #0: 156
Row #0: 177
Row #0: 188
Row #0: 127
Row #0: 179
Row #0: 186
Row #0: 188
Row #0: 201
Row #0: 167
Row #0: 182
Row #0: 178
Row #0: 185
Row #0: 189
Row #0: 175
Row #0: 167
Row #0: 192
Row #0: 173
Row #0: 155
Row #0: 143
Row #0: 190
Row #0: 182
Row #0: 240
Row #0: 207
Row #0: 168
Row #0: 193
Row #0: 180
Row #0: 167
Row #0: 79
Row #0: 136
Row #0: 153
Row #0: 171
Row #0: 176
Row #0: 154
Row #0: 193
Row #0: 168
Row #0: 137
Row #0: 163
Row #0: 158
Row #0: 155
Row #0: 169
Row #0: 175
Row #0: 157
Row #0: 136
Row #0: 162
Row #0: 155
Row #0: 148
Row #0: 115
Row #0: 158
Row #0: 159
Row #0: 137
Row #0: 159
Row #0: 139
Row #0: 194
Row #0: 129
Row #0: 173
Row #0: 135
Row #0: 154
Row #0: 155
Row #0: 147
Row #0: 151
Row #0: 205
Row #0: 179
Row #0: 196
Row #0: 194
Row #0: 171
Row #0: 267
Row #0: 198
Row #0: 136
Row #0: 147
Row #0: 205
Row #0: 214
Row #0: 189
Row #0: 165
Row #0: 184
Row #0: 177
Row #0: 167
Row #0: 124
Row #0: 127
Row #0: 119
Row #0: 160
Row #0: 178
Row #0: 166
Row #0: 191
Row #0: 158
Row #0: 164
Row #0: 140
Row #0: 159
Row #0: 138
Row #0: 137
Row #0: 188
Row #0: 182
Row #0: 162
Row #0: 203
Row #0: 175
Row #0: 158
Row #0: 203
Row #0: 166
Row #0: 166
Row #0: 217
Row #0: 163
Row #0: 177
Row #0: 182
Row #0: 190
Row #0: 176
Row #0: 147
Row #0: 175
Row #0: 175
Row #0: 158
Row #0: 209
Row #0: 166
Row #0: 163
Row #0: 157
Row #0: 154
Row #0: 236
Row #0: 130
Row #0: 135
Row #0: 205
Row #0: 189
Row #0: 208
Row #0: 175
Row #0: 198
Row #0: 148
Row #0: 191
Row #0: 165
Row #0: 227
Row #0: 217
Row #0: 198
Row #0: 186
Row #0: 155
Row #0: 189
Row #0: 193
Row #0: 151
Row #0: 157
Row #0: 150
Row #0: 204
Row #0: 157
Row #0: 167
Row #0: 174
Row #0: 142
Row #0: 194
Row #0: 156
Row #0: 170
Row #0: 212
Row #0: 217
Row #0: 130
Row #0: 148
Row #0: 145
Row #0: 184
Row #0: 166
Row #0: 168
Row #0: 173
Row #0: 191
Row #0: 132
Row #0: 175
Row #0: 129
Row #0: 150
Row #0: 151
Row #0: 174
Row #0: 197
Row #0: 148
Row #0: 163
Row #0: 191
Row #0: 176
Row #0: 199
Row #0: 166
Row #0: 195
Row #0: 223
Row #0: 199
Row #0: 188
Row #0: 161
Row #0: 189
Row #0: 163
Row #0: 168
Row #0: 172
Row #0: 167
Row #0: 194
Row #0: 205
Row #0: 150
Row #0: 204
Row #0: 179
Row #0: 142
Row #0: 173
Row #0: 204
Row #0: 175
Row #0: 187
Row #0: 193
Row #0: 188
Row #0: 192
Row #0: 181
Row #0: 197
Row #0: 209
Row #0: 132
Row #0: 166
Row #0: 198
Row #0: 169
Row #0: 146
Row #0: 163
Row #0: 207
Row #0: 202
Row #0: 190
Row #0: 164
Row #0: 190
Row #0: 184
Row #0: 159
Row #0: 153
Row #0: 164
Row #0: 116
Row #0: 174
Row #0: 160
Row #0: 168
Row #0: 155
Row #0: 180
Row #0: 206
Row #0: 182
Row #0: 161
Row #0: 164
Row #0: 200
Row #0: 185
Row #0: 179
Row #0: 159
Row #0: 200
Row #0: 187
Row #0: 181
Row #0: 139
Row #0: 161
Row #0: 174
Row #0: 192
Row #0: 159
Row #0: 153
Row #0: 171
Row #0: 158
Row #0: 133
Row #0: 176
Row #0: 178
Row #0: 158
Row #0: 129
Row #0: 206
Row #0: 194
Row #0: 150
Row #0: 168
Row #0: 140
Row #0: 149
Row #0: 159
Row #0: 121
Row #0: 161
Row #0: 200
Row #0: 173
Row #0: 216
Row #0: 174
Row #0: 175
Row #0: 124
Row #0: 177
Row #0: 140
Row #0: 185
Row #0: 187
Row #0: 190
Row #0: 184
Row #0: 198
Row #0: 176
Row #0: 163
Row #0: 183
Row #0: 168
Row #0: 196
Row #0: 195
Row #0: 166
Row #0: 167
Row #0: 161
Row #0: 174
Row #0: 150
Row #0: 174
Row #0: 183
Row #0: 164
Row #0: 182
Row #0: 162
Row #0: 205
Row #0: 141
Row #0: 148
Row #0: 176
Row #0: 211
Row #0: 184
Row #0: 123
Row #0: 144
Row #0: 178
Row #0: 167
Row #0: 121
Row #0: 171
Row #0: 135
Row #0: 178
Row #0: 171
Row #0: 156
Row #0: 206
Row #0: 231
Row #0: 141
Row #0: 173
Row #0: 165
Row #0: 160
Row #0: 168
Row #0: 160
Row #0: 204
Row #0: 196
Row #0: 150
Row #0: 163
Row #0: 174
Row #0: 170
Row #0: 177
Row #0: 172
Row #0: 170
Row #0: 153
Row #0: 183
Row #0: 188
Row #0: 177
Row #0: 204
Row #0: 210
Row #0: 163
Row #0: 153
Row #0: 171
Row #0: 157
Row #0: 186
Row #0: 158
Row #0: 214
Row #0: 194
Row #0: 147
Row #0: 172
Row #0: 137
Row #0: 183
Row #0: 169
Row #0: 186
Row #0: 170
Row #0: 207
Row #0: 154
Row #0: 170
Row #0: 174
Row #0: 168
Row #0: 161
Row #0: 148
Row #0: 157
Row #0: 179
Row #0: 136
Row #0: 162
Row #0: 160
Row #0: 152
Row #0: 144
Row #0: 126
Row #0: 130
Row #0: 137
Row #0: 133
Row #0: 203
Row #0: 184
Row #0: 220
Row #0: 166
Row #0: 168
Row #0: 169
Row #0: 176
Row #0: 178
Row #0: 175
Row #0: 226
Row #0: 201
Row #0: 163
Row #0: 110
Row #0: 173
Row #0: 167
Row #0: 160
Row #0: 140
Row #0: 165
Row #0: 149
Row #0: 169
Row #0: 167
Row #0: 199
Row #0: 180
Row #0: 134
Row #0: 192
Row #0: 200
Row #0: 151
Row #0: 193
Row #0: 142
Row #0: 152
Row #0: 184
Row #0: 199
Row #0: 126
Row #0: 172
Row #0: 148
Row #0: 209
Row #0: 154
Row #0: 150
Row #0: 180
Row #0: 175
Row #0: 242
Row #0: 218
Row #0: 205
Row #0: 163
Row #0: 192
Row #0: 146
Row #0: 141
Row #0: 163
Row #0: 180
Row #0: 211
Row #0: 149
Row #0: 166
Row #0: 190
Row #0: 182
Row #0: 184
Row #0: 197
Row #0: 173
Row #0: 232
Row #0: 224
Row #0: 182
Row #0: 146
Row #0: 165
Row #0: 178
Row #0: 176
Row #0: 192
Row #0: 152
Row #0: 161
Row #0: 157
Row #0: 147
Row #0: 171
Row #0: 157
Row #0: 129
Row #0: 175
Row #0: 166
Row #0: 180
Row #0: 185
Row #0: 194
Row #0: 222
Row #0: 170
Row #0: 161
Row #0: 147
Row #0: 180
Row #0: 158
Row #0: 217
Row #0: 107
Row #0: 153
Row #0: 213
Row #0: 196
Row #0: 119
Row #0: 163
Row #0: 169
Row #0: 196
Row #0: 202
Row #0: 152
Row #0: 161
Row #0: 169
Row #0: 174
Row #0: 134
Row #0: 168
Row #0: 189
Row #0: 150
Row #0: 186
Row #0: 179
Row #0: 202
Row #0: 149
Row #0: 186
Row #0: 246
Row #0: 185
Row #0: 186
Row #0: 184
Row #0: 192
Row #0: 157
Row #0: 203
Row #0: 136
Row #0: 146
Row #0: 139
Row #0: 134
Row #0: 155
Row #0: 135
Row #0: 120
Row #0: 141
Row #0: 173
Row #0: 199
Row #0: 149
Row #0: 144
Row #0: 231
Row #0: 204
Row #0: 183
Row #0: 140
Row #0: 168
Row #0: 157
Row #0: 180
Row #0: 134
Row #0: 218
Row #0: 136
Row #0: 155
Row #0: 150
Row #0: 149
Row #0: 169
Row #0: 195
Row #0: 231
Row #0: 200
Row #0: 201
Row #0: 183
Row #0: 202
Row #0: 166
Row #0: 192
Row #0: 158
Row #0: 186
Row #0: 194
Row #0: 156
Row #0: 174
Row #0: 137
Row #0: 192
Row #0: 160
Row #0: 168
Row #0: 133
Row #0: 177
Row #0: 162
Row #0: 160
Row #0: 205
Row #0: 155
Row #0: 156
Row #0: 157
Row #0: 210
Row #0: 185
Row #0: 174
Row #0: 153
Row #0: 169
Row #0: 190
Row #0: 184
Row #0: 182
Row #0: 180
Row #0: 170
Row #0: 127
Row #0: 208
Row #0: 188
Row #0: 174
Row #0: 195
Row #0: 166
Row #0: 151
Row #0: 170
Row #0: 142
Row #0: 184
Row #0: 162
Row #0: 134
Row #0: 168
Row #0: 190
Row #0: 167
Row #0: 170
Row #0: 157
Row #0: 150
Row #0: 235
Row #0: 216
Row #0: 207
Row #0: 162
Row #0: 185
Row #0: 230
Row #0: 173
Row #0: 176
Row #0: 173
Row #0: 212
Row #0: 168
Row #0: 120
Row #0: 131
Row #0: 107
Row #0: 150
Row #0: 94
Row #0: 143
Row #0: 151
Row #0: 164
Row #0: 144
Row #0: 156
Row #0: 179
Row #0: 157
Row #0: 159
Row #0: 142
Row #0: 213
Row #0: 203
Row #0: 182
Row #0: 167
Row #0: 162
Row #0: 154
Row #0: 191
Row #0: 162
Row #0: 165
Row #0: 194
Row #0: 172
Row #0: 151
Row #0: 127
Row #0: 161
Row #0: 148
Row #0: 185
Row #0: 202
Row #0: 151
Row #0: 154
Row #0: 150
Row #0: 152
Row #0: 206
Row #0: 162
Row #0: 199
Row #0: 146
Row #0: 152
Row #0: 203
Row #0: 143
Row #0: 167
Row #0: 211
Row #0: 172
Row #0: 188
Row #0: 148
Row #0: 138
Row #0: 137
Row #0: 210
Row #0: 216
Row #0: 172
Row #0: 160
Row #0: 184
Row #0: 213
Row #0: 202
Row #0: 184
Row #0: 185
Row #0: 167
Row #0: 168
Row #0: 168
Row #0: 165
Row #0: 180
Row #0: 160
Row #0: 161
Row #0: 204
Row #0: 201
Row #0: 156
Row #0: 169
Row #0: 143
Row #0: 179
Row #0: 178
Row #0: 171
Row #0: 123
Row #0: 168
Row #0: 180
Row #0: 169
Row #0: 175
Row #0: 192
Row #0: 177
Row #0: 207
Row #0: 168
Row #0: 127
Row #0: 173
Row #0: 161
Row #0: 207
Row #0: 172
Row #0: 182
Row #0: 154
Row #0: 176
Row #0: 126
Row #0: 210
Row #0: 133
Row #0: 157
Row #0: 153
Row #0: 138
Row #0: 195
Row #0: 205
Row #0: 180
Row #0: 257
Row #0: 202
Row #0: 195
Row #0: 184
Row #0: 211
Row #0: 165
Row #0: 190
Row #0: 239
Row #0: 148
Row #0: 173
Row #0: 180
Row #0: 143
Row #0: 224
Row #0: 192
Row #0: 187
Row #0: 199
Row #0: 221
Row #0: 200
Row #0: 195
Row #0: 220
Row #0: 196
Row #0: 160
Row #0: 139
Row #0: 159
Row #0: 139
Row #0: 148
Row #0: 138
Row #0: 159
Row #0: 186
Row #0: 163
Row #0: 156
Row #0: 176
Row #0: 152
Row #0: 168
Row #0: 162
Row #0: 137
Row #0: 154
Row #0: 143
Row #0: 156
Row #0: 185
Row #0: 151
Row #0: 122
Row #0: 157
Row #0: 161
Row #0: 145
Row #0: 190
Row #0: 174
Row #0: 172
Row #0: 182
Row #0: 182
Row #0: 192
Row #0: 211
Row #0: 186
Row #0: 231
Row #0: 177
Row #0: 197
Row #0: 194
Row #0: 170
Row #0: 188
Row #0: 183
Row #0: 143
Row #0: 148
Row #0: 151
Row #0: 142
Row #0: 184
Row #0: 201
Row #0: 188
Row #0: 207
Row #0: 186
Row #0: 166
Row #0: 158
Row #0: 173
Row #0: 165
Row #0: 209
Row #0: 131
Row #0: 160
Row #0: 193
Row #0: 175
Row #0: 189
Row #0: 148
Row #0: 171
Row #0: 197
Row #0: 158
Row #0: 104
Row #0: 214
Row #0: 128
Row #0: 191
Row #0: 157
Row #0: 149
Row #0: 141
Row #0: 153
Row #0: 160
Row #0: 154
Row #0: 175
Row #0: 186
Row #0: 174
Row #0: 181
Row #0: 119
Row #0: 186
Row #0: 178
Row #0: 198
Row #0: 195
Row #0: 172
Row #0: 131
Row #0: 124
Row #0: 140
Row #0: 106
Row #0: 159
Row #0: 234
Row #0: 159
Row #0: 139
Row #0: 198
Row #0: 215
Row #0: 197
Row #0: 160
Row #0: 163
Row #0: 172
Row #0: 134
Row #0: 184
Row #0: 146
Row #0: 159
Row #0: 132
Row #0: 155
Row #0: 228
Row #0: 174
Row #0: 174
Row #0: 218
Row #0: 177
Row #0: 170
Row #0: 151
Row #0: 142
Row #0: 174
Row #0: 189
Row #0: 158
Row #0: 155
Row #0: 142
Row #0: 141
Row #0: 175
Row #0: 197
Row #0: 144
Row #0: 159
Row #0: 167
Row #0: 183
Row #0: 159
Row #0: 207
Row #0: 214
Row #0: 196
Row #0: 180
Row #0: 163
Row #0: 174
Row #0: 154
Row #0: 188
Row #0: 162
Row #0: 148
Row #0: 213
Row #0: 180
Row #0: 176
Row #0: 164
Row #0: 178
Row #0: 140
Row #0: 194
Row #0: 143
Row #0: 121
Row #0: 162
Row #0: 218
Row #0: 204
Row #0: 173
Row #0: 142
Row #0: 170
Row #0: 130
Row #0: 173
Row #0: 181
Row #0: 197
Row #0: 168
Row #0: 198
Row #0: 172
Row #0: 183
Row #0: 184
Row #0: 186
Row #0: 118
Row #0: 107
Row #0: 152
Row #0: 195
Row #0: 189
Row #0: 189
Row #0: 184
Row #0: 172
Row #0: 135
Row #0: 180
Row #0: 130
Row #0: 220
Row #0: 169
Row #0: 153
Row #0: 131
Row #0: 210
Row #0: 145
Row #0: 171
Row #0: 196
Row #0: 117
Row #0: 158
Row #0: 121
Row #0: 197
Row #0: 198
Row #0: 175
Row #0: 164
Row #0: 195
Row #0: 142
Row #0: 181
Row #0: 186
Row #0: 155
Row #0: 175
Row #0: 149
Row #0: 179
Row #0: 174
Row #0: 158
Row #0: 158
Row #0: 162
Row #0: 205
Row #0: 114
Row #0: 167
Row #0: 110
Row #0: 150
Row #0: 149
Row #0: 175
Row #0: 187
Row #0: 170
Row #0: 154
Row #0: 173
Row #0: 152
Row #0: 163
Row #0: 154
Row #0: 181
Row #0: 179
Row #0: 199
Row #0: 156
Row #0: 188
Row #0: 178
Row #0: 175
Row #0: 210
Row #0: 189
Row #0: 177
Row #0: 129
Row #0: 141
Row #0: 149
Row #0: 157
Row #0: 191
Row #0: 166
Row #0: 149
Row #0: 169
Row #0: 185
Row #0: 193
Row #0: 224
Row #0: 130
Row #0: 193
Row #0: 216
Row #0: 188
Row #0: 167
Row #0: 138
Row #0: 150
Row #0: 258
Row #0: 179
Row #0: 201
Row #0: 219
Row #0: 139
Row #0: 197
Row #0: 152
Row #0: 168
Row #0: 189
Row #0: 215
Row #0: 155
Row #0: 175
Row #0: 192
Row #0: 158
Row #0: 163
Row #0: 131
Row #0: 164
Row #0: 128
Row #0: 184
Row #0: 165
Row #0: 156
Row #0: 177
Row #0: 191
Row #0: 189
Row #0: 188
Row #0: 176
Row #0: 203
Row #0: 167
Row #0: 166
Row #0: 176
Row #0: 203
Row #0: 161
Row #0: 150
Row #0: 156
Row #0: 192
Row #0: 174
Row #0: 177
Row #0: 158
Row #0: 161
Row #0: 157
Row #0: 184
Row #0: 125
Row #0: 155
Row #0: 191
Row #0: 164
Row #0: 174
Row #0: 169
Row #0: 184
Row #0: 220
Row #0: 221
Row #0: 104
Row #0: 192
Row #0: 157
Row #0: 176
Row #0: 186
Row #0: 182
Row #0: 170
Row #0: 175
Row #0: 145
Row #0: 160
Row #0: 153
Row #0: 159
Row #0: 198
Row #0: 175
Row #0: 183
Row #0: 204
Row #0: 152
Row #0: 165
Row #0: 168
Row #0: 136
Row #0: 192
Row #0: 175
Row #0: 146
Row #0: 197
Row #0: 204
Row #0: 154
Row #0: 199
Row #0: 193
Row #0: 185
Row #0: 172
Row #0: 189
Row #0: 184
Row #0: 184
Row #0: 158
Row #0: 183
Row #0: 156
Row #0: 160
Row #0: 127
Row #0: 159
Row #0: 210
Row #0: 205
Row #0: 135
Row #0: 204
Row #0: 183
Row #0: 164
Row #0: 165
Row #0: 183
Row #0: 167
Row #0: 162
Row #0: 174
Row #0: 111
Row #0: 160
Row #0: 196
Row #0: 169
Row #0: 177
Row #0: 203
Row #0: 213
Row #0: 177
Row #0: 199
Row #0: 109
Row #0: 171
Row #0: 148
Row #0: 218
Row #0: 167
Row #0: 195
Row #0: 167
Row #0: 161
Row #0: 141
Row #0: 165
Row #0: 163
Row #0: 151
Row #0: 219
Row #0: 214
Row #0: 199
Row #0: 183
Row #0: 142
Row #0: 188
Row #0: 124
Row #0: 160
Row #0: 125
Row #0: 164
Row #0: 186
Row #0: 188
Row #0: 160
Row #0: 148
Row #0: 185
Row #0: 179
Row #0: 166
Row #0: 163
Row #0: 190
Row #0: 136
Row #0: 174
Row #0: 174
Row #0: 162
Row #0: 149
Row #0: 184
Row #0: 189
Row #0: 162
Row #0: 149
Row #0: 136
Row #0: 208
Row #0: 202
Row #0: 205
Row #0: 192
Row #0: 164
Row #0: 187
Row #0: 171
Row #0: 191
Row #0: 180
Row #0: 199
Row #0: 189
Row #0: 141
Row #0: 181
Row #0: 210
Row #0: 191
Row #0: 140
Row #0: 172
Row #0: 197
Row #0: 154
Row #0: 122
Row #0: 176
Row #0: 159
Row #0: 144
Row #0: 150
Row #0: 194
Row #0: 168
Row #0: 197
Row #0: 182
Row #0: 190
Row #0: 155
Row #0: 234
Row #0: 190
Row #0: 213
Row #0: 228
Row #0: 203
Row #0: 144
Row #0: 142
Row #0: 158
Row #0: 195
Row #0: 158
Row #0: 159
Row #0: 143
Row #0: 123
Row #0: 143
Row #0: 135
Row #0: 183
Row #0: 164
Row #0: 208
Row #0: 183
Row #0: 150
Row #0: 174
Row #0: 130
Row #0: 183
Row #0: 176
Row #0: 166
Row #0: 151
Row #0: 137
Row #0: 153
Row #0: 226
Row #0: 163
Row #0: 133
Row #0: 149
Row #0: 195
Row #0: 175
Row #0: 178
Row #0: 138
Row #0: 141
Row #0: 176
Row #0: 135
Row #0: 147
Row #0: 151
Row #0: 178
Row #0: 158
Row #0: 201
Row #0: 243
Row #0: 154
Row #0: 220
Row #0: 161
Row #0: 133
Row #0: 153
Row #0: 162
Row #0: 207
Row #0: 210
Row #0: 195
Row #0: 151
Row #0: 171
Row #0: 136
Row #0: 166
Row #0: 163
Row #0: 194
Row #0: 138
Row #0: 141
Row #0: 192
Row #0: 143
Row #0: 143
Row #0: 157
Row #0: 165
Row #0: 155
Row #0: 225
Row #0: 171
Row #0: 156
Row #0: 205
Row #0: 139
Row #0: 209
Row #0: 150
Row #0: 147
Row #0: 163
Row #0: 153
Row #0: 182
Row #0: 132
Row #0: 218
Row #0: 137
Row #0: 143
Row #0: 150
Row #0: 198
Row #0: 195
Row #0: 182
Row #0: 221
Row #0: 231
Row #0: 168
Row #0: 161
Row #0: 200
Row #0: 152
Row #0: 179
Row #0: 148
Row #0: 144
Row #0: 172
Row #0: 184
Row #0: 157
Row #0: 143
Row #0: 205
Row #0: 140
Row #0: 203
Row #0: 186
Row #0: 186
Row #0: 160
Row #0: 155
Row #0: 159
Row #0: 126
Row #0: 170
Row #0: 178
Row #0: 188
Row #0: 190
Row #0: 198
Row #0: 118
Row #0: 129
Row #0: 164
Row #0: 159
Row #0: 164
Row #0: 179
Row #0: 142
Row #0: 178
Row #0: 216
Row #0: 164
Row #0: 157
Row #0: 167
Row #0: 189
Row #0: 163
Row #0: 206
Row #0: 156
Row #0: 163
Row #0: 122
Row #0: 176
Row #0: 174
Row #0: 188
Row #0: 195
Row #0: 137
Row #0: 208
Row #0: 164
Row #0: 174
Row #0: 190
Row #0: 146
Row #0: 130
Row #0: 171
Row #0: 145
Row #0: 141
Row #0: 142
Row #0: 201
Row #0: 140
Row #0: 128
Row #0: 131
Row #0: 221
Row #0: 187
Row #0: 197
Row #0: 195
Row #0: 173
Row #0: 185
Row #0: 122
Row #0: 173
Row #0: 169
Row #0: 198
Row #0: 215
Row #0: 186
Row #0: 167
Row #0: 139
Row #0: 168
Row #0: 229
Row #0: 177
Row #0: 180
Row #0: 155
Row #0: 131
Row #0: 121
Row #0: 179
Row #0: 144
Row #0: 135
Row #0: 143
Row #0: 177
Row #0: 171
Row #0: 164
Row #0: 203
Row #0: 181
Row #0: 191
Row #0: 180
Row #0: 202
Row #0: 184
Row #0: 154
Row #0: 170
Row #0: 170
Row #0: 131
Row #0: 171
Row #0: 134
Row #0: 202
Row #0: 170
Row #0: 169
Row #0: 202
Row #0: 156
Row #0: 174
Row #0: 120
Row #0: 146
Row #0: 220
Row #0: 211
Row #0: 178
Row #0: 176
Row #0: 147
Row #0: 217
Row #0: 170
Row #0: 200
Row #0: 170
Row #0: 152
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0", "product_class"."product_department" as "c1", "product_class"."product_category" as "c2", "product_class"."product_subcategory" as "c3", "product"."brand_name" as "c4", "product"."product_name" as "c5", "product"."product_id" as "c6" from "sales_fact_1997" as "sales_fact_1997", "product" as "product", "product_class" as "product_class", "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" and "sales_fact_1997"."time_id" = "time_by_day"."time_id" group by "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id" having (sum("sales_fact_1997"."unit_sales") > 0) order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC, CASE WHEN "product_class"."product_department" IS NULL THEN 1 ELSE 0 END, "product_class"."product_department" ASC, CASE WHEN "product_class"."product_category" IS NULL THEN 1 ELSE 0 END, "product_class"."product_category" ASC, CASE WHEN "product_class"."product_subcategory" IS NULL THEN 1 ELSE 0 END, "product_class"."product_subcategory" ASC, CASE WHEN "product"."brand_name" IS NULL THEN 1 ELSE 0 END, "product"."brand_name" ASC, CASE WHEN "product"."product_name" IS NULL THEN 1 ELSE 0 END, "product"."product_name" ASC, CASE WHEN "product"."product_id" IS NULL THEN 1 ELSE 0 END, "product"."product_id" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id"
FROM "sales_fact_1997"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
GROUP BY "product"."product_id", "product"."brand_name", "product"."product_name", "product_class"."product_subcategory", "product_class"."product_category", "product_class"."product_department", "product_class"."product_family"
HAVING SUM("sales_fact_1997"."unit_sales") > 0
ORDER BY "product_class"."product_family", "product_class"."product_department", "product_class"."product_category", "product_class"."product_subcategory", "product"."brand_name", "product"."product_name", "product"."product_id"
```

*Delta:* rowCount matches (1559); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -722 chars (legacy=1652, calcite=930).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_id") from "product"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_id") AS "c"
FROM "product"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "product"."product_id" as "c1", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product" where "time_by_day"."the_year" = 1997 and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" group by "time_by_day"."the_year", "product"."product_id"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product"."product_id", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product"."product_id"
```

*Delta:* rowCount matches (1559); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-agg-distinct-count-product-family-weekly"></a>agg-distinct-count-product-family-weekly

**MDX:**

```mdx
SELECT
  [Measures].[Unit Sales] on columns,
  [Product].[Product Family].Members on rows
FROM Sales
WHERE {
  [Time].[All Weeklys].[1997].[1].[15],
  [Time].[All Weeklys].[1997].[2].[1]}
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Weekly].[1997].[1].[15]}
{[Time].[Weekly].[1997].[2].[1]}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 32
Row #1: 273
Row #2: 43
```

**SQL executions:** legacy=12 · Calcite=12

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0" from "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 group by "time_by_day"."the_year" order by CASE WHEN "time_by_day"."the_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."the_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."the_year" as "c0" from "time_by_day" as "time_by_day" where "time_by_day"."the_year" = 1997 group by "time_by_day"."the_year" order by CASE WHEN "time_by_day"."the_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."the_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."week_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and "time_by_day"."week_of_year" = 1 group by "time_by_day"."week_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."week_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."week_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."week_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and "time_by_day"."week_of_year" = 1 group by "time_by_day"."week_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."week_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."week_of_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 1) and "time_by_day"."day_of_month" = 15 group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Calcite:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 1) and "time_by_day"."day_of_month" = 15 group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."week_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and "time_by_day"."week_of_year" = 2 group by "time_by_day"."week_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."week_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."week_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."week_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and "time_by_day"."week_of_year" = 2 group by "time_by_day"."week_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."week_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."week_of_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 2) and "time_by_day"."day_of_month" = 1 group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Calcite:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 2) and "time_by_day"."day_of_month" = 1 group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -107 chars (legacy=341, calcite=234).

#### sqlExecution[6]

*Legacy:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 1) group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Calcite:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 1) group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Delta:* rowCount matches (6); SQL identical.

#### sqlExecution[7]

*Legacy:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 2) group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Calcite:*

```sql
select "time_by_day"."time_id" as "c0", "time_by_day"."day_of_month" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."week_of_year" = 2) group by "time_by_day"."time_id", "time_by_day"."day_of_month" order by CASE WHEN "time_by_day"."time_id" IS NULL THEN 0 ELSE 1 END, "time_by_day"."time_id" ASC
```

*Delta:* rowCount matches (11); SQL identical.

#### sqlExecution[8]

*Legacy:*

```sql
select count(distinct "time_id") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "time_id") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[9]

*Legacy:*

```sql
select count(distinct "product_family") from "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[10]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[11]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "time_by_day"."time_id" as "c1", "product_class"."product_family" as "c2", sum("sales_fact_1997"."unit_sales") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "product" as "product", "product_class" as "product_class" where "time_by_day"."the_year" = 1997 and "time_by_day"."time_id" in (367, 715) and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."product_id" = "product"."product_id" and "product"."product_class_id" = "product_class"."product_class_id" group by "time_by_day"."the_year", "time_by_day"."time_id", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "the_year", "time_id0" AS "time_id", "product_family", SUM("unit_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."time_id0" IN (367, 715)
GROUP BY "time_id0", "the_year", "product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1598 chars (legacy=656, calcite=2254).

### <a name="q-agg-distinct-count-customers-levels"></a>agg-distinct-count-customers-levels

**MDX:**

```mdx
select {[Customers].[USA], [Customers].[USA].[OR], [Customers].[USA].[WA]} on columns, {[Measures].[Customer Count]} on rows from [Sales]
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Customer].[Customers].[USA]}
{[Customer].[Customers].[USA].[OR]}
{[Customer].[Customers].[USA].[WA]}
Axis #2:
{[Measures].[Customer Count]}
Row #0: 5,581
Row #0: 1,037
Row #0: 1,828
```

**SQL executions:** legacy=12 · Calcite=12

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Customers') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Calcite:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Customers') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Customers') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Calcite:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Customers') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Customers') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Customers') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Customers') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Calcite:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Customers') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Customers') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Calcite:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Customers') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select "customer"."state_province" as "c0", "customer"."country" as "c1" from "customer" as "customer" where ("customer"."country" = 'USA') and UPPER("customer"."state_province") = UPPER('OR') group by "customer"."state_province", "customer"."country" order by CASE WHEN "customer"."state_province" IS NULL THEN 0 ELSE 1 END, "customer"."state_province" ASC
```

*Calcite:*

```sql
select "customer"."state_province" as "c0", "customer"."country" as "c1" from "customer" as "customer" where ("customer"."country" = 'USA') and UPPER("customer"."state_province") = UPPER('OR') group by "customer"."state_province", "customer"."country" order by CASE WHEN "customer"."state_province" IS NULL THEN 0 ELSE 1 END, "customer"."state_province" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[6]

*Legacy:*

```sql
select "customer"."state_province" as "c0", "customer"."country" as "c1" from "customer" as "customer" where ("customer"."country" = 'USA') and UPPER("customer"."state_province") = UPPER('WA') group by "customer"."state_province", "customer"."country" order by CASE WHEN "customer"."state_province" IS NULL THEN 0 ELSE 1 END, "customer"."state_province" ASC
```

*Calcite:*

```sql
select "customer"."state_province" as "c0", "customer"."country" as "c1" from "customer" as "customer" where ("customer"."country" = 'USA') and UPPER("customer"."state_province") = UPPER('WA') group by "customer"."state_province", "customer"."country" order by CASE WHEN "customer"."state_province" IS NULL THEN 0 ELSE 1 END, "customer"."state_province" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[7]

*Legacy:*

```sql
select count(distinct "the_year") from "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[8]

*Legacy:*

```sql
select count(distinct "country") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "country") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[9]

*Legacy:*

```sql
select count(distinct "state_province") from "customer"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "state_province") AS "c"
FROM "customer"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[10]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "customer"."country" as "c1", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "customer"."country" = 'USA' and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."country"
```

*Calcite:*

```sql
SELECT "the_year", "country", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "customer"."customer_id" AS "customer_id0", "customer"."account_num" AS "account_num", "customer"."lname" AS "lname", "customer"."fname" AS "fname", "customer"."mi" AS "mi", "customer"."address1" AS "address1", "customer"."address2" AS "address2", "customer"."address3" AS "address3", "customer"."address4" AS "address4", "customer"."city" AS "city", "customer"."state_province" AS "state_province", "customer"."postal_code" AS "postal_code", "customer"."country" AS "country", "customer"."customer_region_id" AS "customer_region_id", "customer"."phone1" AS "phone1", "customer"."phone2" AS "phone2", "customer"."birthdate" AS "birthdate", "customer"."marital_status" AS "marital_status", "customer"."yearly_income" AS "yearly_income", "customer"."gender" AS "gender", "customer"."total_children" AS "total_children", "customer"."num_children_at_home" AS "num_children_at_home", "customer"."education" AS "education", "customer"."date_accnt_opened" AS "date_accnt_opened", "customer"."member_card" AS "member_card", "customer"."occupation" AS "occupation", "customer"."houseowner" AS "houseowner", "customer"."num_cars_owned" AS "num_cars_owned", "customer"."fullname" AS "fullname"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."country" = 'USA'
GROUP BY "the_year", "country"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1896 chars (legacy=476, calcite=2372).

#### sqlExecution[11]

*Legacy:*

```sql
select "time_by_day"."the_year" as "c0", "customer"."country" as "c1", "customer"."state_province" as "c2", count(distinct "sales_fact_1997"."customer_id") as "m0" from "sales_fact_1997" as "sales_fact_1997", "time_by_day" as "time_by_day", "customer" as "customer" where "time_by_day"."the_year" = 1997 and "customer"."country" = 'USA' and "customer"."state_province" in ('OR', 'WA') and "sales_fact_1997"."time_id" = "time_by_day"."time_id" and "sales_fact_1997"."customer_id" = "customer"."customer_id" group by "time_by_day"."the_year", "customer"."country", "customer"."state_province"
```

*Calcite:*

```sql
SELECT "the_year", "country", "state_province", COUNT(DISTINCT "customer_id") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "customer"."customer_id" AS "customer_id0", "customer"."account_num" AS "account_num", "customer"."lname" AS "lname", "customer"."fname" AS "fname", "customer"."mi" AS "mi", "customer"."address1" AS "address1", "customer"."address2" AS "address2", "customer"."address3" AS "address3", "customer"."address4" AS "address4", "customer"."city" AS "city", "customer"."state_province" AS "state_province", "customer"."postal_code" AS "postal_code", "customer"."country" AS "country", "customer"."customer_region_id" AS "customer_region_id", "customer"."phone1" AS "phone1", "customer"."phone2" AS "phone2", "customer"."birthdate" AS "birthdate", "customer"."marital_status" AS "marital_status", "customer"."yearly_income" AS "yearly_income", "customer"."gender" AS "gender", "customer"."total_children" AS "total_children", "customer"."num_children_at_home" AS "num_children_at_home", "customer"."education" AS "education", "customer"."date_accnt_opened" AS "date_accnt_opened", "customer"."member_card" AS "member_card", "customer"."occupation" AS "occupation", "customer"."houseowner" AS "houseowner", "customer"."num_cars_owned" AS "num_cars_owned", "customer"."fullname" AS "fullname"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "customer" ON "sales_fact_1997"."customer_id" = "customer"."customer_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."country" = 'USA') AS "t0"
WHERE "state_province" IN ('OR', 'WA')
GROUP BY "the_year", "state_province", "country"
```

*Delta:* rowCount matches (2); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER; length delta 1881 chars (legacy=590, calcite=2471).

## <a name="calc"></a>Calc corpus

### <a name="q-calc-arith-ratio"></a>calc-arith-ratio

**MDX:**

```mdx
with member [Measures].[Sales Per Unit] as '[Measures].[Store Sales] / [Measures].[Unit Sales]'
select {[Measures].[Sales Per Unit]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Sales Per Unit]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 1.99
Row #1: 2.13
Row #2: 2.14
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-arith-sum"></a>calc-arith-sum

**MDX:**

```mdx
with member [Measures].[Sales Plus Cost] as '[Measures].[Store Sales] + [Measures].[Store Cost]'
select {[Measures].[Sales Plus Cost]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Sales Plus Cost]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 68,313.44
Row #1: 572,306.31
Row #2: 150,245.61
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_cost") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_cost") AS "m0", SUM("sales_fact_1997"."store_sales") AS "m1"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-arith-unary-minus"></a>calc-arith-unary-minus

**MDX:**

```mdx
with member [Measures].[Neg Unit Sales] as '-[Measures].[Unit Sales]'
select {[Measures].[Neg Unit Sales]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Neg Unit Sales]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: -24,597
Row #1: -191,940
Row #2: -50,236
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-arith-const-multiply"></a>calc-arith-const-multiply

**MDX:**

```mdx
with member [Measures].[Unit Sales 10pct] as '[Measures].[Unit Sales] * 1.1'
select {[Measures].[Unit Sales 10pct]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Unit Sales 10pct]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 27,057
Row #1: 211,134
Row #2: 55,260
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-iif-numeric"></a>calc-iif-numeric

**MDX:**

```mdx
with member [Measures].[Sales If Big] as 'IIf([Measures].[Unit Sales] > 100, [Measures].[Store Sales], 0)'
select {[Measures].[Sales If Big]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Sales If Big]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 48,836
Row #1: 409,036
Row #2: 107,366
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-coalesce-empty"></a>calc-coalesce-empty

**MDX:**

```mdx
with member [Measures].[Sales Or Zero] as 'CoalesceEmpty([Measures].[Store Sales], 0)'
select {[Measures].[Sales Or Zero]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Sales Or Zero]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 48,836.21
Row #1: 409,035.59
Row #2: 107,366.33
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."store_sales") AS "m0"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-nested-arith"></a>calc-nested-arith

**MDX:**

```mdx
with member [Measures].[Margin Per Unit] as '([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Unit Sales]'
select {[Measures].[Margin Per Unit]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Margin Per Unit]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 1.19
Row #1: 1.28
Row #2: 1.28
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_cost") AS "m1", SUM("sales_fact_1997"."store_sales") AS "m2"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "time_by_day"."the_year", "product_class"."product_family", SUM("sales_fact_1997"."unit_sales") AS "m0", SUM("sales_fact_1997"."store_cost") AS "m1", SUM("sales_fact_1997"."store_sales") AS "m2"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997
GROUP BY "time_by_day"."the_year", "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-arith-with-filter"></a>calc-arith-with-filter

**MDX:**

```mdx
with member [Measures].[Profit] as '[Measures].[Store Sales] - [Measures].[Store Cost]'
select {[Measures].[Profit]} on columns,
  {[Product].[Product Family].members} on rows
from Sales
where ([Time].[1997].[Q1])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997].[Q1]}
Axis #1:
{[Measures].[Profit]}
Axis #2:
{[Product].[Products].[Drink]}
{[Product].[Products].[Food]}
{[Product].[Products].[Non-Consumable]}
Row #0: 6,964.30
Row #1: 60,814.47
Row #2: 16,097.34
```

**SQL executions:** legacy=6 · Calcite=6

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q1') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "product_class"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
SELECT "the_year", "quarter", "product_family", SUM("store_cost") AS "m0", SUM("store_sales") AS "m1"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."quarter" = 'Q1'
GROUP BY "the_year", "quarter", "product_family"
```

*Calcite:*

```sql
SELECT "the_year", "quarter", "product_family", SUM("store_cost") AS "m0", SUM("store_sales") AS "m1"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period", "product"."product_class_id" AS "product_class_id", "product"."product_id" AS "product_id0", "product"."brand_name" AS "brand_name", "product"."product_name" AS "product_name", "product"."SKU" AS "SKU", "product"."SRP" AS "SRP", "product"."gross_weight" AS "gross_weight", "product"."net_weight" AS "net_weight", "product"."recyclable_package" AS "recyclable_package", "product"."low_fat" AS "low_fat", "product"."units_per_case" AS "units_per_case", "product"."cases_per_pallet" AS "cases_per_pallet", "product"."shelf_width" AS "shelf_width", "product"."shelf_height" AS "shelf_height", "product"."shelf_depth" AS "shelf_depth", "product_class"."product_class_id" AS "product_class_id0", "product_class"."product_subcategory" AS "product_subcategory", "product_class"."product_category" AS "product_category", "product_class"."product_department" AS "product_department", "product_class"."product_family" AS "product_family"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
INNER JOIN "product" ON "sales_fact_1997"."product_id" = "product"."product_id"
INNER JOIN "product_class" ON "product"."product_class_id" = "product_class"."product_class_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."quarter" = 'Q1'
GROUP BY "the_year", "quarter", "product_family"
```

*Delta:* rowCount matches (3); SQL identical.

### <a name="q-calc-non-pushable-parent"></a>calc-non-pushable-parent

**MDX:**

```mdx
with member [Measures].[Parent Sales] as '([Measures].[Store Sales], [Store].[Stores].CurrentMember.Parent)'
select {[Measures].[Store Sales], [Measures].[Parent Sales]} on columns,
  {[Store].[All Stores].[USA].[CA].[Los Angeles]} on rows
from Sales
where ([Time].[1997])
```

**Cell-set (Calcite):**

```
Axis #0:
{[Time].[Time].[1997]}
Axis #1:
{[Measures].[Store Sales]}
{[Measures].[Parent Sales]}
Axis #2:
{[Store].[Stores].[USA].[CA].[Los Angeles]}
Row #0: 54,545.28
Row #0: 159,167.84
```

**SQL executions:** legacy=5 · Calcite=5

#### sqlExecution[0]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_state") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "store_city") AS "c"
FROM "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_city") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
SELECT "store_state", "the_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997
GROUP BY "store_state", "the_year"
```

*Calcite:*

```sql
SELECT "store_state", "the_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."the_year" = 1997
GROUP BY "store_state", "the_year"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
SELECT "store_state", "store_city", "the_year", SUM("store_sales") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."store_city" = 'Los Angeles') AS "t0"
WHERE "the_year" = 1997
GROUP BY "store_city", "store_state", "the_year"
```

*Calcite:*

```sql
SELECT "store_state", "store_city", "the_year", SUM("store_sales") AS "m0"
FROM (SELECT *
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "store"."store_id" AS "store_id0", "store"."store_type" AS "store_type", "store"."region_id" AS "region_id", "store"."store_name" AS "store_name", "store"."store_number" AS "store_number", "store"."store_street_address" AS "store_street_address", "store"."store_city" AS "store_city", "store"."store_state" AS "store_state", "store"."store_postal_code" AS "store_postal_code", "store"."store_country" AS "store_country", "store"."store_manager" AS "store_manager", "store"."store_phone" AS "store_phone", "store"."store_fax" AS "store_fax", "store"."first_opened_date" AS "first_opened_date", "store"."last_remodel_date" AS "last_remodel_date", "store"."store_sqft" AS "store_sqft", "store"."grocery_sqft" AS "grocery_sqft", "store"."frozen_sqft" AS "frozen_sqft", "store"."meat_sqft" AS "meat_sqft", "store"."coffee_bar" AS "coffee_bar", "store"."video_store" AS "video_store", "store"."salad_bar" AS "salad_bar", "store"."prepared_food" AS "prepared_food", "store"."florist" AS "florist", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "store" ON "sales_fact_1997"."store_id" = "store"."store_id"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "store"."store_state" = 'CA') AS "t"
WHERE "t"."store_city" = 'Los Angeles') AS "t0"
WHERE "the_year" = 1997
GROUP BY "store_city", "store_state", "the_year"
```

*Delta:* rowCount matches (1); SQL identical.

### <a name="q-calc-non-pushable-ytd"></a>calc-non-pushable-ytd

**MDX:**

```mdx
with member [Measures].[YTD Store Sales] as 'Sum(YTD(), [Measures].[Store Sales])', format_string='#.00'
select {[Measures].[YTD Store Sales]} on columns,
  {[Time].[1997].[Q2].[5]} on rows
from Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[YTD Store Sales]}
Axis #2:
{[Time].[Time].[1997].[Q2].[5]}
Row #0: 226962.89
```

**SQL executions:** legacy=8 · Calcite=8

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) and UPPER("time_by_day"."quarter") = UPPER('Q2') group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 5 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') and "time_by_day"."month_of_year" = 5 group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q1') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Calcite:*

```sql
select "time_by_day"."month_of_year" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997 and "time_by_day"."quarter" = 'Q2') group by "time_by_day"."month_of_year", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."month_of_year" IS NULL THEN 0 ELSE 1 END, "time_by_day"."month_of_year" ASC
```

*Delta:* rowCount matches (3); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "month_of_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "month_of_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[6]

*Legacy:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "time_by_day"
```

*Delta:* rowCount matches (1); SQL identical.

#### sqlExecution[7]

*Legacy:*

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4, 5)
GROUP BY "the_year", "month_of_year"
```

*Calcite:*

```sql
SELECT "the_year", "month_of_year", SUM("store_sales") AS "m0"
FROM (SELECT "sales_fact_1997"."product_id" AS "product_id", "sales_fact_1997"."time_id" AS "time_id", "sales_fact_1997"."customer_id" AS "customer_id", "sales_fact_1997"."promotion_id" AS "promotion_id", "sales_fact_1997"."store_id" AS "store_id", "sales_fact_1997"."store_sales" AS "store_sales", "sales_fact_1997"."store_cost" AS "store_cost", "sales_fact_1997"."unit_sales" AS "unit_sales", "time_by_day"."time_id" AS "time_id0", "time_by_day"."the_date" AS "the_date", "time_by_day"."the_day" AS "the_day", "time_by_day"."the_month" AS "the_month", "time_by_day"."the_year" AS "the_year", "time_by_day"."day_of_month" AS "day_of_month", "time_by_day"."week_of_year" AS "week_of_year", "time_by_day"."month_of_year" AS "month_of_year", "time_by_day"."quarter" AS "quarter", "time_by_day"."fiscal_period" AS "fiscal_period"
FROM "sales_fact_1997"
INNER JOIN "time_by_day" ON "sales_fact_1997"."time_id" = "time_by_day"."time_id"
WHERE "time_by_day"."the_year" = 1997) AS "t"
WHERE "t"."month_of_year" IN (1, 2, 3, 4, 5)
GROUP BY "the_year", "month_of_year"
```

*Delta:* rowCount matches (5); SQL identical.

## <a name="mvhit"></a>Mv-hit corpus

### <a name="q-agg-g-ms-pcat-family-gender"></a>agg-g-ms-pcat-family-gender

**MDX:**

```mdx
SELECT {[Measures].[Unit Sales]} ON COLUMNS,
  CROSSJOIN([Product].[Product Family].Members, [Gender].[Gender].Members) ON ROWS
FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Product].[Products].[Drink], [Customer].[Gender].[F]}
{[Product].[Products].[Drink], [Customer].[Gender].[M]}
{[Product].[Products].[Food], [Customer].[Gender].[F]}
{[Product].[Products].[Food], [Customer].[Gender].[M]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[F]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[M]}
Row #0: 12,202
Row #1: 12,395
Row #2: 94,814
Row #3: 97,126
Row #4: 24,542
Row #5: 25,694
```

**SQL executions:** legacy=6 · Calcite=6

#### sqlExecution[0]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -107 chars (legacy=341, calcite=234).

#### sqlExecution[1]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 1 ELSE 0 END, "customer"."gender" ASC
```

*Calcite:*

```sql
SELECT "gender"
FROM "customer"
GROUP BY "gender"
ORDER BY "gender"
```

*Delta:* rowCount matches (2); keyword case: lower -> UPPER; length delta -116 chars (legacy=183, calcite=67).

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "product_family") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "gender") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "gender") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "the_year") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select "agg_g_ms_pcat_sales_fact_1997"."the_year" as "c0", "agg_g_ms_pcat_sales_fact_1997"."product_family" as "c1", "agg_g_ms_pcat_sales_fact_1997"."gender" as "c2", sum("agg_g_ms_pcat_sales_fact_1997"."unit_sales") as "m0" from "agg_g_ms_pcat_sales_fact_1997" as "agg_g_ms_pcat_sales_fact_1997" where "agg_g_ms_pcat_sales_fact_1997"."the_year" = 1997 group by "agg_g_ms_pcat_sales_fact_1997"."the_year", "agg_g_ms_pcat_sales_fact_1997"."product_family", "agg_g_ms_pcat_sales_fact_1997"."gender"
```

*Calcite:*

```sql
SELECT "the_year", "product_family", "gender", SUM("unit_sales") AS "m0"
FROM "agg_g_ms_pcat_sales_fact_1997"
WHERE "the_year" = 1997
GROUP BY "gender", "product_family", "the_year"
```

*Delta:* rowCount matches (6); Calcite adds AS aliases; keyword case: lower -> UPPER; length delta -315 chars (legacy=496, calcite=181).

### <a name="q-agg-c-year-country"></a>agg-c-year-country

**MDX:**

```mdx
SELECT {[Measures].[Unit Sales]} ON COLUMNS,
  CROSSJOIN([Time].[Year].Members, [Store].[Store Country].Members) ON ROWS
FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Time].[Time].[1997], [Store].[Stores].[Canada]}
{[Time].[Time].[1997], [Store].[Stores].[Mexico]}
{[Time].[Time].[1997], [Store].[Stores].[USA]}
{[Time].[Time].[1998], [Store].[Stores].[Canada]}
{[Time].[Time].[1998], [Store].[Stores].[Mexico]}
{[Time].[Time].[1998], [Store].[Stores].[USA]}
Row #0: 
Row #1: 
Row #2: 266,773
Row #3: 
Row #4: 
Row #5:
```

**SQL executions:** legacy=4 · Calcite=4

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0" from "store" as "store" group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 1 ELSE 0 END, "store"."store_country" ASC
```

*Calcite:*

```sql
SELECT "store_country"
FROM "store"
GROUP BY "store_country"
ORDER BY "store_country"
```

*Delta:* rowCount matches (3); keyword case: lower -> UPPER; length delta -108 chars (legacy=193, calcite=85).

#### sqlExecution[1]

*Legacy:*

```sql
select count(distinct "store_country") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_country") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "the_year") from "agg_c_14_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "agg_c_14_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select "store"."store_country" as "c0", "agg_c_14_sales_fact_1997"."the_year" as "c1", sum("agg_c_14_sales_fact_1997"."unit_sales") as "m0" from "agg_c_14_sales_fact_1997" as "agg_c_14_sales_fact_1997", "store" as "store" where "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id" group by "store"."store_country", "agg_c_14_sales_fact_1997"."the_year"
```

*Calcite:*

```sql
SELECT "store"."store_country", "agg_c_14_sales_fact_1997"."the_year", SUM("agg_c_14_sales_fact_1997"."unit_sales") AS "m0"
FROM "agg_c_14_sales_fact_1997"
INNER JOIN "store" ON "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id"
GROUP BY "agg_c_14_sales_fact_1997"."the_year", "store"."store_country"
```

*Delta:* rowCount matches (1); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-agg-c-quarter-country"></a>agg-c-quarter-country

**MDX:**

```mdx
SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
  CROSSJOIN([Time].[1997].Children, [Store].[Store Country].Members) ON ROWS
FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
{[Measures].[Store Sales]}
Axis #2:
{[Time].[Time].[1997].[Q1], [Store].[Stores].[Canada]}
{[Time].[Time].[1997].[Q1], [Store].[Stores].[Mexico]}
{[Time].[Time].[1997].[Q1], [Store].[Stores].[USA]}
{[Time].[Time].[1997].[Q2], [Store].[Stores].[Canada]}
{[Time].[Time].[1997].[Q2], [Store].[Stores].[Mexico]}
{[Time].[Time].[1997].[Q2], [Store].[Stores].[USA]}
{[Time].[Time].[1997].[Q3], [Store].[Stores].[Canada]}
{[Time].[Time].[1997].[Q3], [Store].[Stores].[Mexico]}
{[Time].[Time].[1997].[Q3], [Store].[Stores].[USA]}
{[Time].[Time].[1997].[Q4], [Store].[Stores].[Canada]}
{[Time].[Time].[1997].[Q4], [Store].[Stores].[Mexico]}
{[Time].[Time].[1997].[Q4], [Store].[Stores].[USA]}
Row #0: 
Row #0: 
Row #1: 
Row #1: 
Row #2: 66,291
Row #2: 139,628.35
Row #3: 
Row #3: 
Row #4: 
Row #4: 
Row #5: 62,610
Row #5: 132,666.27
Row #6: 
Row #6: 
Row #7: 
Row #7: 
Row #8: 65,848
Row #8: 140,271.89
Row #9: 
Row #9: 
Row #10: 
Row #10: 
Row #11: 72,024
Row #11: 152,671.62
```

**SQL executions:** legacy=6 · Calcite=6

#### sqlExecution[0]

*Legacy:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Calcite:*

```sql
select "time_by_day"."quarter" as "c0", "time_by_day"."the_year" as "c1" from "time_by_day" as "time_by_day" where ("time_by_day"."the_year" = 1997) group by "time_by_day"."quarter", "time_by_day"."the_year" order by CASE WHEN "time_by_day"."quarter" IS NULL THEN 0 ELSE 1 END, "time_by_day"."quarter" ASC
```

*Delta:* rowCount matches (4); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "store"."store_country" as "c0" from "store" as "store" group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 1 ELSE 0 END, "store"."store_country" ASC
```

*Calcite:*

```sql
SELECT "store_country"
FROM "store"
GROUP BY "store_country"
ORDER BY "store_country"
```

*Delta:* rowCount matches (3); keyword case: lower -> UPPER; length delta -108 chars (legacy=193, calcite=85).

#### sqlExecution[2]

*Legacy:*

```sql
select count(distinct "store_country") from "store"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "store_country") AS "c"
FROM "store"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[3]

*Legacy:*

```sql
select count(distinct "quarter") from "agg_c_14_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "quarter") AS "c"
FROM "agg_c_14_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[4]

*Legacy:*

```sql
select count(distinct "the_year") from "agg_c_14_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "agg_c_14_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[5]

*Legacy:*

```sql
select "store"."store_country" as "c0", "agg_c_14_sales_fact_1997"."the_year" as "c1", "agg_c_14_sales_fact_1997"."quarter" as "c2", sum("agg_c_14_sales_fact_1997"."unit_sales") as "m0", sum("agg_c_14_sales_fact_1997"."store_sales") as "m1" from "agg_c_14_sales_fact_1997" as "agg_c_14_sales_fact_1997", "store" as "store" where "agg_c_14_sales_fact_1997"."the_year" = 1997 and "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id" group by "store"."store_country", "agg_c_14_sales_fact_1997"."the_year", "agg_c_14_sales_fact_1997"."quarter"
```

*Calcite:*

```sql
SELECT "store"."store_country", "agg_c_14_sales_fact_1997"."the_year", "agg_c_14_sales_fact_1997"."quarter", SUM("agg_c_14_sales_fact_1997"."unit_sales") AS "m0", SUM("agg_c_14_sales_fact_1997"."store_sales") AS "m1"
FROM "agg_c_14_sales_fact_1997"
INNER JOIN "store" ON "agg_c_14_sales_fact_1997"."store_id" = "store"."store_id"
WHERE "agg_c_14_sales_fact_1997"."the_year" = 1997
GROUP BY "agg_c_14_sales_fact_1997"."quarter", "agg_c_14_sales_fact_1997"."the_year", "store"."store_country"
```

*Delta:* rowCount matches (4); join style: comma-join -> ANSI INNER JOIN; Calcite adds AS aliases; keyword case: lower -> UPPER.

### <a name="q-agg-g-ms-pcat-family-gender-marital"></a>agg-g-ms-pcat-family-gender-marital

**MDX:**

```mdx
SELECT {[Measures].[Unit Sales]} ON COLUMNS,
  CROSSJOIN(CROSSJOIN([Product].[Product Family].Members, [Gender].[Gender].Members), [Marital Status].[Marital Status].Members) ON ROWS
FROM Sales
```

**Cell-set (Calcite):**

```
Axis #0:
{}
Axis #1:
{[Measures].[Unit Sales]}
Axis #2:
{[Product].[Products].[Drink], [Customer].[Gender].[F], [Customer].[Marital Status].[M]}
{[Product].[Products].[Drink], [Customer].[Gender].[F], [Customer].[Marital Status].[S]}
{[Product].[Products].[Drink], [Customer].[Gender].[M], [Customer].[Marital Status].[M]}
{[Product].[Products].[Drink], [Customer].[Gender].[M], [Customer].[Marital Status].[S]}
{[Product].[Products].[Food], [Customer].[Gender].[F], [Customer].[Marital Status].[M]}
{[Product].[Products].[Food], [Customer].[Gender].[F], [Customer].[Marital Status].[S]}
{[Product].[Products].[Food], [Customer].[Gender].[M], [Customer].[Marital Status].[M]}
{[Product].[Products].[Food], [Customer].[Gender].[M], [Customer].[Marital Status].[S]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[F], [Customer].[Marital Status].[M]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[F], [Customer].[Marital Status].[S]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[M], [Customer].[Marital Status].[M]}
{[Product].[Products].[Non-Consumable], [Customer].[Gender].[M], [Customer].[Marital Status].[S]}
Row #0: 6,207
Row #1: 5,995
Row #2: 5,969
Row #3: 6,426
Row #4: 47,187
Row #5: 47,627
Row #6: 47,742
Row #7: 49,384
Row #8: 11,942
Row #9: 12,600
Row #10: 12,749
Row #11: 12,945
```

**SQL executions:** legacy=13 · Calcite=13

#### sqlExecution[0]

*Legacy:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Marital Status') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Calcite:*

```sql
select "store"."store_country" as "c0" from "store" as "store" where UPPER("store"."store_country") = UPPER('Marital Status') group by "store"."store_country" order by CASE WHEN "store"."store_country" IS NULL THEN 0 ELSE 1 END, "store"."store_country" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[1]

*Legacy:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Marital Status') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Calcite:*

```sql
select "store"."store_type" as "c0" from "store" as "store" where UPPER("store"."store_type") = UPPER('Marital Status') group by "store"."store_type" order by CASE WHEN "store"."store_type" IS NULL THEN 0 ELSE 1 END, "store"."store_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[2]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Marital Status') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where UPPER("product_class"."product_family") = UPPER('Marital Status') and "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 0 ELSE 1 END, "product_class"."product_family" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[3]

*Legacy:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Marital Status') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Calcite:*

```sql
select "promotion"."media_type" as "c0" from "promotion" as "promotion" where UPPER("promotion"."media_type") = UPPER('Marital Status') group by "promotion"."media_type" order by CASE WHEN "promotion"."media_type" IS NULL THEN 0 ELSE 1 END, "promotion"."media_type" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[4]

*Legacy:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Marital Status') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Calcite:*

```sql
select "promotion"."promotion_name" as "c0" from "promotion" as "promotion" where UPPER("promotion"."promotion_name") = UPPER('Marital Status') group by "promotion"."promotion_name" order by CASE WHEN "promotion"."promotion_name" IS NULL THEN 0 ELSE 1 END, "promotion"."promotion_name" ASC
```

*Delta:* rowCount matches (0); SQL identical.

#### sqlExecution[5]

*Legacy:*

```sql
select "product_class"."product_family" as "c0" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family" order by CASE WHEN "product_class"."product_family" IS NULL THEN 1 ELSE 0 END, "product_class"."product_family" ASC
```

*Calcite:*

```sql
SELECT "product_class"."product_family"
FROM "product_class"
INNER JOIN "product" ON "product_class"."product_class_id" = "product"."product_class_id"
GROUP BY "product_class"."product_family"
ORDER BY "product_class"."product_family"
```

*Delta:* rowCount matches (3); join style: comma-join -> ANSI INNER JOIN; keyword case: lower -> UPPER; length delta -107 chars (legacy=341, calcite=234).

#### sqlExecution[6]

*Legacy:*

```sql
select "customer"."gender" as "c0" from "customer" as "customer" group by "customer"."gender" order by CASE WHEN "customer"."gender" IS NULL THEN 1 ELSE 0 END, "customer"."gender" ASC
```

*Calcite:*

```sql
SELECT "gender"
FROM "customer"
GROUP BY "gender"
ORDER BY "gender"
```

*Delta:* rowCount matches (2); keyword case: lower -> UPPER; length delta -116 chars (legacy=183, calcite=67).

#### sqlExecution[7]

*Legacy:*

```sql
select "customer"."marital_status" as "c0" from "customer" as "customer" group by "customer"."marital_status" order by CASE WHEN "customer"."marital_status" IS NULL THEN 1 ELSE 0 END, "customer"."marital_status" ASC
```

*Calcite:*

```sql
SELECT "marital_status"
FROM "customer"
GROUP BY "marital_status"
ORDER BY "marital_status"
```

*Delta:* rowCount matches (2); keyword case: lower -> UPPER; length delta -124 chars (legacy=215, calcite=91).

#### sqlExecution[8]

*Legacy:*

```sql
select count(distinct "product_family") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "product_family") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[9]

*Legacy:*

```sql
select count(distinct "gender") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "gender") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[10]

*Legacy:*

```sql
select count(distinct "marital_status") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "marital_status") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[11]

*Legacy:*

```sql
select count(distinct "the_year") from "agg_g_ms_pcat_sales_fact_1997"
```

*Calcite:*

```sql
SELECT COUNT(DISTINCT "the_year") AS "c"
FROM "agg_g_ms_pcat_sales_fact_1997"
```

*Delta:* rowCount matches (1); Calcite adds AS aliases; keyword case: lower -> UPPER.

#### sqlExecution[12]

*Legacy:*

```sql
select "agg_g_ms_pcat_sales_fact_1997"."the_year" as "c0", "agg_g_ms_pcat_sales_fact_1997"."product_family" as "c1", "agg_g_ms_pcat_sales_fact_1997"."gender" as "c2", "agg_g_ms_pcat_sales_fact_1997"."marital_status" as "c3", sum("agg_g_ms_pcat_sales_fact_1997"."unit_sales") as "m0" from "agg_g_ms_pcat_sales_fact_1997" as "agg_g_ms_pcat_sales_fact_1997" where "agg_g_ms_pcat_sales_fact_1997"."the_year" = 1997 group by "agg_g_ms_pcat_sales_fact_1997"."the_year", "agg_g_ms_pcat_sales_fact_1997"."product_family", "agg_g_ms_pcat_sales_fact_1997"."gender", "agg_g_ms_pcat_sales_fact_1997"."marital_status"
```

*Calcite:*

```sql
SELECT "the_year", "product_family", "gender", "marital_status", SUM("unit_sales") AS "m0"
FROM "agg_g_ms_pcat_sales_fact_1997"
WHERE "the_year" = 1997
GROUP BY "gender", "marital_status", "product_family", "the_year"
```

*Delta:* rowCount matches (12); Calcite adds AS aliases; keyword case: lower -> UPPER; length delta -387 chars (legacy=604, calcite=217).

## <a name="takeaways"></a>Key takeaways

Derived from the actual captured pairs across 45 queries / 251 paired SQL executions.

1. **Join style.** Legacy emits comma-separated `FROM a, b WHERE a.x = b.y` joins in 54/251 executions; Calcite emits ANSI `INNER JOIN` in 68/251 executions. This is the single largest lexical difference between the two backends.
2. **Keyword case.** Legacy is lowercase (`select`, `from`, `where`) in 209/251 executions; Calcite is uppercase (`SELECT`, `FROM`, `WHERE`) in 182/251. Both backends quote every identifier — that is not a drift source.
3. **Column aliases.** Calcite attaches `AS "c0"`-style aliases in 155/251 executions. Legacy uses the same aliasing convention, but with lowercase `as`. Alias *names* line up 1:1 (`c0`, `c1`, `m0`, …).
4. **IN-list rendering.** Legacy uses `IN (...)` in 12/251 executions; Calcite emits `IN (...)` in 12/251. Behaviour is close but not identical — Calcite sometimes rewrites long OR chains to IN and occasionally the reverse for 2-element lists.
5. **HAVING clauses.** Legacy emits HAVING in 2/251 executions; Calcite in 2/251. Where they differ, Calcite has typically pushed the filter into a subquery WHERE.
6. **Rowset identity.** 0/251 executions have a differing row-content checksum (SQL_DRIFT advisory territory — cell-sets still match). rowCount mismatches: 0/251 (cell-set parity gate should hold these at zero).
7. **Execution-count alignment.** Calcite emitted MORE SQL statements than legacy on 0 queries, FEWER on 0. Same count on 45.
8. **Cell-set drifts across backends:** 0/45 queries. (Should be zero if CELL_SET_DRIFT gate is green.)
9. **SQL-string identity:** 111/251 executions are byte-identical between backends; the rest differ only lexically. Across queries, 34/45 have at least one differing SQL pair.

### Why this matters for the rewrite

The dominant pattern is lexical (case + join style + trivial aliasing). Rowset-identity drifts are the interesting ones — those are where Calcite's planner is making *semantically* different choices (predicate reordering that surfaces ties differently, subquery shapes, etc). The SQL_DRIFT advisory gate is sized to tolerate #1–#5 while still catching #6 if it bleeds into cell-set drift.
