## NDL Assignment 10 Ranzmaier
Link to my repo fork: ()[]
# Part 1 — Environment Setup and Basics

```shell-session
CREATE TABLE
COPY 1000000
postgres=# \timing on
Timing is on.
postgres=# SELECT COUNT(*) FROM people_big;
SELECT * FROM people_big LIMIT 10;
  count  
---------
 1000000
(1 row)

Time: 319.854 ms
 id | first_name | last_name | gender |     department     | salary |   country    
----+------------+-----------+--------+--------------------+--------+--------------
  1 | Andreas    | Scott     | Male   | Audit              |  69144 | Bosnia
  2 | Tim        | Lopez     | Male   | Energy Management  |  62082 | Taiwan
  3 | David      | Ramirez   | Male   | Quality Assurance  |  99453 | South Africa
  4 | Victor     | Sanchez   | Male   | Level Design       |  95713 | Cuba
  5 | Lea        | Edwards   | Female | Energy Management  |  60425 | Iceland
  6 | Oliver     | Baker     | Male   | Payroll            |  74110 | Poland
  7 | Emily      | Lopez     | Female | SOC                |  83526 | Netherlands
  8 | Tania      | King      | Female | IT                 |  95142 | Thailand
  9 | Max        | Hernandez | Male   | Workforce Planning | 101198 | Latvia
 10 | Juliana    | Harris    | Female | Compliance         | 103336 | Chile
(10 rows)

Time: 0.604 ms
postgres=# ```sql
postgres-# SELECT department, AVG(salary)
FROM people_big
GROUP BY department
LIMIT 10;
ERROR:  syntax error at or near "```"
LINE 1: ```sql
        ^
Time: 1.371 ms
postgres=# SELECT department, AVG(salary)
FROM people_big
GROUP BY department
LIMIT 10;
      department       |         avg         
-----------------------+---------------------
 Accounting            |  85150.560834888851
 Alliances             |  84864.832756437315
 Analytics             | 122363.321232406454
 API                   |  84799.041690986409
 Audit                 |  84982.559610499577
 Backend               |  84982.349086542585
 Billing               |  84928.436430727944
 Bioinformatics        |  85138.080510264425
 Brand                 |  85086.881434454358
 Business Intelligence |  85127.097446808511
(10 rows)

Time: 420.807 ms
postgres=# SELECT country, AVG(avg_salary)
FROM (
  SELECT country, department, AVG(salary) AS avg_salary
  FROM people_big
  GROUP BY country, department
) sub
GROUP BY country
LIMIT 10;
  country   |        avg         
------------+--------------------
 Algeria    | 87230.382040504578
 Argentina  | 86969.866763623360
 Armenia    | 87245.059590528218
 Australia  | 87056.715662987876
 Austria    | 87127.824046597584
 Bangladesh | 87063.832793583033
 Belgium    | 86940.103641985310
 Bolivia    | 86960.615658334041
 Bosnia     | 87102.274664951815
 Brazil     | 86977.731228862018
(10 rows)

Time: 327.686 ms
postgres=# SELECT *
FROM people_big
ORDER BY salary DESC
LIMIT 10;
   id   | first_name | last_name | gender |    department    | salary |   country    
--------+------------+-----------+--------+------------------+--------+--------------
 764650 | Tim        | Jensen    | Male   | Analytics        | 160000 | Bulgaria
  10016 | Anastasia  | Edwards   | Female | Analytics        | 159998 | Kuwait
 754528 | Adrian     | Young     | Male   | Game Analytics   | 159997 | UK
 893472 | Mariana    | Cook      | Female | People Analytics | 159995 | South Africa
 240511 | Diego      | Lopez     | Male   | Game Analytics   | 159995 | Malaysia
 359891 | Mariana    | Novak     | Female | Game Analytics   | 159992 | Mexico
  53102 | Felix      | Taylor    | Male   | Data Science     | 159989 | Bosnia
 768143 | Teresa     | Campbell  | Female | Game Analytics   | 159988 | Spain
 729165 | Antonio    | Weber     | Male   | Analytics        | 159987 | Moldova
 952549 | Adrian     | Harris    | Male   | Analytics        | 159986 | Georgia
(10 rows)

Time: 380.459 ms
```

# Part 2 — Exercises

## Exercise 1 - PostgreSQL Analytical Queries (E-commerce)

In the `ecommerce` folder:

1. Generate a new dataset by running the provided Python script.
2. Load the generated data into PostgreSQL in a **new table**.
```sql
DROP TABLE IF EXISTS orders_big;

CREATE TABLE orders_big (
  id SERIAL PRIMARY KEY,
  customer_name TEXT,
  product_category TEXT,
  quantity INTEGER,
  price_per_unit NUMERIC,
  order_date DATE,
  country TEXT
);

\COPY orders_big(customer_name,product_category,quantity,price_per_unit,order_date,country)
FROM '/ecommerce/orders_1M.csv' DELIMITER ',' CSV HEADER;
```

Using SQL ([see the a list of supported SQL commands](https://www.postgresql.org/docs/current/sql-commands.html)), answer the following questions:

**A.** What is the single category with the highest `price_per_category`?

```sql
select product_category, SUM(price_per_unit * quantity) AS price_per_category
from orders_big
group by product_category
order by price_per_category DESC
limit 1;
```

```shell-session

 product_category | price_per_category 
------------------+--------------------
 Automotive       |       306589798.86
(1 row)

Time: 535.423 ms
```

**B.** What are the top 3 products with the highest total quantity sold across all orders?
I was confused about "products" since there is no product name column in the csv, so I assumed it meant product categories:

```sql
SELECT
  product_category,
  SUM(quantity) AS total_quantity_sold
FROM orders_big
GROUP BY product_category
ORDER BY total_quantity_sold DESC
LIMIT 3;
```

```shell-session
 product_category | total_quantity_sold 
------------------+---------------------
 Health & Beauty  |              300842
 Electronics      |              300804
 Toys             |              300598
(3 rows)

Time: 368.100 ms
```

**C.** What is the total revenue per product category?  
(Revenue = `price_per_unit × quantity`)

```sql
SELECT
  product_category,
  SUM(price_per_unit * quantity) AS revenue
FROM orders_big
GROUP BY product_category
ORDER BY revenue DESC;
```

```shell-session
 product_category |   revenue    
------------------+--------------
 Automotive       | 306589798.86
 Electronics      | 241525009.45
 Home & Garden    |  78023780.09
 Sports           |  61848990.83
 Health & Beauty  |  46599817.89
 Office Supplies  |  38276061.64
 Fashion          |  31566368.22
 Toys             |  23271039.02
 Grocery          |  15268355.66
 Books            |  12731976.04
(10 rows)

Time: 495.758 ms
```

**D.** Which customers have the highest total spending?

```sql
SELECT
  customer_name,
  SUM(price_per_unit * quantity) AS total_spending
FROM orders_big
GROUP BY customer_name
ORDER BY total_spending DESC
LIMIT 10;
```

```shell-session
 customer_name  | total_spending 
----------------+----------------
 Carol Taylor   |      991179.18
 Nina Lopez     |      975444.95
 Daniel Jackson |      959344.48
 Carol Lewis    |      947708.57
 Daniel Young   |      946030.14
 Alice Martinez |      935100.02
 Ethan Perez    |      934841.24
 Leo Lee        |      934796.48
 Eve Young      |      933176.86
 Ivy Rodriguez  |      925742.64
(10 rows)

Time: 694.328 ms
```

## Exercise 2

Assuming there are naive joins executed by users, such as:

```sql
SELECT COUNT(*)
FROM people_big p1
JOIN people_big p2
  ON p1.country = p2.country;
```

## Problem Statement

This query takes more than **10 minutes** to complete, significantly slowing down the entire system. Additionally, the **OLTP database** currently in use has inherent limitations in terms of **scalability and efficiency**, especially when operating in **large-scale cloud environments**.

## Discussion Question

Considering the requirements for **scalability** and **efficiency**, what **approaches and/or optimizations** can be applied to improve the system’s:

- Scalability  
- Performance  
- Overall efficiency  

Please **elaborate with a technical discussion**.

> **Optional:** Demonstrate your proposed solution in practice (e.g., architecture diagrams, SQL examples, or code snippets).

## Answer  Exercise 2:
The querie is inefficient because it performs a naive self-join on a large table.
It is multiplying the number of rows in people_big by itself for each country, leading to a quadratic growth in the number of rows needed to be processed. It also includes self pairs which is unnecessary for counting matching countries.

If many such joins are executed we can think about Pre aggregating the data into a summary table that contains the count of people per country. 

```sql
CREATE MATERIALIZED VIEW people_country_counts AS
SELECT country, COUNT(*) AS country_count
FROM people_big
GROUP BY country;

SELECT SUM(country_count * country_count) FROM people_country_counts;
```

This does not help if the tables are frequently updated.
But we can still mitigate this by keeping the summary table updated incrementally:

- On insert: country_count[country] += 1
- On delete: country_count[country] -= 1
  
There are more options in fixing this particular query, like grouping once instead of joining or excluding self pairs, but these are more specific to this query and do not help with the general problem of naive joins.

Another way to improve are guard rail in our OLTP database to prevent such expensive queries from being executed. This can be done by setting query time limits or resource usage limits.

Finally, moving to a more scalable database system designed for analytical workloads, such as a distributed SQL database or a data warehouse solution will be the more general solution to the problem of scalability, performance, and efficiency.

We are doing both analytical and transactional workloads on the same OLTP database. A solution would be to separate these workloads onto different systems.

The OLTP database is best for transactions and a data warehouse or data lake could be used for analytics.

## Exercise 3
## Run with Spark (inside Jupyter)
Now, explain in your own words:

- **What the Spark code does:**  
  Describe the workflow, data loading, and the types of queries executed (aggregations, sorting, self-joins, etc.).

  Starts Spark with Postgres JDBC and small parallelism settings; load people_big via JDBC, count() to materialize, register temp view like we can do in SQL.

  Queries:
  - (a) group by department → avg salary, sorted desc with top 10.
  - (b) nested agg SQL: avg salary per (country, department), then avg of those per country, top 10.
  - (c) sort salaries desc to get top 10 earners.
  - (d) heavy self-join on country just to count pairings.
  - (d-safe) rewrite using pre-aggregated country counts and sum(cnt*cnt) to avoid the huge join; then stop Spark.

- **Architectural contrasts with PostgreSQL:**  
  Compare the Spark distributed architecture versus PostgreSQL’s single-node capabilities, including scalability, parallelism, and data processing models.

  Spark is distributed (the data is partitioned, can cache in memory, scales by adding nodes).
  PostgreSQL is largely single-node, executes plans on local storage with indexes/optimizer, scales only by vertical hardware.

- **Advantages and limitations:**  
  Highlight the benefits of using Spark for large-scale data processing (e.g., in-memory computation, distributed processing) and its potential drawbacks (e.g., setup complexity, overhead for small datasets).

  Pros of Spark:
  It can handle data larger than what would typically fit on one machine, parallel analytics, in-memory speedups, fault tolerance, broad connectors.

  Cons: cluster/setup/tuning overhead, shuffle/serialization cost can make small jobs slower than Postgres, fewer built-in indexing/transaction features, more moving parts to manage.

- **Relation to Exercise 2:**  
  Connect this approach to the concepts explored in Exercise 2, such as performance optimization and scalability considerations.

  This all makes Spark a better fit for a system in need of scalable analytics, especially for heavy queries like self-joins. The rewrite in (d-safe) shows how pre-aggregation can optimize performance, similar to the materialized view approach in Exercise 2. Spark's architecture allows it to handle large datasets and complex queries more efficiently through serialisation than a single-node OLTP database.

## Exercise 4
Port the SQL queries from exercise 1 to spark.

Spark versions of the e-commerce queries now live in `Exercise1/notebooks/spark.ipynb` (cells 8–13) and run against the `orders_big` table loaded via JDBC:

```python
orders_df = spark.read.jdbc(
    url=jdbc_url,
    table="orders_big",
    properties=jdbc_props
)
orders_df.createOrReplaceTempView("orders_big")
```

- Highest `price_per_category`:
```python
orders_df.groupBy("product_category").agg(
    spark_round(_sum(col("price_per_unit") * col("quantity")), 2).alias("price_per_category")
).orderBy(col("price_per_category").desc()).limit(1)
```

- Top 3 categories by quantity sold:
```python
orders_df.groupBy("product_category").agg(
    _sum(col("quantity")).alias("total_quantity_sold")
).orderBy(col("total_quantity_sold").desc()).limit(3)
```

- Total revenue per category:
```python
orders_df.groupBy("product_category").agg(
    spark_round(_sum(col("price_per_unit") * col("quantity")), 2).alias("revenue")
).orderBy(col("revenue").desc())
```

- Customers with the highest total spending:
```python
orders_df.groupBy("customer_name").agg(
    spark_round(_sum(col("price_per_unit") * col("quantity")), 2).alias("total_spending")
).orderBy(col("total_spending").desc()).limit(10)
```
