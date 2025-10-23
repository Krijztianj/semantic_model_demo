# Databricks notebook source
# MAGIC %md
# MAGIC # Semantic Model Demo - Data Warehouse Setup
# MAGIC This notebook creates a dimensional data warehouse model based on the TPC-H dataset.
# MAGIC
# MAGIC ## Features
# MAGIC - Delta tables with surrogate keys (_id suffix)
# MAGIC - Primary and Foreign key constraints
# MAGIC - Star schema design

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your catalog and schema names here

# COMMAND ----------

# Configuration variables
catalog = 'main'
schema = 'demo_tpch'

# Create catalog and schema
spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')

# Set the context
spark.sql(f'USE CATALOG {catalog}')
spark.sql(f'USE SCHEMA {schema}')

# Verify current context
print(f"Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"Current schema: {spark.sql('SELECT current_schema()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date

# COMMAND ----------

dim_date_sql = """
CREATE TABLE IF NOT EXISTS dim_date (
  date_id BIGINT GENERATED ALWAYS AS IDENTITY,
  date_key DATE,
  full_date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT,
  weekday_name STRING,
  is_weekend BOOLEAN,
  PRIMARY KEY (date_id)
) USING DELTA
"""

spark.sql(dim_date_sql)

# Populate dim_date
spark.sql("""
CREATE OR REPLACE TEMP VIEW date_src AS
SELECT explode(sequence(date('1992-01-01'), date('1998-12-31'), interval 1 day)) AS calendar_date
""")

spark.sql("""
INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, weekday_name, is_weekend)
SELECT
  calendar_date,
  calendar_date,
  year(calendar_date),
  quarter(calendar_date),
  month(calendar_date),
  day(calendar_date),
  date_format(calendar_date, 'EEEE'),
  CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END
FROM date_src
""")

print(f"dim_date created with {spark.table('dim_date').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer

# COMMAND ----------

dim_customer_sql = """
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_key BIGINT,
  customer_name STRING,
  market_segment STRING,
  phone STRING,
  account_balance DECIMAL(18,2),
  nation STRING,
  region STRING,
  PRIMARY KEY (customer_id)
) USING DELTA
"""

spark.sql(dim_customer_sql)

# Populate dim_customer
spark.sql("""
INSERT INTO dim_customer (customer_key, customer_name, market_segment, phone, account_balance, nation, region)
SELECT DISTINCT
  c.c_custkey,
  c.c_name,
  c.c_mktsegment,
  c.c_phone,
  c.c_acctbal,
  n.n_name,
  r.r_name
FROM samples.tpch.customer c
JOIN samples.tpch.nation n ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.region r ON n.n_regionkey = r.r_regionkey
""")

print(f"dim_customer created with {spark.table('dim_customer').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_part

# COMMAND ----------

dim_part_sql = """
CREATE TABLE IF NOT EXISTS dim_part (
  part_id BIGINT GENERATED ALWAYS AS IDENTITY,
  part_key BIGINT,
  part_name STRING,
  brand STRING,
  part_type STRING,
  part_size INT,
  container STRING,
  retail_price DECIMAL(18,2),
  PRIMARY KEY (part_id)
) USING DELTA
"""

spark.sql(dim_part_sql)

# Populate dim_part
spark.sql("""
INSERT INTO dim_part (part_key, part_name, brand, part_type, part_size, container, retail_price)
SELECT DISTINCT
  p.p_partkey,
  p.p_name,
  p.p_brand,
  p.p_type,
  p.p_size,
  p.p_container,
  p.p_retailprice
FROM samples.tpch.part p
""")

print(f"dim_part created with {spark.table('dim_part').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_supplier

# COMMAND ----------

dim_supplier_sql = """
CREATE TABLE IF NOT EXISTS dim_supplier (
  supplier_id BIGINT GENERATED ALWAYS AS IDENTITY,
  supplier_key BIGINT,
  supplier_name STRING,
  phone STRING,
  account_balance DECIMAL(18,2),
  nation STRING,
  region STRING,
  PRIMARY KEY (supplier_id)
) USING DELTA
"""

spark.sql(dim_supplier_sql)

# Populate dim_supplier
spark.sql("""
INSERT INTO dim_supplier (supplier_key, supplier_name, phone, account_balance, nation, region)
SELECT DISTINCT
  s.s_suppkey,
  s.s_name,
  s.s_phone,
  s.s_acctbal,
  n.n_name,
  r.r_name
FROM samples.tpch.supplier s
JOIN samples.tpch.nation n ON s.s_nationkey = n.n_nationkey
JOIN samples.tpch.region r ON n.n_regionkey = r.r_regionkey
""")

print(f"dim_supplier created with {spark.table('dim_supplier').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_order_header

# COMMAND ----------

dim_order_header_sql = """
CREATE TABLE IF NOT EXISTS dim_order_header (
  order_header_id BIGINT GENERATED ALWAYS AS IDENTITY,
  order_key BIGINT,
  customer_key BIGINT,
  order_status STRING,
  order_amount DECIMAL(18,2),
  order_date_key DATE,
  order_priority STRING,
  clerk_name STRING,
  ship_priority INT,
  PRIMARY KEY (order_header_id)
) USING DELTA
"""

spark.sql(dim_order_header_sql)

# Populate dim_order_header
spark.sql("""
INSERT INTO dim_order_header (order_key, customer_key, order_status, order_amount, order_date_key, order_priority, clerk_name, ship_priority)
SELECT DISTINCT
  o.o_orderkey,
  o.o_custkey,
  o.o_orderstatus,
  o.o_totalprice,
  o.o_orderdate,
  o.o_orderpriority,
  o.o_clerk,
  o.o_shippriority
FROM samples.tpch.orders o
""")

print(f"dim_order_header created with {spark.table('dim_order_header').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_order_line

# COMMAND ----------

fact_order_line_sql = """
CREATE TABLE IF NOT EXISTS fact_order_line (
  order_header_id BIGINT,
  line_number INT,
  part_id BIGINT,
  supplier_id BIGINT,
  customer_id BIGINT,
  extended_amount DECIMAL(18,2),
  discount_rate DECIMAL(5,2),
  net_amount DECIMAL(18,2),
  tax_rate DECIMAL(5,2),
  discount_amount DECIMAL(18,2),
  gross_revenue DECIMAL(18,2),
  quantity INT,
  ship_date_id BIGINT,
  commit_date_id BIGINT,
  receipt_date_id BIGINT,
  ship_mode STRING,
  PRIMARY KEY (order_header_id, line_number),
  CONSTRAINT fk_order_header FOREIGN KEY (order_header_id) REFERENCES dim_order_header(order_header_id),
  CONSTRAINT fk_part FOREIGN KEY (part_id) REFERENCES dim_part(part_id),
  CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
  CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  CONSTRAINT fk_ship_date FOREIGN KEY (ship_date_id) REFERENCES dim_date(date_id),
  CONSTRAINT fk_commit_date FOREIGN KEY (commit_date_id) REFERENCES dim_date(date_id),
  CONSTRAINT fk_receipt_date FOREIGN KEY (receipt_date_id) REFERENCES dim_date(date_id)
) USING DELTA
"""

spark.sql(fact_order_line_sql)

# Populate fact_order_line
spark.sql("""
INSERT INTO fact_order_line (
  order_header_id, line_number, part_id, supplier_id, customer_id, 
  extended_amount, discount_rate, net_amount, tax_rate, discount_amount, 
  gross_revenue, quantity, ship_date_id, commit_date_id, receipt_date_id, ship_mode
)
SELECT
  doh.order_header_id,
  l.l_linenumber,
  dp.part_id,
  ds.supplier_id,
  dc.customer_id,
  l.l_extendedprice,
  l.l_discount,
  l.l_extendedprice * (1 - l.l_discount),
  l.l_tax,
  l.l_extendedprice * l.l_discount,
  l.l_extendedprice * (1 + l.l_tax - l.l_discount),
  l.l_quantity,
  dship.date_id,
  dcommit.date_id,
  dreceipt.date_id,
  l.l_shipmode
FROM samples.tpch.lineitem l
JOIN dim_order_header doh ON l.l_orderkey = doh.order_key
JOIN dim_part dp ON l.l_partkey = dp.part_key
JOIN dim_supplier ds ON l.l_suppkey = ds.supplier_key
JOIN dim_customer dc ON doh.customer_key = dc.customer_key
JOIN dim_date dship ON l.l_shipdate = dship.date_key
JOIN dim_date dcommit ON l.l_commitdate = dcommit.date_key
JOIN dim_date dreceipt ON l.l_receiptdate = dreceipt.date_key
""")

print(f"fact_order_line created with {spark.table('fact_order_line').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Show all tables
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# Show row counts
summary_sql = """
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_part', COUNT(*) FROM dim_part
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL
SELECT 'dim_order_header', COUNT(*) FROM dim_order_header
UNION ALL
SELECT 'fact_order_line', COUNT(*) FROM fact_order_line
ORDER BY table_name
"""

display(spark.sql(summary_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Query
# MAGIC Test the star schema with a sample query

# COMMAND ----------

sample_query = """
SELECT 
  dd.year,
  dd.quarter,
  dc.region,
  dc.nation,
  SUM(fol.net_amount) AS total_net_amount,
  SUM(fol.quantity) AS total_quantity,
  COUNT(DISTINCT fol.order_header_id) AS order_count
FROM fact_order_line fol
JOIN dim_date dd ON fol.ship_date_id = dd.date_id
JOIN dim_customer dc ON fol.customer_id = dc.customer_id
WHERE dd.year = 1997
GROUP BY dd.year, dd.quarter, dc.region, dc.nation
ORDER BY dd.quarter, total_net_amount DESC
LIMIT 20
"""

display(spark.sql(sample_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Metric View
# MAGIC 
# MAGIC Create the metric view definition using SQL with embedded YAML

# COMMAND ----------

# Create metric view with embedded YAML definition
create_metric_view_sql = f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.order_metrics_mv
WITH METRICS
LANGUAGE YAML
COMMENT 'TPC-H Order Analytics Metric View - Core business metrics'
AS $$
version: 1.1
source: fact_order_line
comment: TPC-H Order Analytics Metric View - Core business metrics for order line analysis

# Star schema joins to dimension tables
joins:
  - table: dim_customer
    join_expr: fact_order_line.customer_id = dim_customer.customer_id
  
  - table: dim_date
    alias: ship_date
    join_expr: fact_order_line.ship_date_id = ship_date.date_id
  
  - table: dim_date
    alias: commit_date
    join_expr: fact_order_line.commit_date_id = commit_date.date_id
  
  - table: dim_date
    alias: receipt_date
    join_expr: fact_order_line.receipt_date_id = receipt_date.date_id
  
  - table: dim_part
    join_expr: fact_order_line.part_id = dim_part.part_id
  
  - table: dim_supplier
    join_expr: fact_order_line.supplier_id = dim_supplier.supplier_id
  
  - table: dim_order_header
    join_expr: fact_order_line.order_header_id = dim_order_header.order_header_id

# Dimensions for grouping and filtering
dimensions:
  # Fact table dimensions
  - name: Order Header ID
    expr: order_header_id
    synonyms: [order id, order number]
  
  - name: Line Number
    expr: line_number
    synonyms: [line item number, item number]
  
  - name: Ship Mode
    expr: ship_mode
    synonyms: [shipping method, delivery method]
  
  # Customer dimensions
  - name: Customer Name
    expr: dim_customer.customer_name
    synonyms: [client name, customer]
  
  - name: Market Segment
    expr: dim_customer.market_segment
    synonyms: [segment, customer segment]
  
  - name: Customer Nation
    expr: dim_customer.nation
    display_name: Nation
    synonyms: [country, customer country]
  
  - name: Customer Region
    expr: dim_customer.region
    display_name: Region
    synonyms: [area, customer region]
  
  # Ship Date dimensions
  - name: Ship Year
    expr: ship_date.year
    synonyms: [shipping year]
  
  - name: Ship Quarter
    expr: ship_date.quarter
    synonyms: [shipping quarter]
  
  - name: Ship Month
    expr: ship_date.month
    synonyms: [shipping month]
  
  - name: Ship Date
    expr: ship_date.date_key
    synonyms: [shipping date, date shipped]
  
  - name: Ship Weekday
    expr: ship_date.weekday_name
    synonyms: [shipping day of week]
  
  - name: Ship Is Weekend
    expr: ship_date.is_weekend
    synonyms: [shipped on weekend]
  
  # Part dimensions
  - name: Part Name
    expr: dim_part.part_name
    synonyms: [product name, item name]
  
  - name: Brand
    expr: dim_part.brand
    synonyms: [product brand, manufacturer]
  
  - name: Part Type
    expr: dim_part.part_type
    synonyms: [product type, item type]
  
  - name: Part Size
    expr: dim_part.part_size
    synonyms: [product size, item size]
  
  - name: Container
    expr: dim_part.container
    synonyms: [packaging, package type]
  
  # Supplier dimensions
  - name: Supplier Name
    expr: dim_supplier.supplier_name
    synonyms: [vendor name, supplier]
  
  - name: Supplier Nation
    expr: dim_supplier.nation
    synonyms: [supplier country]
  
  - name: Supplier Region
    expr: dim_supplier.region
    synonyms: [supplier area]
  
  # Order header dimensions
  - name: Order Status
    expr: dim_order_header.order_status
    synonyms: [status, order state]
  
  - name: Order Priority
    expr: dim_order_header.order_priority
    synonyms: [priority, order priority level]
  
  - name: Clerk Name
    expr: dim_order_header.clerk_name
    synonyms: [clerk, sales clerk]

# Measures - aggregated business metrics
measures:
  # Order counts
  - name: Total Orders
    expr: COUNT(DISTINCT order_header_id)
    display_name: Total Orders
    format: "#,##0"
    synonyms: [order count, number of orders]
  
  - name: Total Line Items
    expr: COUNT(1)
    display_name: Total Line Items
    format: "#,##0"
    synonyms: [line item count, number of line items]
  
  # Volume metrics
  - name: Total Quantity
    expr: SUM(quantity)
    display_name: Total Quantity
    format: "#,##0"
    synonyms: [total units, quantity sold, units ordered]
  
  # Revenue metrics
  - name: Total Extended Amount
    expr: SUM(extended_amount)
    display_name: Total Extended Amount
    format: "$#,##0.00"
    synonyms: [extended price, gross amount before discount]
  
  - name: Total Discount Amount
    expr: SUM(discount_amount)
    display_name: Total Discount Amount
    format: "$#,##0.00"
    synonyms: [total discounts, discount given]
  
  - name: Total Net Amount
    expr: SUM(net_amount)
    display_name: Total Net Amount
    format: "$#,##0.00"
    synonyms: [net revenue, revenue after discounts, net sales]
  
  - name: Total Gross Revenue
    expr: SUM(gross_revenue)
    display_name: Total Gross Revenue
    format: "$#,##0.00"
    synonyms: [gross revenue, total revenue with tax]
  
  # Rate and average metrics
  - name: Average Discount Rate
    expr: AVG(discount_rate)
    display_name: Average Discount Rate
    format: "0.0%"
    synonyms: [avg discount, mean discount rate]
  
  - name: Average Tax Rate
    expr: AVG(tax_rate)
    display_name: Average Tax Rate
    format: "0.0%"
    synonyms: [avg tax, mean tax rate]
  
  # Calculated KPIs using measure references
  - name: Average Order Value
    expr: MEASURE(`Total Net Amount`) / NULLIF(MEASURE(`Total Orders`), 0)
    display_name: Average Order Value
    format: "$#,##0.00"
    synonyms: [aov, avg order value, mean order value]
  
  - name: Average Items Per Order
    expr: MEASURE(`Total Line Items`) / NULLIF(MEASURE(`Total Orders`), 0)
    display_name: Average Items Per Order
    format: "0.0"
    synonyms: [items per order, avg line items per order]
  
  - name: Average Quantity Per Line
    expr: MEASURE(`Total Quantity`) / NULLIF(MEASURE(`Total Line Items`), 0)
    display_name: Average Quantity Per Line
    format: "0.0"
    synonyms: [avg qty per line, units per line item]
  
  # Discount effectiveness
  - name: Discount Percentage
    expr: (MEASURE(`Total Discount Amount`) / NULLIF(MEASURE(`Total Extended Amount`), 0)) * 100
    display_name: Discount Percentage
    format: "0.0%"
    synonyms: [discount rate percentage, pct discounted]
$$;
"""

spark.sql(create_metric_view_sql)
print(f"Metric view created: {catalog}.{schema}.order_metrics_mv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the Metric View

# COMMAND ----------

# Query 1: Revenue by region and quarter using the metric view
query1 = f"""
SELECT 
  `Customer Region`,
  `Ship Quarter`,
  `Ship Year`,
  MEASURE(`Total Net Amount`) as net_revenue,
  MEASURE(`Total Orders`) as order_count,
  MEASURE(`Average Order Value`) as avg_order_value
FROM {catalog}.{schema}.order_metrics_mv
WHERE `Ship Year` = 1997
GROUP BY `Customer Region`, `Ship Quarter`, `Ship Year`
ORDER BY net_revenue DESC
"""
display(spark.sql(query1))

# COMMAND ----------

# Query 2: Top brands by revenue and discount analysis
query2 = f"""
SELECT 
  Brand,
  MEASURE(`Total Net Amount`) as net_revenue,
  MEASURE(`Total Quantity`) as units_sold,
  MEASURE(`Total Orders`) as order_count,
  MEASURE(`Discount Percentage`) as discount_pct
FROM {catalog}.{schema}.order_metrics_mv
GROUP BY Brand
ORDER BY net_revenue DESC
LIMIT 10
"""
display(spark.sql(query2))

# COMMAND ----------

# Query 3: Monthly trends for 1997
query3 = f"""
SELECT 
  `Ship Year`,
  `Ship Month`,
  MEASURE(`Total Orders`) as orders,
  MEASURE(`Total Net Amount`) as revenue,
  MEASURE(`Average Order Value`) as aov,
  MEASURE(`Average Items Per Order`) as items_per_order
FROM {catalog}.{schema}.order_metrics_mv
WHERE `Ship Year` = 1997
GROUP BY `Ship Year`, `Ship Month`
ORDER BY `Ship Month`
"""
display(spark.sql(query3))
