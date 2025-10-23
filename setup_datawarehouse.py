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
  FOREIGN KEY (order_header_id) REFERENCES dim_order_header(order_header_id),
  FOREIGN KEY (part_id) REFERENCES dim_part(part_id),
  FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  FOREIGN KEY (ship_date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (commit_date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (receipt_date_id) REFERENCES dim_date(date_id)
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
