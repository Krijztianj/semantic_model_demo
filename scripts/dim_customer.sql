-- Dimension: dim_customer (Delta Table with Surrogate Key)
-- Note: Run 00_config.sql first to set catalog_name and schema_name variables

CREATE TABLE IF NOT EXISTS ${schema_name}.dim_customer (
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_key BIGINT,
  customer_name STRING,
  market_segment STRING,
  phone STRING,
  account_balance DECIMAL(18,2),
  nation STRING,
  region STRING,
  PRIMARY KEY (customer_id)
) USING DELTA;

-- Populate dim_customer with surrogate key
INSERT INTO ${schema_name}.dim_customer (customer_key, customer_name, market_segment, phone, account_balance, nation, region)
SELECT DISTINCT
  c.c_custkey,
  c.c_name,
  c.c_mktsegment,
  c.c_phone,
  c.c_acctbal,
  n.n_name,
  r.r_name
FROM samples.tpch.customer c
JOIN samples.tpch.nation n   ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.region r   ON n.n_regionkey = r.r_regionkey;