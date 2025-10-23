-- Dimension: dim_supplier (Delta Table with Surrogate Key)
-- Note: Run 00_config.sql first to set catalog_name and schema_name variables

CREATE TABLE IF NOT EXISTS ${schema_name}.dim_supplier (
  supplier_id BIGINT GENERATED ALWAYS AS IDENTITY,
  supplier_key BIGINT,
  supplier_name STRING,
  phone STRING,
  account_balance DECIMAL(18,2),
  nation STRING,
  region STRING,
  PRIMARY KEY (supplier_id)
) USING DELTA;

-- Populate dim_supplier with surrogate key
INSERT INTO ${schema_name}.dim_supplier (supplier_key, supplier_name, phone, account_balance, nation, region)
SELECT DISTINCT
  s.s_suppkey,
  s.s_name,
  s.s_phone,
  s.s_acctbal,
  n.n_name,
  r.r_name
FROM samples.tpch.supplier s
JOIN samples.tpch.nation n ON s.s_nationkey = n.n_nationkey
JOIN samples.tpch.region r ON n.n_regionkey = r.r_regionkey;