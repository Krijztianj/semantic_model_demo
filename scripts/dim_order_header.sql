-- Dimension: dim_order_header (Delta Table with Surrogate Key)
-- Note: Run 00_config.sql first to set catalog_name and schema_name variables

CREATE TABLE IF NOT EXISTS ${schema_name}.dim_order_header (
  order_header_id BIGINT GENERATED ALWAYS AS IDENTITY,
  order_key BIGINT,
  order_status STRING,
  order_amount DECIMAL(18,2),
  order_date_key DATE,
  order_priority STRING,
  clerk_name STRING,
  ship_priority INT,
  PRIMARY KEY (order_header_id)
) USING DELTA;

-- Populate dim_order_header with surrogate key
INSERT INTO ${schema_name}.dim_order_header (order_key, order_status, order_amount, order_date_key, order_priority, clerk_name, ship_priority)
SELECT DISTINCT
  o.o_orderkey,
  o.o_orderstatus,
  o.o_totalprice,
  o.o_orderdate,
  o.o_orderpriority,
  o.o_clerk,
  o.o_shippriority
FROM samples.tpch.orders o;