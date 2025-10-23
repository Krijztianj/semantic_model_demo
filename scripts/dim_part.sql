-- Dimension: dim_part (Delta Table with Surrogate Key)
CREATE TABLE IF NOT EXISTS demo_tpch.dim_part (
  part_id BIGINT GENERATED ALWAYS AS IDENTITY,
  part_key BIGINT,
  part_name STRING,
  brand STRING,
  part_type STRING,
  part_size INT,
  container STRING,
  retail_price DECIMAL(18,2),
  PRIMARY KEY (part_id)
) USING DELTA;

-- Populate dim_part with surrogate key
INSERT INTO demo_tpch.dim_part (part_key, part_name, brand, part_type, part_size, container, retail_price)
SELECT DISTINCT
  p.p_partkey,
  p.p_name,
  p.p_brand,
  p.p_type,
  p.p_size,
  p.p_container,
  p.p_retailprice
FROM samples.tpch.part p;