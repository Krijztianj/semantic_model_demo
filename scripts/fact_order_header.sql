-- Fact Table: fact_order_header (Delta Table with Surrogate and Foreign Keys)
CREATE TABLE IF NOT EXISTS demo_tpch.fact_order_header (
  order_header_id BIGINT,
  customer_id BIGINT,
  order_status STRING,
  order_amount DECIMAL(18,2),
  order_date_id BIGINT,
  order_priority STRING,
  clerk_name STRING,
  ship_priority INT,
  PRIMARY KEY (order_header_id),
  FOREIGN KEY (order_header_id) REFERENCES demo_tpch.dim_order_header(order_header_id),
  FOREIGN KEY (customer_id) REFERENCES demo_tpch.dim_customer(customer_id),
  FOREIGN KEY (order_date_id) REFERENCES demo_tpch.dim_date(date_id)
) USING DELTA;

-- Populate fact_order_header with surrogate keys
INSERT INTO demo_tpch.fact_order_header (order_header_id, customer_id, order_status, order_amount, order_date_id, order_priority, clerk_name, ship_priority)
SELECT
  doh.order_header_id,
  dc.customer_id,
  doh.order_status,
  doh.order_amount,
  dd.date_id,
  doh.order_priority,
  doh.clerk_name,
  doh.ship_priority
FROM demo_tpch.dim_order_header doh
JOIN demo_tpch.dim_customer dc ON doh.order_key = dc.customer_key
JOIN demo_tpch.dim_date dd ON doh.order_date_key = dd.date_key;