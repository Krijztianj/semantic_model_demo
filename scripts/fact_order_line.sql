-- Fact Table: fact_order_line (Delta Table with Surrogate and Foreign Keys)
CREATE TABLE IF NOT EXISTS demo_tpch.fact_order_line (
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
  FOREIGN KEY (order_header_id) REFERENCES demo_tpch.dim_order_header(order_header_id),
  FOREIGN KEY (part_id) REFERENCES demo_tpch.dim_part(part_id),
  FOREIGN KEY (supplier_id) REFERENCES demo_tpch.dim_supplier(supplier_id),
  FOREIGN KEY (customer_id) REFERENCES demo_tpch.dim_customer(customer_id),
  FOREIGN KEY (ship_date_id) REFERENCES demo_tpch.dim_date(date_id),
  FOREIGN KEY (commit_date_id) REFERENCES demo_tpch.dim_date(date_id),
  FOREIGN KEY (receipt_date_id) REFERENCES demo_tpch.dim_date(date_id)
) USING DELTA;

-- Populate fact_order_line with surrogate keys
INSERT INTO demo_tpch.fact_order_line (
  order_header_id, line_number, part_id, supplier_id, customer_id, extended_amount, discount_rate, net_amount, tax_rate, discount_amount, gross_revenue, quantity, ship_date_id, commit_date_id, receipt_date_id, ship_mode
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
JOIN demo_tpch.dim_order_header doh ON l.l_orderkey = doh.order_key
JOIN demo_tpch.dim_part dp ON l.l_partkey = dp.part_key
JOIN demo_tpch.dim_supplier ds ON l.l_suppkey = ds.supplier_key
JOIN demo_tpch.dim_customer dc ON doh.order_key = dc.customer_key
JOIN demo_tpch.dim_date dship ON l.l_shipdate = dship.date_key
JOIN demo_tpch.dim_date dcommit ON l.l_commitdate = dcommit.date_key
JOIN demo_tpch.dim_date dreceipt ON l.l_receiptdate = dreceipt.date_key;