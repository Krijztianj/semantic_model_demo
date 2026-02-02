USE CATALOG main;
USE SCHEMA demo_tpch_semantic;


CREATE OR REPLACE VIEW krjo_demo.semantic_model.order_metrics_mv
WITH METRICS
LANGUAGE YAML
COMMENT 'TPC-H Order Analytics Metric View - Core business metrics'
AS $$
version: 1.1
source: fact_order_line
comment: TPC-H Order Analytics Metric View - Core business metrics for order line analysis
joins:
  - name: dim_customer
    source: dim_customer
    on: source.customer_id = dim_customer.customer_id
  - name: ship_date
    source: dim_date
    on: source.ship_date_id = ship_date.date_id
  - name: commit_date
    source: dim_date
    on: source.commit_date_id = commit_date.date_id
  - name: receipt_date
    source: dim_date
    on: source.receipt_date_id = receipt_date.date_id
  - name: dim_part
    source: dim_part
    on: source.part_id = dim_part.part_id
  - name: dim_supplier
    source: dim_supplier
    on: source.supplier_id = dim_supplier.supplier_id
  - name: dim_order_header
    source: dim_order_header
    on: source.order_header_id = dim_order_header.order_header_id
dimensions:
  - name: Order Header ID
    expr: order_header_id
    synonyms: [order id, order number]
  - name: Line Number
    expr: line_number
    synonyms: [line item number, item number]
  - name: Ship Mode
    expr: ship_mode
    synonyms: [shipping method, delivery method]
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
  - name: Supplier Name
    expr: dim_supplier.supplier_name
    synonyms: [vendor name, supplier]
  - name: Supplier Nation
    expr: dim_supplier.nation
    synonyms: [supplier country]
  - name: Supplier Region
    expr: dim_supplier.region
    synonyms: [supplier area]
  - name: Order Status
    expr: dim_order_header.order_status
    synonyms: [status, order state]
  - name: Order Priority
    expr: dim_order_header.order_priority
    synonyms: [priority, order priority level]
  - name: Clerk Name
    expr: dim_order_header.clerk_name
    synonyms: [clerk, sales clerk]
measures:
  - name: Total Orders
    expr: COUNT(DISTINCT order_header_id)
    display_name: Total Orders
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [order count, number of orders]
  - name: Total Line Items
    expr: COUNT(1)
    display_name: Total Line Items
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: false

    synonyms: [line item count, number of line items]
  - name: Total Quantity
    expr: SUM(quantity)
    display_name: Total Quantity
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: false

    synonyms: [total units, quantity sold, units ordered]
  - name: Total Extended Amount
    expr: SUM(extended_amount)
    display_name: Total Extended Amount
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false

    synonyms: [extended price, gross amount before discount]
  - name: Total Discount Amount
    expr: SUM(discount_amount)
    display_name: Total Discount Amount
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [total discounts, discount given]
  - name: Total Net Amount
    expr: SUM(net_amount)
    display_name: Total Net Amount
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [net revenue, revenue after discounts, net sales]
  - name: Total Gross Revenue
    expr: SUM(gross_revenue)
    display_name: Total Gross Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [gross revenue, total revenue with tax]
  - name: Average Discount Rate
    expr: AVG(discount_rate)
    display_name: Average Discount Rate
    format:
      type: percentage
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [avg discount, mean discount rate]
  - name: Average Tax Rate
    expr: AVG(tax_rate)
    display_name: Average Tax Rate
    format:
      type: percentage
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [avg tax, mean tax rate]
  - name: Average Order Value
    expr: MEASURE(`Total Net Amount`) / NULLIF(MEASURE(`Total Orders`), 0)
    display_name: Average Order Value
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [aov, avg order value, mean order value]
  - name: Average Items Per Order
    expr: MEASURE(`Total Line Items`) / NULLIF(MEASURE(`Total Orders`), 0)
    display_name: Average Items Per Order
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [items per order, avg line items per order]
  - name: Average Quantity Per Line
    expr: MEASURE(`Total Quantity`) / NULLIF(MEASURE(`Total Line Items`), 0)
    display_name: Average Quantity Per Line
    format:
      type: number
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [avg qty per line, units per line item]
  - name: Discount Percentage
    expr: (MEASURE(`Total Discount Amount`) / NULLIF(MEASURE(`Total Extended Amount`), 0)) * 100
    display_name: Discount Percentage
    format:
      type: percentage
      decimal_places:
        type: max
        places: 2
      hide_group_separator: false

    synonyms: [discount rate percentage, pct discounted]
$$;
