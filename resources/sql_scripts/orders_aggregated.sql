USE CATALOG main;
USE SCHEMA demo_tpch_semantic;

CREATE OR REPLACE VIEW orders_aggregated_mv
WITH METRICS
LANGUAGE YAML
COMMENT 'Aggregated Order Analytics Metric View - Summary metrics by time and geography'
AS $$
version: 1.1
source: orders_aggregated
comment: Aggregated order metrics for regional and temporal analysis
dimensions:
  - name: Year
    expr: year
    synonyms: [calendar year, fiscal year]
  - name: Quarter
    expr: quarter
    synonyms: [fiscal quarter, Q]
  - name: Region
    expr: region
    synonyms: [geographic region, market region]
  - name: Nation
    expr: nation
    synonyms: [country, national market]
measures:
  - name: Total Revenue
    expr: SUM(total_net_amount)
    synonyms: [total sales, revenue, net sales]
    comment: Total net revenue amount
  - name: Total Quantity
    expr: SUM(total_quantity)
    synonyms: [total units, units sold, quantity sold]
    comment: Total quantity of items
  - name: Total Orders
    expr: SUM(order_count)
    synonyms: [order count, number of orders, orders]
    comment: Total number of unique orders
  - name: Average Order Value
    expr: SUM(total_net_amount) / SUM(order_count)
    synonyms: [AOV, avg order value, average order]
    comment: Average revenue per order
  - name: Average Revenue per Region
    expr: AVG(total_net_amount)
    synonyms: [avg regional revenue, regional average]
    comment: Average revenue across regions
$$;
