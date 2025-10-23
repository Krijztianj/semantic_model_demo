# Semantic Model Demo - Data Warehouse

This repository contains scripts to create a dimensional data warehouse model based on the TPC-H dataset.

## Quick Start (Recommended)

### Python Notebook Approach
Use the **`setup_datawarehouse.py`** Databricks notebook for the easiest setup:

1. Upload `setup_datawarehouse.py` to your Databricks workspace
2. Edit the configuration cell:
```python
catalog = 'main'        # Change to your catalog name
schema = 'demo_tpch'    # Change to your schema name
```
3. Run all cells - the notebook will:
   - Create catalog and schema
   - Set the context automatically
   - Create all dimension and fact tables with surrogate keys
   - Populate all tables with data
   - Show verification queries

**Benefits**: Variables work across all commands, provides progress feedback, includes sample queries.

## Features

- ✅ Delta table format for ACID transactions
- ✅ Surrogate keys with `_id` suffix (auto-generated using IDENTITY)
- ✅ Primary key constraints on all tables
- ✅ Foreign key constraints on fact tables
- ✅ Dynamic catalog and schema configuration via Python
- ✅ Star schema design pattern

## Schema Design

### Dimension Tables
- **dim_date** - Date dimension with date_id surrogate key
- **dim_customer** - Customer dimension with customer_id surrogate key
- **dim_part** - Part dimension with part_id surrogate key
- **dim_supplier** - Supplier dimension with supplier_id surrogate key
- **dim_order_header** - Order header dimension with order_header_id surrogate key

### Fact Tables
```
fact_order_line
├── order_header_id (FK → dim_order_header)
├── customer_id (FK → dim_customer)
├── part_id (FK → dim_part)
├── supplier_id (FK → dim_supplier)
├── ship_date_id (FK → dim_date)
├── commit_date_id (FK → dim_date)
└── receipt_date_id (FK → dim_date)
```

## Technical Details

- **Source Data**: samples.tpch (Databricks sample dataset)
- **Storage Format**: Delta Lake
- **Surrogate Keys**: Auto-generated using IDENTITY columns
- **Key Naming Convention**: `{table_name}_id` for surrogate keys, `{table_name}_key` for business keys
