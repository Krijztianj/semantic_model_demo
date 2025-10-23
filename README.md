# Semantic Model Demo - Data Warehouse Scripts

This repository contains SQL scripts to create a dimensional data warehouse model based on the TPC-H dataset.

## Structure

### Configuration
- **00_config.sql** - Sets catalog and schema variables. Run this first!

### Dimension Tables (with surrogate keys using `_id` suffix)
- **dim_date.sql** - Date dimension with `date_id`
- **dim_customer.sql** - Customer dimension with `customer_id`
- **dim_part.sql** - Part dimension with `part_id`
- **dim_supplier.sql** - Supplier dimension with `supplier_id`
- **dim_order_header.sql** - Order header dimension with `order_header_id`

### Fact Tables (with foreign keys to dimension surrogate keys)
- **fact_order_header.sql** - Order header facts
- **fact_order_line.sql** - Order line item facts

### Utilities
- **run_all.sql** - Master script to execute all scripts in order

## Usage

### Option 1: Run All Scripts
```sql
SOURCE scripts/run_all.sql;
```

### Option 2: Run Individual Scripts
1. First, set up your configuration:
```sql
SOURCE scripts/00_config.sql;
```

2. Modify the variables in `00_config.sql` if needed:
```sql
SET VAR catalog_name = 'your_catalog';
SET VAR schema_name = 'your_schema';
```

3. Run dimension scripts (in any order):
```sql
SOURCE scripts/dim_date.sql;
SOURCE scripts/dim_customer.sql;
SOURCE scripts/dim_part.sql;
SOURCE scripts/dim_supplier.sql;
SOURCE scripts/dim_order_header.sql;
```

4. Run fact scripts (after dimensions are created):
```sql
SOURCE scripts/fact_order_header.sql;
SOURCE scripts/fact_order_line.sql;
```

## Features

- ✅ Delta table format for ACID transactions
- ✅ Surrogate keys with `_id` suffix (auto-generated using IDENTITY)
- ✅ Primary key constraints on all tables
- ✅ Foreign key constraints on fact tables
- ✅ Dynamic catalog and schema configuration
- ✅ Star schema design pattern

## Schema Design

```
fact_order_line
├── order_header_id (FK → dim_order_header)
├── customer_id (FK → dim_customer)
├── part_id (FK → dim_part)
├── supplier_id (FK → dim_supplier)
├── ship_date_id (FK → dim_date)
├── commit_date_id (FK → dim_date)
└── receipt_date_id (FK → dim_date)

fact_order_header
├── order_header_id (FK → dim_order_header)
├── customer_id (FK → dim_customer)
└── order_date_id (FK → dim_date)
```
