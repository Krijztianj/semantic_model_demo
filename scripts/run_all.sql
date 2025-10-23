-- Master Script: Run all dimension and fact table creation scripts
-- This script executes all table creation and population in the correct order

-- Step 1: Set up catalog and schema
SOURCE 00_config.sql;

-- Step 2: Create and populate dimension tables
SOURCE dim_date.sql;
SOURCE dim_customer.sql;
SOURCE dim_part.sql;
SOURCE dim_supplier.sql;
SOURCE dim_order_header.sql;

-- Step 3: Create and populate fact tables
SOURCE fact_order_header.sql;
SOURCE fact_order_line.sql;

-- Step 4: Verify table creation
SHOW TABLES IN ${schema_name};

-- Step 5: Display row counts
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM ${schema_name}.dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM ${schema_name}.dim_customer
UNION ALL
SELECT 'dim_part', COUNT(*) FROM ${schema_name}.dim_part
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM ${schema_name}.dim_supplier
UNION ALL
SELECT 'dim_order_header', COUNT(*) FROM ${schema_name}.dim_order_header
UNION ALL
SELECT 'fact_order_header', COUNT(*) FROM ${schema_name}.fact_order_header
UNION ALL
SELECT 'fact_order_line', COUNT(*) FROM ${schema_name}.fact_order_line;
