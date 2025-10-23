-- Configuration: Set catalog and schema variables
-- Run this script first before executing any dimension or fact scripts

-- Set your catalog and schema names here
SET VAR catalog_name = 'main';
SET VAR schema_name = 'demo_tpch';

-- Create catalog if it doesn't exist (optional, may require permissions)
-- CREATE CATALOG IF NOT EXISTS ${catalog_name};

-- Use the catalog
USE CATALOG ${catalog_name};

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ${schema_name};

-- Use the schema
USE SCHEMA ${schema_name};

-- Display current configuration
SELECT 
  current_catalog() AS current_catalog,
  current_schema() AS current_schema;
