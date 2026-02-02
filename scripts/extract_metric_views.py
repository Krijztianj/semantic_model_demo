"""
Extract Metric View YAML definitions from Unity Catalog.

This script queries Unity Catalog for metric views and extracts their YAML definitions,
saving them to the resources/metric_views/ directory.
"""

import os
import sys
import re
from databricks.sdk import WorkspaceClient
from pathlib import Path


def extract_yaml_from_view_definition(view_definition: str) -> str:
    """
    Extract YAML content from a metric view definition.
    
    Metric views are created with syntax like:
    CREATE VIEW ... WITH METRICS LANGUAGE YAML AS $$
    <yaml content>
    $$
    """
    # Match the YAML content between $$ delimiters
    pattern = r'\$\$(.*?)\$\$'
    match = re.search(pattern, view_definition, re.DOTALL)
    
    if match:
        yaml_content = match.group(1).strip()
        return yaml_content
    
    return None


def sanitize_filename(name: str) -> str:
    """Convert view name to a safe filename."""
    # Replace dots with underscores and ensure it ends with .yml
    safe_name = name.replace('.', '_')
    if not safe_name.endswith('.yml'):
        safe_name += '.yml'
    return safe_name


def extract_metric_views(catalog: str, schema: str, output_dir: str):
    """
    Extract all metric views from the specified catalog and schema.
    
    Args:
        catalog: Databricks catalog name
        schema: Schema name within the catalog
        output_dir: Directory to save YAML files
    """
    # Initialize Databricks client
    w = WorkspaceClient()
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Extracting metric views from {catalog}.{schema}...")
    
    # List all tables/views in the schema
    tables = w.tables.list(catalog_name=catalog, schema_name=schema)
    
    metric_views_found = 0
    
    for table in tables:
        # Only process views (not tables)
        if table.table_type != "VIEW":
            continue
        
        # Get full table details
        try:
            table_info = w.tables.get(full_name=f"{catalog}.{schema}.{table.name}")
            
            if not table_info.view_definition:
                continue
            
            # Check if it's a metric view (contains WITH METRICS)
            if "WITH METRICS" not in table_info.view_definition.upper():
                continue
            
            print(f"  Found metric view: {table.name}")
            
            # Extract YAML content
            yaml_content = extract_yaml_from_view_definition(table_info.view_definition)
            
            if yaml_content:
                # Save to file
                filename = sanitize_filename(table.name)
                filepath = output_path / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(yaml_content)
                
                print(f"    Saved to: {filepath}")
                metric_views_found += 1
            else:
                print(f"    WARNING: Could not extract YAML content from {table.name}")
        
        except Exception as e:
            print(f"    ERROR processing {table.name}: {e}")
    
    print(f"\nExtraction complete. Found {metric_views_found} metric view(s).")
    return metric_views_found


if __name__ == "__main__":
    # Get parameters from command line or environment variables
    catalog = os.getenv("CATALOG", "demo")
    schema = os.getenv("SCHEMA", "tpch_semantic")
    output_dir = os.getenv("OUTPUT_DIR", "resources/metric_views")
    
    # Allow command line overrides
    if len(sys.argv) > 1:
        catalog = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    if len(sys.argv) > 3:
        output_dir = sys.argv[3]
    
    print(f"Configuration:")
    print(f"  Catalog: {catalog}")
    print(f"  Schema: {schema}")
    print(f"  Output Directory: {output_dir}")
    print()
    
    try:
        count = extract_metric_views(catalog, schema, output_dir)
        sys.exit(0 if count > 0 else 1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
