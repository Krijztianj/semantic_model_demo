"""
Generate Tabular Editor 3 C# script from metric view YAML
This creates a .csx script that TE3 can execute to build the model
"""

import yaml
import sys
import os
from pathlib import Path
from typing import Dict, List


def generate_te3_script(metric_view: Dict, output_bim_path: str) -> str:
    """Generate C# script for Tabular Editor 3 to create the model."""
    
    script = [
        "// Auto-generated script to create Tabular model from metric view YAML",
        "// Execute with: TabularEditor3.exe -S script.csx",
        "",
        "using System;",
        "using System.Linq;",
        "using TabularEditor.TOMWrapper;",
        "",
        "// Create new model",
        'var model = new TabularModelHandler("localhost", "tempdb").Model;',
        "model.Database.CompatibilityLevel = 1600;",
        f'model.Database.Name = "{Path(output_bim_path).stem}";',
        "",
        "// Add data source",
        'var ds = model.AddDataSource("DatabricksSQL");',
        'ds.Provider = "System.Data.OleDb";',
        'ds.ConnectionString = "Provider=MSOLEDBSQL;Data Source=<databricks-host>;Initial Catalog=demo;";',
        "",
    ]
    
    # Create fact table
    script.append("// Create Fact table")
    script.append('var factTable = model.AddTable("Fact");')
    script.append('var factPartition = factTable.AddPartition("Fact");')
    script.append('factPartition.Query = "SELECT * FROM demo.tpch_semantic.fact_order_line";')
    script.append("")
    
    # Add fact table columns from dimensions without joins
    script.append("// Add fact table columns")
    for dimension in metric_view.get('dimensions', []):
        expr = dimension.get('expr', '')
        name = dimension.get('name', expr)
        
        if '.' not in expr or expr.startswith('source.'):
            col_name = expr.replace('source.', '').strip('`')
            script.append(f'var col_{col_name.replace(" ", "_")} = factTable.AddDataColumn("{name}");')
            script.append(f'col_{col_name.replace(" ", "_")}.DataType = DataType.String;')
            script.append(f'col_{col_name.replace(" ", "_")}.SourceColumn = "{col_name}";')
    
    script.append("")
    
    # Create dimension tables from joins
    for join in metric_view.get('joins', []):
        dim_name = join['name']
        source_table = join.get('source', {}).get('table', dim_name)
        
        script.append(f"// Create {dim_name} dimension table")
        script.append(f'var {dim_name.lower()}Table = model.AddTable("{dim_name}");')
        script.append(f'var {dim_name.lower()}Partition = {dim_name.lower()}Table.AddPartition("{dim_name}");')
        script.append(f'{dim_name.lower()}Partition.Query = "SELECT * FROM demo.tpch_semantic.{source_table}";')
        script.append("")
        
        # Add dimension columns
        for dimension in metric_view.get('dimensions', []):
            expr = dimension.get('expr', '')
            name = dimension.get('name', expr)
            
            if expr.startswith(f"{dim_name}."):
                col_name = expr.split('.')[-1].strip('`')
                var_name = f"{dim_name.lower()}_{col_name}".replace(" ", "_")
                script.append(f'var {var_name} = {dim_name.lower()}Table.AddDataColumn("{name}");')
                script.append(f'{var_name}.DataType = DataType.String;')
                script.append(f'{var_name}.SourceColumn = "{col_name}";')
        
        script.append("")
        
        # Create relationship
        on_clause = join.get('on', '')
        if '=' in on_clause:
            parts = [p.strip() for p in on_clause.split('=')]
            from_col = parts[0].split('.')[-1].strip('`')
            to_col = parts[1].split('.')[-1].strip('`')
            
            script.append(f"// Create relationship: Fact -> {dim_name}")
            script.append(f'var rel_{dim_name.lower()} = model.AddRelationship();')
            script.append(f'rel_{dim_name.lower()}.FromTable = factTable;')
            script.append(f'rel_{dim_name.lower()}.FromColumn = factTable.Columns["{from_col}"];')
            script.append(f'rel_{dim_name.lower()}.ToTable = {dim_name.lower()}Table;')
            script.append(f'rel_{dim_name.lower()}.ToColumn = {dim_name.lower()}Table.Columns["{to_col}"];')
            script.append("")
    
    # Add measures
    script.append("// Add measures to Fact table")
    for measure in metric_view.get('measures', []):
        name = measure['name']
        expr = measure['expr']
        
        # Convert SQL to DAX (simplified)
        dax_expr = convert_sql_to_dax(expr)
        format_str = determine_format(name, expr)
        
        var_name = name.replace(' ', '_').replace('-', '_')
        script.append(f'var measure_{var_name} = factTable.AddMeasure("{name}", "{dax_expr}");')
        script.append(f'measure_{var_name}.FormatString = "{format_str}";')
    
    script.append("")
    script.append("// Save model")
    script.append(f'model.Database.SaveToFile(@"{output_bim_path}");')
    script.append('Info("Model saved successfully to: ' + output_bim_path + '");')
    
    return "\n".join(script)


def convert_sql_to_dax(sql_expr: str) -> str:
    """Convert SQL expression to DAX."""
    expr = sql_expr.strip()
    
    # SUM
    if expr.upper().startswith('SUM('):
        field = expr[4:-1].strip().strip('`')
        if '.' in field:
            table, col = field.rsplit('.', 1)
            return f"SUM('{table.strip('`')}'[{col.strip('`')}])"
        return f"SUM([{field}])"
    
    # COUNT DISTINCT
    if 'COUNT' in expr.upper() and 'DISTINCT' in expr.upper():
        field = expr.split('(')[-1].split(')')[0].strip().strip('`')
        if '.' in field:
            table, col = field.rsplit('.', 1)
            return f"DISTINCTCOUNT('{table.strip('`')}'[{col.strip('`')}])"
        return f"DISTINCTCOUNT([{field}])"
    
    # AVG
    if expr.upper().startswith('AVG('):
        field = expr[4:-1].strip().strip('`')
        if '.' in field:
            table, col = field.rsplit('.', 1)
            return f"AVERAGE('{table.strip('`')}'[{col.strip('`')}])"
        return f"AVERAGE([{field}])"
    
    # Calculated measures (with DIVIDE, etc.)
    if 'DIVIDE' in expr.upper():
        return expr  # Assume already in DAX format
    
    return expr


def determine_format(name: str, expr: str) -> str:
    """Determine format string based on measure name."""
    name_lower = name.lower()
    
    if any(word in name_lower for word in ['revenue', 'amount', 'price', 'value', 'cost']):
        return "$#,##0.00"
    elif any(word in name_lower for word in ['percent', 'rate', '%']):
        return "0.0%"
    elif 'count' in name_lower or 'quantity' in name_lower:
        return "#,##0"
    return "#,##0.00"


def main():
    if len(sys.argv) < 3:
        print("Usage: python generate_model_from_yaml.py <yaml_path> <output_directory>")
        sys.exit(1)
    
    yaml_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    # Load YAML
    with open(yaml_path, 'r', encoding='utf-8') as f:
        metric_view = yaml.safe_load(f)
    
    # Generate output paths
    model_name = Path(yaml_path).stem
    output_folder = Path(output_dir) / model_name
    output_folder.mkdir(parents=True, exist_ok=True)
    
    bim_path = output_folder / "Model.bim"
    script_path = output_folder / "generate_model.csx"
    
    # Generate C# script
    script_content = generate_te3_script(metric_view, str(bim_path))
    
    # Save script
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    print(f"✓ Generated C# script: {script_path}")
    print(f"Execute with: TabularEditor3.exe -S {script_path}")
    
    # Return script path for PowerShell to execute
    print(str(script_path))


if __name__ == "__main__":
    main()
