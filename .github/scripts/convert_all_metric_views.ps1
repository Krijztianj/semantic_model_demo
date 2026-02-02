# PowerShell script to convert all Metric View YAML files to Power BI projects
# Requires: Tabular Editor 3 Enterprise with Semantic Bridge

$ErrorActionPreference = "Stop"

$YAML_DIR = "resources/metric_views"
$OUTPUT_DIR = "powerbi"
$SCRIPT_PATH = ".github/scripts/convert_metric_view.csx"

# Check if Tabular Editor 3 CLI is available
$tePath = $null

# Check common locations
$possiblePaths = @(
    "C:\Program Files\Tabular Editor 3\TabularEditor3.exe",
    "$env:ProgramFiles\Tabular Editor 3\TabularEditor3.exe",
    ".\tools\TabularEditor3.exe",
    (Get-Command "TabularEditor3.exe" -ErrorAction SilentlyContinue).Source
)

foreach ($path in $possiblePaths) {
    if ($path -and (Test-Path $path)) {
        $tePath = $path
        Write-Host "Found Tabular Editor 3 at: $tePath" -ForegroundColor Cyan
        break
    }
}

if (-not $tePath) {
    Write-Error "TabularEditor3.exe not found. Please install Tabular Editor 3 Enterprise."
    Write-Error "Download from: https://tabulareditor.com/"
    exit 1
}

# Create output directory
New-Item -ItemType Directory -Force -Path $OUTPUT_DIR | Out-Null

# Get all YAML files
$yamlFiles = Get-ChildItem -Path $YAML_DIR -Filter "*.yml" -File
Write-Host "Found $($yamlFiles.Count) metric view(s) to convert"

# Convert each YAML file
$converted = 0
$failed = 0

foreach ($yamlFile in $yamlFiles) {
    Write-Host ""
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "Converting: $($yamlFile.Name)" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan
    
    try {
        # Step 1: Generate C# script from YAML using Python
        Write-Host "Generating C# script from YAML..." -ForegroundColor Gray
        $scriptPath = python .github/scripts/generate_model_from_yaml.py $yamlFile.FullName $OUTPUT_DIR 2>&1 | Select-Object -Last 1
        
        if ($LASTEXITCODE -ne 0) {
            throw "Python script generation failed with code $LASTEXITCODE"
        }
        
        # Step 2: Execute C# script with Tabular Editor 3
        Write-Host "Executing with Tabular Editor 3..." -ForegroundColor Gray
        & $tePath -S $scriptPath
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Successfully converted" -ForegroundColor Green
            $converted++
        } else {
            throw "Tabular Editor 3 exited with code $LASTEXITCODE"
        }
    }
    catch {
        Write-Host "Conversion failed: $_" -ForegroundColor Red
        $failed++
    }
}

# Summary
Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Conversion Summary" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Total: $($yamlFiles.Count)"
Write-Host "Successful: $converted" -ForegroundColor Green

$failColor = if ($failed -gt 0) { "Red" } else { "Green" }
Write-Host "Failed: $failed" -ForegroundColor $failColor

if ($failed -gt 0) {
    exit 1
}
