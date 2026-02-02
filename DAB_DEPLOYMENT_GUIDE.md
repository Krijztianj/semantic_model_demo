# Databricks Asset Bundle - Deployment Guide

## Overview
This repository uses Databricks Asset Bundles (DAB) to deploy:
- **Notebooks**: Python notebooks for data warehouse setup
- **Jobs**: Scheduled workflows for data processing
- **SQL Scripts**: Metric view definitions

## Prerequisites

1. **Install Databricks CLI** (v0.218.0 or later):
```bash
pip install databricks-cli --upgrade
```

2. **Authenticate with Databricks**:
```bash
databricks configure --profile DEFAULT
```
Or use OAuth:
```bash
databricks auth login --host https://YOUR_WORKSPACE_URL.cloud.databricks.com
```

3. **Verify Installation**:
```bash
databricks bundle validate
```

## Project Structure

```
semantic_model_demo/
├── databricks.yml           # Main bundle configuration
├── resources/
│   ├── jobs/
│   │   └── jobs.yml        # Job definitions
│   ├── notebooks/
│   │   └── setup_datawarehouse.py
│   ├── sql_scripts/
│   │   ├── order_metrics.sql
│   │   └── orders_aggregated.sql
│   └── metric_views/
│       └── order_metrics.yml
├── scripts/
│   └── extract_metric_views.py
├── .github/workflows/
│   ├── validate_bundle.yml
│   ├── deploy_databricks.yml
│   └── sync_metric_view.yml
└── .gitignore
```

## Deployment Commands

### Deploy to Development
```bash
databricks bundle deploy --target dev
```

### Deploy to Staging
```bash
databricks bundle deploy --target staging
```

### Deploy to Production
```bash
databricks bundle deploy --target prod
```

### Validate Configuration
```bash
databricks bundle validate
```

### Run a Job
```bash
databricks bundle run setup_datawarehouse_job --target dev
```

### Destroy Deployment
```bash
databricks bundle destroy --target dev
```

## Environment Targets

### Dev (Default)
- **Catalog**: `demo`
- **Schema**: `tpch_semantic`
- **Location**: `/Workspace/Users/{user}/.bundle/semantic_model_demo/dev`
- **Mode**: Development (allows rapid iteration)

### Prod
- **Catalog**: `demo`
- **Schema**: `tpch_semantic`
- **Location**: `/Workspace/Shared/.bundle/semantic_model_demo/prod`
- **Mode**: Production (requires service principal)

**Note**: Both environments use the same catalog/schema. The bundle configuration can be customized per environment if needed.

## Configuration

### Update Workspace URL
Edit `databricks.yml`:
```yaml
workspace:
  host: https://YOUR_WORKSPACE_URL.cloud.databricks.com
```

### Customize Variables
Override variables per environment in `databricks.yml`:
```yaml
targets:
  dev:
    variables:
      catalog: my_dev_catalog
      schema: my_schema
```

### Add a New Job
Create or edit files in `resources/` directory:
```yaml
resources:
  jobs:
    my_new_job:
      name: "My Job - ${bundle.target}"
      tasks:
        - task_key: my_task
          notebook_task:
            notebook_path: ../notebooks/my_notebook.py
```

## Workflow

1. **Make Changes**: Edit notebooks, SQL scripts, or job configs
2. **Validate**: `databricks bundle validate`
3. **Deploy to Dev**: `databricks bundle deploy --target dev`
4. **Test**: Run jobs and verify outputs
5. **Deploy to Prod**: `databricks bundle deploy --target prod`

## Monitoring

- View deployed resources in Databricks workspace
- Check job runs: Workflows → Jobs → "Setup Data Warehouse"
- Monitor logs in job run details

## Troubleshooting

### Authentication Issues
```bash
databricks auth login --host https://YOUR_WORKSPACE_URL.cloud.databricks.com
```

### Validation Errors
```bash
databricks bundle validate --debug
```

### Check Current Configuration
```bash
databricks bundle summary
```

## Best Practices

1. **Always deploy to dev first** before staging/prod
2. **Use version control** for all bundle changes
3. **Set appropriate permissions** for production service principals
4. **Test locally** with `databricks bundle validate`
5. **Use variables** instead of hardcoded values
6. **Keep secrets in Databricks Secrets** (not in code)

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Bundle Configuration Reference](https://docs.databricks.com/dev-tools/bundles/settings.html)
- [CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
