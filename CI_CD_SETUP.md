# CI/CD Setup Guide for Databricks Asset Bundle

## Prerequisites

⚠️ **IMPORTANT**: Before running any GitHub Actions workflows, you must configure the required secrets in your repository.
1. Validates the DAB configuration
2. Deploys the bundle to Databricks
3. Runs the data warehouse setup job

## Workflow Triggersre Set
After adding secrets:
1. Go to Repository → Settings → Secrets and variables → Actions
2. You should see both `DATABRICKS_HOST` and `DATABRICKS_TOKEN` listed
3. Secrets cannot be viewed after creation (only updated/deleted)

## GitHub Actions Workflow

The repository includes a GitHub Actions workflow (`.github/workflows/deploy_databricks.yml`) that automatically:
1. Validates the DAB configuration
2. Deploys the bundle to Databricks
3. Runs the data warehouse setup job

## Required GitHub Secrets

Add these secrets to your GitHub repository:

### Navigate to: Repository → Settings → Secrets and variables → Actions → New repository secret

1. **DATABRICKS_HOST**
   - Value: `https://dbc-381633f5-fe84.cloud.databricks.com/`
   - Description: Your Databricks workspace URL

2. **DATABRICKS_TOKEN**
   - Value: Your Databricks Personal Access Token
   - How to create:
     - Go to Databricks workspace → User Settings → Developer → Access Tokens
     - Click "Generate New Token"
     - Set expiration and description
     - Copy the token (shown only once!)

## Workflow Triggers

### Validation (validate_bundle.yml)
- **Pull Request to `main`**: Validates bundle configuration (fast feedback)
- No deployment, only validation

### Deployment (deploy_databricks.yml)
- **Push to `main` branch**: Full deployment after PR merge
  - Validates configuration
  - Deploys bundle to Databricks
  - Runs the data warehouse setup job
- Ignores changes to workflows, metric views, and markdown files

### Manual Deployment
- Go to **Actions** tab in GitHub
- Select **"Deploy Databricks Asset Bundle"** workflow
- Click **"Run workflow"**
- Choose environment (`dev` or `prod`)
- Click **"Run workflow"** button

## Environment Setup (Optional)

For better control, set up GitHub Environments:

### Navigate to: Repository → Settings → Environments

#### Dev Environment
- No protection rules needed
- Secrets: Uses repository secrets

#### Prod Environment
- **Protection rules**:
  - ✅ Required reviewers (1-2 approvers)
  - ✅ Wait timer (optional delay)
- Additional secrets if needed (prod-specific tokens)

## Workflow Steps

1. **Checkout code** - Gets latest repository code
2. **Set up Python** - Installs Python 3.11
3. **Install Databricks CLI** - Installs latest CLI version
4. **Validate Bundle** - Checks configuration is valid
5. **Deploy Bundle** - Uploads notebooks, SQL scripts, creates jobs
6. **Run Job** - Executes the data warehouse setup job

## Customization

### Deploy Only (No Job Run)
Remove the last step in `.github/workflows/deploy_databricks.yml`:
```yaml
# Comment out or delete:
# - name: Run Job
#   env:
#     DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
#     DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
#   run: |
#     databricks bundle run setup_datawarehouse_job -t ${{ github.event.inputs.environment || 'dev' }}
```

### Add Additional Jobs
Add more run steps:
```yaml
- name: Run Another Job
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  run: |
    databricks bundle run another_job_name -t ${{ github.event.inputs.environment || 'dev' }}
```

### Change Trigger Branches
Edit the `on.push.branches` section:
```yaml
on:
  push:
    branches:
      - main
      - develop
      - feature/*
```

## Monitoring

- View workflow runs: **Actions** tab in GitHub
- Click on any run to see:
  - Job logs
  - Deployment status
  - Error messages
  - Execution time

## Troubleshooting

### Authentication Failures
- Verify `DATABRICKS_HOST` includes `https://` and trailing `/`
- Regenerate `DATABRICKS_TOKEN` if expired
- Check token has necessary permissions

### Validation Errors
- Run locally: `databricks bundle validate -t dev`
- Check YAML syntax in bundle files
- Verify all referenced files exist

### Deployment Failures
- Check workspace permissions
- Verify catalog/schema names are correct
- Ensure SQL Warehouse ID is valid

### Job Run Failures
- Check job configuration in Databricks workspace
- View job run logs in Databricks
- Verify notebook/SQL script syntax

## Best Practices

1. **Always test in dev first** before deploying to prod
2. **Use Pull Requests** to trigger validation before merging
3. **Set up branch protection** on main branch
4. **Require reviews** for production deployments
5. **Monitor workflow runs** regularly
6. **Rotate tokens periodically** for security
7. **Use environment-specific secrets** for prod

## Next Steps

1. Add secrets to GitHub repository
2. Push code to main branch or create a PR
3. Watch workflow run in Actions tab
4. Verify deployment in Databricks workspace
5. Set up prod environment with approval gates
