# CI/CD Setup Guide

> GitHub Actions configuration for automated Databricks Asset Bundle deployment

## Table of Contents
- [Prerequisites](#prerequisites)
- [GitHub Secrets Setup](#github-secrets-setup)
- [Workflows Overview](#workflows-overview)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)

## Prerequisites

⚠️ **Required before workflows will function:**

1. ✅ GitHub repository with admin access
2. ✅ Databricks workspace
3. ✅ Personal Access Token (PAT) for Databricks
4. ✅ Repository secrets configured (see below)

## GitHub Secrets Setup

### Step 1: Generate Databricks Token

1. Log into your Databricks workspace
2. Go to **User Settings** → **Developer** → **Access Tokens**
3. Click **"Generate New Token"**
4. Set description: `GitHub Actions CI/CD`
5. Set expiration: 90 days (recommended)
6. **Copy the token immediately** (only shown once!)

### Step 2: Add Secrets to GitHub

1. Navigate to: **Repository** → **Settings** → **Secrets and variables** → **Actions**
2. Click **"New repository secret"**
3. Add both secrets:

| Secret Name | Value | Example |
|-------------|-------|----------|
| `DATABRICKS_HOST` | Your workspace URL | `https://dbc-381633f5-fe84.cloud.databricks.com/` |
| `DATABRICKS_TOKEN` | Your PAT token | `dapi1234567890abcdef...` |

### Step 3: Verify Setup

1. Go to **Repository** → **Settings** → **Secrets and variables** → **Actions**
2. Confirm both secrets are listed
3. Secrets cannot be viewed after creation (only updated/deleted)

## Workflows Overview

### 1. Validate Bundle (`validate_bundle.yml`)

**Trigger**: Pull requests to `main`  
**Purpose**: Fast validation without deployment  
**Steps**:
- ✅ Install Databricks CLI
- ✅ Validate bundle configuration
- ✅ Check for syntax errors

**Ignores**: Workflow changes, metric views, markdown files

### 2. Deploy Bundle (`deploy_databricks.yml`)

**Trigger**: Push to `main` (after PR merge)  
**Purpose**: Full deployment pipeline  
**Steps**:
1. ✅ Validate bundle configuration
2. ✅ Deploy to Databricks workspace
3. ✅ Run data warehouse setup job

**Ignores**: Workflow changes, metric views, markdown files

**Manual Trigger**: Available via Actions tab → Run workflow

### 3. Sync Metric Views (`sync_metric_view.yml`)

**Status**: 🚧 Work in Progress (disabled)  
**Purpose**: Extract YAML from Unity Catalog, generate Power BI models  
**Trigger**: Manual only (when ready)

## Usage

### Normal Development Workflow

```bash
# 1. Create feature branch
git checkout -b feature/my-changes

# 2. Make changes
# Edit SQL scripts, notebooks, etc.

# 3. Commit and push
git add .
git commit -m "Add new metric view"
git push origin feature/my-changes

# 4. Create Pull Request
# → validate_bundle.yml runs automatically
# → Review validation results

# 5. Merge PR
# → deploy_databricks.yml runs automatically
# → Check deployment in Actions tab
```

### Manual Deployment

1. Go to **Actions** tab
2. Select **"Deploy Databricks Asset Bundle"**
3. Click **"Run workflow"**
4. Choose environment (`dev` or `prod`)
5. Click **"Run workflow"** button
6. Monitor progress in Actions tab

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

### Self-Hosted Runner Setup

The `sync_metric_view.yml` workflow requires a self-hosted runner with Tabular Editor 3 Enterprise.

**Setup steps:**

1. **Create self-hosted runner:**
   - Go to: `https://github.com/Krijztianj/semantic_model_demo/settings/actions/runners/new`
   - Select Windows
   - Follow installation instructions

2. **Install prerequisites on runner machine:**
   ```powershell
   # Install Python
   winget install Python.Python.3.11
   
   # Install PyYAML
   pip install pyyaml
   
   # Install Tabular Editor 3 Enterprise (requires license)
   # Download from: https://tabulareditor.com/
   # Install to: C:\Program Files\Tabular Editor 3\
   ```

3. **Start the runner:**
   ```powershell
   .\run.cmd
   ```

4. **Enable the workflow:**
   - Edit `.github/workflows/sync_metric_view.yml`
   - Change trigger from `workflow_dispatch` to your desired trigger (e.g., `push`, `schedule`)

**Alternative: Use GitHub-hosted runners with manual conversion**
- Run `python scripts/extract_metric_views.py` locally
- Run `.github/scripts/convert_all_metric_views.ps1` locally
- Commit generated .bim files
- Let CI/CD deploy the committed files

## Troubleshooting (continued)

### Authentication Failures
- Verify `DATABRICKS_HOST` includes `https://` and trailing `/`
- Regenerate `DATABRICKS_TOKEN` if expired
- Check token has workspace access permissions

### Validation Errors
```bash
# Test locally first
databricks bundle validate -t dev
```

### Deployment Failures
- Check Databricks workspace permissions
- Verify SQL Warehouse ID is correct
- Ensure catalog `demo` exists or can be created

### Job Run Failures
- View logs in Databricks: **Workflows** → **Jobs** → Job run
- Check notebook/SQL script syntax
- Verify catalog and schema are accessible

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)
