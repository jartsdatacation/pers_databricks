# pers_databricks

## Deploying resources (not implemented yet)

This project contains [Terraform](https://developer.hashicorp.com/terraform) configurations to provision resources in Azure. You can set these up for this by running:
- `cd /deployment/terraform`
- `terraform init`
- `terraform apply -var-file='.tfvars'`

## Run locally with databricks connect

```
uv sync
uv run run-silver-mock
```

## Run with Databricks asset bundles
```
uv sync
uv build
databricks bundle validate
databricks bundle deploy
databricks bundle run silver_mock
```

Optional to add CRON or "quartz_cron_expression: "0 0 6 * * ?"" as an expression in databricks.yml