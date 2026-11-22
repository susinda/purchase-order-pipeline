# Snowflake workload identity setup for GitHub Actions

This setup binds the Snowflake service user `GITHUB_CICD_USER` to GitHub Actions jobs that use the `test` environment in `susinda/purchase-order-pipeline`. GitHub issues a short-lived OpenID Connect (OIDC) token when the job requests one. [Snowflake workload identity federation](https://docs.snowflake.com/en/user-guide/workload-identity-federation)

## Create and configure the Snowflake user

Run this SQL in Snowflake after creating the `PURCHASE_ORDER_PIPELINE_ROLE` role and the `PO_WAREHOUSE` warehouse.

```sql
CREATE USER GITHUB_CICD_USER
    TYPE = SERVICE
    DEFAULT_ROLE = PURCHASE_ORDER_PIPELINE_ROLE
    DEFAULT_WAREHOUSE = PO_WAREHOUSE
    COMMENT = 'GitHub Actions CI/CD deployment';

ALTER USER GITHUB_CICD_USER
    SET WORKLOAD_IDENTITY = (
        TYPE = OIDC
        ISSUER = 'https://token.actions.githubusercontent.com'
        SUBJECT = 'repo:susinda/purchase-order-pipeline:environment:test'
    );

GRANT ROLE PURCHASE_ORDER_PIPELINE_ROLE
    TO USER GITHUB_CICD_USER;

ALTER USER GITHUB_CICD_USER
    SET DEFAULT_ROLE = PURCHASE_ORDER_PIPELINE_ROLE
        DEFAULT_WAREHOUSE = PO_WAREHOUSE;
```

## Configure the GitHub Actions job

The Snowflake job in [Initialize environment](../.github/workflows/initialize-environment.yml) uses `id-token: write` and runs against `test`. The Snowflake CLI action requests a GitHub OIDC token for `snowflakecomputing.com` and connects as `GITHUB_CICD_USER`. Configure `SNOWFLAKE_ACCOUNT` in GitHub's `test` environment. [Snowflake CLI with GitHub Actions](https://docs.snowflake.com/en/developer-guide/cicd/github-action)

## Verify the Snowflake configuration

```sql
DESCRIBE USER GITHUB_CICD_USER;

SHOW USER WORKLOAD IDENTITY AUTHENTICATION METHODS
    FOR USER GITHUB_CICD_USER;
```

Check the user's default role and warehouse, then confirm the workload identity shows the configured OIDC issuer and subject. After a successful GitHub Actions login, `last_used` shows when the workload identity authenticated. [Snowflake verification command](https://docs.snowflake.com/en/sql-reference/sql/show-user-workload-identity-authentication-methods)
