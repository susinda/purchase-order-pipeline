# Snowflake workload identity setup for GitHub Actions

This setup binds the Snowflake service user `GITHUB_CICD_USER` to GitHub Actions jobs that use the `test` environment in `susinda/purchase-order-pipeline`. GitHub issues a short-lived OpenID Connect (OIDC) token when the job requests one. [Snowflake workload identity federation](https://docs.snowflake.com/en/user-guide/workload-identity-federation)

## Create and configure the Snowflake user

Run this SQL in Snowflake after creating the `PURCHASE_ORDER_PIPELINE_ROLE` role and the `PO_WAREHOUSE` warehouse. If `GITHUB_CICD_USER` already exists, run the `ALTER USER ... SET WORKLOAD_IDENTITY` statement using a role that can alter the user.

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
        SUBJECT = 'repo:susinda@4962136/purchase-order-pipeline@1462237961:environment:test'
    );

GRANT ROLE PURCHASE_ORDER_PIPELINE_ROLE
    TO USER GITHUB_CICD_USER;

ALTER USER GITHUB_CICD_USER
    SET DEFAULT_ROLE = PURCHASE_ORDER_PIPELINE_ROLE
        DEFAULT_WAREHOUSE = PO_WAREHOUSE;
```

The subject must match the GitHub-issued `sub` claim exactly. This repository uses GitHub's immutable subject format, which includes the owner ID and repository ID. [GitHub OIDC subject claims](https://docs.github.com/en/actions/reference/security/oidc#immutable-subject-claims)

## Configure the GitHub Actions job

The Snowflake job in [Initialize environment](../.github/workflows/initialize-environment.yml) uses `id-token: write` and runs against `test`. The Snowflake CLI action requests a GitHub OIDC token for `snowflakecomputing.com` and connects as `GITHUB_CICD_USER`. Configure `SNOWFLAKE_ACCOUNT` in GitHub's `test` environment. [Snowflake CLI with GitHub Actions](https://docs.snowflake.com/en/developer-guide/cicd/github-action)

In a Snowsight worksheet connected to the account where you created the service user, get the client account identifier.

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS SNOWFLAKE_ACCOUNT;
```

Save the result in **GitHub → Settings → Environments → test → Environment variables** as `SNOWFLAKE_ACCOUNT`. Use the `orgname-account_name` value by itself. Both workflows prefer the variable over a same-named secret. A `404` on `/session/v1/login-request` can occur when a bare account locator is missing its region or cloud suffix. The organization-account identifier works across regions. [Snowflake account identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier)

## Verify the Snowflake configuration

```sql
DESCRIBE USER GITHUB_CICD_USER;

SHOW USER WORKLOAD IDENTITY AUTHENTICATION METHODS
    FOR USER GITHUB_CICD_USER;
```

Check the user's default role and warehouse, then confirm the workload identity shows the configured OIDC issuer and subject. After a successful GitHub Actions login, `last_used` shows when the workload identity authenticated. [Snowflake verification command](https://docs.snowflake.com/en/sql-reference/sql/show-user-workload-identity-authentication-methods)
