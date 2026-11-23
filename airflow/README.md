# Deploy and run the purchase-order DAGs on Astro

The Astro project lives in `airflow/`. Its image contains the Python
dependencies, `include/` helpers, non-secret `env.vars`, and the public Aiven CA certificate.
One Astro deployment publishes the image and the `airflow/dags/` bundle together.

## Set up the deployment

1. Enable DAG-only deploys on the Astro deployment. In the GitHub `test`
   environment, set `ASTRO2_DEPLOYMENT_ID` as a variable and `ASTRO2_API_TOKEN`
   as a secret.
2. Use [Initialize environment](../.github/workflows/initialize-environment.yml)
   with `mysql-and-kafka` to create the Target API's MySQL tables and provision
   the Kafka topics, users, and ACLs. Deploy both APIs before running the DAGs.
3. Add the Secret variables below to the Astro deployment and create the
   Airflow Snowflake connection. Enter its credentials in the connection or a
   secrets backend.
   Initialize the warehouse and publish dbt before the first Snowflake
   ingestion run. See [Snowflake and dbt ownership](#snowflake-and-dbt-ownership).
4. Run [Deploy APIs, DAGs, and dbt](../.github/workflows/deploy-apis-dags-dbt.yml)
   with **dags** to deploy the Astro image and DAG bundle together.
   Pushes to `main` also trigger that combined deployment when a file under
   `airflow/` changes. [Astronomer deploy action](https://github.com/astronomer/deploy-action)

| Runtime setting | Value |
|---|---|
| `SOURCE_API_URL`, `TARGET_API_URL` | HTTPS base URLs of the deployed APIs |
| `SOURCE_API_USERNAME`, `SOURCE_API_PASSWORD` | Astro Secret variables for Source API Basic Auth |
| `TARGET_API_USERNAME`, `TARGET_API_PASSWORD` | Astro Secret variables for Target API Basic Auth |
| `KAFKA_BOOTSTRAP_SERVERS` | Aiven broker address |
| `KAFKA_PRODUCER_SASL_USERNAME`, `KAFKA_PRODUCER_SASL_PASSWORD` | Astro Secret variables for the Aiven producer |
| `KAFKA_CONSUMER_SASL_USERNAME`, `KAFKA_CONSUMER_SASL_PASSWORD` | Astro Secret variables for the Aiven consumer shared by both downstream DAGs |
| `KAFKA_TOPIC` | Purchase-order topic, defaults to `purchase-orders` |
| `KAFKA_BACKEND_GROUP_ID`, `KAFKA_ANALYTICS_GROUP_ID` | Defaults to `purchase-orders-backend` and `purchase-orders-analytics` |
| `KAFKA_TARGET_DLT_TOPIC`, `KAFKA_SNOWFLAKE_DLT_TOPIC` | Provisioned DLTs, normally `purchase-orders.backend.dlt` and `purchase-orders.analytics.dlt` |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM` | Defaults to `SASL_SSL` and `SCRAM-SHA-256` |
| `KAFKA_SSL_CAFILE` | Optional certificate path. The image uses its bundled `aiven_ca.pem` by default. |
| `SNOWFLAKE_CONNECTION_ID` | Connection ID used by both Snowflake DAG tasks and the connection check, defaults to `snowflake_default` |
| `SNOWFLAKE_RAW_TABLE` | Existing `DATABASE.SCHEMA.TABLE` with the columns listed below |
| `SNOWFLAKE_DBT_PROJECT` | Existing managed `DATABASE.SCHEMA.PROJECT`. Omit the setting from `env.vars` and Astro for RAW ingestion alone. |
| `KAFKA_TEST_TOPIC`, `KAFKA_TEST_GROUP_ID` | Optional for a diagnostic round-trip. The group defaults to `purchaseorders-connection-test`. |

Match the topic, users, and group IDs in Astro to the provisioned ACLs. The
producer needs topic Write. The consumer needs topic Read, Read for both group
IDs, and Write to both DLTs. The optional round-trip needs producer Write and
consumer Read on its dedicated topic, plus consumer Read for the diagnostic
group. See [Kafka provisioning defaults](../integration/po_pipeline/application.properties).

The image includes the [non-secret deployment settings](env.vars).
`po_settings.py` reads these defaults and gives nonempty Astro environment
variables precedence. Create the credentials above as Secret environment
variables on the Astro2 deployment. Store Snowflake authentication in the
Airflow connection named by `SNOWFLAKE_CONNECTION_ID`. [Manage secrets and connections in Astro](https://www.astronomer.io/docs/astro/manage-connections-variables/)

For local Astro, use a private `.env` populated with the settings in
[env.vars.example](env.vars.example). From `airflow/`, run
`astro dev start --env .env`. The private `.env` stays out of Git and the image.

## Run and verify the DAGs

The four DAGs are manually triggered. Enable the publisher and both consumers
in Airflow. Run `dag_test_connections` first to check the two API health
endpoints, Kafka metadata, and the configured Snowflake connection. Set
`enable_live_kafka_test` only after provisioning `KAFKA_TEST_TOPIC` and its
ACLs.

Trigger `dag_source_api_to_kafka` with `{"load_type":"initial"}` and check
`dag_kafka_to_target_api` and `dag_kafka_to_snowflake`. The publisher waits
for Kafka acknowledgements before triggering both consumers independently.
Check their results before triggering `{"load_type":"incremental"}`. For a
clean demo, expect 100 published records on the initial run and 20 on the
incremental run.

## Delivery and recovery

Each consumer starts from its group's committed offsets. A new group reads
retained records from the beginning. Each run stops at its message limit or
after its idle timeout. The Target and Snowflake limits default to 100 messages
and can be set using `PO_BACKEND_MAX_MESSAGES` and
`PO_ANALYTICS_MAX_MESSAGES`. Trigger the affected consumer again if records
remain after a run, and check its logs and group lag.

The Target DAG supplies the Kafka event ID as the API's idempotency key.
Consumers advance offsets after the destination confirms a write or after a
DLT publish is acknowledged. Repair and replay quarantined records from the
corresponding DLT. After a transient failure, retry the consumer from its
last committed position.

## Snowflake and dbt ownership

The Snowflake DAG merges order snapshots by `EVENT_ID` into
`PURCHASE_ORDER_DB.RAW.PURCHASE_ORDER_EVENTS` by default. `PAYLOAD` contains
the order snapshot and `INGESTED_AT` records when the event first landed.
The Snowflake connection role needs warehouse, database, and schema usage,
SELECT and INSERT on the RAW table, and USAGE on the managed dbt project.

Set `SNOWFLAKE_ACCOUNT` in the GitHub `test` environment as a variable or secret,
using the Snowflake account identifier. If Astro uses a different RAW table,
set the GitHub variable `SNOWFLAKE_RAW_TABLE` to the same fully qualified name.
The Airflow DAG passes that RAW location to dbt when it starts a build.

1. Create `PURCHASE_ORDER_DB` with `RAW`, `CLEAN`, `BIZ`, and `TRANSFORM` schemas
   if they don't exist. Run the [pipeline role grants](../snowflake/init/grant-pipeline-access.sql)
   in Snowsight with an administrator role before CI runs. If you configured a
   custom RAW location, use that database and schema for the RAW grants.
2. Run [Initialize environment](../.github/workflows/initialize-environment.yml)
   with `test` and `snowflake`. The default `all` also provisions MySQL and Aiven.
   The initializer runs the [RAW SQL](../snowflake/init/initialize-raw.sql) and
   the dbt project's [warehouse DDL](../snowflake/dbt/ddl/warehouse.sql) using
   the [GitHub OIDC service user](../docs/snowflake-github-actions-oidc.md).
   The dbt project's DDL defines the CLEAN and BIZ tables and reporting views.
   Reruns preserve table rows. The role that owns an older RAW table must be
   able to add `INGESTED_AT` during initialization. Historical rows without that
   column retain an unknown ingestion time. If your legacy RAW table stores an
   event envelope or has incompatible columns such as required `SNAPSHOT_HASH`,
   migrate it or point both GitHub and Astro to a compatible RAW table.
3. Run [Deploy APIs, DAGs, and dbt](../.github/workflows/deploy-apis-dags-dbt.yml)
   with **dbts** to publish `PURCHASE_ORDER_DB.TRANSFORM.PO_DBT_PROJECT` using
   dbt Core 1.12.3. Snowflake CLI reads [the profile](../snowflake/dbt/profiles.yml)
   from the project directory. Future pushes to `main` that change the dbt models, macros,
   tests, or configuration publish the updated project. Apply DDL changes in
   `snowflake/init/` or `snowflake/dbt/ddl/` with the initialization workflow
   before using models that depend on them. Use explicit `ALTER TABLE`
   statements for changes to existing tables. Set
   `SNOWFLAKE_DBT_PROJECT` in Astro to the same fully qualified name.
4. Run the pipeline. After a successful ingestion run with rows and no
   quarantines, Airflow starts `dbt build`. The models overwrite the contents
   of the initialized tables with one statement per table. Existing table
   definitions, grants, and dependent views are preserved. Existing tables
   created outside this initializer need matching columns and grant the dbt
   role SELECT, INSERT, and DELETE or use that role as their owner.
   [Snowflake dbt deployment](https://docs.snowflake.com/en/developer-guide/snowflake-cli/command-reference/dbt-commands/deploy) · [Snowflake INSERT OVERWRITE](https://docs.snowflake.com/en/sql-reference/sql/insert)

### Remove legacy CLEAN tables

The current dbt project uses four table aliases without the `CLEAN_` prefix.
If an earlier build left prefixed tables, publish the current project and
verify a build populates the four unprefixed tables. Then run the
[legacy table cleanup SQL](../snowflake/init/drop-legacy-clean-tables.sql)
manually with the role that owns those legacy tables. `STG_LATEST_ORDER_EVENT`
is the staging view used by the four CLEAN tables.

### Power BI and management reporting

An administrator can run [the BI grants script](../snowflake/init/grant-bi-view-access.sql)
and assign `PURCHASE_ORDER_BI_READER` to the approved users or Power BI service
identity. Connect to `PURCHASE_ORDER_DB.BIZ` using that role.

| View | Grain |
|---|---|
| `VW_PURCHASE_ORDERS` | One current order with supplier and header totals |
| `VW_PURCHASE_ORDER_LINES` | One order line with supplier, quantities, and line totals |
| `VW_GOODS_RECEIPTS` | One receipt, including negative reversals |
| `VW_CODING_ALLOCATIONS` | One coding split per order line |

Keep currency in totals and join tables at the grain of the report. Joining
receipts to coding splits by line can multiply both measures.
