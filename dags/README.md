# Run the purchase-order DAGs on Astro

The three DAGs have no schedule and allow one active run per DAG. Trigger
`dag_api_to_kafka` with `{"load_type":"initial"}`, then trigger it with
`{"load_type":"incremental"}`. Each publication records Kafka's acknowledged
partition and offset for every event. The publisher passes that manifest,
including the batch ID, load type and expected count, to two parallel trigger
tasks. Publisher success means Kafka acknowledged the batch and both child runs
were triggered or already existed. Check each child run for its delivery result.

## Set up the deployment

1. In GitHub's `test` environment, set the variable `ASTRO_DEPLOYMENT_ID` and
   secret `ASTRO_API_TOKEN`. Enable DAG-only deploys on that Astro deployment.
2. Deploy the updated Target API image before running the DAGs. On existing
   MySQL databases with a restricted runtime account, run
   `python -m api.target.init_db` with a setup account. The Target API runtime
   account needs `SELECT`, `INSERT`, and `UPDATE` on both purchase-order tables.
3. If the Astro runtime does not already have Kafka and Snowflake dependencies,
   manually run **Astronomer CI - Deploy DAGs** with **deploy_runtime** enabled.
   The workflow stages `dags/` into the [Airflow build folder](../airflow/)
   and builds Astro Runtime 3.1 using `airflow/requirements.txt`. If your Astro
   deployment already runs a newer Airflow 3 version, use the matching base tag.
   Subsequent pushes to `main` that
   modify `dags/` deploy the DAG bundle only.
4. Set the runtime values below in Astro. Enable all three DAGs before
   triggering the publisher. Use the same topic and group IDs when provisioning
   Kafka ACLs. The producer needs topic Write. Both consumers need topic Read
   and Read on their own group ID.

| Astro runtime setting | Value |
|---|---|
| `SOURCE_API_URL`, `TARGET_API_URL` | HTTPS base URLs of the Coolify APIs |
| `SOURCE_API_USERNAME`, `SOURCE_API_PASSWORD` | Source Basic Auth credentials |
| `TARGET_API_USERNAME`, `TARGET_API_PASSWORD` | Target Basic Auth credentials |
| `KAFKA_BOOTSTRAP_SERVERS` | Aiven broker address |
| `KAFKA_PRODUCER_SASL_PASSWORD` | Password of the configured producer user |
| `KAFKA_BACKEND_SASL_PASSWORD` | Password of the configured backend user |
| `KAFKA_ANALYTICS_SASL_PASSWORD` | Password of the configured analytics user |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM` | Default to `SASL_SSL` and `SCRAM-SHA-256` |
| `KAFKA_CA_PEM` or `KAFKA_SSL_CAFILE` | Aiven CA certificate contents or a CA file path when the broker requires a custom CA |
| `KAFKA_BATCH_TIMEOUT_SECONDS` | Defaults to 300 seconds per delivery attempt |
| `SNOWFLAKE_CONNECTION_ID` | Airflow Snowflake connection ID, defaults to `snowflake_default` |
| `SNOWFLAKE_RAW_SCHEMA`, `SNOWFLAKE_RELATIONAL_SCHEMA` | Existing fully qualified `DATABASE.SCHEMA` names |
| `SNOWFLAKE_DBT_PROJECT` | Existing Snowflake-managed `DATABASE.SCHEMA.PROJECT` object |

Keep passwords and private keys in Astro secrets or the Airflow connection
configured for the Snowflake account, warehouse and role. Give that role schema
usage, table creation and the relevant read/write permissions, plus the
privileges to execute the existing dbt project. Configure that project to read
the relational schema named above. The DAG does not install or replace its dbt
models.

The default topic, usernames, and group IDs come from the bundled
[application.properties](po_pipeline/application.properties). The runtime resolves
`${ENV_VAR}` references when it needs a setting. An API task can run without
provisioning secrets, and the Snowflake account credentials stay in the Airflow
connection. Existing
`KAFKA_TOPIC`, `KAFKA_BACKEND_GROUP_ID`, and `KAFKA_ANALYTICS_GROUP_ID` settings
override those names. A role-specific SASL username can also be overridden in
Astro. If no dedicated credentials are set for a role, the DAG accepts the
shared `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` settings for compatibility.

## Delivery and recovery

Each publisher run has a unique batch ID. Repeating a publish task for the same
Airflow run and unchanged source snapshot generates the same event IDs. An
event contains its order key, normalized `LastUpdated`, source metadata and the
complete nested order. Use a new publisher run for a new extraction.

Both consumers request only the Kafka positions in the manifest. If an
unrelated record appears between requested offsets, the consumer records it
as outside the manifest and holds the group commit at the gap. Kafka commits
can advance only over a contiguous sequence of positions durably delivered by
that consumer. They may therefore lag when mixing manual Kafka producers with
the Airflow publisher or when a failed publish leaves an unclaimed offset.
Use the Airflow manifests to determine batch completeness. Keep Kafka retention
longer than the recovery window and reserve the two groups for these DAGs.

The backend consumer sends the event ID as `Idempotency-Key`. In one MySQL
transaction the Target API records the ID and either inserts the order,
updates it, or retains an equal or newer snapshot. This also makes a repeated
request safe if the first response was lost after commit. HTTP timeouts, 429,
and 5xx are retried by Airflow. Invalid events and permanent HTTP rejections
fail the task. This implementation keeps failed records in Kafka for repair
and replay and does not advance the batch's offsets on partial failure.
After resolving a failure, clear the affected child task to retry its
original manifest within retention. The provisioning workflow prepares the two
dead-letter topics and write ACLs. Failure routing to those topics is deferred.

## Snowflake and dbt ownership

The analytics consumer merges complete event envelopes into
`<raw_schema>.PURCHASE_ORDER_EVENTS` by event ID. The `PAYLOAD` VARIANT column
preserves the original envelope and nested order. A separate task checks that
RAW contains the exact manifest before reconciliation.

Snowflake SQL in the DAG owns the four relational tables. It compares source
timestamps in UTC and uses a canonical snapshot hash to break ties. It
replaces the winning order and all its child rows in a single transaction, so
removed child rows disappear and older snapshots cannot overwrite newer ones.
The bundled fixtures yield 104 orders, 224 lines, 286 coding rows and 189 goods
receipts after both loads.

The existing Snowflake-managed dbt project owns staging and the dimensional
models. Airflow runs `EXECUTE DBT PROJECT … ARGS = 'build'` only after RAW
and relational tasks succeed. It checks Snowflake's returned success flag and
logs the query ID. A failed build fails the analytics DAG.
