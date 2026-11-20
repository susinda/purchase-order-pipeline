from contextlib import closing
import logging
import re

from .events import PermanentBatchError, json_bytes, validate_manifest
from .kafka_batch import consume_batch, destination_groups
from .settings import application_setting


logger = logging.getLogger(__name__)
NUMBER = "NUMBER(38,9)"
TEXT = "VARCHAR"

# Column names follow the four dbt source grains. Cast JSON nulls to SQL NULL.
HEADER = (
    ("ORDER_TYPE", "OrderType", TEXT), ("ORDER_STATUS", "OrderStatus", TEXT),
    ("CREATION_TIME", "CreationTime", "TIMESTAMP_TZ"), ("SUPPLIER_CODE", "SupplierCode", TEXT),
    ("SUPPLIER_NAME", "SupplierName", TEXT), ("CREATOR", "Creator", TEXT),
    ("PURCHASE_CURRENCY", "PurchaseCurrency", TEXT), ("NET_SUM", "NetSum", NUMBER),
    ("TAX_SUM", "TaxSum", NUMBER), ("GROSS_SUM", "GrossSum", NUMBER),
)
LINES = (
    ("LINE_NUMBER", "LineNumber", TEXT), ("LINE_STATUS", "LineStatus", TEXT),
    ("DESCRIPTION", "Description", TEXT), ("CATEGORY_CODE", "CategoryCode", TEXT),
    ("CATEGORY_NAME", "CategoryName", TEXT), ("QUANTITY", "Quantity", NUMBER),
    ("QUANTITY_UNIT", "QuantityUnit", TEXT), ("NET_UNIT_PRICE", "NetUnitPrice", NUMBER),
    ("NET_SUM", "NetSum", NUMBER), ("TAX_CODE", "TaxCode", TEXT), ("TAX_PERCENT", "TaxPercent", NUMBER),
    ("TAX_TOTAL", "TaxTotal", NUMBER), ("GROSS_SUM", "GrossSum", NUMBER),
    ("DESIRED_DELIVERY_DATE", "DesiredDeliveryDate", "DATE"),
)
CODING = (
    ("ROW_INDEX", "RowIndex", TEXT), ("SPLIT_PERCENT", "SplitPercent", NUMBER),
    ("ACCOUNT_CODE", "AccountCode", TEXT), ("ACCOUNT_NAME", "AccountName", TEXT),
    ("COST_CENTER_CODE", "CostCenterCode", TEXT), ("COST_CENTER_NAME", "CostCenterName", TEXT),
    ("PROJECT_CODE", "ProjectCode", TEXT),
)
RECEIPTS = (
    ("GOODS_RECEIPT_NUMBER", "GoodsReceiptNumber", TEXT),
    ("REVERSED_GOODS_RECEIPT_NUMBER", "ReversedGoodsReceiptNumber", TEXT),
    ("RECEIVING_DATE", "ReceivingDate", "DATE"), ("RECEIVED_QUANTITY", "ReceivedQuantity", NUMBER),
    ("RECEIVED_NET_SUM", "ReceivedNetSum", NUMBER), ("RECIPIENT", "Recipient", TEXT),
)


def qualified_setting(name, components):
    value = application_setting(name)
    parts = value.split(".")
    if len(parts) != components or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", part) for part in parts):
        raise PermanentBatchError(f"{name} requires {components} dot-separated, unquoted Snowflake identifiers")
    return value


def connect():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(
        snowflake_conn_id=application_setting("snowflake.connection.id", override="SNOWFLAKE_CONNECTION_ID"),
    ).get_conn()


def ingest_raw_batch(conf):
    validate_manifest(conf)
    raw_table = f"{qualified_setting('snowflake.raw.schema', 2)}.PURCHASE_ORDER_EVENTS"
    _, group = destination_groups()
    with closing(connect()) as connection, closing(connection.cursor()) as cursor:
        connection.autocommit(True)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {raw_table} (
                EVENT_ID VARCHAR NOT NULL PRIMARY KEY, BATCH_ID VARCHAR NOT NULL,
                ORDER_NUMBER VARCHAR NOT NULL, LAST_UPDATED TIMESTAMP_TZ NOT NULL,
                SNAPSHOT_HASH VARCHAR NOT NULL, PAYLOAD VARIANT NOT NULL,
                INGESTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        def ingest(event):
            cursor.execute(f"""
                MERGE INTO {raw_table} AS target
                USING (
                    SELECT %s AS EVENT_ID, %s AS BATCH_ID, %s AS ORDER_NUMBER,
                           TO_TIMESTAMP_TZ(%s, 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') AS LAST_UPDATED,
                           %s AS SNAPSHOT_HASH, PARSE_JSON(%s) AS PAYLOAD
                ) AS source ON target.EVENT_ID = source.EVENT_ID
                WHEN NOT MATCHED THEN INSERT (EVENT_ID, BATCH_ID, ORDER_NUMBER, LAST_UPDATED, SNAPSHOT_HASH, PAYLOAD)
                VALUES (source.EVENT_ID, source.BATCH_ID, source.ORDER_NUMBER, source.LAST_UPDATED,
                        source.SNAPSHOT_HASH, source.PAYLOAD)
            """, (
                event["event_id"], event["batch_id"], event["order_number"], event["last_updated"],
                event["snapshot_hash"], json_bytes(event).decode("utf-8"),
            ), timeout=30)
            return "ingested" if cursor.rowcount else "duplicate"

        return consume_batch(conf, group, ingest, role="analytics")


def projected_fields(fields, source):
    return [f"""NULLIF({source}:"{key}", PARSE_JSON('null'))::{data_type}""" for _, key, data_type in fields]


def create_table(cursor, table, fixed_columns, fields, primary_key):
    columns = [*fixed_columns, *(f"{column} {data_type}" for column, _, data_type in fields)]
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}, PRIMARY KEY ({primary_key}))")


def replace_rows(cursor, table, columns, fields, projection, variant, source, winners):
    cursor.execute(f"DELETE FROM {table} WHERE ORDER_NUMBER IN (SELECT ORDER_NUMBER FROM {winners})")
    names = [*columns, *(name for name, _, _ in fields)]
    values = [*projection, *projected_fields(fields, variant)]
    cursor.execute(f"INSERT INTO {table} ({', '.join(names)}) SELECT {', '.join(values)} FROM {source}")


def prepare_relational(conf):
    manifest = validate_manifest(conf)
    raw_table = f"{qualified_setting('snowflake.raw.schema', 2)}.PURCHASE_ORDER_EVENTS"
    schema = qualified_setting("snowflake.relational.schema", 2)
    header = f"{schema}.PURCHASE_ORDER"
    line = f"{schema}.PURCHASE_ORDER_LINE"
    coding = f"{schema}.PURCHASE_ORDER_LINE_CODING_ROW"
    receipt = f"{schema}.PURCHASE_ORDER_LINE_GOODS_RECEIPT"
    winners = f"{schema}.PURCHASE_ORDER_BATCH_WINNERS"
    batch_id = manifest["batch_id"]
    expected = {record["event_id"] for record in manifest["records"]}

    with closing(connect()) as connection, closing(connection.cursor()) as cursor:
        connection.autocommit(True)
        cursor.execute(f"SELECT EVENT_ID FROM {raw_table} WHERE BATCH_ID = %s", (batch_id,))
        rows = cursor.fetchall()
        if len(rows) != len(expected) or {row[0] for row in rows} != expected:
            raise PermanentBatchError(f"Snowflake RAW does not contain the full manifest for batch {batch_id}")

        create_table(cursor, header, (
            "ORDER_NUMBER VARCHAR NOT NULL", "LAST_UPDATED TIMESTAMP_TZ NOT NULL",
            "SNAPSHOT_HASH VARCHAR NOT NULL", "SOURCE_EVENT_ID VARCHAR NOT NULL",
        ), HEADER, "ORDER_NUMBER")
        create_table(cursor, line, ("ORDER_NUMBER VARCHAR NOT NULL",), LINES, "ORDER_NUMBER, LINE_NUMBER")
        parent_keys = ("ORDER_NUMBER VARCHAR NOT NULL", "LINE_NUMBER VARCHAR NOT NULL")
        create_table(cursor, coding, parent_keys, CODING, "ORDER_NUMBER, LINE_NUMBER, ROW_INDEX")
        create_table(cursor, receipt, parent_keys, RECEIPTS, "GOODS_RECEIPT_NUMBER")

        # Run DDL before the transaction because Snowflake DDL implicitly commits.
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {winners} AS
            SELECT raw.ORDER_NUMBER, raw.LAST_UPDATED, raw.SNAPSHOT_HASH, raw.EVENT_ID,
                   raw.PAYLOAD:"order" AS SNAPSHOT
            FROM {raw_table} raw
            LEFT JOIN {header} current_order ON current_order.ORDER_NUMBER = raw.ORDER_NUMBER
            WHERE raw.BATCH_ID = %s AND (
                current_order.ORDER_NUMBER IS NULL OR raw.LAST_UPDATED > current_order.LAST_UPDATED
                OR (raw.LAST_UPDATED = current_order.LAST_UPDATED AND raw.SNAPSHOT_HASH > current_order.SNAPSHOT_HASH)
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY raw.ORDER_NUMBER ORDER BY raw.LAST_UPDATED DESC, raw.SNAPSHOT_HASH DESC, raw.EVENT_ID
            ) = 1
        """, (batch_id,))
        order_rows = f"{winners} w"
        line_rows = f"""{order_rows}, LATERAL FLATTEN(INPUT => w.SNAPSHOT:"OrderLines") l"""
        coding_rows = f"""{line_rows}, LATERAL FLATTEN(INPUT => l.VALUE:"CodingRows") c"""
        receipt_rows = f"""{line_rows}, LATERAL FLATTEN(INPUT => l.VALUE:"GoodsReceipts") r"""
        cursor.execute(f"""
            SELECT GOODS_RECEIPT_NUMBER FROM (
                SELECT GOODS_RECEIPT_NUMBER FROM {receipt}
                WHERE ORDER_NUMBER NOT IN (SELECT ORDER_NUMBER FROM {winners})
                UNION ALL
                SELECT r.VALUE:"GoodsReceiptNumber"::VARCHAR FROM {receipt_rows}
            ) GROUP BY GOODS_RECEIPT_NUMBER HAVING COUNT(*) > 1 LIMIT 1
        """)
        if cursor.fetchone():
            raise PermanentBatchError(f"Conflicting goods receipt identity in batch {batch_id}")

        cursor.execute("BEGIN")
        try:
            replace_rows(cursor, header, ("ORDER_NUMBER", "LAST_UPDATED", "SNAPSHOT_HASH", "SOURCE_EVENT_ID"),
                         HEADER, ("w.ORDER_NUMBER", "w.LAST_UPDATED", "w.SNAPSHOT_HASH", "w.EVENT_ID"),
                         "w.SNAPSHOT", order_rows, winners)
            replace_rows(cursor, line, ("ORDER_NUMBER",), LINES, ("w.ORDER_NUMBER",),
                         "l.VALUE", line_rows, winners)
            child_keys = ("ORDER_NUMBER", "LINE_NUMBER")
            parent_projection = ("w.ORDER_NUMBER", 'l.VALUE:"LineNumber"::VARCHAR')
            replace_rows(cursor, coding, child_keys, CODING, parent_projection,
                         "c.VALUE", coding_rows, winners)
            replace_rows(cursor, receipt, child_keys, RECEIPTS, parent_projection,
                         "r.VALUE", receipt_rows, winners)
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
    logger.info("Reconciled the four relational tables for complete batch %s", batch_id)


def build_dbt_project():
    project = qualified_setting("snowflake.dbt.project", 3)
    with closing(connect()) as connection, closing(connection.cursor()) as cursor:
        cursor.execute(f"EXECUTE DBT PROJECT {project} ARGS = 'build'", timeout=900)
        result = cursor.fetchone()
        if not result or str(result[0]).upper() != "TRUE":
            raise RuntimeError(f"Snowflake-managed dbt build failed, query ID {cursor.sfqid}")
        logger.info("Snowflake-managed dbt build succeeded, query ID %s", cursor.sfqid)
