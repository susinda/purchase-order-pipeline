CREATE TABLE IF NOT EXISTS <% raw_table %> (
    EVENT_ID VARCHAR NOT NULL PRIMARY KEY,
    BATCH_ID VARCHAR NOT NULL,
    ORDER_NUMBER VARCHAR NOT NULL,
    LOAD_TYPE VARCHAR,
    PAYLOAD VARIANT NOT NULL,
    INGESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

EXECUTE IMMEDIATE $$
DECLARE
    compatible_columns INTEGER;
    required_extra_columns INTEGER;
    ingested_at_columns INTEGER;
    incompatible_ingested_at INTEGER;
    incompatible_table EXCEPTION (-20001,
        'The RAW table does not match the ingestion schema. Use a compatible SNOWFLAKE_RAW_TABLE or migrate the existing table.');
BEGIN
    SELECT
        COUNT_IF((COLUMN_NAME = 'PAYLOAD' AND DATA_TYPE = 'VARIANT') OR
            (COLUMN_NAME IN ('EVENT_ID', 'BATCH_ID', 'ORDER_NUMBER', 'LOAD_TYPE')
                AND DATA_TYPE IN ('TEXT', 'VARCHAR')
                AND (COLUMN_NAME <> 'LOAD_TYPE' OR IS_NULLABLE = 'YES'))),
        COUNT_IF(COLUMN_NAME NOT IN ('EVENT_ID', 'BATCH_ID', 'ORDER_NUMBER', 'LOAD_TYPE', 'PAYLOAD')
            AND COLUMN_NAME <> 'INGESTED_AT' AND IS_NULLABLE = 'NO'
            AND COLUMN_DEFAULT IS NULL AND IS_IDENTITY = 'NO'),
        COUNT_IF(COLUMN_NAME = 'INGESTED_AT'),
        COUNT_IF(COLUMN_NAME = 'INGESTED_AT' AND DATA_TYPE <> 'TIMESTAMP_LTZ')
    INTO :compatible_columns, :required_extra_columns, :ingested_at_columns, :incompatible_ingested_at
    FROM <% database %>.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER(SPLIT_PART('<% raw_table %>', '.', 2))
        AND TABLE_NAME = UPPER(SPLIT_PART('<% raw_table %>', '.', 3));

    IF (COALESCE(compatible_columns, 0) <> 5 OR COALESCE(required_extra_columns, 0) > 0
        OR COALESCE(incompatible_ingested_at, 0) > 0) THEN
        RAISE incompatible_table;
    END IF;
    IF (COALESCE(ingested_at_columns, 0) = 0) THEN
        ALTER TABLE <% raw_table %> ADD COLUMN IF NOT EXISTS INGESTED_AT TIMESTAMP_LTZ;
    END IF;
    RETURN 'Purchase-order RAW table is ready';
END;
$$;
