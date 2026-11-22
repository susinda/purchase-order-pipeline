-- Complete order snapshots: select the latest update per ORDER_NUMBER.
-- Re-running is safe, and late-arriving older snapshots don't replace newer ones.
select
    EVENT_ID, BATCH_ID, ORDER_NUMBER, LOAD_TYPE, PAYLOAD, INGESTED_AT,
    try_to_timestamp_tz(PAYLOAD:LastUpdated::varchar) as LAST_UPDATED
from {{ source('raw', 'purchase_order_events') }}
where is_object(PAYLOAD)
  and ORDER_NUMBER is not null
  and PAYLOAD:OrderNumber::varchar = ORDER_NUMBER
qualify row_number() over (
    partition by ORDER_NUMBER
    order by LAST_UPDATED desc nulls last, INGESTED_AT desc nulls last, EVENT_ID desc
) = 1
