{{ config(alias='PURCHASE_ORDER') }}

select
    ORDER_NUMBER,
    PAYLOAD:OrderType::varchar as ORDER_TYPE,
    PAYLOAD:OrderStatus::varchar as ORDER_STATUS,
    try_to_timestamp_tz(PAYLOAD:CreationTime::varchar) as CREATION_TIME,
    LAST_UPDATED,
    PAYLOAD:SupplierCode::varchar as SUPPLIER_CODE,
    PAYLOAD:SupplierName::varchar as SUPPLIER_NAME,
    PAYLOAD:Creator::varchar as CREATOR,
    PAYLOAD:PurchaseCurrency::varchar as PURCHASE_CURRENCY,
    try_to_decimal(PAYLOAD:NetSum::varchar, 18, 2) as NET_SUM,
    try_to_decimal(PAYLOAD:TaxSum::varchar, 18, 2) as TAX_SUM,
    try_to_decimal(PAYLOAD:GrossSum::varchar, 18, 2) as GROSS_SUM,
    EVENT_ID as SOURCE_EVENT_ID
from {{ ref('stg_latest_order_event') }}
