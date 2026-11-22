{{ config(alias='PURCHASE_ORDER_LINE') }}

select
    s.ORDER_NUMBER,
    l.value:LineNumber::varchar as LINE_NUMBER,
    s.ORDER_NUMBER || '|' || l.value:LineNumber::varchar as LINE_KEY,
    l.value:LineStatus::varchar as LINE_STATUS,
    l.value:Description::varchar as DESCRIPTION,
    l.value:CategoryCode::varchar as CATEGORY_CODE,
    l.value:CategoryName::varchar as CATEGORY_NAME,
    l.value:Quantity::varchar as QUANTITY,
    l.value:QuantityUnit::varchar as QUANTITY_UNIT,
    try_to_decimal(l.value:NetUnitPrice::varchar, 18, 2) as NET_UNIT_PRICE,
    try_to_decimal(l.value:NetSum::varchar, 18, 2) as NET_SUM,
    l.value:TaxCode::varchar as TAX_CODE,
    try_to_decimal(l.value:TaxPercent::varchar, 18, 3) as TAX_PERCENT,
    try_to_decimal(l.value:TaxTotal::varchar, 18, 2) as TAX_TOTAL,
    try_to_decimal(l.value:GrossSum::varchar, 18, 2) as GROSS_SUM,
    try_to_date(l.value:DesiredDeliveryDate::varchar) as DESIRED_DELIVERY_DATE
from {{ ref('stg_latest_order_event') }} s,
lateral flatten(input => s.PAYLOAD:OrderLines) l
qualify row_number() over (
    partition by s.ORDER_NUMBER, l.value:LineNumber::varchar
    order by l.index desc
) = 1
