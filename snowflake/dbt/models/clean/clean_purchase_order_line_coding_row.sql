{{ config(alias='PURCHASE_ORDER_LINE_CODING_ROW') }}

select
    s.ORDER_NUMBER,
    l.value:LineNumber::varchar as LINE_NUMBER,
    c.value:RowIndex::varchar as ROW_INDEX,
    s.ORDER_NUMBER || '|' || l.value:LineNumber::varchar || '|' || c.value:RowIndex::varchar as CODING_KEY,
    try_to_decimal(c.value:SplitPercent::varchar, 18, 3) as SPLIT_PERCENT,
    c.value:AccountCode::varchar as ACCOUNT_CODE,
    c.value:AccountName::varchar as ACCOUNT_NAME,
    c.value:CostCenterCode::varchar as COST_CENTER_CODE,
    c.value:CostCenterName::varchar as COST_CENTER_NAME,
    c.value:ProjectCode::varchar as PROJECT_CODE
from {{ ref('stg_latest_order_event') }} s,
lateral flatten(input => s.PAYLOAD:OrderLines) l,
lateral flatten(input => l.value:CodingRows) c
qualify row_number() over (
    partition by s.ORDER_NUMBER, l.value:LineNumber::varchar, c.value:RowIndex::varchar
    order by l.index desc, c.index desc
) = 1
