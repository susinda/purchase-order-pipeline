{{ config(alias='PURCHASE_ORDER_LINE_GOODS_RECEIPT') }}

select
    g.value:GoodsReceiptNumber::varchar as GOODS_RECEIPT_NUMBER,
    s.ORDER_NUMBER,
    l.value:LineNumber::varchar as LINE_NUMBER,
    g.value:ReversedGoodsReceiptNumber::varchar as REVERSED_GOODS_RECEIPT_NUMBER,
    try_to_date(g.value:ReceivingDate::varchar) as RECEIVING_DATE,
    try_to_decimal(g.value:ReceivedQuantity::varchar, 18, 3) as RECEIVED_QUANTITY,
    try_to_decimal(g.value:ReceivedNetSum::varchar, 18, 2) as RECEIVED_NET_SUM,
    g.value:Recipient::varchar as RECIPIENT
from {{ ref('stg_latest_order_event') }} s,
lateral flatten(input => s.PAYLOAD:OrderLines) l,
lateral flatten(input => l.value:GoodsReceipts) g
qualify row_number() over (
    partition by g.value:GoodsReceiptNumber::varchar
    order by s.LAST_UPDATED desc nulls last, s.INGESTED_AT desc nulls last,
        s.ORDER_NUMBER, l.index desc, g.index desc
) = 1
