-- Preserve negative receipts for reversals. Do not discard them.
-- Comment-only change to include the dbt path in workflow change detection.
select
    g.GOODS_RECEIPT_NUMBER, g.ORDER_NUMBER, g.LINE_NUMBER,
    o.SUPPLIER_CODE, g.REVERSED_GOODS_RECEIPT_NUMBER,
    g.RECEIVING_DATE, g.RECEIVED_QUANTITY, g.RECEIVED_NET_SUM
from {{ ref('clean_purchase_order_line_goods_receipt') }} g
join {{ ref('clean_purchase_order') }} o using (ORDER_NUMBER)
