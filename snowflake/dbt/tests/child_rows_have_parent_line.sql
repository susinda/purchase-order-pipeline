select 'CODING_ROW' as CHILD_TYPE, c.ORDER_NUMBER, c.LINE_NUMBER
from {{ ref('clean_purchase_order_line_coding_row') }} c
left join {{ ref('clean_purchase_order_line') }} l
    on l.ORDER_NUMBER = c.ORDER_NUMBER and l.LINE_NUMBER = c.LINE_NUMBER
where l.ORDER_NUMBER is null

union all

select 'GOODS_RECEIPT' as CHILD_TYPE, r.ORDER_NUMBER, r.LINE_NUMBER
from {{ ref('clean_purchase_order_line_goods_receipt') }} r
left join {{ ref('clean_purchase_order_line') }} l
    on l.ORDER_NUMBER = r.ORDER_NUMBER and l.LINE_NUMBER = r.LINE_NUMBER
where l.ORDER_NUMBER is null
