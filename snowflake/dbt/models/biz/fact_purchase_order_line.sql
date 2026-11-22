-- Grain: one row per current OrderNumber + LineNumber.
select
    l.LINE_KEY, l.ORDER_NUMBER, l.LINE_NUMBER,
    o.SUPPLIER_CODE, o.PURCHASE_CURRENCY,
    l.LINE_STATUS, l.CATEGORY_CODE, try_to_decimal(l.QUANTITY, 18, 3) as QUANTITY, l.QUANTITY_UNIT,
    l.NET_SUM, l.TAX_TOTAL, l.GROSS_SUM, l.DESIRED_DELIVERY_DATE
from {{ ref('clean_purchase_order_line') }} l
join {{ ref('clean_purchase_order') }} o using (ORDER_NUMBER)
