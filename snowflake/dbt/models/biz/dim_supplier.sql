select SUPPLIER_CODE, SUPPLIER_NAME
from {{ ref('clean_purchase_order') }}
where SUPPLIER_CODE is not null
qualify row_number() over (
    partition by SUPPLIER_CODE
    order by LAST_UPDATED desc nulls last, ORDER_NUMBER desc
) = 1
