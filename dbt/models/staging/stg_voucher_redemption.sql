select 
safe_cast(payment_date as date) as payment_date
,cart_id
,customer_number
,customer_id
,voucher_id
,point_cost
,safe_cast(date_create as date) as date_create
from {{ source('raw_vouchers_source', 'voucher_redemption') }}
