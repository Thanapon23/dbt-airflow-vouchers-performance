select 
cart_id
,voucher_code
,safe_cast(voucher_use_date as date) as voucher_use_date
from {{ source('raw_vouchers_source', 'voucher') }}


