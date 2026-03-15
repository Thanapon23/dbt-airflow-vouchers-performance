select 
voucher_id
,voucher_name
,promotion_name_en
from {{ source('raw_vouchers_source', 'voucher_history') }}



