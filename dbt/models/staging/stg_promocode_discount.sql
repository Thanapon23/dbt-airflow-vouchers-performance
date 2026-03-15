select 
store_code
,ref_no
,promotion_code_discount
from {{ source('raw_vouchers_source', 'promocode_discount') }}



