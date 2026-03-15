select 
store_code
,brand
,store_name
,store_type 
from {{ source('raw_vouchers_source', 'branch') }}
