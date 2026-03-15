select 
transaction_id
,customer_id
,safe_cast(transaction_date as date) as transaction_date 
,store_code
,ref_no
,sales_after_discount
,qty
,voucher_code
,voucher_discount
from {{ source('raw_vouchers_source', 'voucher_sales') }}




