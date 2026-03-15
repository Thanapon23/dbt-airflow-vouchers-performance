SELECT 
transaction_id
,sales_before_discount
,product_cogs
FROM {{ source('raw_vouchers_source', 'bucket_sales') }}


