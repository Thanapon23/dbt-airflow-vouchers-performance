select 
customer_history_id
,customer_id
,account_status
,consent
,safe_cast(consent_date as date) as consent_date
from {{ source('raw_vouchers_source', 'customer') }}


