with vh as (
    select * from {{ ref('stg_voucher_history') }}
),

-- prepare system promotion_discount
vs_discount as (
    select 
        store_code, 
        ref_no, 
        sum(promotion_code_discount) as total_promo_discount 
    from {{ ref('stg_promocode_discount') }}
    group by store_code, ref_no
),

-- prepare sale from voucher discount
vs as (
    select 
        vs.*,
        coalesce(pd.total_promo_discount, 0) as promotion_code_discount
    from {{ ref('stg_voucher_sales') }} vs
    left join vs_discount pd
        on pd.store_code = vs.store_code 
        and pd.ref_no = vs.ref_no
),

--  prepare bucket sale data 
bs as (
    select 
        transaction_id,
        sum(sales_before_discount) as bucket_sales_before_discount,
        sum(product_cogs) as bucket_product_cogs
    from {{ ref('stg_bucket_sales') }}
    group by transaction_id
),

-- prepare latest profile
c as (
    select * except(row_num)
    from (
        select 
            *,
            row_number() over (partition by customer_id order by customer_history_id desc) as row_num
        from {{ ref('stg_customer') }}
    )
    where row_num = 1
)

SELECT 
    -- Date Formatting for BigQuery
    format_date('%Y-%m', vr.payment_date)                             as redeem_month
    ,format_date('%Y-%m-%d', vr.payment_date)                         as redeem_date
    ,format_timestamp('%H:%M:%S', cast(vr.payment_date as timestamp)) as redeem_time
    
    ,vc.voucher_use_date
    ,vr.cart_id                 as redeem_order_id
    ,vr.customer_number         as redeem_customer_number
    ,vr.customer_id             as redeem_accountid
    ,format_date('%Y-%m-%d', c.consent_date) as consent_date
    ,vr.voucher_id              as redeem_voucher_id
    ,vh.voucher_name            as redeem_product_name
    ,vh.promotion_name_en       as redeem_promotion_name
    ,vc.voucher_code            as redeem_voucher_code
    ,vr.point_cost              as redeem_point
    
    ,case
        when vs.transaction_id is not null then 'voucher_used'
        else 'voucher_unused'
    end as voucher_status
    
    ,trim(vs.transaction_id)    as used_transaction_id
    ,format_date('%Y-%m', vs.transaction_date)      as used_month
    ,format_date('%Y-%m-%d', vs.transaction_date)   as used_date
    ,vs.store_code              as used_store_code
    ,vb.store_type              as used_store_type
    ,vb.brand                   as used_brand
    ,vb.store_name              as used_store_name
    ,vs.customer_id             as used_customer_id
    
    ,coalesce(bs.bucket_sales_before_discount, 0) as sales_before_discount

    ,round(safe_divide(     
        (coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0))
        ,sum(coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0)) over(partition by vs.transaction_id)
    ) * coalesce(bs.bucket_sales_before_discount, 0), 2) as allocated_sales_before_discount

    ,coalesce(vs.sales_after_discount, 0) as sales_after_discount

    ,round(safe_divide(
        (coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0))
        ,sum(coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0)) over(partition by vs.transaction_id)
    ) * coalesce(vs.sales_after_discount, 0), 2) as allocated_sales_after_discount

    ,coalesce(vs.qty, 0) as qty
    ,coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0) as total_discount
    ,coalesce(vs.voucher_discount, 0) as voucher_discount
    ,coalesce(vs.promotion_code_discount, 0) as promotion_code_discount
    ,round(coalesce(bs.bucket_product_cogs, 0), 2) as bucket_product_cogs
    ,round(
        safe_divide(
            (coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0)),
            sum(coalesce(vs.voucher_discount, 0) + coalesce(vs.promotion_code_discount, 0)) over(partition by vs.transaction_id)
        ) * coalesce(bs.bucket_product_cogs, 0), 2
    ) as allocated_product_cogs

from {{ ref('stg_voucher_redemption') }} vr
left join vh on vh.voucher_id = vr.voucher_id
left join {{ ref('stg_voucher') }} vc on vc.cart_id = vr.cart_id
left join vs on vs.voucher_code = vc.voucher_code
left join {{ ref('stg_branch') }} vb on vb.store_code = vs.store_code
left join c on c.customer_id = vr.customer_id
left join bs on trim(bs.transaction_id) = trim(vs.transaction_id)
 
order by 2 asc




