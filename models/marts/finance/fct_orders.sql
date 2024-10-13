{{
    config(
        materialized='table'
    )
}}

with orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
)

,payments as (
    select * from {{ ref ('stg_stripe__payments') }}
)

,order_payments as (
    select
        order_id
        ,sum (case when status = 'success' then amount end) as amount

    from payments
    group by 1
)

,final as (

    select
        t1.order_id
        ,t1.customer_id
        ,t1.order_date
        ,coalesce (t2.amount, 0) as amount

    from orders t1
    left join order_payments t2 using (order_id)
)

select * from final