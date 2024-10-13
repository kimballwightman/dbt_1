{{
    config(
        materialized='table'
    )
}}

with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

)

,orders as (

    select * from {{ ref('fct_orders') }}
)

,customer_orders as (

    select
        customer_id

        ,min(order_date) as first_order_date
        ,max(order_date) as most_recent_order_date
        ,count(order_id) as number_of_orders
        ,sum(amount) as lifetime_value

    from orders

    group by 1

)

,final as (

    select
        t1.customer_id
        ,t1.first_name
        ,t1.last_name
        ,t2.first_order_date
        ,t2.most_recent_order_date
        ,coalesce(t2.number_of_orders, 0) as number_of_orders
        ,t2.lifetime_value


    from customers t1

    left join customer_orders t2 using (customer_id)

)

select * from final
