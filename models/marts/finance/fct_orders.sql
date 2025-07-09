with
    orders as (
        select * from {{ ref ('stg_jaffle_shop__orders' ) }}
    )

    , payments as (
        select * from {{ ref ('stg_stripe__payments') }}
    )

    , order_payments as (
        select
            order_id
            , sum(case when payment_status = 'success' then payment_amount end) as payment_amount
        from payments
        group by order_id
    )

    , final as (

        select
            orders.order_id
            , orders.customer_id
            , orders.order_placed_at
            , coalesce(order_payments.payment_amount, 0) as payment_amount
        from orders
        left join order_payments on orders.order_id = order_payments.order_id
    )

select * from final
