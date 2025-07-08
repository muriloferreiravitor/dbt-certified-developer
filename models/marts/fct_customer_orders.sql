with
    p as (
        select
            orderid as order_id
            , max(created) as payment_finalized_date
            , sum(amount) / 100.0 as total_amount_paid
        from dbt_mfvitor__raw__stripe.payment
        where status <> 'fail'
        group by order_id
    )

    , paid_orders as (
        select
            orders.id as order_id
            , orders.user_id as customer_id
            , orders.order_date as order_placed_at
            , orders.status as order_status
            , p.total_amount_paid
            , p.payment_finalized_date
            , c.first_name as customer_first_name
            , c.last_name as customer_last_name
        from dbt_mfvitor__raw__jaffle_shop.orders as orders
        left join p on orders.id = p.order_id
        left join dbt_mfvitor__raw__jaffle_shop.customers as c on orders.user_id = c.id
    )

    , customer_orders as (
        select
            c.id as customer_id
            , min(orders.order_date) as first_order_date
            , max(orders.order_date) as most_recent_order_date
            , count(orders.id) as number_of_orders
        from dbt_mfvitor__raw__jaffle_shop.customers as c
        left join dbt_mfvitor__raw__jaffle_shop.orders as orders
            on c.id = orders.user_id
        group by customer_id
    )

select
    p.*
    , row_number() over (order by p.order_id) as transaction_seq
    , row_number() over (partition by p.customer_id order by p.order_id) as customer_sales_seq
    , case
        when c.first_order_date = p.order_placed_at
            then 'new'
        else 'return'
    end as nvsr
    , x.clv_bad as customer_lifetime_value
    , c.first_order_date as fdos
from paid_orders as p
left join customer_orders as c on p.customer_id = c.customer_id
left outer join
    (
        select
            p.order_id
            , sum(t2.total_amount_paid) as clv_bad
        from paid_orders as p
        left join paid_orders as t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by p.order_id
        order by p.order_id
    ) as x on p.order_id = x.order_id
order by p.order_id
