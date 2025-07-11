with
    source as (
        select * from {{ source('stripe', 'payment') }}
    )

    , transformed as (
        select
            id as payment_id
            , orderid as order_id
            , paymentmethod as payment_method
            , created as payment_created_at
            , status as payment_status
            , round(amount / 100.0, 2) as payment_amount -- amount is stored in cents, convert it to dollars
        from source
    )

select * from transformed
