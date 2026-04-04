with source as (
    select * from {{ source('silver', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_key,
        status_code,
        order_date,
        approved_date,
        shipped_date,
        delivered_date,
        estimated_delivery_date,
        days_to_deliver,
        delivered_on_time
    from source
    where order_id is not null
      and customer_key is not null
      and order_date is not null
)

select * from renamed