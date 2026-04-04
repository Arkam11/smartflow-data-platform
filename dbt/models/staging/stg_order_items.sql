with source as (
    select * from {{ source('silver', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_key,
        seller_id,
        shipping_deadline,
        item_price_brl::decimal(10,2)       as item_price_brl,
        freight_cost_brl::decimal(10,2)     as freight_cost_brl,
        total_item_cost_brl::decimal(10,2)  as total_item_cost_brl
    from source
    where order_id is not null
      and product_key is not null
      and item_price_brl >= 0
)

select * from renamed