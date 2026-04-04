{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        product_key,
        category_name,
        weight_grams,
        length_cm,
        height_cm,
        width_cm,

        -- Derived price tier based on weight as a proxy
        -- (we don't have a price column in the product table itself)
        case
            when weight_grams <= 500  then 'light'
            when weight_grams <= 5000 then 'medium'
            else 'heavy'
        end as weight_category

    from products
)

select * from final