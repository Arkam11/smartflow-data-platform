with source as (
    select * from {{ source('bronze', 'raw_products') }}
),

cleaned as (
    select
        product_id                                  as product_key,
        coalesce(product_category_name, 'unknown')  as category_name,
        product_weight_g                            as weight_grams,
        product_length_cm                           as length_cm,
        product_height_cm                           as height_cm,
        product_width_cm                            as width_cm
    from source
    where product_id is not null
)

select * from cleaned