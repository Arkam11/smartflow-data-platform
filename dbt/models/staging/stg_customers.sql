with source as (
    select * from {{ source('silver', 'customers') }}
),

renamed as (
    select
        customer_key,
        customer_id,
        city,
        state_code,
        zip_prefix
    from source
    where customer_key is not null
)

select * from renamed