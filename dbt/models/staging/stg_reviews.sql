with source as (
    select * from {{ source('silver', 'reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        rating,
        review_text,
        review_date,
        sentiment_label,
        annotation_confidence,
        primary_topic
    from source
    where review_id is not null
      and order_id is not null
)

select * from renamed