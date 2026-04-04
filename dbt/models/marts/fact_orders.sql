{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

reviews as (
    select
        order_id,
        rating,
        sentiment_label,
        annotation_confidence,
        primary_topic
    from {{ ref('stg_reviews') }}
),

exchange_rate as (
    select brl_per_usd, usd_per_brl
    from {{ ref('stg_exchange_rates') }}
    limit 1
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

-- Join orders with their line items
-- Each row in fact_orders = one line item in one order
orders_with_items as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_key,
        oi.seller_id,
        o.customer_key,
        o.status_code,
        o.order_date,
        o.delivered_date,
        o.days_to_deliver,
        o.delivered_on_time,
        oi.item_price_brl,
        oi.freight_cost_brl,
        oi.total_item_cost_brl
    from order_items oi
    inner join orders o using (order_id)
),

-- Add review sentiment (one review per order — take first if multiple)
with_reviews as (
    select
        owi.*,
        r.rating            as review_rating,
        r.sentiment_label,
        r.primary_topic     as review_topic
    from orders_with_items owi
    left join reviews r using (order_id)
),

-- Add currency conversion to USD
with_usd as (
    select
        wr.*,
        er.brl_per_usd,

        -- Convert BRL amounts to USD
        round((wr.item_price_brl      / er.brl_per_usd)::numeric, 2)
            as item_price_usd,
        round((wr.freight_cost_brl    / er.brl_per_usd)::numeric, 2)
            as freight_cost_usd,
        round((wr.total_item_cost_brl / er.brl_per_usd)::numeric, 2)
            as total_amount_usd

    from with_reviews wr
    cross join exchange_rate er
),

-- Add date key for joining to dim_date
final as (
    select
        -- Surrogate key — unique identifier for this fact row
        row_number() over (
            order by order_id, order_item_id
        )                                           as fact_id,

        -- Foreign keys to dimensions
        order_id,
        customer_key,
        product_key,
        to_char(order_date::date, 'YYYYMMDD')::integer as date_key,

        -- Order details
        order_item_id,
        status_code,
        order_date,
        delivered_date,
        days_to_deliver,
        delivered_on_time,

        -- Financial measures in BRL
        item_price_brl,
        freight_cost_brl,
        total_item_cost_brl                         as total_amount_brl,

        -- Financial measures in USD
        item_price_usd,
        freight_cost_usd,
        total_amount_usd,

        -- Exchange rate used (for auditability)
        brl_per_usd                                 as exchange_rate_used,

        -- Review and sentiment
        review_rating,
        sentiment_label,
        review_topic

    from with_usd
)

select * from final