{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

churn as (
    select * from {{ ref('stg_churn_scores') }}
),

final as (
    select
        c.customer_key,
        c.customer_id,
        c.city,
        c.state_code,
        c.zip_prefix,

        -- Enrich with ML churn data
        coalesce(ch.churn_score, 0.0)       as churn_score,
        coalesce(ch.churn_risk, 'unknown')   as churn_risk,
        coalesce(ch.predicted_churn, false)  as predicted_churn,

        -- Customer segment based on churn risk
        case
            when coalesce(ch.churn_risk, 'unknown') = 'high'   then 'At Risk'
            when coalesce(ch.churn_risk, 'unknown') = 'medium' then 'Watch'
            when coalesce(ch.churn_risk, 'unknown') = 'low'    then 'Loyal'
            else 'Unknown'
        end                                  as customer_segment

    from customers c
    left join churn ch using (customer_key)
)

select * from final