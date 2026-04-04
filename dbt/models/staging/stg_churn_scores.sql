with source as (
    select * from {{ source('ml', 'churn_scores') }}
)

select
    customer_key,
    churn_score::float                          as churn_score,
    churn_risk,
    case when predicted_churn = 1 then true
         else false
    end                                         as predicted_churn
from source
where customer_key is not null