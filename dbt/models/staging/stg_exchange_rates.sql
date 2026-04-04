with source as (
    select * from {{ source('silver', 'exchange_rates') }}
),

latest_brl_rate as (
    select
        rate,
        captured_at
    from source
    where target_currency = 'BRL'
    order by captured_at desc
    limit 1
)

select
    rate                        as brl_per_usd,
    1.0 / rate                  as usd_per_brl,
    captured_at                 as rate_date
from latest_brl_rate