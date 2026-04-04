{{
  config(
    materialized='table',
    schema='gold'
  )
}}

-- Generate a date dimension spanning all order dates
-- We use generate_series to create one row per day
-- This is a standard SQL technique for building date dimensions

with date_spine as (
    select
        generate_series(
            '2016-01-01'::date,
            '2019-12-31'::date,
            '1 day'::interval
        )::date as full_date
),

final as (
    select
        -- date_key in YYYYMMDD format — e.g. 20171002
        -- This integer format is standard in data warehousing
        -- because it is fast to filter and sort
        to_char(full_date, 'YYYYMMDD')::integer as date_key,
        full_date,
        extract(year    from full_date)::integer as year,
        extract(quarter from full_date)::integer as quarter,
        extract(month   from full_date)::integer as month,
        to_char(full_date, 'Month')              as month_name,
        extract(week    from full_date)::integer as week_of_year,
        extract(isodow  from full_date)::integer as day_of_week,
        to_char(full_date, 'Day')                as day_name,
        extract(isodow  from full_date) in (6,7) as is_weekend,

        -- Business quarters as labels
        'Q' || extract(quarter from full_date)::text as quarter_label,

        -- Year-month label for reporting e.g. "2017-10"
        to_char(full_date, 'YYYY-MM')            as year_month
    from date_spine
)

select * from final