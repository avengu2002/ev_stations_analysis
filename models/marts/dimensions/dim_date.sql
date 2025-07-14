{{ 
    config(
            materialized="table", 
            schema='marts'
) 
}}

with base_dates as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2050-01-01' as date)"
    ) }}
)
select
--    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as SK_DATE,
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) as SK_DATE,
    date_day as d_date,
    year(date_day) as d_year,
    month(date_day) as d_month,
    day(date_day) as d_day,
    dayname(date_day) as d_dayname,
    dayofweek(date_day) as d_dayofweek,
    quarter(date_day) as d_quarter,
    dayofyear(date_day) as d_dayofyear
from base_dates