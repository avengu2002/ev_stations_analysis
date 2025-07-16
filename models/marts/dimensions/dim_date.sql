-- models/marts/dimensions/dim_date.sql
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
),

enhanced_dates as (
    select
        date_day,
        -- Basic date components
        year(date_day) as d_year,
        month(date_day) as d_month,
        day(date_day) as d_day,
        dayname(date_day) as d_dayname,
        dayofweek(date_day) as d_dayofweek,
        quarter(date_day) as d_quarter,
        dayofyear(date_day) as d_dayofyear,
        
        -- Current date for relative comparisons
        current_date() as today,
        
        -- Year-over-Year flags
        case 
            when year(date_day) = year(current_date()) - 1 then true
            else false
        end as is_previous_year,
        
        case 
            when year(date_day) = year(current_date()) then true
            else false
        end as is_current_year,
        
        -- Month-over-Month flags
        case 
            when year(date_day) = year(current_date()) 
                and month(date_day) = month(current_date()) - 1 then true
            when year(date_day) = year(current_date()) - 1 
                and month(date_day) = 12 
                and month(current_date()) = 1 then true
            else false
        end as is_previous_month,
        
        case 
            when year(date_day) = year(current_date()) 
                and month(date_day) = month(current_date()) then true
            else false
        end as is_current_month,
        
        -- Quarter-over-Quarter flags
        case 
            when year(date_day) = year(current_date()) 
                and quarter(date_day) = quarter(current_date()) - 1 then true
            when year(date_day) = year(current_date()) - 1 
                and quarter(date_day) = 4 
                and quarter(current_date()) = 1 then true
            else false
        end as is_previous_quarter,
        
        case 
            when year(date_day) = year(current_date()) 
                and quarter(date_day) = quarter(current_date()) then true
            else false
        end as is_current_quarter,
        
        -- Week flags
        case 
            when date_day >= date_trunc('week', current_date()) - interval '7 days'
                and date_day < date_trunc('week', current_date()) then true
            else false
        end as is_previous_week,
        
        case 
            when date_day >= date_trunc('week', current_date())
                and date_day < date_trunc('week', current_date()) + interval '7 days' then true
            else false
        end as is_current_week,
        
        -- Rolling period flags (useful for analytics)
        case 
            when date_day >= current_date() - interval '30 days' 
                and date_day <= current_date() then true
            else false
        end as is_last_30_days,
        
        case 
            when date_day >= current_date() - interval '90 days' 
                and date_day <= current_date() then true
            else false
        end as is_last_90_days,
        
        case 
            when date_day >= current_date() - interval '365 days' 
                and date_day <= current_date() then true
            else false
        end as is_last_365_days,
        
        -- Relative period calculations
        datediff('day', date_day, current_date()) as days_from_today,
        datediff('week', date_day, current_date()) as weeks_from_today,
        datediff('month', date_day, current_date()) as months_from_today,
        datediff('year', date_day, current_date()) as years_from_today,
        
        -- Same day last year (for YoY comparisons)
        dateadd('year', -1, date_day) as same_day_last_year,
        dateadd('year', 1, date_day) as same_day_next_year,
        
        -- Fiscal year support (assuming fiscal year starts in April)
        case 
            when month(date_day) >= 4 then year(date_day)
            else year(date_day) - 1
        end as fiscal_year,
        
        case 
            when month(date_day) >= 4 then 
                case 
                    when month(date_day) between 4 and 6 then 1
                    when month(date_day) between 7 and 9 then 2
                    when month(date_day) between 10 and 12 then 3
                    else 4
                end
            else 
                case 
                    when month(date_day) between 1 and 3 then 4
                    else null
                end
        end as fiscal_quarter,
        
        -- Holiday/Weekend flags
        case 
            when dayofweek(date_day) in (1, 7) then true  -- Sunday=1, Saturday=7
            else false
        end as is_weekend,
        
        case 
            when dayofweek(date_day) not in (1, 7) then true
            else false
        end as is_weekday
        
    from base_dates
)

select
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) as SK_DATE,
    date_day as d_date,
    d_year,
    d_month,
    d_day,
    d_dayname,
    d_dayofweek,
    d_quarter,
    d_dayofyear,
    
    -- Relative time flags
    is_previous_year,
    is_current_year,
    is_previous_month,
    is_current_month,
    is_previous_quarter,
    is_current_quarter,
    is_previous_week,
    is_current_week,
    is_last_30_days,
    is_last_90_days,
    is_last_365_days,
    
    -- Relative calculations
    days_from_today,
    weeks_from_today,
    months_from_today,
    years_from_today,
    
    -- Comparison dates
    same_day_last_year,
    same_day_next_year,
    
    -- Fiscal year support
    fiscal_year,
    fiscal_quarter,
    
    -- Day type flags
    is_weekend,
    is_weekday
    
from enhanced_dates