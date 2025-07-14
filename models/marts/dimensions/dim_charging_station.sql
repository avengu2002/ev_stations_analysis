{{ 
    config(
            materialized="table", 
            schema='marts'
) 
}}
select 
        dbt_scd_id as sk_charging_station_key,
        charging_station_id,
        station_name,
        operator_name,
        owner_name,
        station_address,
        is_24_hours_flag,
        car_park_count,
        has_carpark_cost_flag,
        max_time_limit,
        has_tourist_attraction_flag,
        latitude,
        longitude,
        date_first_operational,
        station_status,
        has_charging_cost_flag , 
        global_id,
        region,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current_flag,
        dbt_updated_at
from    
    {{ ref('snap__ev_charging_stations') }}        