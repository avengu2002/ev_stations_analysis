{{ 
config(
    materialized="table", 
    schema='marts',
    post_hook = [ 
        """
            INSERT INTO EV_STATIONS_ANALYTICS.AUDIT_SCHEMA.INCREMENTAL_RUN_AUDIT 
            SELECT '{{ this.name }}' AS model_name, 
            CURRENT_TIMESTAMP AS run_time, 
            COUNT(*) AS row_count
            FROM {{ this }} 
        """
    ]    
) }}

with nearest_stations as (
SELECT
    cs.sk_charging_station_key as fk_charging_station_key,
    ncs.sk_charging_station_key as fk_nearest_charging_station_key,
    ns.station_id as station_id,
    ns.nearest_station_id as nearest_station_id,    
    d.sk_date as sk_date,
    ns.distance_km as distance_km,
    1 as station_count,
    ns.dbt_valid_from as valid_from,
    ns.dbt_valid_to as valid_to,
    row_number() over (partition by ns.station_id order by ns.dbt_valid_from desc) as rn,
    ns.dbt_updated_at        
from 
    {{ ref('snap__nearest_station_metrics') }} ns
LEFT JOIN    
    {{ ref("dim_charging_station") }} cs
ON
    cs.charging_station_id = ns.station_id  
    -- AND ns.dbt_valid_from >= cs.valid_from
    -- AND ns.dbt_valid_from <  COALESCE(cs.valid_to, CURRENT_DATE)  
LEFT JOIN    
    {{ ref("dim_charging_station") }} ncs
ON
    ncs.charging_station_id = ns.nearest_station_id   
    -- AND ns.dbt_valid_from >= ncs.valid_from
    -- AND ns.dbt_valid_from <  COALESCE(ncs.valid_to, CURRENT_DATE)       
       
LEFT JOIN
    {{ ref("dim_date") }} d   
ON
    cs.date_first_operational = d.d_date
WHERE
     cs.is_current_flag = TRUE    
)
select 
    fk_charging_station_key,
    fk_nearest_charging_station_key,
    sk_date as fk_date_first_operational,
    valid_from,
    valid_to,
    case when rn = 1 then true else false end as is_current_flag,
    distance_km as distance_km,
    station_count
from nearest_stations
--where is_current_flag = TRUE