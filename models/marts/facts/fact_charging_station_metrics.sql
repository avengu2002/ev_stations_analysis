{{ config(
    materialized = 'incremental',
    unique_key = ['station_id','nearest_station_id'],
    schema='marts',
    on_schema_change='sync_all_columns' ,
    post_hook = [ 
        """
            INSERT INTO EV_STATIONS_ANALYTICS.STAGING.INCREMENTAL_RUN_AUDIT 
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
    d.sk_date as fk_date_first_operational,
    nearest_cs.sk_charging_station_key as fk_nearest_charging_station_key,
    ns.station_id as station_id,
    --ns.station_name as station_name,
    ns.nearest_station_id as nearest_station_id,
    --ns.nearest_station_name as nearest_station_name ,
    ns.distance_km as distance_km,
    1 as station_count    
from 
    {{ ref("int_ev_charging_stations__nearest_stations")}} ns
JOIN    
    {{ ref("dim_charging_station") }} cs
ON
    cs.charging_station_id = ns.station_id 
JOIN    
    {{ ref("dim_charging_station") }} nearest_cs
ON
    nearest_cs.charging_station_id = ns.nearest_station_id
JOIN
    {{ ref("dim_date") }} d   
ON
    cs.date_first_operational = d.d_date
WHERE   
    cs.DBT_VALID_TO is null and nearest_cs.DBT_VALID_TO is null
)
select * from nearest_stations

{% if is_incremental() %}
    where station_id not in (select station_id from {{ this }})
{% endif %}


