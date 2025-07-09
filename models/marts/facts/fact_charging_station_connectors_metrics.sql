{{ config(
    materialized='incremental',
    unique_key='fk_connector_key',
    schema='marts',
    on_schema_change='sync_all_columns',
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

with stg_stations as (
    select * from {{ ref('stg_ev_charging_stations__stations') }}
),

dim_connector as (
    select * from {{ ref('dim_charging_connector') }}
),
fact as (
    select
        c.sk_connector_key as fk_connector_key,
        c.charging_station_id as charging_station_id,
        c.connector_count as connector_count
   from dim_connector c
    left join stg_stations s on c.charging_station_id = s.charging_station_id
    where c.DBT_VALID_TO is null
)
select * from fact
{% if is_incremental() %}
    where fk_connector_key not in (select fk_connector_key from {{ this }})
{% endif %}
