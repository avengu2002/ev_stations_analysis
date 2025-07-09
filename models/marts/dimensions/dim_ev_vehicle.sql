{{ 
    config(
            materialized="table", 
            schema='marts',
            post_hook = "ALTER TABLE {{ this }} MODIFY COLUMN vehicle_vin11 SET TAG EV_STATIONS_ANALYTICS.MARTS.SENSITIVE_TAG = 'TRUE';"
) 
}}
with stg_vehicle as (

    select
        sk_vehicle_key,
        vehicle_id,
        vehicle_vin11,
        vehicle_class,
        vehicle_make,
        vehicle_model,
        vehicle_colour,
	    vehicle_type,
        vehicle_year,        
        vehicle_nz_registration_year,
        vehicle_nz_registration_month,
        vehicle_region,
        vehicle_synthetic_greenhouse_gas,
        vehicle_motive_power,
        vechicle_is_ev_flag,
        current_timestamp() as dbt_updated_at
    from {{ ref('int_mv_register__ev_registrations') }}
    --where vechicle_is_ev_flag = True  -- Filter only EVs

)
select
        sk_vehicle_key,
        vehicle_id,
        vehicle_vin11,
        vehicle_class,
        vehicle_make,
        vehicle_model,
        vehicle_colour,
	    vehicle_type,
        vehicle_year,        
        vehicle_nz_registration_year,
        vehicle_nz_registration_month,
        vehicle_region,
        vehicle_synthetic_greenhouse_gas,
        vehicle_motive_power,
        vechicle_is_ev_flag,
        dbt_updated_at
from stg_vehicle
