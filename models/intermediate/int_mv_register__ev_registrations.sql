{{ config(materialized='ephemeral') }}

WITH eletric_vehicles AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vehicle_id']) }} as sk_vehicle_key,
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
        vehicle_nz_registration_year_month_date,
        vehicle_region,
        vehicle_synthetic_greenhouse_gas,
        vehicle_motive_power,
        CASE 
            WHEN TRIM(LOWER(vehicle_motive_power)) = 'electric' THEN TRUE
            WHEN TRIM(LOWER(vehicle_motive_power)) = 'plugin petrol hybrid' THEN TRUE
            WHEN TRIM(LOWER(vehicle_motive_power)) = 'electric [petrol extended]' THEN TRUE
            WHEN TRIM(LOWER(vehicle_motive_power)) = 'electric fuel cell hydrogen' THEN TRUE
            WHEN TRIM(LOWER(vehicle_motive_power)) = 'electric fuel cell other' THEN TRUE
            ELSE FALSE
        END AS vechicle_is_ev_flag     
    FROM {{ ref('stg_mv_register__registrations') }}
        where 
        vehicle_nz_registration_year_month_date is not null
)
Select * from eletric_vehicles    



