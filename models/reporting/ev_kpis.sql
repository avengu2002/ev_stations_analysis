-- models/reporting/ev_kpis.sql


{{ config(materialized='view', schema='marts') }}
SELECT
    vehicle_nz_registration_date,
    vehicle_nz_registration_year,
    vehicle_region,
    vehicle_type,
    SUM(registration_count) AS total_ev_registrations
FROM {{ ref('ev_vehicle_dashboard') }}
where vechicle_is_ev_flag = TRUE
GROUP BY vehicle_nz_registration_date, vehicle_nz_registration_year,vehicle_region,vehicle_type