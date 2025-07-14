-- models/reporting/ev_vehicle_dashboard.sql

{{ config(materialized='view', schema='marts') }}

SELECT
    f.fk_vehicle_key,
    fk_vehicle_reg_date_key,
    d_date as vehicle_nz_registration_date,
    YEAR(d_date) as vehicle_nz_registration_year,
    dv.vehicle_class,
    dv.vehicle_make,
    dv.vehicle_model,
    dv.vehicle_type,
    dv.vehicle_vin11,
    dv.vehicle_region,
    dv.vechicle_is_ev_flag,
    f.vehicle_count as registration_count
FROM {{ ref('fact_ev_registration_metrics') }} f
LEFT JOIN {{ ref('dim_ev_vehicle') }} dv
    ON f.fk_vehicle_key = dv.sk_vehicle_key
LEFT JOIN {{ ref('dim_date') }} dt
    ON f.fk_vehicle_reg_date_key = dt.sk_date
