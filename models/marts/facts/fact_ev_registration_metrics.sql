{{ config(
    materialized='incremental',
    unique_key='vehicle_id',
    schema='marts'
) }}

with source as (
    select * from {{ ref('stg_mv_register__registrations') }}
),

dim_vehicle as (
    select sk_vehicle_key, vehicle_id
    from {{ ref('dim_ev_vehicle') }}
),

dim_date as (
    select sk_date, d_date
    from {{ ref('dim_date') }}
),
final as (
    select
        v.sk_vehicle_key as fk_vehicle_key,
        d.sk_date as fk_vehicle_reg_date_key,
        src.vehicle_id,
        src.vehicle_vdam_weight,
        src.vehicle_gross_mass,
        src.vehicle_width,
        src.vehicle_height,
        src.vehicle_power_rating,
        src.vehicle_number_of_axles,
        src.vehicle_number_of_seats,
        src.vehicle_fc_combined,
        src.vehicle_fc_urban,
        src.vehicle_fc_extra_urban,
        1 as vehicle_count
from source src 
left join dim_vehicle v 
on src.vehicle_id = v.vehicle_id
left join dim_date d
on src.vehicle_nz_registration_year_month_date = d.d_date
where 
    src.vehicle_nz_registration_year_month_date is not null
)
Select * from final
{% if is_incremental() %}
    where vehicle_id not in (select vehicle_id from {{ this }})
{% endif %}