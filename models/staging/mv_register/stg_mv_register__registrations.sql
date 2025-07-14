-- models/staging/mv_register/stg_mv_register__registrations.sql


{{ config(
    materialized='table',
    QUERY_TAG = "ev_analytics"
) }}
WITH source_data AS (
    SELECT * FROM {{ source('raw', 'motor_vehicle_register_raw') }}
),
renamed as (
SELECT
    objectid AS vehicle_id,
	alternative_motive_power AS vehicle_alternative_motive_power,
	basic_colour AS vehicle_colour,
	body_type AS vehicle_body_type,
	cc_rating AS vehicle_cc_rating,
	chassis7 AS vehicle_chassis7,
	class AS vehicle_class,
	engine_number AS vehicle_engine_number,
	cast(first_nz_registration_year as integer) AS vehicle_nz_registration_year,
	cast(first_nz_registration_month as integer) AS vehicle_nz_registration_month,
    TO_DATE(TO_CHAR(cast(first_nz_registration_year as integer)) || '-' || LPAD(cast(first_nz_registration_month as integer), 2, '0') || '-01') AS vehicle_nz_registration_year_month_date,
	cast(gross_vehicle_mass  as integer) AS vehicle_gross_mass,
	cast(height  as integer) AS vehicle_height,
	import_status AS vehicle_import_status,
	industry_class AS vehicle_industry_class,
	industry_model_code AS vehicle_industry_model_code,
	make AS vehicle_make,
	model AS vehicle_model,
	motive_power AS vehicle_motive_power,
	mvma_model_code AS vehicle_mvma_model_code,
	cast(number_of_axles  as integer) AS vehicle_number_of_axles,
	cast(number_of_seats  as integer) AS vehicle_number_of_seats,
	nz_assembled AS vehicle_nz_assembled,
	original_country AS vehicle_original_country,
	cast(power_rating as integer) AS vehicle_power_rating,
	previous_country AS vehicle_previous_country,
	road_transport_code AS vehicle_road_transport_code,
	submodel AS vehicle_submodel,
	tla AS vehicle_region,
	transmission_type AS vehicle_transmission_type,
	cast(vdam_weight  as integer) AS vehicle_vdam_weight,
	vehicle_type,
	vehicle_usage,
	cast(vehicle_year as integer) AS vehicle_year ,
	vin11 AS vehicle_vin11,
	cast(width as integer) AS vehicle_width,
	synthetic_greenhouse_gas AS vehicle_synthetic_greenhouse_gas,
	cast(fc_combined as numeric(5,2)) AS vehicle_fc_combined,
	cast(fc_urban as numeric(5,2)) AS vehicle_fc_urban,
	cast(fc_extra_urban as numeric(5,2)) AS vehicle_fc_extra_urban,
    load_timestamp,
from source_data
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY load_timestamp DESC) AS row_num
    FROM renamed
)
select * from deduplicated_data
where row_num = 1