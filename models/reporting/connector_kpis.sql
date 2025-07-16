-- models/reporting/ev_vehicle_dashboard.sql

{{ config(materialized='view', schema='marts') }}

SELECT
    fc.fk_charging_station_key,
    fc.fk_connector_key,
    fc.total_connector_count,
    fcs.station_count,
    fcs.distance_km,
    dd.d_year
FROM {{ ref('fact_charging_station_connectors_metrics') }} fc
LEFT JOIN {{ ref('dim_charging_station') }} dcs
ON dcs.sk_charging_station_key = fc.fk_charging_station_key
LEFT JOIN {{ ref('dim_connector') }} dc
ON dc.sk_connector_key = fc.fk_connector_key
LEFT JOIN  {{ ref('fact_charging_station_metrics') }} fcs
ON dcs.sk_charging_station_key = fcs.fk_charging_station_key
LEFT JOIN {{ ref('dim_date') }} dd
ON dd.sk_date = fcs.fk_date_first_operational
where   
    fc.is_current_flag = True
    and
    fcs.is_current_flag = True
