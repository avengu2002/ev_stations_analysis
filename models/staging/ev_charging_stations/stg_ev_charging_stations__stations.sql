-- models/staging/ev_charging_stations/stg_ev_charging_stations__stations.sql


{{ config(
    materialized='view'
) }}
WITH source_data AS (
    SELECT
        raw_data AS raw,
        file_name,
        load_timestamp
    FROM {{ source('raw', 'ev_roam_charging_stations_raw') }}
),

parsed_data AS (
    SELECT
        raw:"OBJECTID"::NUMBER AS charging_station_id,
        raw:"NAME"::STRING AS station_name,
        raw:"OPERATOR"::STRING AS operator_name,
        raw:"OWNER"::STRING AS owner_name,
        raw:"ADDRESS"::STRING AS station_address,
        raw:"is24Hours"::STRING AS is_24_hours_text,
        CASE 
            WHEN LOWER(raw:"is24Hours"::STRING) = 'true' THEN TRUE
            ELSE FALSE
        END AS is_24_hours_flag,
        raw:"carParkCount"::NUMBER AS car_park_count,
        CASE 
            WHEN LOWER(raw:"hasCarparkCost"::STRING) = 'true' THEN TRUE
            ELSE FALSE
        END AS has_carpark_cost_flag,
        raw:"maxTimeLimit"::STRING AS max_time_limit,
        CASE 
            WHEN LOWER(raw:"hasTouristAttraction"::STRING) = 'true' THEN TRUE
            ELSE FALSE
        END AS has_tourist_attraction_flag,            
        raw:"latitude"::FLOAT AS latitude,
        raw:"longitude"::FLOAT AS longitude,
        raw:"currentType"::STRING AS current_type,
        to_date(raw:dateFirstOperational::STRING, 'DD/MM/YYYY') AS date_first_operational,
        COALESCE(raw:"status"::STRING, 'Operational', raw:"status"::STRING) AS station_status,
        raw:"numberOfConnectors"::NUMBER AS num_of_connectors,
        REGEXP_REPLACE(raw:connectorsList::STRING, '^\\{(.*)\\}$', '\\1') AS connectors_raw,  -- This may be an array to flatten later
        CASE 
            WHEN LOWER(raw:"hasChargingCost"::STRING) = 'true' THEN TRUE
            ELSE FALSE
        END AS has_charging_cost_flag,  
        raw:"GlobalID"  AS global_id,         
        file_name,
        load_timestamp,
        CURRENT_TIMESTAMP() AS processed_at
    FROM source_data
    WHERE raw IS NOT NULL
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY charging_station_id ORDER BY load_timestamp DESC) AS row_num
    FROM parsed_data
)

SELECT
    charging_station_id,
    station_name,
    operator_name,
    owner_name,
    station_address,
    is_24_hours_flag,
    car_park_count,
    has_carpark_cost_flag,
    max_time_limit,
    has_tourist_attraction_flag,
    latitude,
    longitude,
    date_first_operational,
    station_status,
    connectors_raw,  
    has_charging_cost_flag, 
    global_id,            
    file_name,
    load_timestamp,
    processed_at
FROM deduplicated_data
WHERE row_num = 1
