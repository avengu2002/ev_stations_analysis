WITH split_data 
AS (
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
    SPLIT(connectors_raw, '},{') AS connector_array,
    has_charging_cost_flag
FROM 
    {{ ref('stg_ev_charging_stations__stations') }}
),
flattened AS 
(
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
    TRIM(connector.value) AS connector_details,
    has_charging_cost_flag
    FROM 
        split_data,
        LATERAL FLATTEN(input => connector_array) AS connector
)
Select 
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
    has_charging_cost_flag,
    connector_details,
    -- Extract Type: first field
    TRIM(REGEXP_SUBSTR(connector_details, '^[^,]+')) AS connector_type,
    
    -- Extract Power: second field
    TRIM(REGEXP_SUBSTR(connector_details, '^[^,]+,\\s*([^,]+)', 1, 1, 'e')) AS connector_power,

    -- Extract Connector Standard: third field (corrected)
    TRIM(REGEXP_SUBSTR(connector_details, '^[^,]+,\\s*[^,]+,\\s*([^,]+)', 1, 1, 'e')) AS connector_stand,
    
    -- Extract Status
    TRIM(REGEXP_SUBSTR(connector_details, 'Status:\\s*([^,]+)', 1, 1, 'e')) AS connector_status,
    
    -- Extract Count
    TRIM(REGEXP_SUBSTR(connector_details, 'Count:\\s*(\\d+)', 1, 1, 'e')) AS connector_count

FROM flattened
ORDER BY charging_station_id