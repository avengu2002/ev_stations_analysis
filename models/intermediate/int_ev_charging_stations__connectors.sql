-- models/intermediate/int_ev_charging_stations__connectors.sql

{{ config(materialized='view') }}
WITH split_data 
AS (
SELECT
    charging_station_id,
    SPLIT(connectors_raw, '},{') AS connector_array
FROM 
    {{ ref('stg_ev_charging_stations__stations') }}
),
flattened AS 
(
    SELECT
        charging_station_id,
        TRIM(connector.value) AS connector_details
    FROM 
        split_data,
        LATERAL FLATTEN(input => connector_array) AS connector
)
Select 
    charging_station_id AS station_id,
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
    cast(TRIM(REGEXP_SUBSTR(connector_details, 'Count:\\s*(\\d+)', 1, 1, 'e')) AS NUMBER) AS connector_count

FROM flattened
ORDER BY charging_station_id