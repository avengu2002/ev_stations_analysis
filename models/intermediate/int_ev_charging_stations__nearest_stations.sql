{{ config(materialized='table') }}

WITH ranked_distances AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY station_a ORDER BY distance_km ASC, station_b ASC) AS distance_rank
    FROM {{ ref('int_ev_charging_stations__distances') }}
)

SELECT
    station_a AS station_id,
    station_name_a AS station_name,
    station_b AS nearest_station_id,
    station_name_b AS nearest_station_name,
    distance_km
FROM ranked_distances
WHERE distance_rank = 1
