-- models/intermediate/int_ev_charging_stations__distances.sql

{{ config(materialized='ephemeral') }}

WITH base AS (
    SELECT
        a.charging_station_id AS station_a,
        a.station_name AS station_name_a,
        a.latitude AS lat_a,
        a.longitude AS lon_a,
        b.charging_station_id AS station_b,
        b.station_name AS station_name_b,
        b.latitude AS lat_b,
        b.longitude AS lon_b
    FROM {{ ref('stg_ev_charging_stations__stations') }} a
    JOIN {{ ref('stg_ev_charging_stations__stations') }} b
        ON a.charging_station_id != b.charging_station_id
),
distances AS (
    SELECT
        station_a,
        station_name_a,
        station_b,
        station_name_b,
        {{ haversine_distance('lat_a','lon_a','lat_b','lon_b') }} AS distance_km
    FROM base
)

SELECT * FROM distances
