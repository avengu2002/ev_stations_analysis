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
        6371 * 2 * ATAN2(
            SQRT(
                SIN(RADIANS(lat_b - lat_a) / 2) * SIN(RADIANS(lat_b - lat_a) / 2) +
                COS(RADIANS(lat_a)) * COS(RADIANS(lat_b)) *
                SIN(RADIANS(lon_b - lon_a) / 2) * SIN(RADIANS(lon_b - lon_a) / 2)
            ),
            SQRT(
                1 - (
                    SIN(RADIANS(lat_b - lat_a) / 2) * SIN(RADIANS(lat_b - lat_a) / 2) +
                    COS(RADIANS(lat_a)) * COS(RADIANS(lat_b)) *
                    SIN(RADIANS(lon_b - lon_a) / 2) * SIN(RADIANS(lon_b - lon_a) / 2)
                )
            )
        ) AS distance_km
    FROM base
)

SELECT * FROM distances
