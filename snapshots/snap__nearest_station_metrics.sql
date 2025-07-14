{% snapshot snap__nearest_station_metrics %}
{{
    config(
      target_schema='snapshots',
      unique_key='station_id',
      strategy='check',
      check_cols=['nearest_station_id', 'distance_km']
    )
}}
SELECT
    station_id,
    nearest_station_id,
    distance_km
FROM 
    {{ ref('int_ev_charging_stations__nearest_stations') }}
{% endsnapshot %}