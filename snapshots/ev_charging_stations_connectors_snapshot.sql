{% snapshot ev_charging_stations_connectors_snapshot %}

{{
    config(
        unique_key=['charging_station_id', 'connector_type','connector_stand','connector_power','connector_status'],
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes = True,
        schema ='snapshots'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['charging_station_id', 'connector_type','connector_stand','connector_power','connector_status']) }} as sk_connector_key,
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
    connector_type,
    connector_power,
    connector_stand,
    connector_status,
    connector_count
from 
    {{ ref('int_ev_charging_stations__connectors') }}

{% endsnapshot %}    