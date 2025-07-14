{% snapshot snap__ev_charging_stations %}
{{
    config(
        unique_key='CHARGING_STATION_ID',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes = True,
        schema ='snapshots' 
    )
}}

select 
    -- {{ dbt_utils.generate_surrogate_key(['s.CHARGING_STATION_ID']) }} as sk_charging_station_key,
        s.charging_station_id as charging_station_id,
        s.station_name as station_name,
        s.operator_name as operator_name,
        s.owner_name as owner_name,
        s.station_address as station_address,
        s.is_24_hours_flag as is_24_hours_flag,
        s.car_park_count as car_park_count,
        s.has_carpark_cost_flag as has_carpark_cost_flag,
        s.max_time_limit as max_time_limit,
        s.has_tourist_attraction_flag as has_tourist_attraction_flag,
        s.latitude as latitude,
        s.longitude as longitude,
        s.date_first_operational as date_first_operational,
        s.station_status as station_status,
        s.has_charging_cost_flag as has_charging_cost_flag , 
        s.global_id as global_id,
        r.region as region
    from {{ ref('stg_ev_charging_stations__stations') }} s
    join
        {{ ref('seed_charging_stations_with_regions')}} r
    on
        r.charging_station_id = s.charging_station_id
{% endsnapshot %}