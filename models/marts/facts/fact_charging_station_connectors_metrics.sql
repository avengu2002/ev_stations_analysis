-- models/marts/facts/fact_charging_station_metrics.sql

{{ config(
    materialized = 'table',
    schema='marts'
) }}

with connector_snapshot as (

    select
        station_id as charging_station_id,
        connector_type,
        connector_stand,
        connector_power,
        connector_status,
        connector_count,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap__ev_connectors') }}

),

dim_connector as (

    select *
    from {{ ref('dim_connector') }}

),

connector_with_key as (

    select
        cs.charging_station_id,
        dc.sk_connector_key,
        cs.connector_count,
        cs.dbt_valid_from,
        cs.dbt_valid_to
    from connector_snapshot cs
    join dim_connector dc
    on cs.connector_type = dc.connector_type
     and cs.connector_stand = dc.connector_stand
     and cs.connector_power = dc.connector_power
     and cs.connector_status = dc.connector_status

),

aggregated_fact as (

    select
        sk_connector_key as fk_connector_key,
        charging_station_id,
        to_date(dbt_valid_from) as valid_from,
        to_date(dbt_valid_to) as valid_to,
        SUM(connector_count) as total_connector_count
    from connector_with_key
    group by 1, 2, 3, 4

),

add_date_key as (

    select
        fk_connector_key,
        charging_station_id,
        valid_from,
        valid_to,
        total_connector_count,
        dd.SK_DATE as fk_valid_from_date,
        dd2.SK_DATE as fk_valid_to_date
    from aggregated_fact af
    left join {{ ref('dim_date') }} dd
      on dd.d_date = af.valid_from
    left join {{ ref('dim_date') }} dd2
      on dd2.d_date= af.valid_to
)

select 
    fk_connector_key,
    charging_station_id,
    valid_from,
    valid_to,
    total_connector_count,
    fk_valid_from_date,
    fk_valid_to_date
from add_date_key
