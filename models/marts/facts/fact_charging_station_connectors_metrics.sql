-- models/marts/facts/fact_charging_station_metrics.sql

{{ config(
    materialized = 'table',
    schema='marts'
) }}

with connector_snapshot as (

    select
        station_id as charging_station_id,
        date_first_operational,
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

dim_station as (

    select *
    from {{ ref('dim_charging_station') }}

),

connector_with_key as (

    select
        ds.sk_charging_station_key,
        dc.sk_connector_key,
        cs.date_first_operational,
        cs.connector_count,
        cs.dbt_valid_from as valid_from,
        coalesce(cs.dbt_valid_to,ds.valid_to, cs.dbt_valid_to) as valid_to
    from connector_snapshot cs
    Left join dim_station ds
    on cs.charging_station_id = ds.charging_station_id 
    Left join dim_connector dc
    on cs.connector_type = dc.connector_type
     and cs.connector_stand = dc.connector_stand
     and cs.connector_power = dc.connector_power
     and cs.connector_status = dc.connector_status

),

aggregated_fact as (

    select
        sk_connector_key as fk_connector_key,
        sk_charging_station_key as fk_charging_station_key,
        date_first_operational,
        to_date(valid_from) as valid_from,
        to_date(valid_to) as valid_to,
        SUM(connector_count) as total_connector_count
    from connector_with_key
    group by 1, 2, 3, 4, 5

),

add_date_key as (

    select
        fk_connector_key,
        fk_charging_station_key,
        dd.SK_DATE as fk_date_first_operational,
        valid_from,
        valid_to,
        total_connector_count,
        dd1.SK_DATE as fk_valid_from_date,
        dd2.SK_DATE as fk_valid_to_date
    from aggregated_fact af
    Left join {{ ref('dim_date') }} dd
        on dd.d_date = af.date_first_operational
    Left join {{ ref('dim_date') }} dd1
      on dd1.d_date = af.valid_from
    Left join {{ ref('dim_date') }} dd2
      on dd2.d_date= af.valid_to
)

select 
    fk_connector_key,
    fk_charging_station_key,
    fk_date_first_operational,
    valid_from,
    valid_to,
    case when valid_to is null then true else false end as is_current_flag,
    total_connector_count,
    fk_valid_from_date,
    fk_valid_to_date
from add_date_key
