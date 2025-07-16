-- models/marts/dimensions/dim_connector.sql

{{ config(
    materialized = 'table',
    schema='marts'
    
) }}

with source_data as (

    select distinct
        connector_type,
        connector_stand,
        connector_power,
        connector_status
    from {{ ref('snap__ev_connectors') }}
),

with_surrogate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(['connector_type','connector_stand','connector_power','connector_status']) }} as sk_connector_key,
        connector_type,
        connector_stand,
        connector_power,
        connector_status

    from source_data
)

select * from with_surrogate_key
