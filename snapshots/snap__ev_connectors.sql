-- snapshots/snap__ev_connectors.sql


{% snapshot snap__ev_connectors %}

{{
  config(
    target_schema='snapshots',
    unique_key='station_id || connector_type || connector_stand || connector_power || connector_status',
    strategy='check',
    check_cols=[
      'connector_type',
      'connector_stand',
      'connector_power',
      'connector_status',
      'connector_count'
    ]
  )
}}

select
    station_id,
    date_first_operational,
    connector_type,
    connector_stand,
    connector_power,
    connector_status,
    connector_count
from {{ ref('int_ev_charging_stations__connectors') }}

{% endsnapshot %}
