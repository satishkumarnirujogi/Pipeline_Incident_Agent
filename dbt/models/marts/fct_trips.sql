{# Incremental by month: the pre-hook deletes the month before appending it again,
   so a rerun replaces the month instead of duplicating it, and rows removed from
   raw (e.g. quarantined) also disappear here. #}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="{% if is_incremental() %}delete from {{ this }} where pickup_month = '{{ var('month') }}'{% endif %}"
) }}

select
    trip_id,
    pickup_month,
    vendor_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
from {{ ref('stg_yellow_trips') }}
